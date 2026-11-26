from socket import socketpair
from threading import Event, Thread
from time import monotonic, sleep
from unittest import TestCase

from rpc import Busy, Client, PeerClosed, RemoteError, StreamClosed, serve


def wait_for(predicate, timeout=2.0, interval=0.005):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        # Polling is only used for public state/event assertions.
        sleep(interval)
    return predicate()


class GateIterator:
    def __init__(self, entered=None, gate=None, cleaned=None, chunks=(), fail=False):
        self.entered = entered or Event()
        self.gate = gate or Event()
        self.cleaned = cleaned or Event()
        self.chunks = list(chunks)
        self.fail = fail
        self.index = 0
        self.close_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.entered.set()
        if self.index == 0 and self.gate is not None:
            if not self.gate.wait(2):
                raise AssertionError("gate timed out")
        if self.index < len(self.chunks):
            chunk = self.chunks[self.index]
            self.index += 1
            return chunk
        if self.fail:
            raise RuntimeError("stream boom")
        raise StopIteration

    def close(self):
        self.close_count += 1
        self.cleaned.set()


class StreamAcceptanceTests(TestCase):
    def test_workers_one_window_one_event_gate(self):
        left, right = socketpair()
        first_entered = Event()
        release_first = Event()
        second_entered = Event()

        def stream_handler(payload):
            class It:
                def __init__(self):
                    self.n = 0

                def __iter__(self):
                    return self

                def __next__(self):
                    if self.n == 0:
                        self.n += 1
                        first_entered.set()
                        if not release_first.wait(2):
                            raise AssertionError("first gate timeout")
                        return b""
                    second_entered.set()
                    if self.n == 1:
                        self.n += 1
                        return b"second"
                    raise StopIteration

            return It()

        server = Thread(
            target=serve,
            args=(right, lambda b: b),
            kwargs={"workers": 1, "stream_handler": stream_handler},
        )
        server.start()
        try:
            with Client(left) as client:
                stream = client.open_stream(b"start", window=1)
                self.assertTrue(first_entered.wait(2))

                with self.assertRaisesRegex(RemoteError, "server busy"):
                    client.submit(b"control").result(2)

                with self.assertRaises(TimeoutError):
                    stream.read(timeout=0.05)

                release_first.set()

                def control_ready():
                    future = client.submit(b"ping")
                    try:
                        return future.result(timeout=0.05) == b"ping"
                    except RemoteError as exc:
                        if "server busy" not in str(exc):
                            raise
                        return False

                self.assertTrue(wait_for(control_ready))
                self.assertFalse(second_entered.is_set())

                self.assertEqual(stream.read(2), b"")
                self.assertTrue(second_entered.wait(2))
                self.assertEqual(stream.read(2), b"second")
                self.assertIsNone(stream.read(2))
        finally:
            release_first.set()
            server.join(2)
            self.assertFalse(server.is_alive())

    def test_abandonment_frees_slot_only_after_cleanup_and_settlement(self):
        left, right = socketpair()
        old_entered = Event()
        old_gate = Event()
        old_cleaned = Event()
        old_iterator = None
        normal_entered = Event()
        normal_gate = Event()

        def stream_handler(payload):
            nonlocal old_iterator
            old_iterator = GateIterator(
                entered=old_entered,
                gate=old_gate,
                cleaned=old_cleaned,
                chunks=[b"late"],
            )
            return old_iterator

        def ordinary(payload):
            normal_entered.set()
            normal_gate.wait(2)
            return payload

        server = Thread(
            target=serve,
            args=(right, ordinary),
            kwargs={"workers": 2, "stream_handler": stream_handler, "stream_slots": 1},
        )
        server.start()
        try:
            with Client(left) as client:
                stream = client.open_stream(b"old", window=1)
                self.assertTrue(old_entered.wait(2))

                outcomes = []

                def blocked_read():
                    try:
                        outcomes.append(stream.read(2))
                    except BaseException as exc:
                        outcomes.append(exc)

                reader = Thread(target=blocked_read)
                reader.start()

                stream.close()
                reader.join(2)
                self.assertFalse(reader.is_alive())
                self.assertEqual(len(outcomes), 1)
                self.assertIsInstance(outcomes[0], StreamClosed)

                busy_stream = client.open_stream(b"busy", window=1)
                with self.assertRaisesRegex(RemoteError, "server streams busy"):
                    busy_stream.read(2)

                with self.assertRaises(TimeoutError):
                    stream.wait_closed(timeout=0.05)

                old_gate.set()
                self.assertTrue(old_cleaned.wait(2))
                self.assertTrue(wait_for(lambda: old_iterator.close_count == 1))
                stream.wait_closed(2)

                retry = client.open_stream(b"retry", window=2)
                self.assertEqual(retry.read(2), b"late")
                self.assertIsNone(retry.read(2))

                normal_gate.set()
                self.assertEqual(client.submit(b"ordinary").result(2), b"ordinary")
        finally:
            old_gate.set()
            normal_gate.set()
            server.join(2)
            self.assertFalse(server.is_alive())

    def test_max_pending_one_until_settlement(self):
        left, right = socketpair()
        entered = Event()
        gate = Event()

        def stream_handler(payload):
            return GateIterator(entered=entered, gate=gate, chunks=[b"a"])

        server = Thread(
            target=serve,
            args=(right, lambda b: b),
            kwargs={"workers": 1, "stream_handler": stream_handler},
        )
        server.start()
        try:
            with Client(left, max_pending=1) as client:
                stream = client.open_stream(b"x", window=1)
                self.assertTrue(entered.wait(2))
                stream.close()

                with self.assertRaises(Busy):
                    client.submit(b"busy")

                with self.assertRaises(TimeoutError):
                    stream.wait_closed(0.05)

                gate.set()
                stream.wait_closed(2)
                self.assertEqual(client.submit(b"ok").result(2), b"ok")
        finally:
            gate.set()
            server.join(2)
            self.assertFalse(server.is_alive())

    def test_settled_stream_keeps_pending_quota_until_chunks_read(self):
        left, right = socketpair()

        server = Thread(
            target=serve,
            args=(right, lambda b: b),
            kwargs={
                "stream_handler": lambda p: iter([b"only"]),
            },
        )
        server.start()
        try:
            with Client(left, max_pending=1) as client:
                stream = client.open_stream(b"x", window=2)
                stream.wait_closed(2)
                with self.assertRaises(Busy):
                    client.submit(b"busy")
                self.assertEqual(stream.read(2), b"only")
                future = client.submit(b"free")
                self.assertEqual(future.result(2), b"free")
                self.assertIsNone(stream.read(2))
        finally:
            server.join(2)
            self.assertFalse(server.is_alive())

    def test_window_bounds_advancement_of_infinite_stream(self):
        left, right = socketpair()
        advanced = 0
        lock = __import__("threading").Lock()

        def stream_handler(payload):
            def iterator():
                nonlocal advanced
                while True:
                    with lock:
                        advanced += 1
                    yield b"x"

            return iterator()

        server = Thread(
            target=serve,
            args=(right, lambda b: b),
            kwargs={"workers": 1, "stream_handler": stream_handler},
        )
        server.start()
        try:
            with Client(left) as client:
                stream = client.open_stream(b"x", window=3)

                def advanced_at_least_three():
                    with lock:
                        return advanced >= 3

                self.assertTrue(wait_for(advanced_at_least_three))
                sleep(0.05)
                with lock:
                    self.assertLessEqual(advanced, 3)

                self.assertEqual(stream.read(2), b"x")

                def advanced_at_most_four():
                    with lock:
                        return advanced <= 4

                self.assertTrue(wait_for(advanced_at_most_four))
                sleep(0.05)
                with lock:
                    self.assertLessEqual(advanced, 4)
                stream.close()
                stream.wait_closed(2)
        finally:
            server.join(2)

    def test_prefix_then_remote_error_and_illegal_chunk(self):
        left, right = socketpair()

        def handler(payload):
            if payload == b"bad-chunk":
                yield b"prefix"
                yield b"x" * 65537
            else:
                yield b"one"
                yield b""
                raise RuntimeError("boom")

        server = Thread(
            target=serve,
            args=(right, lambda b: b),
            kwargs={"stream_handler": handler},
        )
        server.start()
        try:
            with Client(left) as client:
                stream = client.open_stream(b"fail", window=2)
                self.assertEqual(stream.read(2), b"one")
                self.assertEqual(stream.read(2), b"")
                with self.assertRaises(RemoteError):
                    stream.read(2)
                with self.assertRaises(RemoteError):
                    stream.read(0.05)
                stream.wait_closed(2)

                bad = client.open_stream(b"bad-chunk", window=2)
                self.assertEqual(bad.read(2), b"prefix")
                with self.assertRaises(RemoteError):
                    bad.read(2)
                bad.wait_closed(2)
        finally:
            server.join(2)
            self.assertFalse(server.is_alive())

    def test_unconfigured_stream_handler(self):
        left, right = socketpair()
        server = Thread(target=serve, args=(right, lambda b: b))
        server.start()
        try:
            with Client(left) as client:
                stream = client.open_stream(b"x")
                with self.assertRaisesRegex(RemoteError, "streaming unavailable"):
                    stream.read(2)
                stream.wait_closed(2)
        finally:
            server.join(2)

    def test_disconnect_fails_unsettled_calls_and_streams_and_cleans_up(self):
        left, right = socketpair()
        normal_entered = Event()
        normal_gate = Event()
        stream_entered = Event()
        stream_gate = Event()
        cleaned = Event()
        iterator = GateIterator(
            entered=stream_entered, gate=stream_gate, cleaned=cleaned
        )

        def ordinary(payload):
            normal_entered.set()
            normal_gate.wait(2)
            return payload

        server = Thread(
            target=serve,
            args=(right, ordinary),
            kwargs={"workers": 2, "stream_handler": lambda p: iterator},
        )
        server.start()
        try:
            client = Client(left)
            normal = client.submit(b"slow")
            self.assertTrue(normal_entered.wait(2))
            stream = client.open_stream(b"x")
            self.assertTrue(stream_entered.wait(2))

            read_error = []
            wait_error = []

            def reader():
                try:
                    stream.read(2)
                except BaseException as exc:
                    read_error.append(exc)

            def waiter():
                try:
                    stream.wait_closed(2)
                except BaseException as exc:
                    wait_error.append(exc)

            t1 = Thread(target=reader)
            t2 = Thread(target=waiter)
            t1.start()
            t2.start()

            client.close()
            t1.join(2)
            t2.join(2)
            self.assertFalse(t1.is_alive())
            self.assertFalse(t2.is_alive())
            self.assertTrue(read_error and isinstance(read_error[0], PeerClosed))
            self.assertTrue(wait_error and isinstance(wait_error[0], PeerClosed))
            with self.assertRaises(PeerClosed):
                normal.result(2)

            stream_gate.set()
            normal_gate.set()
            self.assertTrue(cleaned.wait(2))
        finally:
            stream_gate.set()
            normal_gate.set()
            server.join(2)
            self.assertFalse(server.is_alive())

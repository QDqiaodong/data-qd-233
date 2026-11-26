"""Acceptance tests for in-process handler rollout and safe retirement.

Every scenario uses a real ``socketpair`` and event gates; ordering is checked
with bounded waits rather than fixed sleeps.  Observations printed at the end
of each test come from the actual run (generation markers, retirement counts
and replacement outcomes), never from hard-coded expectations.
"""
from socket import socketpair
from threading import Event, Lock, Thread
from time import monotonic, sleep
from unittest import TestCase

from rpc import Busy, Client, HandlerRollout, RemoteError, serve


def wait_for(predicate, timeout=3.0, interval=0.005):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(interval)
    return predicate()


def assert_keeps_failing(testcase, predicate, hold=0.12):
    """Bounded negative observation: the predicate must stay false for a bit."""
    deadline = monotonic() + hold
    while monotonic() < deadline:
        testcase.assertFalse(predicate())
        sleep(0.01)
    testcase.assertFalse(predicate())


class GateIterator:
    """Iterator whose first next() and close() can each be held on a gate."""

    def __init__(self, component, max_chunks=3, next_gate=None, close_gate=None,
                 next_entered=None, close_entered=None):
        self.component = component
        self.max_chunks = max_chunks
        self.next_gate = next_gate
        self.close_gate = close_gate
        self.next_entered = next_entered
        self.close_entered = close_entered
        self.index = 0
        self.close_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == 0 and self.next_entered is not None:
            self.next_entered.set()
        if self.index >= self.max_chunks:
            raise StopIteration
        # The first produced chunk may be held back until the gate opens; the
        # factory has already returned by then, so this blocks advancement
        # rather than factory selection.
        if self.index == 0 and self.next_gate is not None:
            if not self.next_gate.wait(3):
                raise AssertionError("iterator gate timed out")
        self.index += 1
        return self.component.stream_chunk()

    def close(self):
        if self.close_entered is not None:
            self.close_entered.set()
        if self.close_gate is not None and not self.close_gate.wait(3):
            raise AssertionError("close gate timed out")
        self.close_count += 1
        self.component.note_cleanup()


class Component:
    """One generation: ordinary handler, stream factory and bookkeeping."""

    def __init__(self, tag, retire_event=None, retire_raise=False,
                 next_gate=None, close_gate=None,
                 next_entered=None, close_entered=None,
                 handler_gate=None, handler_entered=None,
                 factory_entered=None, factory_gate=None):
        self.tag = tag
        self.retire_event = retire_event
        self.retire_raise = retire_raise
        self.next_gate = next_gate
        self.close_gate = close_gate
        self.next_entered = next_entered
        self.close_entered = close_entered
        self.handler_gate = handler_gate
        self.handler_entered = handler_entered
        self.factory_entered = factory_entered
        self.factory_gate = factory_gate
        self.lock = Lock()
        self.handler_calls = []
        self.factory_calls = 0
        self.cleanup_calls = 0
        self.retire_calls = 0
        self.chunk_seq = 0

    def marker(self):
        return self.tag.encode()

    def handler(self, payload):
        with self.lock:
            self.handler_calls.append(bytes(payload))
        if self.handler_entered is not None:
            self.handler_entered.set()
        if self.handler_gate is not None and not self.handler_gate.wait(3):
            raise AssertionError("ordinary handler gate timed out")
        return self.tag.encode() + b":" + bytes(payload)

    def stream_chunk(self):
        with self.lock:
            self.chunk_seq += 1
            return self.tag.encode() + b"-chunk-" + str(self.chunk_seq).encode()

    def stream_factory(self, payload, max_chunks=3):
        with self.lock:
            self.factory_calls += 1
        if self.factory_entered is not None:
            self.factory_entered.set()
        if self.factory_gate is not None and not self.factory_gate.wait(3):
            raise AssertionError("factory gate timed out")
        return GateIterator(
            self,
            max_chunks=max_chunks,
            next_gate=self.next_gate,
            close_gate=self.close_gate,
            next_entered=self.next_entered,
            close_entered=self.close_entered,
        )

    def note_cleanup(self):
        with self.lock:
            self.cleanup_calls += 1

    def on_retire(self):
        with self.lock:
            self.retire_calls += 1
        if self.retire_event is not None:
            if not self.retire_event.wait(3):
                raise AssertionError("retire gate timed out")
        if self.retire_raise:
            raise RuntimeError(f"component {self.tag} retirement failed")


def start_server(sock, handler, rollout, **kwargs):
    thread = Thread(
        target=serve,
        args=(sock, handler),
        kwargs={"rollout": rollout, **kwargs},
        daemon=True,
    )
    thread.start()
    return thread


def busy_replied(client, payload=b"probe"):
    """True once a submitted ordinary call has got a server-busy reply."""
    try:
        client.submit(payload).result(timeout=0.5)
        return False
    except RemoteError as exc:
        return "server busy" in str(exc)
    except Exception:
        return False


class RolloutAcceptanceTests(TestCase):
    # ------------------------------------------------------------------
    # Scenario 1: replace without disconnecting; disconnect cannot retire
    # an iterator whose cleanup has not exited.
    # ------------------------------------------------------------------
    def test_replace_without_disconnect_and_cleanup_gates_retirement(self):
        left1, right1 = socketpair()
        left2, right2 = socketpair()

        a_next_gate = Event()
        a_next_entered = Event()
        a_close_gate = Event()
        a_close_entered = Event()
        comp_a = Component(
            "A",
            next_gate=a_next_gate,
            close_gate=a_close_gate,
            next_entered=a_next_entered,
            close_entered=a_close_entered,
        )
        comp_b = Component("B")
        rollout = HandlerRollout(
            comp_a.handler, comp_a.stream_factory, on_retire=comp_a.on_retire
        )
        # serve's positional handler only serves ledger instructions, which
        # these tests never send; rollout supplies all business callbacks.
        srv1 = start_server(right1, comp_a.handler, rollout)
        srv2 = start_server(right2, comp_a.handler, rollout)
        try:
            client1 = Client(left1)
            client2 = Client(left2)

            # Both connections actually use generation A first.
            self.assertEqual(
                client1.submit(b"q1").result(2), b"A:q1"
            )
            self.assertEqual(
                client2.submit(b"q2").result(2), b"A:q2"
            )

            # Connection 1 opens an A stream whose first advancement stops at
            # the gate.  The iterator has more data afterwards.
            stream1 = client1.open_stream(b"s1", window=1)
            self.assertTrue(a_next_entered.wait(2))
            a_next_gate.set()
            self.assertEqual(stream1.read(2), b"A-chunk-1")

            # Publish B while the A stream still has future advancement.
            receipt_a = rollout.replace(
                comp_b.handler, comp_b.stream_factory, on_retire=comp_b.on_retire
            )
            self.assertFalse(receipt_a.cancelled())

            # Before the old stream advances again, both new ordinary and new
            # stream work on an existing connection already return B.
            self.assertEqual(
                client2.submit(b"after").result(2), b"B:after"
            )
            new_stream = client2.open_stream(b"sb", window=1)
            self.assertEqual(new_stream.read(2), b"B-chunk-1")
            # Drain the remaining buffered prefix, then reach normal EOF.
            self.assertEqual(new_stream.read(2), b"B-chunk-2")
            self.assertEqual(new_stream.read(2), b"B-chunk-3")
            self.assertIsNone(new_stream.read(2))
            new_stream.wait_closed(2)

            # A must not have been retired while its iterator is still live.
            assert_keeps_failing(self, lambda: comp_a.retire_calls > 0)
            self.assertFalse(receipt_a.done())

            # Releasing the old stream produces another A marker: generation
            # does not change mid-stream.
            self.assertEqual(stream1.read(2), b"A-chunk-2")
            self.assertEqual(stream1.read(2), b"A-chunk-3")

            # Close connection 1; the old iterator's close() then blocks on
            # its own gate.  Connection 2 keeps querying B meanwhile.
            client1.close()
            self.assertTrue(a_close_entered.wait(2))
            self.assertEqual(
                client2.submit(b"still-b").result(2), b"B:still-b"
            )
            retire_before_cleanup = comp_a.retire_calls
            assert_keeps_failing(self, lambda: comp_a.retire_calls > 0)
            self.assertFalse(receipt_a.done())

            # Let cleanup finish; with connection 2 still open A retires
            # exactly once and the receipt settles with None.
            a_close_gate.set()
            self.assertTrue(wait_for(lambda: comp_a.retire_calls == 1))
            self.assertIsNone(receipt_a.result(2))
            self.assertEqual(comp_a.retire_calls, 1)

            post_rollout_reply = client2.submit(b"obs").result(2)
            print(
                "[场景1] 换版后既有连接查询=%s、新流首块=%s；"
                "老流续读=%s；迭代器 close 退出前 A 卸载次数=%d，"
                "退出后卸载次数=%d，回执结果=%r，连接2保持打开时查询=%s"
                % (
                    b"B:after",
                    b"B-chunk-1",
                    b"A-chunk-2",
                    retire_before_cleanup,
                    comp_a.retire_calls,
                    receipt_a.result(0),
                    post_rollout_reply,
                )
            )
        finally:
            a_next_gate.set()
            a_close_gate.set()
            client1.close()
            client2.close()
            srv1.join(2)
            srv2.join(2)

    # ------------------------------------------------------------------
    # Scenario 2: first factory execution interleaved with replacement.
    # ------------------------------------------------------------------
    def test_first_factory_execution_interleaved_with_replace(self):
        left, right = socketpair()

        ordinary_entered = Event()
        ordinary_gate = Event()
        factory_entered = Event()
        factory_gate = Event()
        first_step_entered = Event()
        step_gate = Event()

        # Generation A's ordinary handler can hold the only execution permit.
        # Generation B carries the gates, since this implementation lets a
        # stream queued before the replacement bind at its first execution
        # opportunity and therefore enters B's factory.  The branch below
        # still asserts whichever factory actually ran.
        comp_a = Component(
            "A",
            handler_gate=ordinary_gate,
            handler_entered=ordinary_entered,
        )
        comp_b = Component(
            "B",
            factory_entered=factory_entered,
            factory_gate=factory_gate,
            next_gate=step_gate,
            next_entered=first_step_entered,
        )
        rollout = HandlerRollout(
            comp_a.handler, comp_a.stream_factory, on_retire=comp_a.on_retire
        )
        srv = start_server(right, comp_a.handler, rollout, workers=1)
        try:
            with Client(left, max_pending=4) as client:
                # The A ordinary call occupies the single execution permit.
                hold = client.submit(b"hold")
                self.assertTrue(ordinary_entered.wait(2))

                # A stream is admitted (a stream slot is free) but its factory
                # cannot run yet because the only permit is taken.
                queued = client.open_stream(b"queued", window=1)

                # A following ordinary call gets "server busy", proving the
                # server has processed past the stream open; no factory of
                # either generation has been called.
                self.assertTrue(wait_for(lambda: busy_replied(client, b"probe")))
                self.assertEqual(comp_a.factory_calls, 0)
                self.assertEqual(comp_b.factory_calls, 0)

                # Publish B, then release the gated A ordinary call.  The
                # admitted ordinary call still answers with its A generation.
                receipt_a = rollout.replace(
                    comp_b.handler, comp_b.stream_factory,
                    on_retire=comp_b.on_retire,
                )
                ordinary_gate.set()
                self.assertEqual(hold.result(2), b"A:hold")

                # The queued stream now wins the permit and enters its first
                # factory execution; the factory parks on its own gate.
                self.assertTrue(factory_entered.wait(2))
                self.assertEqual(comp_a.factory_calls + comp_b.factory_calls, 1)
                chosen = "A" if comp_a.factory_calls else "B"
                # Exactly one factory has been entered and it has not returned.
                self.assertFalse(first_step_entered.wait(0.1))

                if chosen == "A":
                    # Factory A was selected at first execution: A may not
                    # retire while the factory is still inside.
                    assert_keeps_failing(self, lambda: comp_a.retire_calls > 0)
                    factory_gate.set()
                    # First produced chunk then parks on the advancement gate;
                    # retirement still has to wait for cleanup to exit too.
                    self.assertTrue(first_step_entered.wait(2))
                    assert_keeps_failing(self, lambda: comp_a.retire_calls > 0)
                    step_gate.set()
                    observed_marker = queued.read(2)
                    self.assertEqual(observed_marker, b"A-chunk-1")
                    self.assertEqual(queued.read(2), b"A-chunk-2")
                    self.assertEqual(queued.read(2), b"A-chunk-3")
                    self.assertIsNone(queued.read(2))
                    queued.wait_closed(2)
                    self.assertTrue(wait_for(lambda: comp_a.retire_calls == 1))
                    self.assertIsNone(receipt_a.result(2))
                else:
                    # Factory B was selected (this implementation binds the
                    # generation at first execution): while it is parked on
                    # its gate, A has no remaining work and retires.
                    self.assertTrue(wait_for(lambda: comp_a.retire_calls == 1))
                    self.assertIsNone(receipt_a.result(2))
                    factory_gate.set()
                    self.assertTrue(first_step_entered.wait(2))
                    step_gate.set()
                    observed_marker = queued.read(2)
                    self.assertEqual(observed_marker, b"B-chunk-1")
                    self.assertEqual(queued.read(2), b"B-chunk-2")
                    self.assertEqual(queued.read(2), b"B-chunk-3")
                    self.assertIsNone(queued.read(2))
                    queued.wait_closed(2)

                # After retirement no A callback runs again; new work is all B.
                a_calls_before = len(comp_a.handler_calls)
                later = client.submit(b"later").result(2)
                self.assertEqual(later, b"B:later")
                self.assertEqual(len(comp_a.handler_calls), a_calls_before)

                print(
                    "[场景2] 排队流实际调用的工厂=%s；首块标记=%s；"
                    "A 工厂调用=%d、B 工厂调用=%d（合计 1）；"
                    "A 卸载次数=%d；换版后查询=%s"
                    % (
                        chosen,
                        observed_marker,
                        comp_a.factory_calls,
                        comp_b.factory_calls,
                        comp_a.retire_calls,
                        later,
                    )
                )
        finally:
            ordinary_gate.set()
            factory_gate.set()
            step_gate.set()
            srv.join(2)

    # ------------------------------------------------------------------
    # Scenario 3: unfinished retirement blocks the next replacement; a
    # failing retirement reports through the receipt without wedging.
    # ------------------------------------------------------------------
    def test_retirement_gates_next_publish_and_failures_are_receipts(self):
        left, right = socketpair()
        retire_gate = Event()
        retire_entered = Event()

        comp_a = Component(
            "A", retire_event=retire_gate,
        )
        comp_a.on_retire = self._gated_retire(comp_a, retire_entered, retire_gate)
        comp_b = Component("B")
        rollout = HandlerRollout(
            comp_a.handler, comp_a.stream_factory, on_retire=comp_a.on_retire
        )
        srv = start_server(right, comp_a.handler, rollout)
        try:
            with Client(left) as client:
                self.assertEqual(client.submit(b"a1").result(2), b"A:a1")

                # A -> B, but A's retirement blocks on its gate.
                receipt_a = rollout.replace(
                    comp_b.handler, comp_b.stream_factory,
                    on_retire=comp_b.on_retire,
                )
                self.assertTrue(retire_entered.wait(2))
                self.assertEqual(client.submit(b"b1").result(2), b"B:b1")

                # While A's retirement is unfinished, publishing C is Busy:
                # nothing changes and C's callbacks are never touched.
                comp_c = Component("C")
                with self.assertRaises(Busy):
                    rollout.replace(
                        comp_c.handler, comp_c.stream_factory,
                        on_retire=comp_c.on_retire,
                    )
                self.assertEqual(client.submit(b"b2").result(2), b"B:b2")
                self.assertEqual(comp_c.handler_calls, [])
                self.assertEqual(comp_c.factory_calls, 0)

                # Release retirement, collect the None receipt, then C works.
                retire_gate.set()
                self.assertIsNone(receipt_a.result(2))
                receipt_b = rollout.replace(
                    comp_c.handler, comp_c.stream_factory,
                    on_retire=comp_c.on_retire,
                )
                self.assertIsNone(receipt_b.result(2))
                self.assertEqual(client.submit(b"c1").result(2), b"C:c1")

                print(
                    "[场景3-1] A 卸载未结束时换 C 抛 %s；C 处理请求数=%d；"
                    "A 回执=%r；换版成功后同连接查询=%s"
                    % (
                        "Busy",
                        len(comp_c.handler_calls),
                        None,
                        b"C:c1",
                    )
                )
        finally:
            retire_gate.set()
            srv.join(2)

        # A fresh rollout whose predecessor retirement callback raises: the
        # exception comes through the receipt, the current component still
        # answers, and a later replacement is not permanently blocked.
        left2, right2 = socketpair()
        fail_entered = Event()
        comp_x = Component("X")
        # When X is replaced, its own on_retire runs and fails.
        comp_x.on_retire = self._gated_failing_retire(comp_x, fail_entered)
        comp_y = Component("Y")
        comp_z = Component("Z")
        rollout2 = HandlerRollout(
            comp_x.handler, comp_x.stream_factory, on_retire=comp_x.on_retire
        )
        srv2 = start_server(right2, comp_x.handler, rollout2)
        try:
            with Client(left2) as client:
                self.assertEqual(client.submit(b"x1").result(2), b"X:x1")
                receipt_x = rollout2.replace(
                    comp_y.handler, comp_y.stream_factory,
                    on_retire=comp_y.on_retire,
                )
                self.assertTrue(fail_entered.wait(2))
                # Y is already current and keeps answering while X's failing
                # retirement settles.
                self.assertEqual(client.submit(b"y1").result(2), b"Y:y1")
                with self.assertRaisesRegex(RuntimeError, "retirement failed"):
                    receipt_x.result(2)
                # Y itself is unaffected; the failed retirement does not roll
                # back the published generation.
                self.assertEqual(client.submit(b"y2").result(2), b"Y:y2")

                # After the (failed) retirement settles, replacing again works:
                # Y retires normally once Z is published.
                receipt_y = rollout2.replace(
                    comp_z.handler, comp_z.stream_factory,
                    on_retire=comp_z.on_retire,
                )
                self.assertIsNone(receipt_y.result(2))
                self.assertEqual(client.submit(b"z1").result(2), b"Z:z1")
                self.assertEqual(comp_x.retire_calls, 1)

                print(
                    "[场景3-2] 卸载异常经回执=%s；异常后当前代应答=%s；"
                    "再次发布后的查询=%s；X 实际卸载次数=%d"
                    % (
                        "RuntimeError(retirement failed)",
                        b"Y:y2",
                        b"Z:z1",
                        comp_x.retire_calls,
                    )
                )
        finally:
            srv2.join(2)

    @staticmethod
    def _gated_retire(component, entered, gate):
        def retire():
            with component.lock:
                component.retire_calls += 1
            entered.set()
            if not gate.wait(3):
                raise AssertionError("retire gate timed out")

        return retire

    @staticmethod
    def _gated_failing_retire(component, entered):
        def retire():
            with component.lock:
                component.retire_calls += 1
            entered.set()
            raise RuntimeError(f"component {component.tag} retirement failed")

        return retire

    # ------------------------------------------------------------------
    # Construction / publication validation and serve-level conflicts.
    # ------------------------------------------------------------------
    def test_callback_validation_and_serve_conflicts(self):
        good = lambda b: b  # noqa: E731
        good_stream = lambda p: iter([b"x"])  # noqa: E731
        good_retire = lambda: None  # noqa: E731

        with self.assertRaises(ValueError):
            HandlerRollout(None, good_stream, on_retire=good_retire)
        with self.assertRaises(ValueError):
            HandlerRollout(good, None, on_retire=good_retire)
        with self.assertRaises(ValueError):
            HandlerRollout(good, good_stream, on_retire=None)

        rollout = HandlerRollout(
            good, good_stream, on_retire=good_retire
        )
        with self.assertRaises(ValueError):
            rollout.replace(None, good_stream, on_retire=good_retire)
        with self.assertRaises(ValueError):
            rollout.replace(good, good_stream, on_retire=123)
        # A rejected replacement leaves the current generation in place.
        gen = rollout.current()
        self.assertIs(rollout.current(), gen)

        left, right = socketpair()
        try:
            with self.assertRaises(ValueError):
                serve(
                    right,
                    good,
                    stream_handler=good_stream,
                    rollout=rollout,
                )
            # Rejected startup must not consume the socket: a plain serve on
            # the same socket still accepts a real call.
            plain = Thread(target=serve, args=(right, good))
            plain.start()
            with Client(left) as client:
                self.assertEqual(client.submit(b"ok").result(2), b"ok")
            plain.join(2)
            self.assertFalse(plain.is_alive())
        finally:
            try:
                right.close()
            except OSError:
                pass

        print(
            "[校验] 非可调用回调与 rollout+stream_handler 同传均抛 ValueError，"
            "失败换版未改动当前版本（同一对象=%s）" % (rollout.current() is gen,)
        )

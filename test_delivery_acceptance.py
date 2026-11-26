from socket import socketpair
from threading import Event, Lock, Thread
from time import monotonic, sleep
from unittest import TestCase

from rpc import (
    Client,
    DeliveryLedger,
    PeerClosed,
    RemoteError,
    serve,
)


def wait_for(predicate, timeout=2.0, interval=0.005):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(interval)
    return predicate()


def outcome(future, timeout=2.0):
    """Return ("ok", value) or ("err", message), or None while pending."""
    try:
        return "ok", future.result(timeout)
    except RemoteError as exc:
        return "err", str(exc)
    except TimeoutError:
        return None


def start_server(sock, handler, **kwargs):
    server = Thread(target=serve, args=(sock, handler), kwargs=kwargs)
    server.start()
    return server


class DeliveryAcceptanceTests(TestCase):
    def test_reclaim_after_disconnect_and_single_execution(self):
        ledger = DeliveryLedger(capacity=2)
        calls = []
        calls_lock = Lock()
        entered = Event()
        gate = Event()
        payload = b"write-1"
        result = b"done-1"

        def handler(body):
            with calls_lock:
                calls.append(body)
            if body != payload:
                return body
            entered.set()
            if not gate.wait(4):
                raise AssertionError("handler gate timed out")
            return result

        # Connection A: submit seq 1, then disconnect while it runs.
        left_a, right_a = socketpair()
        server_a = start_server(right_a, handler, workers=1, ledger=ledger)
        client_a = Client(left_a)
        future_a = client_a.submit_delivery(1, payload)
        self.assertTrue(entered.wait(2))
        client_a.close()
        with self.assertRaises(PeerClosed):
            future_a.result(2)

        # Connection B shares the same ledger.
        left_b, right_b = socketpair()
        server_b = start_server(right_b, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server_b.join(4))
        self.addCleanup(lambda: server_a.join(4))
        client_b = Client(left_b)
        self.addCleanup(client_b.close)
        self.addCleanup(gate.set)

        # Different payload is a conflict; early confirm is still pending.
        self.assertEqual(
            outcome(client_b.submit_delivery(1, b"other")),
            ("err", "delivery conflict"),
        )
        self.assertEqual(
            outcome(client_b.confirm_delivery(1)), ("err", "delivery pending")
        )

        # Two identical claims while running: at least one replies pending
        # immediately, the other may remain parked.
        claim_1 = client_b.submit_delivery(1, payload)
        claim_2 = client_b.submit_delivery(1, payload)
        claims = [claim_1, claim_2]

        self.assertTrue(
            wait_for(
                lambda: any(
                    outcome(f, 0.05) == ("err", "delivery pending")
                    for f in claims
                )
            )
        )
        parked = [f for f in claims if not f.done()]
        self.assertEqual(len(parked), 1)

        # The parked claim and the parked-handler record hold no execution
        # slot here, so a normal request still runs on B.
        self.assertEqual(client_b.submit(b"ping").result(2), b"ping")

        # While the handler is gated the instruction is still running.
        self.assertEqual(
            outcome(client_b.confirm_delivery(1)), ("err", "delivery pending")
        )

        gate.set()

        # The parked claim receives the original terminal result.
        self.assertEqual(parked[0].result(2), result)
        # A fresh claim after completion also gets it.
        self.assertEqual(client_b.submit_delivery(1, payload).result(2), result)
        # The handler ran exactly once for this write.
        with calls_lock:
            self.assertEqual(calls.count(payload), 1)

        # Confirming the terminal result frees the ledger slot.
        self.assertEqual(client_b.confirm_delivery(1).result(2), b"")
        # Retired sequences can no longer be submitted or re-confirmed.
        self.assertEqual(
            outcome(client_b.submit_delivery(1, payload)),
            ("err", "delivery retired"),
        )
        self.assertEqual(client_b.confirm_delivery(1).result(2), b"")

    def test_capacity_gates_new_sequences_until_confirmed(self):
        ledger = DeliveryLedger(capacity=1)
        calls = []
        calls_lock = Lock()

        def handler(body):
            with calls_lock:
                calls.append(body)
            return body.upper()

        left, right = socketpair()
        server = start_server(right, handler, workers=2, ledger=ledger)
        self.addCleanup(lambda: server.join(4))
        with Client(left) as client:
            self.assertEqual(client.submit_delivery(1, b"one").result(2), b"ONE")
            # Capacity is full even though execution slots are free.
            self.assertEqual(
                outcome(client.submit_delivery(2, b"two")),
                ("err", "delivery full"),
            )
            # The rejected sequence was not admitted: nothing advanced.
            self.assertEqual(client.confirm_delivery(1).result(2), b"")
            self.assertEqual(client.submit_delivery(2, b"two").result(2), b"TWO")

            # Sequence 1 is retired; repeated confirms must not evict 2.
            self.assertEqual(
                outcome(client.submit_delivery(1, b"one")),
                ("err", "delivery retired"),
            )
            self.assertEqual(client.confirm_delivery(1).result(2), b"")
            self.assertEqual(client.submit_delivery(2, b"two").result(2), b"TWO")
            self.assertEqual(client.confirm_delivery(2).result(2), b"")
            with calls_lock:
                self.assertEqual(calls, [b"one", b"two"])

    def test_slot_exhaustion_does_not_admit_or_advance(self):
        ledger = DeliveryLedger(capacity=2)
        calls = []
        calls_lock = Lock()
        entered = Event()
        release = Event()

        def handler(body):
            with calls_lock:
                calls.append(body)
            if body == b"gate":
                entered.set()
                if not release.wait(4):
                    raise AssertionError("gate timed out")
            return body

        # Connection A finishes and confirms one instruction on the ledger.
        left_a, right_a = socketpair()
        server_a = start_server(right_a, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server_a.join(4))
        client_a = Client(left_a)
        self.addCleanup(client_a.close)
        self.assertEqual(client_a.submit_delivery(5, b"five").result(2), b"five")
        self.assertEqual(client_a.confirm_delivery(5).result(2), b"")

        # A separate connection B has its only execution slot gated.
        left_b, right_b = socketpair()
        server_b = start_server(right_b, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server_b.join(4))
        self.addCleanup(release.set)
        with Client(left_b) as client_b:
            gated = client_b.submit(b"gate")
            self.assertTrue(entered.wait(2))

            # Ledger has room but this connection has no free execution slot.
            self.assertEqual(
                outcome(client_b.submit_delivery(10, b"ten")),
                ("err", "server busy"),
            )
            # Not admitted, so it is not "unknown" and did not advance.
            self.assertEqual(
                outcome(client_b.confirm_delivery(10)),
                ("err", "delivery unknown"),
            )

            release.set()
            self.assertEqual(gated.result(2), b"gate")
            self.assertEqual(client_b.submit_delivery(10, b"ten").result(2), b"ten")
            with calls_lock:
                self.assertEqual(calls, [b"five", b"gate", b"ten"])

    def test_parked_claim_is_freed_when_its_connection_leaves(self):
        ledger = DeliveryLedger(capacity=1)
        entered = Event()
        gate = Event()

        def handler(body):
            if body == b"running":
                entered.set()
                if not gate.wait(4):
                    raise AssertionError("gate timed out")
            return body

        left_a, right_a = socketpair()
        server_a = start_server(right_a, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server_a.join(4))
        self.addCleanup(gate.set)
        client_a = Client(left_a)
        self.addCleanup(client_a.close)
        running = client_a.submit_delivery(1, b"running")
        self.assertTrue(entered.wait(2))

        # B parks a claim and then leaves while the instruction still runs.
        left_b, right_b = socketpair()
        server_b = start_server(right_b, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server_b.join(4))
        client_b = Client(left_b)
        parked = client_b.submit_delivery(1, b"running")
        with self.assertRaises(TimeoutError):
            parked.result(0.1)  # still running
        client_b.close()

        # C can take over the single wait slot, so B left none permanently.
        left_c, right_c = socketpair()
        server_c = start_server(right_c, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server_c.join(4))
        with Client(left_c) as client_c:
            claim = None
            deadline = monotonic() + 2
            while claim is None and monotonic() < deadline:
                candidate = client_c.submit_delivery(1, b"running")
                try:
                    candidate.result(0.05)
                except TimeoutError:
                    claim = candidate
            self.assertIsNotNone(claim)
            gate.set()
            self.assertEqual(claim.result(2), b"running")

    def test_handler_failure_is_terminal_and_reclaimable(self):
        ledger = DeliveryLedger(capacity=1)
        calls = []
        calls_lock = Lock()

        def handler(body):
            with calls_lock:
                calls.append(body)
            raise ValueError("boom")

        left, right = socketpair()
        server = start_server(right, handler, workers=1, ledger=ledger)
        self.addCleanup(lambda: server.join(4))
        with Client(left) as client:
            self.assertEqual(
                outcome(client.submit_delivery(1, b"x")),
                ("err", "handler failed"),
            )
            self.assertEqual(
                outcome(client.submit_delivery(1, b"x")),
                ("err", "handler failed"),
            )
            self.assertEqual(client.confirm_delivery(1).result(2), b"")
            with calls_lock:
                self.assertEqual(calls, [b"x"])

    def test_local_validation_sends_no_frame_and_keeps_slot(self):
        left, right = socketpair()
        server = start_server(right, lambda b: b, ledger=DeliveryLedger(1))
        self.addCleanup(lambda: server.join(4))
        with Client(left, max_pending=1) as client:
            for bad in (0, -1, 1 << 64, True, False, 1.0, "1"):
                with self.assertRaises(ValueError):
                    client.submit_delivery(bad, b"x")
                with self.assertRaises(ValueError):
                    client.confirm_delivery(bad)
            with self.assertRaises(ValueError):
                client.submit_delivery(1, b"x" * 65529)
            with self.assertRaises(ValueError):
                client.submit_delivery(1, bytearray(b"x"))

            # The rejected calls consumed neither a slot nor a request id in a
            # way that disturbs a real submission.
            self.assertEqual(client.submit_delivery(1, b"ok").result(2), b"ok")

    def test_unconfigured_ledger_replies_without_disconnect(self):
        left, right = socketpair()
        server = start_server(
            right,
            lambda b: b[::-1],
            stream_handler=lambda p: iter([b"chunk"]),
        )
        self.addCleanup(lambda: server.join(4))
        with Client(left) as client:
            self.assertEqual(
                outcome(client.submit_delivery(1, b"x")),
                ("err", "delivery unavailable"),
            )
            self.assertEqual(
                outcome(client.confirm_delivery(1)),
                ("err", "delivery unavailable"),
            )
            # Connection is still healthy for ordinary calls and streams.
            self.assertEqual(client.submit(b"abc").result(2), b"cba")
            stream = client.open_stream(b"x")
            self.assertEqual(stream.read(2), b"chunk")
            self.assertIsNone(stream.read(2))
            stream.wait_closed(2)

    def test_ledger_capacity_bounds(self):
        for capacity in (0, -1, 65, 1.0, "2", True):
            with self.assertRaises(ValueError):
                DeliveryLedger(capacity)
        for capacity in (1, 64):
            self.assertIsInstance(DeliveryLedger(capacity), DeliveryLedger)
        left, right = socketpair()
        with left, right:
            with self.assertRaises(ValueError):
                serve(left, lambda b: b, ledger="nope")

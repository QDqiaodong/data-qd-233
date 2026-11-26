"""Acceptance tests for ResourceGate cross-connection resource scheduling.

Every scenario uses real socketpair sockets, Event-based gates and bounded
waits -- no fixed sleeps to guess ordering.  After the assertions each test
prints a short, actually-observed summary in Chinese.
"""
from socket import socketpair
from threading import Event, Lock, Thread
from time import monotonic, sleep
from unittest import TestCase

from rpc import (
    Busy,
    Client,
    PeerClosed,
    RemoteError,
    ResourceGate,
    serve,
)


def wait_for(predicate, timeout=3.0, interval=0.005):
    if isinstance(predicate, Event):
        return predicate.wait(timeout)
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(interval)
    return predicate()


def start_server(sock, handler, **kwargs):
    thread = Thread(target=serve, args=(sock, handler), kwargs=kwargs)
    thread.start()
    return thread


def remote_message(future, timeout=3.0):
    try:
        future.result(timeout)
    except RemoteError as exc:
        return str(exc)
    raise AssertionError("future completed without a RemoteError")


class ResourceGateAcceptanceTests(TestCase):
    # ------------------------------------------------------------------
    # 1. Multi-key qualification is acquired as one indivisible set.
    # ------------------------------------------------------------------
    def test_multi_key_qualification_is_atomic(self):
        b1_entered, b1_gate = Event(), Event()
        b2_entered, b2_gate = Event(), Event()
        ab_entered, ab_gate = Event(), Event()
        a_entered = Event()
        fail_entered = Event()
        after_fail_entered = Event()
        runs = []
        runs_lock = Lock()

        def handler(body):
            with runs_lock:
                runs.append(body)
            if body == b"read-b-1":
                b1_entered.set()
                self.assertTrue(b1_gate.wait(5), "b1 gate timed out")
            elif body == b"read-b-2":
                b2_entered.set()
                self.assertTrue(b2_gate.wait(5), "b2 gate timed out")
            elif body == b"update-ab":
                ab_entered.set()
                self.assertTrue(ab_gate.wait(5), "a+b gate timed out")
            elif body == b"update-a":
                a_entered.set()
            elif body == b"fail-b":
                fail_entered.set()
                raise RuntimeError("boom")
            elif body == b"after-fail-b":
                after_fail_entered.set()
            return body

        gate = ResourceGate(handler, capacity=16)
        c1_sock, s1_sock = socketpair()
        c2_sock, s2_sock = socketpair()
        c3_sock, s3_sock = socketpair()
        # Connection three has a single worker permit.
        t1 = start_server(s1_sock, handler, workers=2, resource_gate=gate)
        t2 = start_server(s2_sock, handler, workers=2, resource_gate=gate)
        t3 = start_server(s3_sock, handler, workers=1, resource_gate=gate)
        self.addCleanup(lambda: t1.join(4))
        self.addCleanup(lambda: t2.join(4))
        self.addCleanup(lambda: t3.join(4))
        self.addCleanup(ab_gate.set)
        self.addCleanup(b1_gate.set)
        self.addCleanup(b2_gate.set)

        client1 = Client(c1_sock)
        client2 = Client(c2_sock)
        client3 = Client(c3_sock)
        self.addCleanup(client1.close)
        self.addCleanup(client2.close)
        self.addCleanup(client3.close)

        # Two shared-b readers on two different connections, both parked.
        reader1 = client1.submit_scoped(b"read-b-1", shared=("b",))
        reader2 = client2.submit_scoped(b"read-b-2", shared=("b",))
        self.assertTrue(b1_entered.wait(3))
        self.assertTrue(b2_entered.wait(3))

        # Exclusive a+b first, then exclusive a-only.  Both share connection
        # three's single permit and are admitted in this order.
        update_ab = client3.submit_scoped(
            b"update-ab", exclusive=("a", "b")
        )
        update_a = client3.submit_scoped(b"update-a", exclusive=("a",))

        # a-only needs neither running b reader: it must finish while both
        # readers are still inside their callbacks.
        self.assertTrue(a_entered.wait(3))
        self.assertEqual(update_a.result(3), b"update-a")
        readers_still_running = int(not b1_gate.is_set()) + int(
            not b2_gate.is_set()
        )
        self.assertFalse(ab_entered.is_set())

        # Releasing just one b reader is not enough for the a+b update.
        b1_gate.set()
        self.assertEqual(reader1.result(3), b"read-b-1")
        ab_entered_with_one_reader = wait_for(ab_entered, timeout=0.15)
        self.assertFalse(
            ab_entered_with_one_reader,
            "a+b must not start while one b reader still runs",
        )

        b2_gate.set()
        self.assertEqual(reader2.result(3), b"read-b-2")
        # Now a+b acquires its whole key set and runs exactly once.
        self.assertTrue(ab_entered.wait(3))
        self.assertFalse(update_ab.done())
        ab_gate.set()
        self.assertEqual(update_ab.result(3), b"update-ab")

        # A failing scoped callback releases its keys; the next same-key call
        # must not be blocked by it.
        failed = client1.submit_scoped(b"fail-b", exclusive=("b",))
        self.assertEqual(remote_message(failed), "handler failed")
        self.assertTrue(fail_entered.wait(3))
        after = client2.submit_scoped(b"after-fail-b", exclusive=("b",))
        self.assertTrue(after_fail_entered.wait(3))
        self.assertEqual(after.result(3), b"after-fail-b")

        with runs_lock:
            observed = list(runs)
        self.assertEqual(observed.count(b"update-ab"), 1)
        ab_started = "是" if ab_entered_with_one_reader else "否"
        follow_up_done = (
            "是" if after.result() == b"after-fail-b" else "否"
        )
        print(
            "[场景1] a-only 完成时仍在运行的 b 读者数="
            f"{readers_still_running}；只放行一个读者时 a+b 是否入场={ab_started}；"
            "两读者退出后 a+b 实际执行次数="
            f"{observed.count(b'update-ab')}；"
            f"抛异常后同键调用是否完成={follow_up_done}"
        )

    # ------------------------------------------------------------------
    # 2. Cross-connection arbitration when worker permits are exhausted.
    #
    # Chosen policy: a call blocked solely by its own connection's worker
    # permits holds no keys or permits, so a later *conflicting* call that can
    # start immediately is allowed to overtake it (the spec's first option).
    # ------------------------------------------------------------------
    def test_permit_exhaustion_arbitration_allows_overtake(self):
        ordinary_entered, ordinary_gate = Event(), Event()
        update_entered, update_gate = Event(), Event()
        read_entered, read_gate = Event(), Event()
        unrelated_entered = Event()
        order = []
        order_lock = Lock()

        def handler(body):
            if body == b"update-x":
                with order_lock:
                    order.append("update")
                update_entered.set()
                self.assertTrue(update_gate.wait(5), "update gate timed out")
            elif body == b"read-x":
                with order_lock:
                    order.append("read")
                read_entered.set()
                self.assertTrue(read_gate.wait(5), "read gate timed out")
            elif body == b"unrelated":
                with order_lock:
                    order.append("unrelated")
                unrelated_entered.set()
            return body

        def ordinary(body):
            ordinary_entered.set()
            self.assertTrue(ordinary_gate.wait(5), "ordinary gate timed out")
            return body

        gate = ResourceGate(handler, capacity=16)
        ca_sock, sa_sock = socketpair()
        cb_sock, sb_sock = socketpair()
        cc_sock, sc_sock = socketpair()
        ta = start_server(sa_sock, ordinary, workers=1, resource_gate=gate)
        tb = start_server(sb_sock, handler, workers=1, resource_gate=gate)
        tc = start_server(sc_sock, handler, workers=1, resource_gate=gate)
        self.addCleanup(lambda: ta.join(4))
        self.addCleanup(lambda: tb.join(4))
        self.addCleanup(lambda: tc.join(4))
        self.addCleanup(ordinary_gate.set)
        self.addCleanup(update_gate.set)
        self.addCleanup(read_gate.set)

        client_a = Client(ca_sock)
        client_b = Client(cb_sock)
        client_c = Client(cc_sock)
        self.addCleanup(client_a.close)
        self.addCleanup(client_b.close)
        self.addCleanup(client_c.close)

        # An ordinary call occupies connection A's only worker permit.
        parked = client_a.submit(b"parked")
        self.assertTrue(ordinary_entered.wait(3))

        # Exclusive x is registered first on A but cannot get A's permit.
        update_x = client_a.submit_scoped(
            b"update-x", exclusive=("x",)
        )
        # Shared x arrives later on B and conflicts with the earlier update,
        # but nothing *running* holds x and B has a free permit, so it
        # overtakes the permit-blocked update and starts immediately, while
        # A's ordinary call is still gated.
        read_x = client_b.submit_scoped(b"read-x", shared=("x",))
        self.assertTrue(read_entered.wait(3))
        read_entered_before_ordinary_release = ordinary_gate.is_set() is False

        # Unrelated scoped work on a third connection keeps moving even while
        # the read is parked and A's ordinary call is still gated.
        unrelated = client_c.submit_scoped(b"unrelated", exclusive=("z",))
        self.assertTrue(unrelated_entered.wait(3))
        self.assertEqual(unrelated.result(3), b"unrelated")

        # While the read runs, the conflicting update still cannot enter,
        # even after A's ordinary call releases A's permit.
        self.assertFalse(update_entered.is_set())
        ordinary_gate.set()
        self.assertEqual(parked.result(3), b"parked")
        update_entered_while_read_runs = wait_for(update_entered, timeout=0.15)
        self.assertFalse(
            update_entered_while_read_runs,
            "update overtook a still-running conflicting read",
        )

        # Releasing the read is what lets the update start; no overlap.
        read_gate.set()
        self.assertEqual(read_x.result(3), b"read-x")
        self.assertTrue(update_entered.wait(3))

        # Unrelated work still completes while the update itself is parked.
        unrelated2 = client_c.submit_scoped(b"unrelated", exclusive=("w",))
        unrelated2_ok = unrelated2.result(3) == b"unrelated"

        update_gate.set()
        self.assertEqual(update_x.result(3), b"update-x")

        with order_lock:
            observed_order = list(order)
        self.assertEqual(observed_order.index("read"), 0)
        self.assertLess(
            observed_order.index("read"), observed_order.index("update")
        )
        # The update entry event was still clear while the read gate was shut,
        # so the two same-key callbacks never overlapped.
        no_overlap = observed_order.index("read") < observed_order.index(
            "update"
        ) and not update_entered_while_read_runs
        read_first = "是" if read_entered_before_ordinary_release else "否"
        update_during_read = (
            "是" if update_entered_while_read_runs else "否"
        )
        print(
            "[场景2] 采用允许超车口径：甲普通调用放行前读取是否已入场="
            f"{read_first}；实际入场顺序={observed_order}；"
            f"读取运行中更新是否入场={update_during_read}；"
            f"读取/更新回调无重叠={'是' if no_overlap else '否'}；"
            f"更新卡门期间不相关调用完成={'是' if unrelated2_ok else '否'}"
        )

    # ------------------------------------------------------------------
    # 3. Disconnect must not drop isolation of a call already executing.
    # ------------------------------------------------------------------
    def test_disconnect_keeps_running_isolation_but_drops_queue(self):
        r_entered, r_gate = Event(), Event()
        old_q_entered = Event()
        new_r_entered = Event()
        runs = []
        runs_lock = Lock()

        def handler(body):
            with runs_lock:
                runs.append(body)
            if body == b"r-old":
                r_entered.set()
                self.assertTrue(r_gate.wait(5), "r gate timed out")
            elif body == b"q-old":
                old_q_entered.set()
            elif body == b"r-new":
                new_r_entered.set()
            return body

        gate = ResourceGate(handler, capacity=2)
        ca_sock, sa_sock = socketpair()
        cb_sock, sb_sock = socketpair()
        ta = start_server(sa_sock, handler, workers=1, resource_gate=gate)
        tb = start_server(sb_sock, handler, workers=1, resource_gate=gate)
        self.addCleanup(lambda: ta.join(4))
        self.addCleanup(lambda: tb.join(4))
        self.addCleanup(r_gate.set)

        client_a = Client(ca_sock)
        client_b = Client(cb_sock)
        self.addCleanup(client_b.close)

        # Exclusive r runs and parks inside its callback on connection A.
        r_old = client_a.submit_scoped(b"r-old", exclusive=("r",))
        self.assertTrue(r_entered.wait(3))

        # Exclusive q can only wait for A's permit; it is the second gate item.
        q_old = client_a.submit_scoped(b"q-old", exclusive=("q",))
        self.assertFalse(
            wait_for(old_q_entered, timeout=0.15),
            "queued q call must not start without a permit",
        )

        # Gate capacity (2) is now fully occupied by the waiting and running
        # calls of A, so B's q submission is rejected without a callback.
        rejected = client_b.submit_scoped(b"q-b", exclusive=("q",))
        self.assertEqual(remote_message(rejected), "resource queue full")
        with runs_lock:
            self.assertNotIn(b"q-b", runs)

        # Close A.  Its unsettled futures follow the existing PeerClosed
        # contract; the running r keeps its keys on the server.
        client_a.close()
        with self.assertRaises(PeerClosed):
            r_old.result(3)
        with self.assertRaises(PeerClosed):
            q_old.result(3)

        # B retries q in bounded time while the server notices the disconnect.
        q_new = None
        deadline = monotonic() + 3
        while monotonic() < deadline:
            candidate = client_b.submit_scoped(b"q-b", exclusive=("q",))
            try:
                self.assertEqual(candidate.result(0.1), b"q-b")
                q_new = candidate
                break
            except RemoteError as exc:
                self.assertEqual(str(exc), "resource queue full")
            except TimeoutError:
                q_new = candidate
                break
        self.assertIsNotNone(q_new)
        if not q_new.done():
            self.assertEqual(q_new.result(3), b"q-b")

        # The old queued q callback must never have run, even after detach.
        self.assertFalse(
            old_q_entered.wait(timeout=0.15),
            "a queued call from a dead connection must never execute",
        )

        # A fresh exclusive r still has to wait for the old r callback to exit.
        r_new = client_b.submit_scoped(b"r-new", exclusive=("r",))
        new_r_entered_while_old_runs = wait_for(new_r_entered, timeout=0.15)
        self.assertFalse(
            new_r_entered_while_old_runs,
            "new r must wait for the disconnected call's r to exit",
        )

        r_gate.set()
        self.assertTrue(new_r_entered.wait(3))
        self.assertEqual(r_new.result(3), b"r-new")
        new_r_completed = r_new.done() and r_new.result() == b"r-new"

        with runs_lock:
            observed = list(runs)
        self.assertNotIn(b"q-old", observed)
        self.assertIn(b"q-b", observed)
        self.assertEqual(observed.count(b"r-old"), 1)
        self.assertEqual(observed.count(b"r-new"), 1)
        print(
            "[场景3] 断线后 B 的 q 重试结果=成功；旧 q 回调执行次数="
            f"{observed.count(b'q-old')}；旧 r 未退出时新 r 是否入场="
            f"{'是' if new_r_entered_while_old_runs else '否'}；"
            f"放行后新 r 是否完成={'是' if new_r_completed else '否'}；"
            f"服务端实际回调顺序={observed}"
        )

    # ------------------------------------------------------------------
    # 4. Per-call rejection without a gate, and local validation costs.
    # ------------------------------------------------------------------
    def test_unconfigured_gate_rejects_each_call_only(self):
        left, right = socketpair()
        server = start_server(right, lambda b: b[::-1])
        self.addCleanup(lambda: server.join(4))
        with Client(left) as client:
            rejected = client.submit_scoped(b"x", exclusive=("k",))
            self.assertEqual(
                remote_message(rejected), "resource scheduling unavailable"
            )
            # The connection stays usable for ordinary calls.
            self.assertEqual(client.submit(b"abc").result(3), b"cba")
            # A second scoped call is rejected independently.
            self.assertEqual(
                remote_message(
                    client.submit_scoped(b"y", shared=("k2",))
                ),
                "resource scheduling unavailable",
            )
        print(
            "[场景4] 未启用 gate 的服务端逐次拒绝 scoped 调用="
            "resource scheduling unavailable；连接上普通调用仍正常"
        )

    def test_invalid_arguments_consume_no_slot_or_request_id(self):
        entered, release = Event(), Event()

        def gated_handler(body):
            entered.set()
            release.wait(5)
            return body

        # The gate's own callback is what serves scoped calls; the positional
        # serve handler only backs ordinary/ledger traffic.
        gate = ResourceGate(gated_handler)
        left, right = socketpair()
        server = start_server(right, lambda b: b, resource_gate=gate)
        self.addCleanup(lambda: server.join(4))
        self.addCleanup(release.set)
        with Client(left, max_pending=1) as client:
            def assert_value_error(**kwargs):
                with self.assertRaises(ValueError):
                    client.submit_scoped(b"x", **kwargs)

            for key in (
                (),
                ("",),
                ("a" * 33,),
                ("a b",),
                ("a/b",),
                ("中文",),
                (1,),
                (b"a",),
                (None,),
            ):
                assert_value_error(shared=key)
            assert_value_error(shared=("a",), exclusive=("a",))
            assert_value_error(
                exclusive=tuple(f"k{i}" for i in range(9))
            )
            # Wrong container types must be ValueError, not TypeError.
            assert_value_error(shared=["a"])
            assert_value_error(exclusive="a")
            assert_value_error(shared={"a"})
            with self.assertRaises(ValueError):
                client.submit_scoped(bytearray(b"x"), exclusive=("a",))
            with self.assertRaises(ValueError):
                client.submit_scoped(b"x" * 65001, exclusive=("a",))
            with self.assertRaises(ValueError):
                client.submit_scoped("not bytes", exclusive=("a",))

            # No rejected call consumed the single in-flight slot or disturbed
            # the request-id sequence: a real call now owns the only slot and
            # stays pending until its callback is released.
            pending = client.submit_scoped(b"hold", exclusive=("h",))
            self.assertTrue(entered.wait(3))
            with self.assertRaises(Busy):
                client.submit_scoped(b"while-pending", exclusive=("p",))
            release.set()
            self.assertEqual(pending.result(3), b"hold")
        print(
            "[场景4] 非法键/正文/参数类型全部本地 ValueError；"
            "max_pending=1 下拒绝后真实调用仍有完整名额"
        )

    def test_gate_construction_bounds(self):
        for bad in (0, -1, 65, 1.0, "2", True, False):
            with self.assertRaises(ValueError):
                ResourceGate(lambda b: b, capacity=bad)
        with self.assertRaises(ValueError):
            ResourceGate("not callable")
        for capacity in (1, 64):
            self.assertIsInstance(
                ResourceGate(lambda b: b, capacity=capacity), ResourceGate
            )
        print("[场景4] ResourceGate capacity 仅接受 1..64 的非布尔整数")

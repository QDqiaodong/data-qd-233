"""Acceptance tests for SharedReads coalesced read-only device queries.

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
    SharedReads,
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
    server = Thread(target=serve, args=(sock, handler), kwargs=kwargs)
    server.start()
    return server


def outcome(future, timeout=3.0):
    """Return ("ok", value) or ("err", message), or None while pending."""
    try:
        return "ok", future.result(timeout)
    except RemoteError as exc:
        return "err", str(exc)
    except TimeoutError:
        return None


class SharedReadsAcceptanceTests(TestCase):
    # ------------------------------------------------------------------
    # 1. Joining bypasses both quotas; each member times out independently.
    # ------------------------------------------------------------------
    def test_join_bypasses_quotas_and_times_out_independently(self):
        read_entered, read_gate = Event(), Event()
        ordinary_entered, ordinary_gate = Event(), Event()
        calls = []
        calls_lock = Lock()

        def reader(payload, stop_event):
            with calls_lock:
                calls.append(payload)
            if payload == b"slow-read":
                read_entered.set()
                if not read_gate.wait(5):
                    raise AssertionError("read gate timed out")
            with calls_lock:
                number = calls.count(payload)
            return b"read-%s-%d" % (payload, number)

        def ordinary(body):
            if body == b"park":
                ordinary_entered.set()
                if not ordinary_gate.wait(5):
                    raise AssertionError("ordinary gate timed out")
            return body

        reads = SharedReads(reader, capacity=16)
        a_sock, a_peer = socketpair()
        b_sock, b_peer = socketpair()
        server_a = start_server(a_peer, ordinary, workers=1, shared_reads=reads)
        server_b = start_server(b_peer, ordinary, workers=1, shared_reads=reads)
        self.addCleanup(lambda: server_a.join(4))
        self.addCleanup(lambda: server_b.join(4))
        self.addCleanup(read_gate.set)
        self.addCleanup(ordinary_gate.set)

        client_a = Client(a_sock)
        client_b = Client(b_sock)
        self.addCleanup(client_a.close)
        self.addCleanup(client_b.close)

        # 甲 starts the one physical read and parks inside it.
        long_a = client_a.submit_read(b"slow-read", wait_ms=5000)
        self.assertTrue(read_entered.wait(3))

        # 乙 fills its only execution permit with an ordinary call.
        parked = client_b.submit(b"park")
        self.assertTrue(ordinary_entered.wait(3))

        # 乙's short read must join the running physical read, not be told
        # "busy", even though 乙 has no free execution permit.
        short_b = client_b.submit_read(b"slow-read", wait_ms=150)
        self.assertEqual(
            outcome(short_b), ("err", "shared read timed out")
        )
        self.assertEqual(short_b.done(), True)

        # With the physical read still parked, another same-body long request
        # joins it too.
        long_b = client_b.submit_read(b"slow-read", wait_ms=5000)
        self.assertFalse(long_b.done())

        # A different body cannot start a second physical read: 乙 has no free
        # execution permit, so it is rejected immediately.
        other = client_b.submit_read(b"other-read", wait_ms=5000)
        self.assertEqual(outcome(other), ("err", "shared read busy"))
        with calls_lock:
            self.assertNotIn(b"other-read", calls)
            calls_before_release = list(calls)

        # Releasing the reader delivers the *same* physical result to both
        # still-live members.
        read_gate.set()
        result_a = long_a.result(3)
        result_b = long_b.result(3)
        self.assertEqual(result_a, result_b)
        self.assertEqual(result_a, b"read-slow-read-1")
        with calls_lock:
            self.assertEqual(calls.count(b"slow-read"), 1)

        # The short request keeps its terminal timeout and gets no second
        # reply: the connection stays usable for later calls.
        other_still = outcome(short_b, timeout=0.05)
        self.assertEqual(other_still, ("err", "shared read timed out"))

        ordinary_gate.set()
        self.assertEqual(parked.result(3), b"park")
        # A stray second reply would break the client's reply-id contract.
        self.assertEqual(client_b.submit(b"ping").result(3), b"ping")
        self.assertEqual(client_a.submit(b"ping").result(3), b"ping")

        with calls_lock:
            observed = list(calls)
        print(
            "[场景1] 同读实际回调次数="
            f"{observed.count(b'slow-read')}；短请求终态={other_still}；"
            "放行前回调记录="
            f"{calls_before_release}；两个未到期请求是否拿到同一次读取="
            f"{'是' if result_a == result_b else '否'}；"
            "不同正文无执行位时是否启动="
            f"{'是' if b'other-read' in observed else '否'}"
        )

    # ------------------------------------------------------------------
    # 2. The only member times out; the last-member policy is allow-rejoin.
    #
    # Chosen policy (documented in README): the reader keeps running with no
    # stop signal, so a later same-body request may join it and receive its
    # result.  A physical read that has exited is never reused as a cache.
    # ------------------------------------------------------------------
    def test_last_member_timeout_policy_allow_rejoin(self):
        entered, gate = Event(), Event()
        stop_observed = []
        calls = []
        calls_lock = Lock()

        def reader(payload, stop_event):
            with calls_lock:
                calls.append(payload)
                number = calls.count(payload)
            if payload == b"last-read":
                entered.set()
                if not gate.wait(5):
                    raise AssertionError("read gate timed out")
                stop_observed.append(stop_event.is_set())
            return b"call-%d" % number

        reads = SharedReads(reader, capacity=1)
        a_sock, a_peer = socketpair()
        b_sock, b_peer = socketpair()
        server_a = start_server(a_peer, lambda b: b, workers=1, shared_reads=reads)
        server_b = start_server(b_peer, lambda b: b, workers=1, shared_reads=reads)
        self.addCleanup(lambda: server_a.join(4))
        self.addCleanup(lambda: server_b.join(4))
        self.addCleanup(gate.set)

        client_a = Client(a_sock)
        client_b = Client(b_sock)
        self.addCleanup(client_a.close)
        self.addCleanup(client_b.close)

        # 甲 is the only request on the only read; it times out for real
        # while 甲 stays connected and the old callback stays parked.
        timed_out = client_a.submit_read(b"last-read", wait_ms=150)
        self.assertTrue(entered.wait(3))
        self.assertEqual(
            outcome(timed_out), ("err", "shared read timed out")
        )

        # 甲's execution permit is still held by the unexited callback.
        self.assertEqual(
            outcome(client_a.submit(b"ordinary")), ("err", "server busy")
        )

        # 乙's different body cannot start: the single shared slot is held.
        self.assertEqual(
            outcome(client_b.submit_read(b"other-read", wait_ms=5000)),
            ("err", "shared read busy"),
        )

        # 乙 asks for the original body: allow-rejoin says it may wait on the
        # old callback rather than being told busy.
        rejoined = client_b.submit_read(b"last-read", wait_ms=5000)
        self.assertIsNone(outcome(rejoined, timeout=0.2))

        self.assertEqual(gate.wait(0.05), False)  # the gate is still shut
        gate.set()

        old_value = rejoined.result(3)
        self.assertEqual(old_value, b"call-1")
        with calls_lock:
            self.assertEqual(calls.count(b"last-read"), 1)

        # The timed-out member must not receive a second, late reply: a
        # duplicate RESULT/ERROR would violate the client's reply-id contract
        # and close the connection.  A fresh ordinary call proves it stayed
        # healthy, and the timed-out future keeps its timeout.
        self.assertEqual(
            outcome(timed_out), ("err", "shared read timed out")
        )
        ping = None
        deadline = monotonic() + 3
        while monotonic() < deadline:
            candidate = client_a.submit(b"ping")
            try:
                if candidate.result(0.1) == b"ping":
                    ping = candidate
                    break
            except RemoteError as exc:
                self.assertEqual(str(exc), "server busy")
        self.assertIsNotNone(ping, "甲 never regained an execution permit")

        # After the old read exited, the same body starts a genuinely new
        # physical read with a different, actual-call-number value.
        retry = client_b.submit_read(b"last-read", wait_ms=5000)
        new_value = retry.result(3)
        self.assertNotEqual(new_value, old_value)
        self.assertEqual(new_value, b"call-2")

        # Allow-rejoin never asks the reader to stop.
        self.assertFalse(any(stop_observed))

        # And a previously rejected different body now actually executes.
        other = client_b.submit_read(b"other-read", wait_ms=5000)
        other_value = other.result(3)
        with calls_lock:
            observed = list(calls)
        self.assertEqual(observed.count(b"other-read"), 1)
        self.assertEqual(observed.count(b"last-read"), 2)

        print(
            "[场景2] 采用允许重新加入口径：旧回调放行时停止信号是否设置="
            f"{'是' if any(stop_observed) else '否'}；同读实际回调次数="
            f"{observed.count(b'last-read')}（退出前 1，退出后重试 +1）；"
            "超时者是否只收到一次远端超时="
            f"{'是' if outcome(timed_out) == ('err', 'shared read timed out') else '否'}；"
            "最后等待者离开后乙实际得到="
            f"旧结果 {old_value!r}（旧读取退出后重试才取得新结果 {new_value!r}）；"
            f"此前被拒的不同正文随后实际执行值={other_value!r}"
        )

    # ------------------------------------------------------------------
    # 3a. A disconnect withdraws only that connection's members.
    # ------------------------------------------------------------------
    def test_disconnect_keeps_read_for_remaining_members(self):
        entered, gate = Event(), Event()
        joined_marker = Event()
        calls = []
        calls_lock = Lock()

        def reader(payload, stop_event):
            with calls_lock:
                calls.append(payload)
                number = calls.count(payload)
            if payload == b"join-read":
                entered.set()
                if not gate.wait(5):
                    raise AssertionError("read gate timed out")
            return b"call-%d" % number

        def handler(body):
            if body == b"marker":
                # This ordinary frame is handled strictly after the preceding
                # SHARED_READ frame of the same connection, so its completion
                # proves 乙 has joined.
                joined_marker.set()
            return body

        reads = SharedReads(reader, capacity=1)
        a_sock, a_peer = socketpair()
        b_sock, b_peer = socketpair()
        server_a = start_server(a_peer, handler, workers=1, shared_reads=reads)
        server_b = start_server(b_peer, handler, workers=1, shared_reads=reads)
        self.addCleanup(lambda: server_a.join(4))
        self.addCleanup(lambda: server_b.join(4))
        self.addCleanup(gate.set)

        client_a = Client(a_sock)
        client_b = Client(b_sock)
        self.addCleanup(client_b.close)
        self.addCleanup(client_a.close)

        started = client_a.submit_read(b"join-read", wait_ms=5000)
        self.assertTrue(entered.wait(3))

        joined = client_b.submit_read(b"join-read", wait_ms=5000)
        self.assertEqual(client_b.submit(b"marker").result(3), b"marker")
        self.assertTrue(joined_marker.wait(3))
        self.assertFalse(joined.done())

        # 甲 leaves; its request follows the ordinary PeerClosed contract.
        client_a.close()
        with self.assertRaises(PeerClosed):
            started.result(3)

        # 乙 still receives the one physical read's value, and no second read
        # was started on the departing connection's behalf.
        gate.set()
        joined_value = joined.result(3)
        self.assertEqual(joined_value, b"call-1")
        with calls_lock:
            self.assertEqual(calls.count(b"join-read"), 1)

        # Once that read has exited it is not a cache: a new same-body request
        # starts a new physical read with a new actual call number.
        retry = client_b.submit_read(b"join-read", wait_ms=5000)
        retry_value = retry.result(3)
        self.assertNotEqual(retry_value, joined_value)
        self.assertEqual(retry_value, b"call-2")
        with calls_lock:
            observed = list(calls)

        print(
            "[场景3a] 甲断开后乙是否仍收到原读取结果="
            f"{'是' if joined_value == b'call-1' else '否'}；"
            f"断线前后同读实际回调次数={observed.count(b'join-read')}"
            "（乙未额外启动）；同正文重试得到新值="
            f"{retry_value!r}（不同于旧值 {joined_value!r}）"
        )

    # ------------------------------------------------------------------
    # 3b. Reader failure and invalid return reach every still-live member.
    # ------------------------------------------------------------------
    def test_reader_failure_and_invalid_value_reach_members(self):
        boom_entered, boom_gate = Event(), Event()
        marker = Event()
        calls = []
        calls_lock = Lock()

        def reader(payload, stop_event):
            with calls_lock:
                calls.append(payload)
            if payload == b"boom-read":
                boom_entered.set()
                if not boom_gate.wait(5):
                    raise AssertionError("boom gate timed out")
                raise RuntimeError("device exploded")
            if payload == b"bad-read":
                return "not bytes"
            return payload[::-1]

        def handler(body):
            if body == b"marker":
                marker.set()
            return body

        reads = SharedReads(reader, capacity=8)
        a_sock, a_peer = socketpair()
        b_sock, b_peer = socketpair()
        server_a = start_server(a_peer, handler, workers=2, shared_reads=reads)
        server_b = start_server(b_peer, handler, workers=2, shared_reads=reads)
        self.addCleanup(lambda: server_a.join(4))
        self.addCleanup(lambda: server_b.join(4))
        self.addCleanup(boom_gate.set)

        client_a = Client(a_sock)
        client_b = Client(b_sock)
        self.addCleanup(client_a.close)
        self.addCleanup(client_b.close)

        failing = client_a.submit_read(b"boom-read", wait_ms=5000)
        self.assertTrue(boom_entered.wait(3))
        joined = client_b.submit_read(b"boom-read", wait_ms=5000)
        self.assertEqual(client_b.submit(b"marker").result(3), b"marker")
        self.assertTrue(marker.wait(3))

        boom_gate.set()
        self.assertEqual(
            outcome(failing), ("err", "handler failed")
        )
        self.assertEqual(
            outcome(joined), ("err", "handler failed")
        )

        # An invalid return value fails only its members, the same way.
        self.assertEqual(
            outcome(client_a.submit_read(b"bad-read", wait_ms=5000)),
            ("err", "handler failed"),
        )
        with calls_lock:
            observed = list(calls)
        self.assertEqual(observed.count(b"boom-read"), 1)
        # The connection is still healthy after the isolated failures.
        self.assertEqual(client_b.submit(b"later").result(3), b"later")

        print(
            "[场景3b] reader 抛异常时两个有效成员均收到=handler failed；"
            f"实际 boom 回调次数={observed.count(b'boom-read')}；"
            "非法返回值仅本次收到 handler failed，连接后续普通调用正常"
        )

    # ------------------------------------------------------------------
    # 3c. Unconfigured service rejects only the current call.
    # ------------------------------------------------------------------
    def test_unconfigured_service_rejects_only_the_current_call(self):
        left, right = socketpair()
        server = start_server(right, lambda body: body[::-1])
        self.addCleanup(lambda: server.join(4))
        with Client(left) as client:
            self.assertEqual(
                outcome(client.submit_read(b"x", wait_ms=1000)),
                ("err", "shared reads unavailable"),
            )
            self.assertEqual(client.submit(b"abc").result(3), b"cba")
            self.assertEqual(
                outcome(client.submit_read(b"", wait_ms=1000)),
                ("err", "shared reads unavailable"),
            )
            self.assertEqual(client.submit(b"def").result(3), b"fed")
        print(
            "[场景3c] 未配置 shared_reads 时每次只读调用收到="
            "shared reads unavailable；同连接普通调用始终保持可用"
        )

    # ------------------------------------------------------------------
    # 3d. Local validation sends no frame and keeps in-flight capacity.
    # ------------------------------------------------------------------
    def test_local_validation_keeps_inflight_capacity(self):
        reads = SharedReads(lambda payload, stop: payload, capacity=1)
        entered, gate = Event(), Event()

        def handler(body):
            entered.set()
            if not gate.wait(5):
                raise AssertionError("handler gate timed out")
            return body

        left, right = socketpair()
        server = start_server(right, handler, workers=1, shared_reads=reads)
        self.addCleanup(lambda: server.join(4))
        self.addCleanup(gate.set)

        with Client(left, max_pending=1) as client:
            def bad_payload(payload):
                with self.assertRaises(ValueError):
                    client.submit_read(payload, wait_ms=1000)

            bad_payload(bytearray(b"x"))
            bad_payload("not bytes")
            bad_payload(b"x" * 65535)
            for wait in (0, -1, 60001, True, False, 1.0, "1", None):
                with self.assertRaises(ValueError):
                    client.submit_read(b"x", wait_ms=wait)

            # None of the rejections consumed the single in-flight slot, and
            # a real call still owns it until released.
            pending = client.submit(b"hold")
            self.assertTrue(entered.wait(3))
            with self.assertRaises(Busy):
                client.submit_read(b"x", wait_ms=1000)
            gate.set()
            self.assertEqual(pending.result(3), b"hold")

            # The read future is not cancellable, like an ordinary submit.
            future = client.submit_read(b"abc", wait_ms=1000)
            self.assertFalse(future.cancel())
            self.assertEqual(future.result(3), b"abc")

        for bad in (0, -1, 65, 1.0, "2", True, False):
            with self.assertRaises(ValueError):
                SharedReads(lambda payload, stop: payload, capacity=bad)
        with self.assertRaises(ValueError):
            SharedReads("not callable")
        for capacity in (1, 64):
            self.assertIsInstance(
                SharedReads(lambda payload, stop: payload, capacity=capacity),
                SharedReads,
            )
        print(
            "[场景3d] 非法正文/期限全部本地 ValueError，未发帧、未占 "
            "max_pending=1 名额；submit_read 的 future 不可取消；"
            "SharedReads capacity 仅接受 1..64 的非布尔整数"
        )

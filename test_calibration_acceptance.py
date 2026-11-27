"""Acceptance tests for server-driven calibrated conversions.

Every scenario uses real socketpair sockets, Event-based gates and bounded
waits -- no fake transport and no fixed sleeps to guess ordering.  The
sockets are wrapped only to observe the frames that actually cross the
connection; every byte still travels over a real kernel socketpair.

After the assertions each test prints a short, actually-measured summary in
Chinese.
"""
from socket import socketpair
from threading import Event, Lock, Thread
from time import monotonic, sleep
from unittest import TestCase

from rpc import (
    Busy,
    Calibration,
    Client,
    PeerClosed,
    RemoteError,
    serve,
)
from wire import (
    CALIB_ANSWER,
    CALIB_QUERY,
    CALIB_REQUEST,
    _HEADER,
)


def wait_for(predicate, timeout=5.0, interval=0.005):
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


def remote_message(future, timeout=5.0):
    try:
        future.result(timeout)
    except RemoteError as exc:
        return str(exc)
    raise AssertionError("future completed without a RemoteError")


class RecordingSocket:
    """Observing wrapper: records frames actually handed to the real socket.

    Only ``sendall`` is intercepted (one RPC frame per call); every other call
    is forwarded unchanged, so transport behavior is exactly the real
    socketpair's.
    """

    def __init__(self, sock, label, log, log_lock):
        self._sock = sock
        self._label = label
        self._log = log
        self._log_lock = log_lock

    def sendall(self, data):
        kind, request_id, length = _HEADER.unpack(data[: _HEADER.size])
        with self._log_lock:
            self._log.append((self._label, kind, request_id, length))
        return self._sock.sendall(data)

    def recv(self, size):
        return self._sock.recv(size)

    def shutdown(self, how):
        return self._sock.shutdown(how)

    def close(self):
        return self._sock.close()

    def fileno(self):
        return self._sock.fileno()

    def setsockopt(self, *args):
        return self._sock.setsockopt(*args)

    def getsockopt(self, *args):
        return self._sock.getsockopt(*args)

    def __enter__(self):
        self._sock.__enter__()
        return self

    def __exit__(self, *exc):
        return self._sock.__exit__(*exc)


def frames_of(log, label, kind):
    return [(rid, length) for tag, k, rid, length in log if tag == label and k == kind]


class CalibrationHarness:
    """Builds one wrapped socketpair with a recording server and client."""

    def __init__(self, *, workers=1, calibration_slots=3,
                 calibration=None, reader=None, make_client=True):
        self.frame_log = []
        self.frame_lock = Lock()
        client_sock, server_sock = socketpair()
        self.client_sock = RecordingSocket(
            client_sock, "client->server", self.frame_log, self.frame_lock
        )
        self.server_sock = RecordingSocket(
            server_sock, "server->client", self.frame_log, self.frame_lock
        )
        self.prepared = []          # request payloads that ran prepare
        self.disposed = []          # state identities disposed
        self.committed = []         # (state identity, answer) committed
        self.reader_calls = []      # queries handed to the device
        self.callback_lock = Lock()
        calibration = calibration if calibration is not None else self.build()
        self.server = start_server(
            self.server_sock,
            lambda body: body,
            workers=workers,
            calibration=calibration,
            calibration_slots=calibration_slots,
        )
        self.client = (
            Client(self.client_sock, calibration_reader=reader)
            if make_client
            else None
        )

    def connect_client(self, reader):
        assert self.client is None
        self.client = Client(self.client_sock, calibration_reader=reader)
        return self.client

    def build(self):
        def prepare(payload):
            with self.callback_lock:
                index = len(self.prepared)
                self.prepared.append(payload)
            # Distinct 32768-byte query per conversion; the first one (A) is
            # all zero bytes so gated readers can recognize it.
            fill = 0 if index == 0 else (index & 0xFF)
            query = bytes([fill]) * 32768
            state = {"payload": payload, "index": index}
            return state, query

        def commit(state, answer):
            with self.callback_lock:
                self.committed.append((state["index"], answer))
            return b"result:" + state["payload"]

        def dispose(state):
            with self.callback_lock:
                self.disposed.append(state["index"])

        return Calibration(prepare, commit, dispose)

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass
        self.server.join(5)


class CalibrationAcceptanceTests(TestCase):
    # ------------------------------------------------------------------
    # 1. Overload point with the device gate shut, then disconnect.
    # ------------------------------------------------------------------
    def test_overload_disconnect_cleanup_and_query_handoff_boundary(self):
        harness = CalibrationHarness(
            workers=1, calibration_slots=3, make_client=False
        )
        device_entered = Event()
        device_gate = Event()
        read_started = Event()

        def gated_reader(query):
            # Identify A by its all-zero 32768-byte query.
            if query == b"\x00" * 32768:
                device_entered.set()
                read_started.set()
                if not device_gate.wait(10):
                    raise AssertionError("device gate timed out")
            with harness.callback_lock:
                harness.reader_calls.append(query)
            return b"answer-for:" + query[:4]

        harness.connect_client(gated_reader)
        self.addCleanup(device_gate.set)
        self.addCleanup(harness.close)

        # A enters prepare, gets its query handed over and parks in the device.
        future_a = harness.client.submit_calibrated(b"payload-A")
        self.assertTrue(device_entered.wait(5))

        # B and C each run prepare (workers=1 serializes them) and acquire
        # distinct states and distinct 32768-byte queries.
        future_b = harness.client.submit_calibrated(b"payload-B")
        future_c = harness.client.submit_calibrated(b"payload-C")
        self.assertTrue(
            wait_for(lambda: len(harness.prepared) == 3),
            "all three prepares did not run",
        )
        with harness.callback_lock:
            prepared = list(harness.prepared)
        self.assertEqual(prepared, [b"payload-A", b"payload-B", b"payload-C"])

        # Ordinary status query still round-trips while the device is stuck:
        # its handler needs the only worker permit, which no parked
        # conversion is holding while waiting for the device.
        status = harness.client.submit(b"status")
        self.assertEqual(status.result(5), b"status")

        # A fourth calibrated conversion is rejected busy without prepare.
        future_d = harness.client.submit_calibrated(b"payload-D")
        self.assertEqual(remote_message(future_d), "calibration busy")
        with harness.callback_lock:
            self.assertNotIn(b"payload-D", harness.prepared)

        # ---- observe the query handoff boundary on the real wire ----
        with harness.frame_lock:
            queries_sent = list(
                frames_of(harness.frame_log, "server->client", CALIB_QUERY)
            )
        # Exactly one complete query body crossed the connection (A's); B and
        # C bodies were never handed off because the single device channel is
        # still occupied by A's read.
        self.assertEqual(len(queries_sent), 1)
        self.assertEqual(queries_sent[0][1], 32768)
        crossed_ids = {rid for rid, _ in queries_sent}

        # Map request ids by reading the client's own CALIB_REQUEST frames.
        with harness.frame_lock:
            requests_sent = list(
                frames_of(harness.frame_log, "client->server", CALIB_REQUEST)
            )
        request_ids = [rid for rid, _ in requests_sent]
        id_a, id_b, id_c, id_d = request_ids
        self.assertEqual(crossed_ids, {id_a})
        self.assertNotIn(id_b, crossed_ids)
        self.assertNotIn(id_c, crossed_ids)
        # No answer frame has crossed either: A's read never returned.
        with harness.frame_lock:
            answers = list(
                frames_of(harness.frame_log, "client->server", CALIB_ANSWER)
            )
        self.assertEqual(answers, [])

        # ---- disconnect while the device stays gated ----
        harness.client.close()   # must return despite the stuck device read
        for future in (future_a, future_b, future_c):
            with self.assertRaises(PeerClosed):
                future.result(5)

        harness.server.join(5)
        self.assertFalse(harness.server.is_alive())

        # Only now release A's device gate: its late result cannot revive
        # anything.
        device_gate.set()
        self.assertTrue(read_started.wait(1))

        # All three acquired states are disposed exactly once; no commit ran;
        # the device callback ran only for A (B and C queries never started).
        with harness.callback_lock:
            disposed = sorted(harness.disposed)
            committed = list(harness.committed)
            reader_calls = len(harness.reader_calls)
        self.assertEqual(disposed, [0, 1, 2])
        self.assertEqual(len(disposed), 3)
        self.assertEqual(committed, [])
        self.assertEqual(reader_calls, 0)

        print(
            "[场景1-过载断线] 门内状态回复=status；断线前跨过连接的待查正文数="
            f"{len(queries_sent)}（仅 A 的请求 {id_a}，B={id_b}/C={id_c} 均未发出）；"
            f"第四笔拒绝=calibration busy 且未运行 prepare；"
            f"三笔 future 均以 PeerClosed 结束；"
            f"清理记录={disposed}（各一次）；提交记录={committed}（无提交）；"
            f"设备回调次数={reader_calls}（B/C 未开始读取）"
        )

    # ------------------------------------------------------------------
    # 2. No disconnect: all three proceed, each using its own answer.
    # ------------------------------------------------------------------
    def test_happy_path_each_uses_its_own_answer(self):
        harness = CalibrationHarness(
            workers=1, calibration_slots=3, make_client=False
        )
        device_entered = Event()
        device_gate = Event()

        def gated_reader(query):
            if query == b"\x00" * 32768:
                device_entered.set()
                if not device_gate.wait(10):
                    raise AssertionError("device gate timed out")
            with harness.callback_lock:
                harness.reader_calls.append(query)
            # The answer is derived from this query's own distinct fill byte,
            # so every conversion can be checked against its own lookup value
            # regardless of the order in which reads are scheduled.
            return b"answer-" + bytes([query[0]])

        harness.connect_client(gated_reader)
        self.addCleanup(device_gate.set)
        self.addCleanup(harness.close)

        fa = harness.client.submit_calibrated(b"payload-A")
        self.assertTrue(device_entered.wait(5))
        fb = harness.client.submit_calibrated(b"payload-B")
        fc = harness.client.submit_calibrated(b"payload-C")
        self.assertTrue(wait_for(lambda: len(harness.prepared) == 3))

        # B/C queries must not cross while A's read is outstanding.
        with harness.frame_lock:
            before = list(
                frames_of(harness.frame_log, "server->client", CALIB_QUERY)
            )
        self.assertEqual(len(before), 1)

        device_gate.set()
        self.assertEqual(fa.result(5), b"result:payload-A")
        self.assertEqual(fb.result(5), b"result:payload-B")
        self.assertEqual(fc.result(5), b"result:payload-C")

        with harness.callback_lock:
            committed = list(harness.committed)
            disposed = sorted(harness.disposed)
            reader_calls = len(harness.reader_calls)
        # Every commit saw the answer derived from its own query's fill byte:
        # A's query is all 0x00, B's 0x01, C's 0x02.  Commit scheduling order
        # is not asserted; the (state index, answer) pairing is what matters.
        by_state = {index: answer for index, answer in committed}
        self.assertEqual(by_state.get(0), b"answer-\x00")
        self.assertEqual(by_state.get(1), b"answer-\x01")
        self.assertEqual(by_state.get(2), b"answer-\x02")
        self.assertEqual(len(committed), 3)
        self.assertEqual(reader_calls, 3)
        self.assertEqual(disposed, [0, 1, 2])
        answers = sorted(by_state.values())

        # Exactly three query bodies crossed in total, one per read.
        with harness.frame_lock:
            queries = list(
                frames_of(harness.frame_log, "server->client", CALIB_QUERY)
            )
        self.assertEqual(len(queries), 3)

        print(
            "[场景2-正常完成] 三笔均提交成功且各自使用自己的查找值="
            f"{answers}；设备读取次数={reader_calls}；跨连接查询正文总数="
            f"{len(queries)}（每次读取一份，不提前交接）；清理记录={disposed}"
        )

    # ------------------------------------------------------------------
    # 3. One device read fails: that conversion is cleaned up, not committed;
    #    the other two still commit with their own results.
    # ------------------------------------------------------------------
    def test_one_device_failure_is_isolated(self):
        harness = CalibrationHarness(
            workers=1, calibration_slots=3, make_client=False
        )
        device_entered = Event()
        device_gate = Event()

        def gated_reader(query):
            if query == b"\x00" * 32768:
                device_entered.set()
                if not device_gate.wait(10):
                    raise AssertionError("device gate timed out")
            with harness.callback_lock:
                harness.reader_calls.append(query)
            if query[:1] == b"\x01":   # B's own query (fill byte 0x01) raises
                raise RuntimeError("device exploded")
            return b"answer-" + bytes([query[0]])

        harness.connect_client(gated_reader)
        self.addCleanup(device_gate.set)
        self.addCleanup(harness.close)

        fa = harness.client.submit_calibrated(b"payload-A")
        self.assertTrue(device_entered.wait(5))
        fb = harness.client.submit_calibrated(b"payload-B")
        fc = harness.client.submit_calibrated(b"payload-C")
        self.assertTrue(wait_for(lambda: len(harness.prepared) == 3))
        device_gate.set()

        self.assertEqual(fa.result(5), b"result:payload-A")
        self.assertEqual(remote_message(fb), "calibration read failed")
        self.assertEqual(fc.result(5), b"result:payload-C")

        with harness.callback_lock:
            committed = list(harness.committed)
            disposed = sorted(harness.disposed)
        committed_indexes = [index for index, _ in committed]
        self.assertEqual(committed_indexes, [0, 2])  # B (index 1) skipped
        self.assertEqual(disposed, [0, 1, 2])        # B still cleaned up

        print(
            "[场景3-设备失败隔离] 失败笔终态=calibration read failed 且未提交；"
            f"提交记录={committed_indexes}（另两笔各自拿到自己的结果）；"
            f"清理记录={disposed}（失败笔仍恰好清理一次）"
        )

    # ------------------------------------------------------------------
    # 4. Empty answer is legal and reaches commit.
    # ------------------------------------------------------------------
    def test_empty_answer_is_legal(self):
        harness = CalibrationHarness(
            workers=1, calibration_slots=1, reader=lambda query: b""
        )
        self.addCleanup(harness.close)

        future = harness.client.submit_calibrated(b"empty")
        self.assertEqual(future.result(5), b"result:empty")
        with harness.callback_lock:
            committed = list(harness.committed)
            answer = committed[0][1]
        self.assertEqual(answer, b"")

        print("[场景4-空答复] 空 bytes 作为合法答复进入 commit，结果正常返回")

    # ------------------------------------------------------------------
    # 5. No reader configured on the client; server feature otherwise on.
    # ------------------------------------------------------------------
    def test_missing_reader_is_reported_and_connection_stays_usable(self):
        harness = CalibrationHarness(
            workers=1, calibration_slots=1, reader=None
        )
        self.addCleanup(harness.close)

        future = harness.client.submit_calibrated(b"need-calib")
        self.assertEqual(
            remote_message(future), "calibration reader unavailable"
        )
        # The connection stays usable for ordinary and further calibrated
        # traffic.
        self.assertEqual(
            harness.client.submit(b"ordinary").result(5), b"ordinary"
        )
        again = harness.client.submit_calibrated(b"again")
        self.assertEqual(
            remote_message(again), "calibration reader unavailable"
        )
        with harness.callback_lock:
            # prepare ran, but commit never did; state was disposed.
            self.assertEqual(harness.committed, [])
            self.assertEqual(sorted(harness.disposed), [0, 1])

        print(
            "[场景5-无设备回调] 每次转换收到 calibration reader unavailable；"
            "普通状态查询仍可往返；state 均被清理且无提交"
        )

    # ------------------------------------------------------------------
    # 6. Server feature unconfigured: only that call is rejected.
    # ------------------------------------------------------------------
    def test_unconfigured_server_rejects_only_the_call(self):
        frame_log, frame_lock = [], Lock()
        client_sock, server_sock = socketpair()
        client_sock = RecordingSocket(
            client_sock, "client->server", frame_log, frame_lock
        )
        server_sock = RecordingSocket(
            server_sock, "server->client", frame_log, frame_lock
        )
        server = start_server(server_sock, lambda body: body[::-1])
        client = Client(client_sock)
        self.addCleanup(client.close)
        self.addCleanup(lambda: server.join(5))

        future = client.submit_calibrated(b"x")
        self.assertEqual(
            remote_message(future), "calibration unavailable"
        )
        # Ordinary behavior on the same connection is untouched.
        self.assertEqual(client.submit(b"abc").result(5), b"cba")
        self.assertEqual(client.submit(b"").result(5), b"")

        print(
            "[场景6-服务端未配置] 校准调用逐次收到 calibration unavailable；"
            "同连接普通调用行为完全不变"
        )

    # ------------------------------------------------------------------
    # 7. Construction/argument validation.
    # ------------------------------------------------------------------
    def test_validation_at_construction_and_submission(self):
        # Calibration triple must all be callable; this constructs no socket.
        ok = lambda *a: None
        with self.assertRaises(ValueError):
            Calibration("not callable", ok, ok)
        with self.assertRaises(ValueError):
            Calibration(ok, ok, None)
        with self.assertRaises(ValueError):
            Calibration(ok, "nope", ok)

        # Client-side reader must be callable.  A failing constructor starts no
        # reader thread and leaves the socket for the caller to own.
        client_sock, server_sock = socketpair()
        try:
            with self.assertRaises(ValueError):
                Client(client_sock, calibration_reader=123)
        finally:
            client_sock.close()
            server_sock.close()

        # calibration_slots accepts only non-bool ints in 1..16.  The value
        # check runs before the accept loop starts, so calling serve directly
        # raises on this thread without consuming the socket.
        calibration_ok = Calibration(
            lambda p: (None, b""),
            lambda state, answer: b"",
            lambda state: None,
        )
        for bad in (0, 17, -1, 1.0, "4", True, False):
            c, s = socketpair()
            try:
                with self.assertRaises(ValueError):
                    serve(
                        s,
                        lambda body: body,
                        calibration=calibration_ok,
                        calibration_slots=bad,
                    )
            finally:
                c.close()
                s.close()

        # Boundary values 1 and 16 are accepted: serve blocks in its accept
        # loop and returns once the client end closes.
        for good in (1, 16):
            c, s = socketpair()
            thread = start_server(
                s,
                lambda body: body,
                calibration=calibration_ok,
                calibration_slots=good,
            )
            c.close()
            thread.join(5)
            self.assertFalse(thread.is_alive())

        # submit_calibrated shares submit's payload validation; a bad payload
        # sends no frame and reserves no slot.
        c2, s2 = socketpair()
        try:
            with Client(c2, max_pending=1) as client:
                for bad in ("not bytes", bytearray(b"x"), b"x" * 65537, None):
                    with self.assertRaises(ValueError):
                        client.submit_calibrated(bad)
                # The single slot is still fully available for a real call.
                held = client.submit(b"hold")
                with self.assertRaises(Busy):
                    client.submit_calibrated(b"x")
                # uncancellable future like submit()
                self.assertFalse(held.cancel())
        finally:
            s2.close()

        print(
            "[场景7-参数校验] 非可调用回调/非 1..16 非布尔槽数构造即 ValueError；"
            "非法正文不发帧不占 max_pending；future 不可取消"
        )

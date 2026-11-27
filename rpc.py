"""Bounded, multiplexed request and stream RPC over one local stream."""
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from itertools import count
import socket
from struct import Struct
from threading import (
    BoundedSemaphore,
    Condition,
    Event,
    Lock,
    Thread,
    current_thread,
)
from time import monotonic

from wire import (
    DELIVERY_CONFIRM,
    DELIVERY_MAX_PAYLOAD,
    DELIVERY_SEQ_SIZE,
    DELIVERY_SUBMIT,
    ERROR,
    MAX_PAYLOAD,
    MAX_STREAM_SLOTS,
    MAX_WINDOW,
    REQUEST,
    RESULT,
    SCOPED_MAX_KEY_SIZE,
    SCOPED_MAX_KEYS,
    SCOPED_MAX_PAYLOAD,
    SCOPED_REQUEST,
    SHARED_READ,
    SHARED_READ_MAX_PAYLOAD,
    SHARED_READ_WAIT_SIZE,
    STREAM_CANCEL,
    STREAM_CREDIT,
    STREAM_DATA,
    STREAM_END,
    STREAM_OPEN,
    ProtocolError,
    receive,
    send,
)


class Busy(RuntimeError):
    pass


class PeerClosed(ConnectionError):
    pass


class RemoteError(RuntimeError):
    pass


class StreamClosed(RuntimeError):
    pass


def _terminate(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _payload(payload):
    return isinstance(payload, bytes) and len(payload) <= MAX_PAYLOAD


def _delivery_seq(seq):
    return type(seq) is int and 1 <= seq < (1 << 64)


def _scoped_key(key):
    if not isinstance(key, str) or not 1 <= len(key) <= SCOPED_MAX_KEY_SIZE:
        return False
    for char in key:
        if not (
            "a" <= char <= "z"
            or "A" <= char <= "Z"
            or "0" <= char <= "9"
            or char in "._-"
        ):
            return False
    return True


def _scoped_scope(shared, exclusive):
    if type(shared) is not tuple or type(exclusive) is not tuple:
        raise ValueError("shared and exclusive must be tuples of keys")
    combined = shared + exclusive
    if not all(_scoped_key(key) for key in combined):
        raise ValueError(
            "keys must be ASCII strings of 1 to 32 letters, digits, dots, "
            "underscores or hyphens"
        )
    total = len(combined)
    if not 1 <= total <= SCOPED_MAX_KEYS:
        raise ValueError(
            f"declare 1 to {SCOPED_MAX_KEYS} keys in total"
        )
    if len(set(shared) | set(exclusive)) != total:
        raise ValueError("resource keys must not repeat")
    return shared, exclusive


def _encode_scoped_body(shared, exclusive, payload):
    chunks = [bytes((len(shared), len(exclusive)))]
    for key in shared + exclusive:
        encoded = key.encode("ascii")
        chunks.append(bytes((len(encoded),)))
        chunks.append(encoded)
    chunks.append(payload)
    return b"".join(chunks)


def _parse_scoped_body(body):
    """Decode a SCOPED_REQUEST body or raise ProtocolError."""
    if len(body) < 4:
        raise ProtocolError("invalid scoped request frame")
    shared_count, exclusive_count = body[0], body[1]
    total = shared_count + exclusive_count
    if not 1 <= total <= SCOPED_MAX_KEYS:
        raise ProtocolError("invalid scoped request frame")
    offset = 2
    keys = []
    for _ in range(total):
        if offset >= len(body):
            raise ProtocolError("invalid scoped request frame")
        size = body[offset]
        offset += 1
        if size < 1 or offset + size > len(body):
            raise ProtocolError("invalid scoped request frame")
        raw = body[offset : offset + size]
        offset += size
        try:
            key = raw.decode("ascii")
        except UnicodeDecodeError:
            raise ProtocolError("invalid scoped request frame")
        if not _scoped_key(key):
            raise ProtocolError("invalid scoped request frame")
        keys.append(key)
    if len(set(keys)) != total:
        raise ProtocolError("invalid scoped request frame")
    payload = body[offset:]
    if not isinstance(payload, bytes) or len(payload) > SCOPED_MAX_PAYLOAD:
        raise ProtocolError("invalid scoped request frame")
    return tuple(keys[:shared_count]), tuple(keys[shared_count:]), payload


class DeliveryLedger:
    """Process-local record of accepted, still-unconfirmed instructions.

    One ledger represents one producer's sequence of delivery instructions and
    may be shared by several concurrent or successive connections.  It keeps
    only unconfirmed records (at most *capacity*) plus the highest accepted
    sequence number; it performs no persistence and no automatic retry.
    """

    __slots__ = ("_capacity", "_cond", "_records", "_highest")

    def __init__(self, capacity):
        if type(capacity) is not int or not 1 <= capacity <= 64:
            raise ValueError("capacity must be an integer from 1 to 64")
        self._capacity = capacity
        self._cond = Condition()
        self._records = {}
        self._highest = 0


class _DeliveryRecord:
    __slots__ = ("seq", "payload", "done", "failed", "value", "waiter")

    def __init__(self, seq, payload):
        self.seq = seq
        self.payload = payload
        self.done = False
        self.failed = False
        self.value = b""
        self.waiter = None


class _DeliveryWaiter:
    __slots__ = ("request_id", "record", "abandoned", "event", "thread")

    def __init__(self, request_id, record):
        self.request_id = request_id
        self.record = record
        self.abandoned = False
        self.event = Event()
        self.thread = None


_SEQ = Struct(">Q")
_WAIT = Struct(">H")


class Stream:
    """Client-side handle for one ordered, flow-controlled stream."""

    def __init__(self, client, request_id, window):
        self._client = client
        self.id = request_id
        self.window = window
        self._cond = Condition()
        self._chunks = deque()
        self._local_closed = False
        self._remote_settled = False
        self._eof = False
        self._remote_error = None
        self._peer_error = None
        self._quota_held = True

    # ---- methods used by the client reader thread ----
    def deliver_data(self, chunk):
        with self._cond:
            # A locally abandoned stream keeps its record only until the
            # remote settlement; late data is deliberately discarded.
            if self._local_closed:
                return
            self._chunks.append(chunk)
            self._cond.notify_all()

    def deliver_end(self):
        with self._cond:
            if self._remote_settled:
                return
            self._remote_settled = True
            self._eof = True
            self._release_quota_if_idle_locked()
            self._cond.notify_all()

    def deliver_error(self, message):
        error = RemoteError(message)
        with self._cond:
            if self._remote_settled:
                return
            self._remote_settled = True
            self._remote_error = error
            self._release_quota_if_idle_locked()
            self._cond.notify_all()

    def peer_closed(self, error):
        with self._cond:
            # A terminal frame already processed is a settled result and must
            # not be changed by a later connection close.
            if not self._remote_settled and self._peer_error is None:
                self._peer_error = error
            self._cond.notify_all()

    def _release_quota_if_idle_locked(self):
        if (
            self._quota_held
            and self._remote_settled
            and not self._chunks
        ):
            self._quota_held = False
            self._client._release_record(self.id, self)

    # ---- public handle API ----
    def read(self, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        deadline = None if timeout is None else monotonic() + timeout
        credit = False

        with self._cond:
            while True:
                if self._local_closed:
                    raise StreamClosed("stream was closed locally")

                if self._chunks:
                    chunk = self._chunks.popleft()
                    if self._remote_settled:
                        self._release_quota_if_idle_locked()
                    else:
                        # The chunk has now been delivered to the application,
                        # so its window share can be returned to the server.
                        credit = True
                    break

                if self._remote_error is not None:
                    raise self._remote_error

                if self._eof:
                    return None

                if self._peer_error is not None:
                    raise self._peer_error

                if deadline is None:
                    self._cond.wait()
                    continue

                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for stream chunk")
                self._cond.wait(remaining)

        if credit:
            self._client._send_control(STREAM_CREDIT, self.id)
        return chunk

    def close(self):
        with self._cond:
            if self._local_closed:
                return
            self._local_closed = True
            # Local abandonment discards chunks which have not been handed to
            # read().  This also makes the max_pending share releasable once
            # the remote settlement arrives.
            self._chunks.clear()
            cancel_remote = not self._remote_settled
            self._release_quota_if_idle_locked()
            self._cond.notify_all()

        if cancel_remote:
            self._client._send_control(STREAM_CANCEL, self.id)

    def wait_closed(self, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        deadline = None if timeout is None else monotonic() + timeout

        with self._cond:
            while not self._remote_settled:
                if self._peer_error is not None:
                    raise self._peer_error
                if deadline is None:
                    self._cond.wait()
                    continue
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for stream settlement")
                self._cond.wait(remaining)
        return None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# Descriptive alias for callers that prefer the "handle" terminology.
StreamHandle = Stream


class Client:
    def __init__(self, sock, *, max_pending=16):
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be positive")
        self._sock = sock
        self._limit = max_pending
        self._pending = {}
        self._numbers = count(1)
        self._lock = Lock()
        self._sending = Lock()
        self._closed = False
        self._reader = Thread(target=self._dispatch, name="rpc-reader", daemon=True)
        self._reader.start()

    def _allocate(self):
        """Allocate the next monotonic id and reserve one max_pending slot."""
        with self._lock:
            if self._closed:
                raise PeerClosed("client is closed")
            if len(self._pending) >= self._limit:
                raise Busy("client in-flight limit reached")
            request_id = next(self._numbers)
            if request_id >= 1 << 64:
                raise OverflowError("request id space exhausted")
            return request_id

    def _insert(self, request_id, record):
        with self._lock:
            # _allocate reserved space; a concurrent close is the only state
            # that can invalidate it.
            if self._closed:
                raise PeerClosed("client is closed")
            self._pending[request_id] = record

    def submit(self, payload):
        if not _payload(payload):
            raise ValueError("expected up to 65536 bytes")
        return self._submit_frame(REQUEST, b"", payload)

    def submit_scoped(self, payload, *, shared=(), exclusive=()):
        """Submit one resource-scoped call, sharing this connection's limits.

        Local validation happens before any request id or max_pending slot is
        taken, so rejected arguments send no frame and leave no trace.
        """
        if not isinstance(payload, bytes) or len(payload) > SCOPED_MAX_PAYLOAD:
            raise ValueError(
                f"expected up to {SCOPED_MAX_PAYLOAD} bytes"
            )
        shared, exclusive = _scoped_scope(shared, exclusive)
        body = _encode_scoped_body(shared, exclusive, payload)
        return self._submit_frame(SCOPED_REQUEST, b"", body)

    def submit_delivery(self, seq, payload):
        """Submit a ledger-backed instruction under business sequence *seq*."""
        if not _delivery_seq(seq):
            raise ValueError("seq must be an integer from 1 to 2**64-1")
        if not isinstance(payload, bytes) or len(payload) > DELIVERY_MAX_PAYLOAD:
            raise ValueError("expected up to 65528 bytes")
        return self._submit_frame(
            DELIVERY_SUBMIT, _SEQ.pack(seq), payload
        )

    def submit_read(self, payload, *, wait_ms):
        """Submit one shared read, coalesced by payload within a SharedReads.

        *payload* is at most 65534 bytes (empty is allowed) and *wait_ms* is an
        integer from 1 to 60000.  Invalid local arguments raise ValueError
        before any frame is sent or any in-flight slot or request id is used.
        """
        if not isinstance(payload, bytes) or len(payload) > SHARED_READ_MAX_PAYLOAD:
            raise ValueError(
                f"expected up to {SHARED_READ_MAX_PAYLOAD} bytes"
            )
        if type(wait_ms) is not int or not 1 <= wait_ms <= 60000:
            raise ValueError("wait_ms must be an integer from 1 to 60000")
        return self._submit_frame(
            SHARED_READ, _WAIT.pack(wait_ms), payload
        )

    def confirm_delivery(self, seq):
        """Confirm receipt of a terminal delivery result, freeing its slot."""
        if not _delivery_seq(seq):
            raise ValueError("seq must be an integer from 1 to 2**64-1")
        return self._submit_frame(DELIVERY_CONFIRM, _SEQ.pack(seq), b"")

    def _submit_frame(self, kind, prefix, payload):
        send_error = None
        with self._sending:
            request_id = self._allocate()
            future = Future()
            future.set_running_or_notify_cancel()
            self._insert(request_id, future)
            try:
                send(self._sock, kind, request_id, prefix + payload)
            except OSError as exc:
                send_error = exc
                _terminate(self._sock)

        if send_error is not None:
            self._shutdown(send_error)
        return future

    def open_stream(self, payload, *, window=1):
        if not _payload(payload):
            raise ValueError("expected up to 65536 bytes")
        if type(window) is not int or not 1 <= window <= MAX_WINDOW:
            raise ValueError(f"window must be an integer from 1 to {MAX_WINDOW}")

        send_error = None
        with self._sending:
            request_id = self._allocate()
            stream = Stream(self, request_id, window)
            self._insert(request_id, stream)
            try:
                send(
                    self._sock,
                    STREAM_OPEN,
                    request_id,
                    bytes((window,)) + payload,
                )
            except OSError as exc:
                send_error = exc
                _terminate(self._sock)

        if send_error is not None:
            self._shutdown(send_error)
        return stream

    def _send_control(self, kind, request_id):
        try:
            with self._sending:
                send(self._sock, kind, request_id, b"")
        except OSError as exc:
            self._shutdown(exc)

    def _release_record(self, request_id, record):
        with self._lock:
            if self._pending.get(request_id) is record:
                self._pending.pop(request_id, None)

    def _shutdown(self, error):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            records, self._pending = self._pending, {}
            closed_error = PeerClosed(str(error))

        _terminate(self._sock)
        for record in records.values():
            if isinstance(record, Future):
                if not record.done():
                    record.set_exception(closed_error)
            else:
                record.peer_closed(closed_error)

    def _dispatch(self):
        try:
            while True:
                frame = receive(self._sock)
                if frame is None:
                    raise PeerClosed("peer disconnected")
                kind, request_id, payload = frame

                with self._lock:
                    record = self._pending.get(request_id)
                    if record is None:
                        raise ProtocolError("unexpected reply id")

                    if isinstance(record, Future):
                        if kind not in (RESULT, ERROR):
                            raise ProtocolError("expected a result or error frame")
                        self._pending.pop(request_id, None)
                    elif not isinstance(record, Stream) or kind not in (
                        STREAM_DATA,
                        STREAM_END,
                        ERROR,
                    ):
                        raise ProtocolError("unexpected frame for request")

                if isinstance(record, Future):
                    if kind == RESULT:
                        record.set_result(payload)
                    else:
                        record.set_exception(
                            RemoteError(payload.decode("utf-8", "replace"))
                        )
                elif kind == STREAM_DATA:
                    record.deliver_data(payload)
                elif kind == STREAM_END:
                    record.deliver_end()
                else:
                    record.deliver_error(payload.decode("utf-8", "replace"))
        except OSError as exc:
            self._shutdown(exc)

    def close(self):
        self._shutdown(PeerClosed("client is closed"))
        if current_thread() is not self._reader:
            self._reader.join()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class _ServerStream:
    def __init__(self, request_id, payload, window):
        self.id = request_id
        self.payload = payload
        self.window = window
        self.cond = Condition()
        self.credits = window
        self.cancel_requested = False
        self.iterator = None
        self.close_attempted = False
        # Generation owning this stream.  None until the first factory
        # execution; from then until cleanup exit it never changes.
        self.generation = None

    def cancel(self):
        with self.cond:
            self.cancel_requested = True
            self.cond.notify_all()

    def grant_credit(self):
        with self.cond:
            # A correctly behaving client returns at most the initial window
            # number of shares.  Cap defensively to preserve the bound.
            if self.credits < self.window:
                self.credits += 1
            self.cond.notify_all()

    def is_cancelled(self):
        with self.cond:
            return self.cancel_requested


def _rollout_callables(handler, stream_handler, on_retire):
    return (
        callable(handler)
        and callable(stream_handler)
        and callable(on_retire)
    )


class _Generation:
    """One published handler/stream/retire triple with in-use accounting.

    Work that may touch this generation's resources is represented by a pin:
    an ordinary handler call pins for the duration of the call; a stream pins
    from its first factory execution until its iterator cleanup has exited.
    A generation may run its retirement callback only once retiring has been
    published and its pin count reaches zero.
    """

    __slots__ = (
        "handler",
        "stream_handler",
        "on_retire",
        "_future",
        "_cond",
        "_active",
        "_retiring",
        "_retirement_started",
    )

    def __init__(self, handler, stream_handler, on_retire):
        self.handler = handler
        self.stream_handler = stream_handler
        self.on_retire = on_retire
        self._future = None
        self._cond = Condition()
        self._active = 0
        self._retiring = False
        self._retirement_started = False

    def pin(self):
        with self._cond:
            self._active += 1

    def unpin(self):
        """Release one pin; return True if retirement may now start."""
        with self._cond:
            self._active -= 1
            return self._retirement_ready_locked()

    def begin_retirement(self, future):
        """Mark this generation as replaced; return True if it is safe now."""
        with self._cond:
            self._retiring = True
            self._future = future
            return self._retirement_ready_locked()

    def _retirement_ready_locked(self):
        if self._retirement_started or not self._retiring or self._active != 0:
            return False
        self._retirement_started = True
        return True


class HandlerRollout:
    """Process-local switchable supplier of ordinary and stream callbacks.

    A rollout is shared by any number of concurrent ``serve`` connections.
    Each :meth:`replace` publishes one new generation and returns a
    :class:`~concurrent.futures.Future` receipt for the generation it replaced.
    At most one generation may be waiting out retirement at a time, so a
    second replacement before that receipt settles raises :class:`Busy`.
    """

    def __init__(self, handler, stream_handler, *, on_retire):
        if not _rollout_callables(handler, stream_handler, on_retire):
            raise ValueError(
                "handler, stream_handler and on_retire must be callable"
            )
        self._lock = Condition()
        self._current = _Generation(handler, stream_handler, on_retire)
        self._retiring = None

    def current(self):
        with self._lock:
            return self._current

    def pin(self, generation):
        generation.pin()

    def release(self, generation):
        if generation.unpin():
            self._spawn_retirement(generation)

    def replace(self, handler, stream_handler, *, on_retire):
        if not _rollout_callables(handler, stream_handler, on_retire):
            raise ValueError(
                "handler, stream_handler and on_retire must be callable"
            )
        # The receipt is born running so it can never be cancelled.
        future = Future()
        future.set_running_or_notify_cancel()

        with self._lock:
            if self._retiring is not None:
                raise Busy("previous generation has not finished retiring")
            old = self._current
            new = _Generation(handler, stream_handler, on_retire)
            self._current = new
            self._retiring = old
            ready = old.begin_retirement(future)

        if ready:
            self._spawn_retirement(old)
        return future

    def _spawn_retirement(self, generation):
        # Retirement runs on its own thread and never occupies a connection's
        # execution permit.  The condition flag guarantees a single starter.
        thread = Thread(
            target=self._run_retirement,
            args=(generation,),
            name="rpc-rollout-retire",
            daemon=True,
        )
        thread.start()

    def _run_retirement(self, generation):
        future = generation._future
        try:
            generation.on_retire()
        except Exception as exc:
            future.set_exception(exc)
        else:
            future.set_result(None)
        finally:
            with self._lock:
                if self._retiring is generation:
                    self._retiring = None
                    self._lock.notify_all()


# Shared declarations may run alongside each other; any exclusive claim on a
# key excludes every other claim on that same key.
_SCOPED_SHARED = "shared"
_SCOPED_EXCLUSIVE = "exclusive"


class _ScopedCall:
    """One gate-admitted call, from admission until its callback exits."""

    __slots__ = (
        "seq",
        "conn",
        "request_id",
        "payload",
        "keys",
        "modes",
        "running",
    )

    def __init__(self, seq, conn, request_id, shared, exclusive, payload):
        self.seq = seq
        self.conn = conn
        self.request_id = request_id
        self.payload = payload
        self.keys = tuple(shared) + tuple(exclusive)
        self.modes = {key: _SCOPED_SHARED for key in shared}
        self.modes.update({key: _SCOPED_EXCLUSIVE for key in exclusive})
        self.running = False

    def conflicts_with(self, other):
        for key in self.keys:
            mode = other.modes.get(key)
            if mode is None:
                continue
            if (
                self.modes[key] == _SCOPED_EXCLUSIVE
                or mode == _SCOPED_EXCLUSIVE
            ):
                return True
        return False


class ResourceGate:
    """Process-local coordinator of resource-scoped callbacks.

    A gate is shared by any number of concurrent ``serve`` connections through
    the ``resource_gate`` keyword.  It serializes access per resource key while
    letting unrelated work proceed: shared claims on one key run in parallel,
    and any exclusive claim on a key excludes every other claim on that key.
    The gate bounds only its own admitted calls (waiting plus running); each
    call still needs one of its own connection's existing worker permits
    before it starts, and waiting never holds a permit or a partial key set.

    Arbitration walks the queue in gate admission order and starts every call
    whose whole key set is compatible with the currently running calls and
    whose connection currently holds a free worker permit.  A key-blocked call
    never seals its other, idle keys, so unrelated work always proceeds.  A
    call blocked solely by its own connection's worker permits holds no keys
    or permits; this implementation lets a later conflicting call that can
    start immediately overtake it.  The full key set and the worker permit are
    acquired together, so waiting never occupies a permit or partial keys.
    """

    def __init__(self, handler, *, capacity=16):
        if not callable(handler):
            raise ValueError("handler must be callable")
        if type(capacity) is not int or not 1 <= capacity <= 64:
            raise ValueError("capacity must be an integer from 1 to 64")
        self._handler = handler
        self._capacity = capacity
        self._lock = Condition()
        self._seq = 0
        self._connections = {}
        self._calls = deque()
        self._arbiter = None

    # ---- connection registration, used by serve ----

    def attach(self, conn):
        with self._lock:
            starting = not self._connections
            self._connections[conn.token] = conn
            if starting:
                # One arbiter per attached-connection epoch; it exits once the
                # last connection detaches, so gates without users leave no
                # background thread behind.
                self._arbiter = Thread(
                    target=self._arbitrate,
                    name="rpc-resource-gate",
                    daemon=True,
                )
                self._arbiter.start()
            self._lock.notify_all()

    def detach(self, conn):
        """Forget a leaving connection and drop its still-queued calls.

        Calls which already started keep their keys and run their callback to
        completion; the connection simply cannot send their reply anymore.
        """
        with self._lock:
            if self._connections.pop(conn.token, None) is None:
                return
            remaining = deque()
            for call in self._calls:
                if call.conn is conn and not call.running:
                    continue
                remaining.append(call)
            self._calls = remaining
            self._lock.notify_all()

    # ---- admission, used by the serve accept loop ----

    def admit(self, conn, request_id, shared, exclusive, payload):
        """Register one scoped call; return False if the gate is full."""
        with self._lock:
            if len(self._calls) >= self._capacity:
                return False
            self._seq += 1
            call = _ScopedCall(
                self._seq, conn, request_id, shared, exclusive, payload
            )
            self._calls.append(call)
            self._lock.notify_all()
            return True

    # ---- arbitration ----

    def _blocked_by_running_locked(self, call):
        """True if any running call conflicts with *call* on a shared key."""
        for active in self._calls:
            if active.running and call.conflicts_with(active):
                return True
        return False

    def _scan_locked(self):
        """Admit startable calls in gate order; return the chosen batch.

        A call starts exactly when its whole key set is compatible with the
        running set and its connection has a free worker permit.  Key-blocked
        calls seal none of their idle keys, so later calls on those keys and
        unrelated calls keep moving; a call blocked solely by its own
        connection's permits holds nothing, so an eligible later call may
        overtake it, as the arbitration policy explicitly permits.
        """
        started = []
        for call in self._calls:
            if call.running or not call.conn.alive:
                continue
            if self._blocked_by_running_locked(call):
                continue
            # Acquire the whole qualification atomically: the worker permit
            # plus, effectively, every declared key (checked above).  Waiting
            # calls never hold permits or partial resources.
            if not call.conn.work_slots.acquire(blocking=False):
                continue
            call.running = True
            started.append(call)
        return started

    def _launch(self, call):
        try:
            call.conn.pool.submit(self._run, call)
        except RuntimeError:
            # serve is shutting its pool down; release what the scan took and
            # let the connection teardown settle the request.
            with self._lock:
                call.running = False
                self._calls = deque(
                    item for item in self._calls if item is not call
                )
                self._lock.notify_all()
            call.conn.work_slots.release()

    def _run(self, call):
        conn = call.conn
        try:
            try:
                response = self._handler(call.payload)
                if not _payload(response):
                    raise ValueError("invalid handler response")
                kind, body = RESULT, response
            except Exception:
                kind, body = ERROR, b"handler failed"
            try:
                conn.write_frame(kind, call.request_id, body)
            except OSError:
                conn.terminate()
        finally:
            # Gate accounting and keys are released when the callback exits,
            # independently of whether the peer ever reads the reply.
            with self._lock:
                self._calls = deque(
                    item for item in self._calls if item is not call
                )
                self._lock.notify_all()
            conn.work_slots.release()

    def _reap_dead_locked(self):
        dead = [
            token
            for token, conn in self._connections.items()
            if not conn.alive
        ]
        for token in dead:
            self._connections.pop(token, None)
        if dead:
            self._calls = deque(
                call
                for call in self._calls
                if call.conn.alive or call.running
            )

    def _arbitrate(self):
        while True:
            with self._lock:
                self._reap_dead_locked()
                if not self._connections and not self._calls:
                    # The last connection left and every callback it owned has
                    # exited; this epoch is over until a new attach().
                    self._arbiter = None
                    return
                started = self._scan_locked()
                if not started:
                    # Permits occupied by ordinary, stream or ledger work on a
                    # connection are released outside the gate, so a periodic
                    # re-check closes that window in bounded time.
                    self._lock.wait(0.05)
                    continue
            for call in started:
                self._launch(call)


class _GateConnection:
    """Per-connection facade handed to a shared ResourceGate."""

    __slots__ = ("token", "work_slots", "pool", "write_frame", "terminate", "alive")

    def __init__(self, token, work_slots, pool, write_frame, terminate):
        self.token = token
        self.work_slots = work_slots
        self.pool = pool
        self.write_frame = write_frame
        self.terminate = terminate
        self.alive = True


# ---- shared, coalesced device reads --------------------------------

_SHARED_READ_TIMEOUT = b"shared read timed out"
_SHARED_READ_FAILED = b"handler failed"


class _SharedConnection:
    """Per-connection facade handed to a shared SharedReads instance."""

    __slots__ = (
        "token",
        "work_slots",
        "pool",
        "write_frame",
        "terminate",
        "alive",
    )

    def __init__(self, token, work_slots, pool, write_frame, terminate):
        self.token = token
        self.work_slots = work_slots
        self.pool = pool
        self.write_frame = write_frame
        self.terminate = terminate
        self.alive = True


class _SharedMember:
    """One client request waiting on a shared physical read."""

    __slots__ = ("read", "conn", "request_id", "deadline", "state")

    def __init__(self, read, conn, request_id, deadline):
        self.read = read
        self.conn = conn
        self.request_id = request_id
        self.deadline = deadline
        # "waiting" until exactly one terminal reply has been decided.
        self.state = "waiting"


class _SharedRead:
    """One physical reader call, shared by every joined request."""

    __slots__ = ("payload", "owner", "stop_event", "members")

    def __init__(self, payload, owner):
        self.payload = payload
        self.owner = owner
        self.stop_event = Event()
        self.members = set()


class SharedReads:
    """Process-local coalescer for identical read-only device queries.

    One instance may be passed to any number of concurrent ``serve``
    connections through the ``shared_reads`` keyword.  A request for a payload
    with an in-progress reader joins that reader instead of starting another;
    joining needs neither the global read slot nor a worker permit.  A new
    physical read starts only when the global ``capacity`` has room *and* the
    accepting connection has a free existing worker permit; otherwise the
    request is rejected immediately with ``shared read busy``.

    Each request has its own deadline, measured from when this server started
    processing it.  The deadline only ends that member's wait: the reader is
    never cancelled on its behalf and other members keep waiting.  When the
    last member leaves, this implementation keeps the reader running and does
    not set its stop event, so a later identical request may still join it and
    receive its result.  A physical read that has exited is never retained as a
    cache.
    """

    __slots__ = (
        "_reader",
        "_capacity",
        "_lock",
        "_reads",
        "_conn_members",
        "_timer",
    )

    def __init__(self, reader, *, capacity=16):
        if not callable(reader):
            raise ValueError("reader must be callable")
        if type(capacity) is not int or not 1 <= capacity <= 64:
            raise ValueError("capacity must be an integer from 1 to 64")
        self._reader = reader
        self._capacity = capacity
        self._lock = Condition()
        self._reads = {}
        self._conn_members = {}
        self._timer = None

    # ---- accept loop integration ----

    def accept(self, conn, request_id, payload, wait_ms):
        """Register one read request; reply busy here when it cannot start."""
        deadline = monotonic() + wait_ms / 1000.0
        busy = False

        with self._lock:
            read = self._reads.get(payload)
            if read is not None:
                # Joining bypasses both quotas: an existing reader answers it.
                self._add_member_locked(read, conn, request_id, deadline)
            elif len(self._reads) >= self._capacity:
                busy = True
            elif not conn.work_slots.acquire(blocking=False):
                busy = True
            else:
                read = _SharedRead(payload, conn)
                self._reads[payload] = read
                self._add_member_locked(read, conn, request_id, deadline)
                try:
                    conn.pool.submit(self._run, read)
                except RuntimeError:
                    # The accepting pool is shutting down; undo admission and
                    # return the worker permit without queuing anything.
                    read.members.clear()
                    self._drop_conn_member_locked(conn, request_id)
                    if self._reads.get(payload) is read:
                        del self._reads[payload]
                    conn.work_slots.release()
                    busy = True
            if not busy:
                self._ensure_timer_locked()
                self._lock.notify_all()

        if busy:
            self._reply(conn, ERROR, request_id, b"shared read busy")

    def detach(self, conn):
        """Forget every waiting member of a connection which has left.

        Other connections' members keep waiting for the same physical read.
        Under this implementation's last-member policy the reader keeps
        running, so a later identical request may still join it.
        """
        with self._lock:
            members = self._conn_members.pop(conn.token, None)
            if members:
                for member in list(members):
                    if member.state == "waiting":
                        member.state = "settled"
                        member.read.members.discard(member)
                self._lock.notify_all()

    # ---- membership and reply bookkeeping ----

    def _add_member_locked(self, read, conn, request_id, deadline):
        member = _SharedMember(read, conn, request_id, deadline)
        read.members.add(member)
        self._conn_members.setdefault(conn.token, set()).add(member)
        return member

    def _drop_conn_member_locked(self, conn, request_id):
        members = self._conn_members.get(conn.token)
        if not members:
            return
        for member in list(members):
            if member.request_id == request_id:
                members.discard(member)
                if not members:
                    self._conn_members.pop(conn.token, None)
                return

    def _settle_locked(self, member, kind, body, replies):
        """Decide the single terminal reply for a still-waiting member."""
        if member.state != "waiting":
            return
        member.state = "settled"
        member.read.members.discard(member)
        bucket = self._conn_members.get(member.conn.token)
        if bucket is not None:
            bucket.discard(member)
            if not bucket:
                self._conn_members.pop(member.conn.token, None)
        replies.append((member.conn, member.request_id, kind, body))

    def _ensure_timer_locked(self):
        if self._timer is None:
            self._timer = Thread(
                target=self._deadline_loop,
                name="rpc-shared-read-deadlines",
                daemon=True,
            )
            self._timer.start()

    def _reply(self, conn, kind, request_id, body):
        # A dedicated thread keeps one slow or blocked peer from delaying any
        # other connection's replies or the accept loop.
        try:
            Thread(
                target=self._deliver,
                args=(conn, kind, request_id, body),
                name="rpc-shared-read-reply",
                daemon=True,
            ).start()
        except RuntimeError:
            conn.terminate()

    def _deliver(self, conn, kind, request_id, body):
        try:
            conn.write_frame(kind, request_id, body)
        except OSError:
            conn.terminate()

    def _spawn_replies(self, replies):
        for conn, request_id, kind, body in replies:
            self._reply(conn, kind, request_id, body)

    # ---- the physical reader ----

    def _run(self, read):
        kind, body = ERROR, _SHARED_READ_FAILED
        try:
            try:
                value = self._reader(read.payload, read.stop_event)
            except Exception:
                pass
            else:
                if _payload(value):
                    kind, body = RESULT, value

            now = monotonic()
            replies = []
            with self._lock:
                if self._reads.get(read.payload) is read:
                    del self._reads[read.payload]
                for member in list(read.members):
                    if member.state != "waiting":
                        continue
                    if now < member.deadline:
                        self._settle_locked(member, kind, body, replies)
                    else:
                        # A completion decided at or after the deadline never
                        # overrides that member's timeout, even if the timeout
                        # reply has not been written yet.
                        self._settle_locked(
                            member, ERROR, _SHARED_READ_TIMEOUT, replies
                        )
                self._lock.notify_all()
            self._spawn_replies(replies)
        finally:
            # Both quotas stay held until the callback has actually exited.
            # The global read slot is released by removing the entry above;
            # this permit belongs to the connection which accepted the read.
            read.owner.work_slots.release()

    # ---- per-member deadlines ----

    def _earliest_deadline_locked(self, now):
        """Earliest deadline among waiting members, or None.

        A member already due returns *now* so the caller re-scans at once.
        """
        earliest = None
        for read in list(self._reads.values()):
            for member in list(read.members):
                if member.state != "waiting":
                    continue
                if member.deadline <= now:
                    return now
                if earliest is None or member.deadline < earliest:
                    earliest = member.deadline
        return earliest

    def _deadline_loop(self):
        while True:
            replies = []
            with self._lock:
                if not self._reads:
                    self._timer = None
                    return
                now = monotonic()
                for read in list(self._reads.values()):
                    for member in list(read.members):
                        if (
                            member.state == "waiting"
                            and now >= member.deadline
                        ):
                            self._settle_locked(
                                member,
                                ERROR,
                                _SHARED_READ_TIMEOUT,
                                replies,
                            )
                still_running = bool(self._reads)

            # Replies go out before this thread parks again; one slow or
            # blocked peer cannot delay another member's timeout reply.
            self._spawn_replies(replies)
            if not still_running:
                with self._lock:
                    if not self._reads:
                        self._timer = None
                        return
                    # A read appeared after the scan; keep owning the timer.
                continue

            with self._lock:
                if not self._reads:
                    self._timer = None
                    return
                # Recompute while holding the very lock we will wait on, so a
                # member registered after the scan above cannot be missed.
                now = monotonic()
                earliest = self._earliest_deadline_locked(now)
                if earliest is None:
                    self._lock.wait()
                elif earliest > now:
                    self._lock.wait(earliest - now)
                # Otherwise a member is already due: loop straight back.



def serve(
    sock,
    handler,
    workers=2,
    stream_handler=None,
    stream_slots=8,
    ledger=None,
    rollout=None,
    resource_gate=None,
    shared_reads=None,
):
    """Own sock; answer ordinary calls, bounded streams, and deliveries.

    Ordinary handlers and stream creation/advancement/cleanup share the
    *workers* execution permits.  stream_slots bounds admitted streams,
    including streams paused solely because their reader is slow.  When a
    DeliveryLedger is supplied, delivery submissions use that ledger and the
    same ordinary handler; the ledger survives this connection.

    When *rollout* is given it supplies the ordinary and stream callbacks for
    this connection and may be replaced concurrently via
    :meth:`HandlerRollout.replace`; the positional *handler* then only backs
    ledger instructions, which never participate in rollout.  Passing both a
    rollout and a non-empty *stream_handler* raises ValueError.

    When *resource_gate* is given, SCOPED_REQUEST frames are admitted to that
    gate and run its own callback; they share this connection's worker
    permits but never use the ordinary, stream or ledger callbacks.

    When *shared_reads* is given, SHARED_READ frames join its coalescing
    readers; a joined request needs neither a worker permit nor a global read
    slot, and each request keeps its own server-measured deadline.
    """
    if type(workers) is not int or workers < 1:
        raise ValueError("workers must be positive")
    if (
        type(stream_slots) is not int
        or not 1 <= stream_slots <= MAX_STREAM_SLOTS
    ):
        raise ValueError(
            f"stream_slots must be an integer from 1 to {MAX_STREAM_SLOTS}"
        )
    if ledger is not None and not isinstance(ledger, DeliveryLedger):
        raise ValueError("ledger must be a DeliveryLedger or None")
    if rollout is not None and not isinstance(rollout, HandlerRollout):
        raise ValueError("rollout must be a HandlerRollout or None")
    if rollout is not None and stream_handler is not None:
        raise ValueError(
            "rollout supplies the stream handler; do not pass both"
        )
    if resource_gate is not None and not isinstance(
        resource_gate, ResourceGate
    ):
        raise ValueError("resource_gate must be a ResourceGate or None")
    if shared_reads is not None and not isinstance(
        shared_reads, SharedReads
    ):
        raise ValueError("shared_reads must be a SharedReads or None")

    writing = Lock()
    work_slots = BoundedSemaphore(workers)
    admitted_streams = BoundedSemaphore(stream_slots)
    stopping = Event()
    streams = {}
    streams_lock = Lock()
    runner_threads = set()
    threads_lock = Lock()
    delivery_waiters = set()
    delivery_waiters_lock = Lock()
    gate_attached = False
    shared_attached = False

    def write_frame(kind, request_id, body=b""):
        with writing:
            send(sock, kind, request_id, body)

    def send_error(request_id, message):
        try:
            write_frame(ERROR, request_id, message)
        except OSError:
            _terminate(sock)

    def send_result(request_id, body):
        try:
            write_frame(RESULT, request_id, body)
        except OSError:
            _terminate(sock)

    def handle_scoped_request(request_id, body):
        if resource_gate is None:
            # Frame-by-frame rejection keeps the connection usable for every
            # other frame kind.
            send_error(request_id, b"resource scheduling unavailable")
            return
        # A malformed scoped frame is a protocol violation: the accept loop's
        # outer handler terminates the connection just like any other bad frame.
        shared, exclusive, payload = _parse_scoped_body(body)
        if not resource_gate.admit(
            gate_conn, request_id, shared, exclusive, payload
        ):
            send_error(request_id, b"resource queue full")

    def handle_shared_read(request_id, body):
        # An unconfigured service rejects every syntactically framed read the
        # same way, keeping the connection usable for other frame kinds.
        if shared_reads is None:
            send_error(request_id, b"shared reads unavailable")
            return
        # A malformed shared-read frame is a protocol violation; the payload
        # length and wait range are the frame's own contract.
        payload = body[SHARED_READ_WAIT_SIZE:]
        wait_ms = _WAIT.unpack_from(body)[0]
        if not 1 <= wait_ms <= 60000 or len(payload) > SHARED_READ_MAX_PAYLOAD:
            raise ProtocolError("invalid shared read frame")
        # Registration happens while the accept loop is still processing this
        # frame; per-request deadlines start now, independently of any other
        # member and of the reader's own runtime.
        shared_reads.accept(shared_conn, request_id, payload, wait_ms)

    def handle_request(request_id, body, generation=None):
        # When generation is given, the accept loop already holds one of its
        # pins for this admitted call; it covers both pool queueing and the
        # user call and is released exactly once here.
        try:
            try:
                if generation is not None:
                    response = generation.handler(body)
                else:
                    response = handler(body)
                if not _payload(response):
                    raise ValueError("invalid handler response")
                kind = RESULT
            except Exception:
                kind, response = ERROR, b"handler failed"
            write_frame(kind, request_id, response)
        except OSError:
            _terminate(sock)
        finally:
            if generation is not None:
                rollout.release(generation)
            work_slots.release()

    # ---- delivery ledger integration ----

    def send_terminal(request_id, record):
        if record.failed:
            send_error(request_id, b"handler failed")
        else:
            send_result(request_id, record.value)

    def run_delivery(record, request_id):
        # One admitted instruction runs the ordinary handler exactly once,
        # even if the submitting connection has already gone away.
        try:
            try:
                value = handler(record.payload)
                if not _payload(value):
                    raise ValueError("invalid handler response")
                failed = False
            except Exception:
                value, failed = b"handler failed", True

            waiter = None
            with ledger._cond:
                record.done = True
                record.failed = failed
                record.value = value
                waiter = record.waiter
                record.waiter = None
                if waiter is not None:
                    # A claim registered while the instruction was running is
                    # entitled to this terminal state, even if the record is
                    # confirmed and retired later on another connection.
                    waiter.event.set()
            # The record is already terminal; delivery on a still-live
            # submitting connection is best effort and never gates readiness.
            send_terminal(request_id, record)
        finally:
            work_slots.release()

    def run_delivery_waiter(waiter):
        try:
            waiter.event.wait()
            if waiter.abandoned:
                return
            send_terminal(waiter.request_id, waiter.record)
        finally:
            with delivery_waiters_lock:
                delivery_waiters.discard(waiter)

    def attach_delivery_waiter(request_id, record):
        waiter = _DeliveryWaiter(request_id, record)
        with ledger._cond:
            if record.done:
                outcome = "done"
            elif record.waiter is not None or stopping.is_set():
                outcome = "pending"
            else:
                record.waiter = waiter
                outcome = "wait"

        if outcome == "done":
            send_terminal(request_id, record)
            return
        if outcome == "pending":
            send_error(request_id, b"delivery pending")
            return

        thread = Thread(
            target=run_delivery_waiter,
            args=(waiter,),
            name=f"rpc-delivery-wait-{request_id}",
            daemon=True,
        )
        waiter.thread = thread
        with delivery_waiters_lock:
            delivery_waiters.add(waiter)
        try:
            thread.start()
        except Exception:
            # Unable to park a request; the instruction itself is unaffected.
            waiter.abandoned = True
            with ledger._cond:
                if record.waiter is waiter:
                    record.waiter = None
            with delivery_waiters_lock:
                delivery_waiters.discard(waiter)
            send_error(request_id, b"delivery pending")

    def existing_delivery_locked(seq, payload):
        """Classify a submission for an already-accepted sequence.

        Returns (action, record) where action is "retired", "conflict",
        "done", "pending", or "wait".
        """
        record = ledger._records.get(seq)
        if record is None:
            return "retired", None
        if record.payload != payload:
            return "conflict", None
        if record.done:
            return "done", record
        if record.waiter is not None:
            return "pending", None
        return "wait", record

    def classify_submission_locked(seq, payload):
        """Classify a delivery submission while holding the ledger lock."""
        if seq <= ledger._highest:
            return existing_delivery_locked(seq, payload)
        if len(ledger._records) >= ledger._capacity:
            return "full", None
        return "admit", None

    def reply_submission(request_id, action, record):
        """Reply for every non-fresh submission action; returns True if handled."""
        if action == "admit":
            return False
        if action == "retired":
            send_error(request_id, b"delivery retired")
        elif action == "conflict":
            send_error(request_id, b"delivery conflict")
        elif action == "full":
            send_error(request_id, b"delivery full")
        elif action == "done":
            send_terminal(request_id, record)
        elif action == "pending":
            send_error(request_id, b"delivery pending")
        elif action == "wait":
            attach_delivery_waiter(request_id, record)
        return True

    def handle_delivery_submit(request_id, body):
        if ledger is None:
            send_error(request_id, b"delivery unavailable")
            return
        seq = _SEQ.unpack_from(body)[0]
        payload = body[DELIVERY_SEQ_SIZE:]

        with ledger._cond:
            action, record = classify_submission_locked(seq, payload)
        if reply_submission(request_id, action, record):
            return

        # Fresh instruction: it needs one of this connection's execution
        # slots before it may occupy a ledger record.
        if not work_slots.acquire(blocking=False):
            # Another connection may have admitted this sequence meanwhile.
            # An already-accepted instruction is handled by the ledger and
            # never needs a slot for its claim.
            with ledger._cond:
                action, record = classify_submission_locked(seq, payload)
            if reply_submission(request_id, action, record):
                return
            send_error(request_id, b"server busy")
            return

        with ledger._cond:
            previous_highest = ledger._highest
            action, record = classify_submission_locked(seq, payload)
            if action == "admit":
                if stopping.is_set():
                    action, record = "busy", None
                else:
                    record = _DeliveryRecord(seq, payload)
                    ledger._records[seq] = record
                    ledger._highest = seq

        if action == "busy":
            work_slots.release()
            send_error(request_id, b"server busy")
            return
        if reply_submission(request_id, action, record):
            work_slots.release()
            return

        try:
            pool.submit(run_delivery, record, request_id)
        except RuntimeError:
            work_slots.release()
            with ledger._cond:
                if ledger._records.get(seq) is record and not record.done:
                    del ledger._records[seq]
                    if ledger._highest == seq:
                        ledger._highest = previous_highest
            send_error(request_id, b"server busy")

    def handle_delivery_confirm(request_id, body):
        if ledger is None:
            send_error(request_id, b"delivery unavailable")
            return
        seq = _SEQ.unpack_from(body)[0]

        with ledger._cond:
            if seq > ledger._highest:
                action = "unknown"
            else:
                record = ledger._records.get(seq)
                if record is not None and not record.done:
                    action = "pending"
                else:
                    # Confirmed or never-admitted-but-skipped sequences are
                    # idempotently already settled.
                    ledger._records.pop(seq, None)
                    action = "ok"

        if action == "unknown":
            send_error(request_id, b"delivery unknown")
        elif action == "pending":
            send_error(request_id, b"delivery pending")
        else:
            send_result(request_id, b"")

    def wait_for_worker(stream, *, skip_if_cancelled):
        # Time-bounded acquisition lets a stream which never started user code
        # settle promptly after abandonment.  Once an iterator exists, cleanup
        # must wait for its shared execution permit.
        while not work_slots.acquire(timeout=0.02):
            if stopping.is_set():
                return False
            if skip_if_cancelled and stream.is_cancelled():
                return False
        return True

    def close_iterator(stream):
        # Caller has acquired one work_slot.  The runner is the only thread
        # that touches the iterator, so iterator operations never overlap.
        if stream.close_attempted:
            return
        stream.close_attempted = True
        iterator = stream.iterator
        try:
            close = getattr(iterator, "close", None) if iterator is not None else None
            if callable(close):
                close()
        except Exception:
            # Cleanup has exited, which is what settlement waits for.
            pass

    def run_stream(stream):
        request_id = stream.id
        terminal_kind = STREAM_END
        terminal_body = b""
        send_terminal = True
        permit_held = False
        chunk = None

        try:
            while True:
                # Create the iterator under a shared execution permit.
                if stream.iterator is None:
                    if stopping.is_set() or stream.is_cancelled():
                        break
                    if not wait_for_worker(stream, skip_if_cancelled=True):
                        break
                    permit_held = True
                    try:
                        if stopping.is_set() or stream.is_cancelled():
                            break
                        if rollout is not None:
                            # A stream still queued for its first execution
                            # picks up the generation now in force.  Pinning
                            # here binds every later step and the cleanup to
                            # that single generation until this runner exits.
                            generation = rollout.current()
                            rollout.pin(generation)
                            stream.generation = generation
                            try:
                                iterator = generation.stream_handler(
                                    stream.payload
                                )
                            except BaseException:
                                rollout.release(generation)
                                stream.generation = None
                                raise
                        else:
                            iterator = stream_handler(stream.payload)
                        if not hasattr(iterator, "__next__"):
                            raise TypeError("stream_handler must return an iterator")
                        stream.iterator = iterator
                    except Exception:
                        terminal_kind = ERROR
                        terminal_body = b"stream handler failed"
                        break
                    finally:
                        if permit_held:
                            work_slots.release()
                            permit_held = False

                # Wait for a window share.  This wait holds no execution
                # permit, so a reader-paused stream occupies only a slot.
                with stream.cond:
                    while (
                        stream.credits <= 0
                        and not stream.cancel_requested
                        and not stopping.is_set()
                    ):
                        stream.cond.wait()

                    if stopping.is_set():
                        send_terminal = False
                        break
                    if stream.cancel_requested:
                        break
                    # Reserve before waiting for/running the next step.
                    stream.credits -= 1

                work_slots.acquire()
                permit_held = True
                try:
                    if stopping.is_set() or stream.cancel_requested:
                        if stopping.is_set():
                            send_terminal = False
                        close_iterator(stream)
                        break

                    try:
                        chunk = next(stream.iterator)
                    except StopIteration:
                        close_iterator(stream)
                        break
                    except Exception:
                        terminal_kind = ERROR
                        terminal_body = b"stream failed"
                        close_iterator(stream)
                        break

                    if not isinstance(chunk, bytes) or len(chunk) > MAX_PAYLOAD:
                        terminal_kind = ERROR
                        terminal_body = b"invalid stream chunk"
                        close_iterator(stream)
                        break

                    if stopping.is_set() or stream.cancel_requested:
                        if stopping.is_set():
                            send_terminal = False
                        # A step which started before cancellation may have
                        # returned a chunk, but it must not be sent now.
                        close_iterator(stream)
                        break
                finally:
                    if permit_held:
                        work_slots.release()
                        permit_held = False

                write_frame(STREAM_DATA, request_id, chunk)
                chunk = None

        except OSError as exc:
            send_terminal = False
            _terminate(sock)
        finally:
            iterator = stream.iterator
            if iterator is not None and not stream.close_attempted:
                if not permit_held:
                    work_slots.acquire()
                    permit_held = True
                try:
                    close_iterator(stream)
                finally:
                    if permit_held:
                        work_slots.release()
                        permit_held = False

            generation = stream.generation
            if generation is not None:
                # Cleanup has exited and no further factory, step or cleanup
                # of this generation can be entered from this stream.
                rollout.release(generation)
                stream.generation = None

            with streams_lock:
                streams.pop(request_id, None)
            admitted_streams.release()

            if send_terminal:
                try:
                    write_frame(terminal_kind, request_id, terminal_body)
                except OSError:
                    _terminate(sock)

    def stream_main(stream):
        try:
            run_stream(stream)
        finally:
            with threads_lock:
                runner_threads.discard(current_thread())

    with sock:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            gate_conn = _GateConnection(
                object(), work_slots, pool, write_frame, lambda: _terminate(sock)
            )
            shared_conn = _SharedConnection(
                object(), work_slots, pool, write_frame, lambda: _terminate(sock)
            )
            if resource_gate is not None:
                resource_gate.attach(gate_conn)
                gate_attached = True
            if shared_reads is not None:
                shared_attached = True
            last_id = 0
            try:
                while True:
                    frame = receive(sock)
                    if frame is None:
                        break
                    kind, request_id, body = frame

                    if kind in (
                        REQUEST,
                        STREAM_OPEN,
                        DELIVERY_SUBMIT,
                        DELIVERY_CONFIRM,
                        SCOPED_REQUEST,
                        SHARED_READ,
                    ):
                        if request_id <= last_id:
                            raise ProtocolError("expected increasing request ids")
                        last_id = request_id

                    if kind == REQUEST:
                        if work_slots.acquire(blocking=False):
                            # Snapshot and pin the generation while admitting
                            # the call: overload rejection is not admission,
                            # and an admitted ordinary call (even one still
                            # queued for a pool thread) finishes on its
                            # accepted generation even if replaced meanwhile.
                            generation = (
                                rollout.current() if rollout is not None else None
                            )
                            if generation is not None:
                                rollout.pin(generation)
                            try:
                                pool.submit(
                                    handle_request,
                                    request_id,
                                    body,
                                    generation,
                                )
                            except RuntimeError:
                                if generation is not None:
                                    rollout.release(generation)
                                work_slots.release()
                                raise
                        else:
                            write_frame(ERROR, request_id, b"server busy")

                    elif kind == DELIVERY_SUBMIT:
                        handle_delivery_submit(request_id, body)

                    elif kind == DELIVERY_CONFIRM:
                        handle_delivery_confirm(request_id, body)

                    elif kind == SCOPED_REQUEST:
                        handle_scoped_request(request_id, body)

                    elif kind == SHARED_READ:
                        handle_shared_read(request_id, body)

                    elif kind == STREAM_OPEN:
                        if len(body) < 1:
                            raise ProtocolError("missing stream window")
                        window = body[0]
                        payload = body[1:]
                        if not 1 <= window <= MAX_WINDOW or not _payload(payload):
                            raise ProtocolError("invalid stream open frame")

                        if stream_handler is None and rollout is None:
                            write_frame(
                                ERROR, request_id, b"streaming unavailable"
                            )
                            continue

                        if not admitted_streams.acquire(blocking=False):
                            write_frame(ERROR, request_id, b"server streams busy")
                            continue

                        stream = _ServerStream(request_id, payload, window)
                        with streams_lock:
                            streams[request_id] = stream
                        thread = Thread(
                            target=stream_main,
                            args=(stream,),
                            name=f"rpc-stream-{request_id}",
                            daemon=True,
                        )
                        with threads_lock:
                            runner_threads.add(thread)
                        try:
                            thread.start()
                        except Exception:
                            with streams_lock:
                                streams.pop(request_id, None)
                            with threads_lock:
                                runner_threads.discard(thread)
                            admitted_streams.release()
                            raise

                    elif kind == STREAM_CANCEL:
                        with streams_lock:
                            stream = streams.get(request_id)
                        if stream is not None:
                            stream.cancel()

                    elif kind == STREAM_CREDIT:
                        with streams_lock:
                            stream = streams.get(request_id)
                        if stream is not None:
                            stream.grant_credit()

                    else:
                        # RESULT/ERROR/DATA/END are server-to-client frames.
                        raise ProtocolError("unexpected frame kind from client")

            except OSError:
                pass
            finally:
                _terminate(sock)
                stopping.set()
                # Release this connection's gate wait slots and priority at
                # once.  Already-started scoped callbacks are pool work: the
                # pool context below keeps running until each exits, and their
                # keys stay held until then.
                if gate_attached:
                    gate_conn.alive = False
                    resource_gate.detach(gate_conn)
                if shared_attached:
                    shared_conn.alive = False
                    # Withdraw this connection's waiting members.  The physical
                    # reader keeps running and other connections' members keep
                    # waiting for it; this connection's caller already follows
                    # the ordinary PeerClosed contract.
                    shared_reads.detach(shared_conn)
                with streams_lock:
                    active = list(streams.values())
                for stream in active:
                    stream.cancel()

                # A parked delivery claim must never leave a permanent wait
                # slot behind once its connection is leaving.  It gives up
                # its claim immediately; the owning instruction keeps running.
                with delivery_waiters_lock:
                    waiters = list(delivery_waiters)
                for waiter in waiters:
                    waiter.abandoned = True
                    with ledger._cond:
                        if waiter.record.waiter is waiter:
                            waiter.record.waiter = None
                    waiter.event.set()

        # Pool worker calls have finished here.  Wait for stream user code and
        # iterator cleanup to exit so serve does not outlive those callbacks.
        with threads_lock:
            threads = list(runner_threads)
        for thread in threads:
            thread.join()

        for waiter in waiters:
            thread = waiter.thread
            if thread is not None and thread is not current_thread():
                try:
                    thread.join()
                except RuntimeError:
                    # The waiter was registered but its thread had not started
                    # yet; it observes *abandoned* as soon as it does.
                    pass

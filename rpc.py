"""Bounded, multiplexed request and stream RPC over one local stream."""
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from itertools import count
import socket
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
    ERROR,
    MAX_PAYLOAD,
    MAX_STREAM_SLOTS,
    MAX_WINDOW,
    REQUEST,
    RESULT,
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

        send_error = None
        with self._sending:
            request_id = self._allocate()
            future = Future()
            future.set_running_or_notify_cancel()
            self._insert(request_id, future)
            try:
                send(self._sock, REQUEST, request_id, payload)
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


def serve(
    sock,
    handler,
    workers=2,
    stream_handler=None,
    stream_slots=8,
):
    """Own sock; answer ordinary calls and bounded streams.

    Ordinary handlers and stream creation/advancement/cleanup share the
    *workers* execution permits.  stream_slots bounds admitted streams,
    including streams paused solely because their reader is slow.
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

    writing = Lock()
    work_slots = BoundedSemaphore(workers)
    admitted_streams = BoundedSemaphore(stream_slots)
    stopping = Event()
    streams = {}
    streams_lock = Lock()
    runner_threads = set()
    threads_lock = Lock()

    def write_frame(kind, request_id, body=b""):
        with writing:
            send(sock, kind, request_id, body)

    def handle_request(request_id, body):
        try:
            try:
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
            work_slots.release()

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
            last_id = 0
            try:
                while True:
                    frame = receive(sock)
                    if frame is None:
                        break
                    kind, request_id, body = frame

                    if kind in (REQUEST, STREAM_OPEN):
                        if request_id <= last_id:
                            raise ProtocolError("expected increasing request ids")
                        last_id = request_id

                    if kind == REQUEST:
                        if work_slots.acquire(blocking=False):
                            try:
                                pool.submit(handle_request, request_id, body)
                            except RuntimeError:
                                work_slots.release()
                                raise
                        else:
                            write_frame(ERROR, request_id, b"server busy")

                    elif kind == STREAM_OPEN:
                        if len(body) < 1:
                            raise ProtocolError("missing stream window")
                        window = body[0]
                        payload = body[1:]
                        if not 1 <= window <= MAX_WINDOW or not _payload(payload):
                            raise ProtocolError("invalid stream open frame")

                        if stream_handler is None:
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
                with streams_lock:
                    active = list(streams.values())
                for stream in active:
                    stream.cancel()

        # Pool worker calls have finished here.  Wait for stream user code and
        # iterator cleanup to exit so serve does not outlive those callbacks.
        with threads_lock:
            threads = list(runner_threads)
        for thread in threads:
            thread.join()

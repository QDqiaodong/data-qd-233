"""Bounded, multiplexed request and reply over one local stream."""
from concurrent.futures import Future, ThreadPoolExecutor
from itertools import count
import socket
from threading import BoundedSemaphore, Lock, Thread, current_thread

from wire import ERROR, MAX_PAYLOAD, REQUEST, RESULT, ProtocolError, receive, send


class Busy(RuntimeError):
    pass


class PeerClosed(ConnectionError):
    pass


class RemoteError(RuntimeError):
    pass


def _terminate(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


def _payload(payload):
    return isinstance(payload, bytes) and len(payload) <= MAX_PAYLOAD


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

    def submit(self, payload):
        if not _payload(payload):
            raise ValueError("expected up to 65536 bytes")
        with self._sending:
            with self._lock:
                if self._closed:
                    raise PeerClosed("client is closed")
                if len(self._pending) >= self._limit:
                    raise Busy("client in-flight limit reached")
                request_id = next(self._numbers)
                if request_id >= 1 << 64:
                    raise OverflowError("request id space exhausted")
                future = Future()
                future.set_running_or_notify_cancel()
                self._pending[request_id] = future
            try:
                send(self._sock, REQUEST, request_id, payload)
            except OSError:
                # The reader owns terminal completion and never needs the send lock.
                _terminate(self._sock)
        return future

    def _shutdown(self, error):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending, self._pending = self._pending, {}
        _terminate(self._sock)
        for result in pending.values():
            result.set_exception(PeerClosed(str(error)))

    def _dispatch(self):
        try:
            while True:
                frame = receive(self._sock)
                if frame is None:
                    raise PeerClosed("peer disconnected")
                kind, request_id, payload = frame
                if kind not in (RESULT, ERROR):
                    raise ProtocolError("expected a reply")
                with self._lock:
                    future = self._pending.pop(request_id, None)
                    if future is None:
                        raise ProtocolError("unexpected reply id")
                if kind == RESULT:
                    future.set_result(payload)
                else:
                    future.set_exception(RemoteError(payload.decode("utf-8", "replace")))
        except OSError as exc:
            self._shutdown(exc)

    def close(self):
        self._shutdown(PeerClosed("client closed"))
        if current_thread() is not self._reader:
            self._reader.join()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def serve(sock, handler, *, workers=2):
    """Own sock; receive trusted requests and answer using a bounded handler pool."""
    if type(workers) is not int or workers < 1:
        raise ValueError("workers must be positive")
    writing = Lock()
    capacity = BoundedSemaphore(workers)

    def reply(kind, request_id, body):
        with writing:
            send(sock, kind, request_id, body)

    def handle(request_id, body):
        try:
            try:
                response = handler(body)
                if not _payload(response):
                    raise ValueError("invalid handler response")
                kind = RESULT
            except Exception:
                kind, response = ERROR, b"handler failed"
            reply(kind, request_id, response)
        except OSError:
            _terminate(sock)
        finally:
            capacity.release()

    with sock, ThreadPoolExecutor(max_workers=workers) as pool:
        last_id = 0
        try:
            while True:
                frame = receive(sock)
                if frame is None:
                    break
                kind, request_id, body = frame
                if kind != REQUEST or request_id <= last_id:
                    raise ProtocolError("expected increasing request ids")
                last_id = request_id
                if capacity.acquire(blocking=False):
                    pool.submit(handle, request_id, body)
                else:
                    reply(ERROR, request_id, b"server busy")
        except OSError:
            pass
        finally:
            _terminate(sock)

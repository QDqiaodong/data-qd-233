"""Fixed header framing for already-connected, trusted stream peers."""
from struct import Struct

REQUEST, RESULT, ERROR = 1, 2, 3
# Stream extension.  STREAM_OPEN's body is one window byte followed by the
# ordinary request payload.  STREAM_DATA's body is one chunk.  The remaining
# stream frames have empty bodies.
STREAM_OPEN, STREAM_DATA, STREAM_END = 4, 5, 6
STREAM_CANCEL, STREAM_CREDIT = 7, 8
# Delivery extension.  DELIVERY_SUBMIT's body is an 8-byte sequence followed
# by a payload, so the payload is at most 65528 bytes.  DELIVERY_CONFIRM's
# body is exactly one 8-byte sequence.  Replies reuse RESULT/ERROR.
DELIVERY_SUBMIT, DELIVERY_CONFIRM = 9, 10
# Resource gate extension.  SCOPED_REQUEST's body is one shared-count byte and
# one exclusive-count byte, then one length byte plus up to 32 ASCII bytes per
# declared key (shared first, then exclusive), then the at most 65000-byte
# payload.  Replies reuse RESULT/ERROR.
SCOPED_REQUEST = 11
# Shared-read extension.  SHARED_READ's body is a 2-byte big-endian wait in
# milliseconds followed by an at most 65534-byte payload (empty is allowed).
# Replies reuse RESULT/ERROR.
SHARED_READ = 12
DELIVERY_SEQ_SIZE = 8
MAX_PAYLOAD = 65536
DELIVERY_MAX_PAYLOAD = MAX_PAYLOAD - DELIVERY_SEQ_SIZE
SHARED_READ_WAIT_SIZE = 2
SHARED_READ_MAX_PAYLOAD = MAX_PAYLOAD - SHARED_READ_WAIT_SIZE
SCOPED_MAX_PAYLOAD = 65000
SCOPED_MAX_KEYS = 8
SCOPED_MAX_KEY_SIZE = 32
MAX_CHUNK = MAX_PAYLOAD
MAX_WINDOW = 16
MAX_STREAM_SLOTS = 32
_HEADER = Struct(">BQI")

_FRAME_KINDS = frozenset(
    {
        REQUEST,
        RESULT,
        ERROR,
        STREAM_OPEN,
        STREAM_DATA,
        STREAM_END,
        STREAM_CANCEL,
        STREAM_CREDIT,
        DELIVERY_SUBMIT,
        DELIVERY_CONFIRM,
        SCOPED_REQUEST,
        SHARED_READ,
    }
)
_EMPTY_BODY_KINDS = frozenset({STREAM_END, STREAM_CANCEL, STREAM_CREDIT})


class ProtocolError(ConnectionError):
    pass


def read_exact(sock, count, *, eof_ok=False):
    chunks = bytearray()
    while len(chunks) < count:
        chunk = sock.recv(count - len(chunks))
        if not chunk:
            if eof_ok and not chunks:
                return None
            raise ProtocolError("incomplete frame")
        chunks.extend(chunk)
    return bytes(chunks)


def receive(sock):
    header = read_exact(sock, _HEADER.size, eof_ok=True)
    if header is None:
        return None
    kind, request_id, length = _HEADER.unpack(header)
    if kind not in _FRAME_KINDS or request_id == 0:
        raise ProtocolError("invalid frame header")
    if kind == STREAM_OPEN:
        # One byte of window plus the ordinary <=65536-byte payload.
        if length < 1 or length > MAX_PAYLOAD + 1:
            raise ProtocolError("invalid stream open frame")
    elif kind in _EMPTY_BODY_KINDS:
        if length != 0:
            raise ProtocolError("invalid stream control frame")
    elif kind == DELIVERY_SUBMIT:
        # Sequence number plus a payload; the sum stays within MAX_PAYLOAD.
        if length < DELIVERY_SEQ_SIZE or length > MAX_PAYLOAD:
            raise ProtocolError("invalid delivery submit frame")
    elif kind == DELIVERY_CONFIRM:
        if length != DELIVERY_SEQ_SIZE:
            raise ProtocolError("invalid delivery confirm frame")
    elif kind == SCOPED_REQUEST:
        # Two count bytes plus at least one key declaration; the detailed key
        # and payload validation happens in rpc.py once the body is complete.
        if length < 2 + 2 or length > MAX_PAYLOAD:
            raise ProtocolError("invalid scoped request frame")
    elif kind == SHARED_READ:
        # A 2-byte wait plus an optional body; the wait value and payload are
        # validated in rpc.py once the body is complete.
        if length < SHARED_READ_WAIT_SIZE or length > MAX_PAYLOAD:
            raise ProtocolError("invalid shared read frame")
    elif length > MAX_PAYLOAD:
        raise ProtocolError("invalid frame header")
    return kind, request_id, read_exact(sock, length)


def send(sock, kind, request_id, payload):
    sock.sendall(_HEADER.pack(kind, request_id, len(payload)) + payload)

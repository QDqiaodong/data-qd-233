"""Fixed header framing for already-connected, trusted stream peers."""
from struct import Struct

REQUEST, RESULT, ERROR = 1, 2, 3
# Stream extension.  STREAM_OPEN's body is one window byte followed by the
# ordinary request payload.  STREAM_DATA's body is one chunk.  The remaining
# stream frames have empty bodies.
STREAM_OPEN, STREAM_DATA, STREAM_END = 4, 5, 6
STREAM_CANCEL, STREAM_CREDIT = 7, 8
MAX_PAYLOAD = 65536
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
    elif length > MAX_PAYLOAD:
        raise ProtocolError("invalid frame header")
    return kind, request_id, read_exact(sock, length)


def send(sock, kind, request_id, payload):
    sock.sendall(_HEADER.pack(kind, request_id, len(payload)) + payload)

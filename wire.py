"""Fixed header framing for already-connected, trusted stream peers."""
from struct import Struct

REQUEST, RESULT, ERROR = 1, 2, 3
MAX_PAYLOAD = 65536
_HEADER = Struct(">BQI")


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
    if kind not in (REQUEST, RESULT, ERROR) or request_id == 0 or length > MAX_PAYLOAD:
        raise ProtocolError("invalid frame header")
    return kind, request_id, read_exact(sock, length)


def send(sock, kind, request_id, payload):
    sock.sendall(_HEADER.pack(kind, request_id, len(payload)) + payload)

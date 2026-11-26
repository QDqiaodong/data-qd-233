from socket import socketpair
from threading import Event, Thread

from rpc import Client, RemoteError, serve


def run():
    left, right = socketpair()
    free = Event()

    def handler(data):
        if data == b"slow" and not free.wait(4):
            raise RuntimeError("slow call did not release")
        if data == b"fail":
            raise ValueError("sample failure")
        return data.upper()

    server = Thread(target=serve, args=(right, handler), kwargs={"workers": 3})
    server.start()
    try:
        with Client(left) as client:
            slow = client.submit(b"slow")
            try:
                fast = client.submit(b"fast").result(3)
                assert not slow.done()
            finally:
                free.set()
            result = slow.result(3)
            try:
                client.submit(b"fail").result(3)
            except RemoteError as err:
                return fast, result, str(err)
            raise AssertionError("expected handler failure")
    finally:
        free.set()
        server.join()


if __name__ == "__main__":
    before, after, failure = run()
    assert (before, after, failure) == (b"FAST", b"SLOW", "handler failed")
    print("reply order:", before.decode(), after.decode())
    print("isolated error:", failure)

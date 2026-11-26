from socket import SHUT_WR, socketpair
from threading import Event, Thread
from unittest import TestCase

from demo import run
from rpc import Busy, Client, PeerClosed, RemoteError, serve
from wire import MAX_PAYLOAD, RESULT, ProtocolError, receive, send


class RPCTest(TestCase):
    def test_out_of_order_and_isolated_error(self):
        self.assertEqual(run(), (b"FAST", b"SLOW", "handler failed"))

    def test_disconnect_releases_every_pending_call(self):
        left, right = socketpair()
        with right, Client(left) as client:
            calls = [client.submit(data) for data in (b"a", b"b")]
            self.assertEqual([receive(right)[2] for _ in calls], [b"a", b"b"])
            right.close()
            for result in calls:
                with self.assertRaises(PeerClosed):
                    result.result(2)
            with self.assertRaises(PeerClosed):
                client.submit(b"c")

    def test_client_limit_and_malformed_reply(self):
        left, right = socketpair()
        with right, Client(left, max_pending=1) as client:
            with self.assertRaises(ValueError):
                client.submit(b"x" * (MAX_PAYLOAD + 1))
            first = client.submit(b"pending")
            with self.assertRaises(Busy):
                client.submit(b"busy")
            _, ident, _ = receive(right)
            send(right, RESULT, ident + 1, b"wrong")
            with self.assertRaisesRegex(PeerClosed, "unexpected reply id"):
                first.result(2)

    def test_server_backpressure_does_not_cancel_running_call(self):
        left, right = socketpair()
        started, release = Event(), Event()

        def handler(body):
            started.set()
            if not release.wait(4):
                raise RuntimeError("test handler timed out")
            return body

        server = Thread(target=serve, args=(right, handler), kwargs={"workers": 1})
        server.start()
        try:
            with Client(left) as client:
                running = client.submit(b"running")
                self.assertTrue(started.wait(2))
                with self.assertRaisesRegex(RemoteError, "server busy"):
                    client.submit(b"excess").result(2)
                release.set()
                self.assertEqual(running.result(2), b"running")
        finally:
            release.set()
            server.join()

    def test_empty_and_maximum_payload_over_real_stream(self):
        left, right = socketpair()
        server = Thread(target=serve, args=(right, lambda payload: payload[::-1]))
        server.start()
        try:
            with Client(left) as client:
                self.assertEqual(client.submit(b"").result(2), b"")
                payload = bytes(range(256)) * (MAX_PAYLOAD // 256)
                self.assertEqual(client.submit(payload).result(2), payload[::-1])
        finally:
            server.join()

    def test_partial_response_is_not_mistaken_for_clean_eof(self):
        left, right = socketpair()
        with left, right:
            left.sendall(b"\x02\x00")
            left.shutdown(SHUT_WR)
            with self.assertRaisesRegex(ProtocolError, "incomplete frame"):
                receive(right)

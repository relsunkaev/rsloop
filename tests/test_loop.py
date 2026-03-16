from __future__ import annotations

import asyncio
import socket
import threading
import unittest

import kioto


class KiotoLoopTests(unittest.TestCase):
    def run_async(self, coro) -> None:
        return kioto.run(coro)

    def test_callback_scheduling(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            seen: list[str] = []
            done = loop.create_future()

            loop.call_soon(seen.append, "soon")
            loop.call_later(0.01, done.set_result, "later")

            self.assertEqual(await done, "later")
            self.assertEqual(seen, ["soon"])

        self.run_async(main())

    def test_call_soon_threadsafe(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            done = loop.create_future()

            thread = threading.Thread(
                target=lambda: loop.call_soon_threadsafe(done.set_result, "ok")
            )
            thread.start()
            self.assertEqual(await done, "ok")
            thread.join()

        self.run_async(main())

    def test_cancelled_callbacks_do_not_run(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            fired: list[str] = []
            done = loop.create_future()

            soon = loop.call_soon(fired.append, "soon")
            later = loop.call_later(0.01, fired.append, "later")
            soon.cancel()
            later.cancel()

            loop.call_later(0.02, done.set_result, None)
            await done

            self.assertTrue(soon.cancelled())
            self.assertTrue(later.cancelled())
            self.assertEqual(fired, [])

        self.run_async(main())

    def test_add_reader(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            reader_sock, writer_sock = socket.socketpair()
            reader_sock.setblocking(False)
            writer_sock.setblocking(False)

            try:
                done = loop.create_future()

                def on_readable() -> None:
                    if not done.done():
                        done.set_result(reader_sock.recv(5))
                    loop.remove_reader(reader_sock.fileno())

                loop.add_reader(reader_sock.fileno(), on_readable)
                writer_sock.send(b"hello")

                self.assertEqual(await done, b"hello")
            finally:
                reader_sock.close()
                writer_sock.close()

        self.run_async(main())

    def test_socket_helpers(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            reader_sock, writer_sock = socket.socketpair()
            reader_sock.setblocking(False)
            writer_sock.setblocking(False)

            try:
                await loop.sock_sendall(writer_sock, b"ping")
                self.assertEqual(await loop.sock_recv(reader_sock, 4), b"ping")
            finally:
                reader_sock.close()
                writer_sock.close()

        self.run_async(main())

    def test_tcp_socket_helpers_repeated_roundtrip(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.setblocking(False)
            host, port = listener.getsockname()[:2]

            server_done = loop.create_future()

            async def server() -> None:
                conn, _ = await loop.sock_accept(listener)
                conn.setblocking(False)
                try:
                    for _ in range(512):
                        data = await asyncio.wait_for(loop.sock_recv(conn, 1), 1)
                        self.assertEqual(data, b"x")
                        await asyncio.wait_for(loop.sock_sendall(conn, data), 1)
                    server_done.set_result(None)
                finally:
                    conn.close()

            server_task = asyncio.create_task(server())
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setblocking(False)

            try:
                await asyncio.wait_for(loop.sock_connect(client, (host, port)), 1)
                for _ in range(512):
                    await asyncio.wait_for(loop.sock_sendall(client, b"x"), 1)
                    self.assertEqual(
                        await asyncio.wait_for(loop.sock_recv(client, 1), 1), b"x"
                    )
                await asyncio.wait_for(server_done, 1)
            finally:
                client.close()
                listener.close()
                await server_task

        self.run_async(main())

    def test_tcp_socket_helpers_runner_reuse(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.setblocking(False)
            host, port = listener.getsockname()[:2]

            server_done = loop.create_future()

            async def server() -> None:
                conn, _ = await loop.sock_accept(listener)
                conn.setblocking(False)
                try:
                    for _ in range(256):
                        data = await loop.sock_recv(conn, 1)
                        await loop.sock_sendall(conn, data)
                    server_done.set_result(None)
                finally:
                    conn.close()

            server_task = asyncio.create_task(server())
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setblocking(False)

            try:
                await loop.sock_connect(client, (host, port))
                for _ in range(256):
                    await loop.sock_sendall(client, b"x")
                    self.assertEqual(await loop.sock_recv(client, 1), b"x")
                await server_done
            finally:
                client.close()
                listener.close()
                await server_task

        for _ in range(4):
            self.run_async(main())

    def test_tcp_server_roundtrip(self) -> None:
        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                payload = await reader.readexactly(4)
                writer.write(payload[::-1])
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async def main() -> None:
            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            addr = server.sockets[0].getsockname()

            try:
                reader, writer = await asyncio.open_connection(*addr[:2])
                writer.write(b"ping")
                await writer.drain()
                self.assertEqual(await reader.readexactly(4), b"gnip")
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        self.run_async(main())

    def test_tokio_bridge(self) -> None:
        async def python_work() -> str:
            await asyncio.sleep(0.01)
            return "bridge-ok"

        async def main() -> None:
            self.assertEqual(await kioto.run_in_tokio(python_work()), "bridge-ok")
            await kioto.sleep(0.01)

        self.run_async(main())


if __name__ == "__main__":
    unittest.main()

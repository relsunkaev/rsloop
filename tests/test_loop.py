from __future__ import annotations

import asyncio
import os
import signal
import socket
import ssl
import subprocess
import sys
import threading
import unittest
import tempfile
from unittest import mock
from pathlib import Path

import rsloop
import rsloop.loop as rsloop_loop


def make_tls_contexts() -> tuple[ssl.SSLContext, ssl.SSLContext]:
    tempdir = Path(tempfile.mkdtemp(prefix="rsloop-test-tls-"))
    certfile = tempdir / "cert.pem"
    keyfile = tempdir / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(keyfile),
            "-out",
            str(certfile),
            "-subj",
            "/CN=localhost",
            "-days",
            "1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "keyUsage=critical,digitalSignature,keyEncipherment",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certfile=str(certfile), keyfile=str(keyfile))
    client_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    client_context.check_hostname = False
    client_context.verify_mode = ssl.CERT_NONE
    return server_context, client_context


class RsloopLoopTests(unittest.TestCase):
    def run_async(self, coro) -> None:
        return rsloop.run(coro)

    def test_new_event_loop_returns_rsloop_loop(self) -> None:
        loop = rsloop.new_event_loop()
        try:
            self.assertIsInstance(loop, rsloop.RsloopEventLoop)
        finally:
            loop.close()

    def test_install_sets_rsloop_policy(self) -> None:
        previous_policy = asyncio.get_event_loop_policy()
        try:
            policy = rsloop.install()
            self.assertIsInstance(policy, rsloop.RsloopEventLoopPolicy)
            loop = policy.new_event_loop()
            try:
                self.assertIsInstance(loop, rsloop.RsloopEventLoop)
            finally:
                loop.close()
        finally:
            asyncio.set_event_loop_policy(previous_policy)

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

    def test_socket_recv_into(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            reader_sock, writer_sock = socket.socketpair()
            reader_sock.setblocking(False)
            writer_sock.setblocking(False)

            try:
                payload = bytearray(b"-----")
                await loop.sock_sendall(writer_sock, b"hello")
                self.assertEqual(await loop.sock_recv_into(reader_sock, payload), 5)
                self.assertEqual(payload, bytearray(b"hello"))
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

    def test_native_protocol_transport_roundtrip(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            payload = b"proto-native"
            done = loop.create_future()

            class EchoServer(asyncio.Protocol):
                transport: asyncio.Transport | None = None

                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.transport = transport  # type: ignore[assignment]

                def data_received(self, data: bytes) -> None:
                    assert self.transport is not None
                    self.transport.write(data)

            class Client(asyncio.Protocol):
                transport: asyncio.Transport | None = None

                def __init__(self) -> None:
                    self.received = bytearray()

                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.transport = transport  # type: ignore[assignment]
                    self.transport.write(payload)

                def data_received(self, data: bytes) -> None:
                    self.received.extend(data)
                    if bytes(self.received) == payload and not done.done():
                        done.set_result(None)
                    assert self.transport is not None
                    self.transport.close()

                def connection_lost(self, exc: BaseException | None) -> None:
                    if exc is not None and not done.done():
                        done.set_exception(exc)

            server = await loop.create_server(EchoServer, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            try:
                transport, _protocol = await loop.create_connection(Client, host, port)
                try:
                    await asyncio.wait_for(done, 1)
                finally:
                    transport.close()
            finally:
                server.close()
                await server.wait_closed()

        self.run_async(main())

    def test_native_buffered_protocol_transport(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            payload = b"buffered-native"
            done = loop.create_future()

            class Server(asyncio.Protocol):
                transport: asyncio.Transport | None = None

                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.transport = transport  # type: ignore[assignment]
                    self.transport.write(payload)
                    self.transport.write_eof()

            class Client(asyncio.BufferedProtocol):
                transport: asyncio.Transport | None = None

                def __init__(self) -> None:
                    self.received = bytearray()
                    self.buffer = bytearray(128)

                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.transport = transport  # type: ignore[assignment]

                def get_buffer(self, sizehint: int) -> memoryview:
                    return memoryview(self.buffer)

                def buffer_updated(self, nbytes: int) -> None:
                    self.received.extend(self.buffer[:nbytes])

                def eof_received(self) -> bool:
                    if bytes(self.received) == payload and not done.done():
                        done.set_result(None)
                    return False

                def connection_lost(self, exc: BaseException | None) -> None:
                    if exc is not None and not done.done():
                        done.set_exception(exc)

            server = await loop.create_server(Server, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            try:
                transport, _protocol = await loop.create_connection(Client, host, port)
                try:
                    await asyncio.wait_for(done, 1)
                finally:
                    transport.close()
            finally:
                server.close()
                await server.wait_closed()

        self.run_async(main())

    def test_native_readuntil_limit_overrun_found(self) -> None:
        async def main() -> None:
            release = asyncio.get_running_loop().create_future()

            async def handle(
                _reader: asyncio.StreamReader, writer: asyncio.StreamWriter
            ) -> None:
                try:
                    writer.write((b"x" * 32) + b"\n")
                    await writer.drain()
                    await release
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]

            try:
                reader, writer = await asyncio.open_connection(host, port, limit=16)
                try:
                    with self.assertRaises(asyncio.LimitOverrunError) as ctx:
                        await reader.readuntil(b"\n")
                    self.assertEqual(ctx.exception.consumed, 32)
                    self.assertEqual(bytes(reader._buffer), (b"x" * 32) + b"\n")
                    self.assertEqual(
                        await reader.readexactly(33),
                        (b"x" * 32) + b"\n",
                    )
                finally:
                    if not release.done():
                        release.set_result(None)
                    writer.close()
                    await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        self.run_async(main())

    def test_native_readuntil_limit_overrun_unfound(self) -> None:
        async def main() -> None:
            release = asyncio.get_running_loop().create_future()

            async def handle(
                _reader: asyncio.StreamReader, writer: asyncio.StreamWriter
            ) -> None:
                try:
                    writer.write(b"x" * 40)
                    await writer.drain()
                    await release
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]

            try:
                reader, writer = await asyncio.open_connection(host, port, limit=16)
                try:
                    while len(reader._buffer) < 40:
                        await asyncio.sleep(0)

                    with self.assertRaises(asyncio.LimitOverrunError) as ctx:
                        await reader.readuntil(b"\r\n\r\n")
                    self.assertEqual(ctx.exception.consumed, 37)
                    self.assertEqual(bytes(reader._buffer), b"x" * 40)
                    self.assertEqual(await reader.readexactly(40), b"x" * 40)
                finally:
                    if not release.done():
                        release.set_result(None)
                    writer.close()
                    await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        self.run_async(main())

    def test_stream_sendfile_static(self) -> None:
        payload = (b"0123456789abcdef" * 4096) + b"tail"
        offset = 1024
        count = 32 * 1024
        expected = payload[offset : offset + count]

        async def roundtrip(use_loop_sendfile: bool) -> None:
            with tempfile.TemporaryDirectory(prefix="rsloop-sendfile-") as tmpdir:
                path = Path(tmpdir) / "payload.bin"
                path.write_bytes(payload)
                loop = asyncio.get_running_loop()

                async def handle(
                    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
                ) -> None:
                    try:
                        with path.open("rb") as file:
                            if use_loop_sendfile:
                                sent = await loop.sendfile(
                                    writer.transport,
                                    file,
                                    offset=offset,
                                    count=count,
                                )
                            else:
                                sent = await writer.sendfile_static(
                                    file,
                                    offset=offset,
                                    count=count,
                                )
                        self.assertEqual(sent, len(expected))
                    finally:
                        writer.close()
                        await writer.wait_closed()

                server = await asyncio.start_server(handle, "127.0.0.1", 0)
                host, port = server.sockets[0].getsockname()[:2]

                try:
                    reader, writer = await asyncio.open_connection(host, port)
                    try:
                        self.assertEqual(await reader.readexactly(len(expected)), expected)
                    finally:
                        writer.close()
                        await writer.wait_closed()
                finally:
                    server.close()
                    await server.wait_closed()

        async def main() -> None:
            await roundtrip(use_loop_sendfile=False)
            await roundtrip(use_loop_sendfile=True)

        self.run_async(main())

    def test_native_datagram_and_signal_coverage(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            server_ready = loop.create_future()

            class Echo(asyncio.DatagramProtocol):
                transport: asyncio.DatagramTransport | None = None

                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.transport = transport  # type: ignore[assignment]
                    if not server_ready.done():
                        server_ready.set_result(transport)

                def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
                    assert self.transport is not None
                    self.transport.sendto(data, addr)

            transport, _protocol = await loop.create_datagram_endpoint(
                Echo, local_addr=("127.0.0.1", 0)
            )
            server_transport = await server_ready
            server_addr = server_transport.get_extra_info("sockname")

            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.setblocking(False)
            try:
                await loop.sock_sendto(client, b"ping", server_addr)
                recv_buf = bytearray(8)
                nbytes, addr = await loop.sock_recvfrom_into(client, recv_buf)
                self.assertEqual(bytes(recv_buf[:nbytes]), b"ping")
                self.assertEqual(addr, server_addr)

                await loop.sock_sendto(client, b"pong", server_addr)
                data, addr = await loop.sock_recvfrom(client, 8)
                self.assertEqual(data, b"pong")
                self.assertEqual(addr, server_addr)
            finally:
                client.close()
                transport.close()

            done = loop.create_future()
            sig = signal.SIGUSR1
            loop.add_signal_handler(sig, done.set_result, "signal-ok")
            try:
                os.kill(os.getpid(), sig)
                self.assertEqual(await done, "signal-ok")
            finally:
                loop.remove_signal_handler(sig)

        self.run_async(main())

    def test_native_subprocess_pipes(self) -> None:
        async def main() -> None:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "data = sys.stdin.buffer.read(); "
                    "sys.stdout.buffer.write(data[::-1]); "
                    "sys.stderr.buffer.write(b'err')"
                ),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate(b"abc123")
            self.assertEqual(stdout, b"321cba")
            self.assertEqual(stderr, b"err")
            self.assertEqual(await proc.wait(), 0)
            self.assertEqual(proc.returncode, 0)

            shell_proc = await asyncio.create_subprocess_shell(
                "printf shell-ok",
                stdout=asyncio.subprocess.PIPE,
            )
            shell_stdout, _stderr = await shell_proc.communicate()
            self.assertEqual(shell_stdout, b"shell-ok")
            self.assertEqual(await shell_proc.wait(), 0)

            merged_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "sys.stdout.buffer.write(b'out'); "
                    "sys.stderr.buffer.write(b'err')"
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            merged_stdout, merged_stderr = await merged_proc.communicate()
            self.assertEqual(merged_stdout, b"outerr")
            self.assertIsNone(merged_stderr)
            self.assertEqual(await merged_proc.wait(), 0)

            bare_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import sys; sys.exit(0)",
            )
            self.assertIsNone(bare_proc.stdin)
            self.assertIsNone(bare_proc.stdout)
            self.assertIsNone(bare_proc.stderr)
            self.assertEqual(await bare_proc.wait(), 0)

            failed_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import sys; sys.stdout.write('x'); sys.stderr.write('y'); sys.exit(7)",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            failed_stdout, failed_stderr = await failed_proc.communicate()
            self.assertEqual(failed_stdout, b"x")
            self.assertEqual(failed_stderr, b"y")
            self.assertEqual(await failed_proc.wait(), 7)
            self.assertEqual(failed_proc.returncode, 7)

            terminate_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            )
            terminate_proc.terminate()
            terminate_rc = await terminate_proc.wait()
            self.assertNotEqual(terminate_rc, 0)

            kill_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            )
            kill_proc.kill()
            kill_rc = await kill_proc.wait()
            self.assertNotEqual(kill_rc, 0)

            no_input_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "data = sys.stdin.buffer.read(); "
                    "sys.stdout.buffer.write(b'none' if not data else data)"
                ),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
            )
            no_input_stdout, _ = await no_input_proc.communicate()
            self.assertEqual(no_input_stdout, b"none")
            self.assertEqual(await no_input_proc.wait(), 0)

            close_proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                stdout=asyncio.subprocess.PIPE,
            )
            close_proc._transport.close()
            close_rc = await close_proc.wait()
            self.assertIsInstance(close_rc, int)

        self.run_async(main())

    def test_native_subprocess_protocol_ordering(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            events: list[str] = []
            done = loop.create_future()

            class Protocol(asyncio.SubprocessProtocol):
                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    events.append("connection_made")

                def pipe_data_received(self, fd: int, data: bytes) -> None:
                    events.append(f"pipe_data_received:{fd}:{data.decode()}")

                def pipe_connection_lost(
                    self, fd: int, exc: BaseException | None
                ) -> None:
                    events.append(f"pipe_connection_lost:{fd}")

                def process_exited(self) -> None:
                    events.append("process_exited")

                def connection_lost(self, exc: BaseException | None) -> None:
                    events.append("connection_lost")
                    if not done.done():
                        done.set_result(None)

            transport, _ = await loop.subprocess_exec(
                Protocol,
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "sys.stdout.write('out'); "
                    "sys.stdout.flush(); "
                    "sys.stderr.write('err'); "
                    "sys.stderr.flush()"
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await done
            await transport._wait()

            self.assertLess(events.index("connection_made"), events.index("process_exited"))
            self.assertLess(events.index("process_exited"), events.index("connection_lost"))
            self.assertTrue(
                any(event.startswith("pipe_data_received:1:") for event in events)
            )
            self.assertTrue(
                any(event.startswith("pipe_data_received:2:") for event in events)
            )
            self.assertIn("pipe_connection_lost:1", events)
            self.assertIn("pipe_connection_lost:2", events)
            self.assertEqual(events.count("pipe_connection_lost:1"), 1)
            self.assertEqual(events.count("pipe_connection_lost:2"), 1)

        self.run_async(main())

    def test_native_subprocess_callbacks_run_on_loop_thread(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            expected_thread_id = getattr(loop, "_thread_id", None)
            event_threads: list[tuple[str, int]] = []
            waiter_thread_id: asyncio.Future[int] = loop.create_future()
            done = loop.create_future()

            class Protocol(asyncio.SubprocessProtocol):
                transport: asyncio.Transport | None = None

                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.transport = transport  # type: ignore[assignment]
                    event_threads.append(("connection_made", threading.get_ident()))
                    wait_future = transport._wait()  # type: ignore[attr-defined]

                    def on_wait_done(_future: asyncio.Future[int]) -> None:
                        if not waiter_thread_id.done():
                            waiter_thread_id.set_result(threading.get_ident())

                    wait_future.add_done_callback(on_wait_done)

                def pipe_data_received(self, fd: int, data: bytes) -> None:
                    event_threads.append((f"pipe_data_received:{fd}", threading.get_ident()))

                def pipe_connection_lost(
                    self, fd: int, exc: BaseException | None
                ) -> None:
                    event_threads.append((f"pipe_connection_lost:{fd}", threading.get_ident()))

                def process_exited(self) -> None:
                    event_threads.append(("process_exited", threading.get_ident()))

                def connection_lost(self, exc: BaseException | None) -> None:
                    event_threads.append(("connection_lost", threading.get_ident()))
                    if not done.done():
                        done.set_result(None)

            await loop.subprocess_exec(
                Protocol,
                sys.executable,
                "-c",
                "import sys; sys.stdout.write('ok\\n')",
                stdout=asyncio.subprocess.PIPE,
            )

            await asyncio.wait_for(done, 2.0)
            observed_waiter_thread_id = await asyncio.wait_for(waiter_thread_id, 2.0)

            self.assertIsNotNone(expected_thread_id)
            self.assertTrue(event_threads)
            self.assertTrue(
                all(thread_id == expected_thread_id for _, thread_id in event_threads)
            )
            self.assertEqual(observed_waiter_thread_id, expected_thread_id)

        self.run_async(main())

    def test_native_read_pipe_callbacks_run_on_loop_thread(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            expected_thread_id = getattr(loop, "_thread_id", None)
            observed: list[tuple[str, int]] = []
            chunks: list[bytes] = []
            done = loop.create_future()

            class Protocol(asyncio.Protocol):
                def data_received(self, data: bytes) -> None:
                    chunks.append(data)
                    observed.append(("data_received", threading.get_ident()))

                def connection_lost(self, exc: BaseException | None) -> None:
                    observed.append(("connection_lost", threading.get_ident()))
                    if not done.done():
                        done.set_result(exc)

            read_fd, write_fd = os.pipe()
            read_file = os.fdopen(read_fd, "rb", buffering=0)

            def writer() -> None:
                try:
                    os.write(write_fd, b"hello")
                    os.write(write_fd, b"-world")
                finally:
                    os.close(write_fd)

            transport, _protocol = await loop.connect_read_pipe(Protocol, read_file)
            writer_task = asyncio.create_task(asyncio.to_thread(writer))
            try:
                self.assertIsNone(await asyncio.wait_for(done, 2.0))
                await asyncio.wait_for(writer_task, 2.0)
            finally:
                transport.close()

            self.assertEqual(b"".join(chunks), b"hello-world")
            self.assertIsNotNone(expected_thread_id)
            self.assertTrue(observed)
            self.assertTrue(
                all(thread_id == expected_thread_id for _, thread_id in observed)
            )

        self.run_async(main())

    def test_native_write_pipe_callbacks_run_on_loop_thread(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            expected_thread_id = getattr(loop, "_thread_id", None)
            observed: list[tuple[str, int]] = []
            read_fd, write_fd = os.pipe()
            write_file = os.fdopen(write_fd, "wb", buffering=0)
            read_chunks: list[bytes] = []
            done = loop.create_future()

            class Protocol(asyncio.Protocol):
                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    observed.append(("connection_made", threading.get_ident()))

                def connection_lost(self, exc: BaseException | None) -> None:
                    observed.append(("connection_lost", threading.get_ident()))
                    if not done.done():
                        done.set_result(exc)

            def reader() -> bytes:
                try:
                    while True:
                        chunk = os.read(read_fd, 4096)
                        if not chunk:
                            break
                        read_chunks.append(chunk)
                    return b"".join(read_chunks)
                finally:
                    os.close(read_fd)

            reader_task = asyncio.create_task(asyncio.to_thread(reader))
            transport, _protocol = await loop.connect_write_pipe(Protocol, write_file)
            try:
                transport.write(b"hello")
                transport.write(b"-world")
                transport.close()
                self.assertIsNone(await asyncio.wait_for(done, 2.0))
                payload = await asyncio.wait_for(reader_task, 2.0)
            finally:
                transport.close()

            self.assertEqual(payload, b"hello-world")
            self.assertIsNotNone(expected_thread_id)
            self.assertTrue(observed)
            self.assertTrue(
                all(thread_id == expected_thread_id for _, thread_id in observed)
            )

        self.run_async(main())

    def test_native_subprocess_fallback_kwargs(self) -> None:
        async def main() -> None:
            env = os.environ.copy()
            env["RSLOOP_SUBPROCESS_FALLBACK"] = "fallback-ok"
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                "import os,sys; sys.stdout.write(os.environ['RSLOOP_SUBPROCESS_FALLBACK'])",
                stdout=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, _ = await proc.communicate()
            self.assertEqual(stdout, b"fallback-ok")
            self.assertEqual(await proc.wait(), 0)

            with tempfile.TemporaryDirectory(prefix="rsloop-subprocess-cwd-") as tmpdir:
                marker = Path(tmpdir) / "cwd-marker.txt"
                marker.write_text("cwd-ok")
                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path; "
                        "import sys; "
                        "sys.stdout.write(Path('cwd-marker.txt').read_text())"
                    ),
                    stdout=asyncio.subprocess.PIPE,
                    cwd=tmpdir,
                )
                stdout, _ = await proc.communicate()
                self.assertEqual(stdout, b"cwd-ok")
                self.assertEqual(await proc.wait(), 0)

        self.run_async(main())

    def test_native_subprocess_dispatch_selection(self) -> None:
        async def main() -> None:
            native_calls = 0
            fallback_calls = 0

            original_spawn = rsloop_loop._rsloop.spawn_subprocess
            original_popen = rsloop_loop.subprocess.Popen

            def counting_spawn(*args, **kwargs):
                nonlocal native_calls
                native_calls += 1
                return original_spawn(*args, **kwargs)

            def counting_popen(*args, **kwargs):
                nonlocal fallback_calls
                fallback_calls += 1
                return original_popen(*args, **kwargs)

            with (
                mock.patch.object(
                    rsloop_loop._rsloop,
                    "spawn_subprocess",
                    side_effect=counting_spawn,
                ),
                mock.patch.object(
                    rsloop_loop.subprocess,
                    "Popen",
                    side_effect=counting_popen,
                ),
            ):
                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    "import sys; sys.exit(0)",
                )
                self.assertEqual(await proc.wait(), 0)
                self.assertEqual(native_calls, 1)
                self.assertEqual(fallback_calls, 0)

                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    "import os,sys; sys.exit(0)",
                    cwd=os.getcwd(),
                )
                self.assertEqual(await proc.wait(), 0)
                self.assertEqual(native_calls, 2)
                self.assertEqual(fallback_calls, 0)

                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    "import os,sys; sys.exit(0)",
                    preexec_fn=lambda: None,
                )
                self.assertEqual(await proc.wait(), 0)
                self.assertEqual(native_calls, 2)
                self.assertEqual(fallback_calls, 1)

        self.run_async(main())

    def test_native_sock_sendfile(self) -> None:
        payload = (b"sendfile-native-" * 2048) + b"tail"
        offset = 128
        count = 24 * 1024
        expected = payload[offset : offset + count]

        async def main() -> None:
            with tempfile.TemporaryDirectory(prefix="rsloop-sock-sendfile-") as tmpdir:
                path = Path(tmpdir) / "payload.bin"
                path.write_bytes(payload)
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
                        with path.open("rb") as file:
                            sent = await loop.sock_sendfile(conn, file, offset=offset, count=count)
                        self.assertEqual(sent, len(expected))
                    finally:
                        conn.close()
                        listener.close()
                        if not server_done.done():
                            server_done.set_result(None)

                server_task = asyncio.create_task(server())
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setblocking(False)
                try:
                    await loop.sock_connect(client, (host, port))
                    chunks: list[bytes] = []
                    remaining = len(expected)
                    while remaining:
                        chunk = await loop.sock_recv(client, remaining)
                        if not chunk:
                            break
                        chunks.append(chunk)
                        remaining -= len(chunk)
                    self.assertEqual(b"".join(chunks), expected)
                    await server_done
                finally:
                    client.close()
                    await server_task

        self.run_async(main())

    def test_native_start_tls(self) -> None:
        server_context, client_context = make_tls_contexts()

        async def main() -> None:
            loop = asyncio.get_running_loop()

            async def handle(
                reader: asyncio.StreamReader, writer: asyncio.StreamWriter
            ) -> None:
                try:
                    self.assertEqual(await reader.readexactly(9), b"STARTTLS\n")
                    writer.write(b"READY\n")
                    await writer.drain()
                    await writer.start_tls(server_context)
                    self.assertEqual(await reader.readexactly(4), b"ping")
                    writer.write(b"pong")
                    await writer.drain()
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]

            try:
                reader, writer = await asyncio.open_connection(host, port)
                writer.write(b"STARTTLS\n")
                await writer.drain()
                self.assertEqual(await reader.readexactly(6), b"READY\n")
                await writer.start_tls(client_context, server_hostname="localhost")
                writer.write(b"ping")
                await writer.drain()
                self.assertEqual(await reader.readexactly(4), b"pong")
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        self.run_async(main())

    def test_rsloop_tls_context_accepts_generated_v3_cert(self) -> None:
        RsloopTLSContext = getattr(rsloop._rsloop, "RsloopTLSContext", None)
        self.assertIsNotNone(RsloopTLSContext)

        tempdir = Path(tempfile.mkdtemp(prefix="rsloop-test-native-tls-"))
        certfile = tempdir / "cert.pem"
        keyfile = tempdir / "key.pem"
        try:
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-keyout",
                    str(keyfile),
                    "-out",
                    str(certfile),
                    "-subj",
                    "/CN=localhost",
                    "-days",
                    "1",
                    "-addext",
                    "basicConstraints=critical,CA:FALSE",
                    "-addext",
                    "keyUsage=critical,digitalSignature,keyEncipherment",
                    "-addext",
                    "extendedKeyUsage=serverAuth",
                    "-addext",
                    "subjectAltName=DNS:localhost,IP:127.0.0.1",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            server_context = RsloopTLSContext.server(
                certfile.read_bytes(),
                keyfile.read_bytes(),
            )
            self.assertTrue(server_context.is_server)
        finally:
            for path in (certfile, keyfile):
                path.unlink(missing_ok=True)
            tempdir.rmdir()

    def test_tcp_server_runner_shutdown_regression(self) -> None:
        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            try:
                while True:
                    payload = await reader.readexactly(1)
                    writer.write(payload)
                    await writer.drain()
            except asyncio.IncompleteReadError:
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        async def main() -> None:
            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            addr = server.sockets[0].getsockname()

            try:
                reader, writer = await asyncio.open_connection(*addr[:2])
                for _ in range(256):
                    writer.write(b"x")
                    await writer.drain()
                    self.assertEqual(await reader.readexactly(1), b"x")
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        for _ in range(2):
            self.run_async(main())

    def test_tokio_bridge(self) -> None:
        async def python_work() -> str:
            await asyncio.sleep(0.01)
            return "bridge-ok"

        async def main() -> None:
            self.assertEqual(await rsloop.run_in_tokio(python_work()), "bridge-ok")
            await rsloop.sleep(0.01)

        self.run_async(main())


if __name__ == "__main__":
    unittest.main()

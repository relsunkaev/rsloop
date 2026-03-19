from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
import socket
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import kioto

try:
    import uvloop
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    uvloop = None


LoopFactory = Callable[[], asyncio.AbstractEventLoop] | None
BenchFn = Callable[[int], Awaitable[None]]


@dataclass(frozen=True)
class BenchmarkSpec:
    name: str
    description: str
    category: str
    default_iterations: int
    unit: str
    func: BenchFn


@dataclass(frozen=True)
class BenchResult:
    loop: str
    benchmark: str
    category: str
    description: str
    iterations: int
    repeats: int
    unit: str
    samples: list[float]
    profiles: list[dict[str, Any]] | None = None

    @property
    def median(self) -> float:
        return statistics.median(self.samples)

    @property
    def minimum(self) -> float:
        return min(self.samples)

    @property
    def mean(self) -> float:
        return statistics.fmean(self.samples)

    @property
    def ns_per_unit(self) -> float:
        return self.median * 1e9 / self.iterations


def bench_result_from_payload(payload: dict[str, Any]) -> BenchResult:
    return BenchResult(
        loop=payload["loop"],
        benchmark=payload["benchmark"],
        category=payload["category"],
        description=payload["description"],
        iterations=payload["iterations"],
        repeats=payload["repeats"],
        unit=payload["unit"],
        samples=list(payload["samples"]),
        profiles=payload.get("profiles"),
    )


def parse_runtime_profile(stderr: str) -> dict[str, Any] | None:
    scheduler_runs: list[dict[str, Any]] = []
    stream_runs: list[dict[str, Any]] = []
    for line in stderr.splitlines():
        if line.startswith("KIOTO_PROFILE_SCHED_JSON "):
            scheduler_runs.append(
                json.loads(line.removeprefix("KIOTO_PROFILE_SCHED_JSON "))
            )
        elif line.startswith("KIOTO_PROFILE_STREAM_JSON "):
            stream_runs.append(
                json.loads(line.removeprefix("KIOTO_PROFILE_STREAM_JSON "))
            )
    if not scheduler_runs and not stream_runs:
        return None
    profile: dict[str, Any] = {}
    if scheduler_runs:
        profile["scheduler_runs"] = scheduler_runs
        profile["scheduler"] = max(
            scheduler_runs,
            key=lambda item: (
                item.get("iterations", 0),
                item.get("fd_events", 0),
                item.get("ready_handles", 0),
            ),
        )
    if stream_runs:
        profile["stream_runs"] = stream_runs
        profile["stream"] = max(
            stream_runs,
            key=lambda item: (
                len(item.get("events", [])),
                item.get("dropped", 0),
            ),
        )
    return profile


def get_loop_factory(loop_name: str) -> LoopFactory:
    if loop_name == "asyncio":
        return None
    if loop_name == "kioto":
        return kioto.KiotoEventLoop
    if loop_name == "uvloop":
        if uvloop is None:
            raise RuntimeError("uvloop is not installed in this environment")
        return uvloop.new_event_loop
    raise ValueError(f"unknown loop {loop_name}")


def run_on_loop(loop_name: str, coro: Awaitable[None]) -> None:
    loop_factory = get_loop_factory(loop_name)
    with asyncio.Runner(loop_factory=loop_factory, debug=False) as runner:
        runner.run(coro)


async def bench_call_soon(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = count

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    for _ in range(count):
        loop.call_soon(tick)

    await done


async def bench_call_soon_threadsafe(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = count

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    def producer() -> None:
        for _ in range(count):
            loop.call_soon_threadsafe(tick)

    await asyncio.to_thread(producer)
    await done


async def bench_sleep_zero(count: int) -> None:
    for _ in range(count):
        await asyncio.sleep(0)


async def bench_timer_zero(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = count

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    for _ in range(count):
        loop.call_later(0, tick)

    await done


async def bench_socketpair_small(count: int) -> None:
    await socketpair_roundtrip(count, payload_size=1)


async def bench_socketpair_large(count: int) -> None:
    await socketpair_roundtrip(count, payload_size=4096)


async def socketpair_roundtrip(count: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    payload = b"x" * payload_size

    try:
        for _ in range(count):
            await loop.sock_sendall(left, payload)
            data = await recv_exact(loop, right, payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
            await loop.sock_sendall(right, payload)
            data = await recv_exact(loop, left, payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
    finally:
        left.close()
        right.close()


async def bench_tcp_echo_small(count: int) -> None:
    await tcp_stream_echo(count, payload_size=1)


async def bench_tcp_echo_large(count: int) -> None:
    await tcp_stream_echo(count, payload_size=4096)


async def bench_tcp_echo_parallel(count: int) -> None:
    await tcp_stream_echo_parallel(count, clients=32, payload_size=64)


async def bench_tcp_rpc(count: int) -> None:
    await tcp_stream_rpc(count, request_size=64, response_size=256)


async def bench_tcp_rpc_parallel(count: int) -> None:
    await tcp_stream_rpc_parallel(count, clients=32, request_size=64, response_size=256)


async def bench_tcp_rpc_pipeline(count: int) -> None:
    await tcp_stream_rpc_pipeline(count, request_size=64, response_size=256, pipeline=8)


async def bench_tcp_rpc_pipeline_parallel(count: int) -> None:
    await tcp_stream_rpc_pipeline_parallel(
        count,
        clients=16,
        request_size=64,
        response_size=256,
        pipeline=4,
    )


async def bench_tcp_echo_mixed_parallel(count: int) -> None:
    await tcp_stream_echo_mixed_parallel(count, clients=16, payload_sizes=(64, 256, 1024, 4096))


async def bench_tcp_bulk(count: int) -> None:
    await tcp_stream_echo_parallel(count, clients=8, payload_size=65536)


async def bench_tcp_sock_small(count: int) -> None:
    await tcp_sock_echo(count, payload_size=1)


async def bench_tcp_connect_close(count: int) -> None:
    accepted = 0
    done = asyncio.get_running_loop().create_future()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal accepted
        accepted += 1
        writer.close()
        await writer.wait_closed()
        if accepted == count and not done.done():
            done.set_result(None)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(256, count))
    host, port = server.sockets[0].getsockname()[:2]

    try:
        for _ in range(count):
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
        await done
    finally:
        server.close()
        await server.wait_closed()


async def bench_tcp_idle_fanout(count: int) -> None:
    accepted = 0
    done = asyncio.get_running_loop().create_future()
    peers: list[asyncio.StreamWriter] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal accepted
        accepted += 1
        peers.append(writer)
        if accepted == count and not done.done():
            done.set_result(None)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(128, count))
    host, port = server.sockets[0].getsockname()[:2]
    writers: list[asyncio.StreamWriter] = []

    try:
        for _ in range(count):
            _reader, writer = await asyncio.open_connection(host, port)
            writers.append(writer)
        await done
    finally:
        for writer in writers:
            writer.close()
        for writer in writers:
            await writer.wait_closed()
        for writer in peers:
            writer.close()
        for writer in peers:
            await writer.wait_closed()
        server.close()
        await server.wait_closed()


async def tcp_stream_echo(count: int, payload_size: int) -> None:
    payload = b"x" * payload_size

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(payload_size)
                writer.write(data)
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        reader, writer = await asyncio.open_connection(host, port)
        for _ in range(count):
            writer.write(payload)
            await writer.drain()
            data = await reader.readexactly(payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_echo_parallel(count: int, clients: int, payload_size: int) -> None:
    payload = b"x" * payload_size
    per_client = max(1, count // clients)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(payload_size)
                writer.write(data)
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> None:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            for _ in range(per_client):
                writer.write(payload)
                await writer.drain()
                data = await reader.readexactly(payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client() for _ in range(clients)))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_rpc(count: int, request_size: int, response_size: int) -> None:
    request = b"r" * request_size
    response = b"s" * response_size

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(request_size)
                if data != request:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                writer.write(response)
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        reader, writer = await asyncio.open_connection(host, port)
        for _ in range(count):
            writer.write(request)
            await writer.drain()
            data = await reader.readexactly(response_size)
            if data != response:
                raise RuntimeError(f"unexpected payload size={len(data)}")
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_rpc_parallel(
    count: int,
    clients: int,
    request_size: int,
    response_size: int,
) -> None:
    request = b"r" * request_size
    response = b"s" * response_size
    per_client = max(1, count // clients)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(request_size)
                if data != request:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                writer.write(response)
                await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> None:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            for _ in range(per_client):
                writer.write(request)
                await writer.drain()
                data = await reader.readexactly(response_size)
                if data != response:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client() for _ in range(clients)))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_rpc_pipeline(
    count: int,
    request_size: int,
    response_size: int,
    pipeline: int,
) -> None:
    request = b"r" * request_size
    response = b"s" * response_size
    pipeline = max(1, pipeline)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(request_size)
                if data != request:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                writer.write(response)
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        reader, writer = await asyncio.open_connection(host, port)
        remaining = count
        while remaining:
            burst = min(pipeline, remaining)
            for _ in range(burst):
                writer.write(request)
            await writer.drain()
            for _ in range(burst):
                data = await reader.readexactly(response_size)
                if data != response:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
            remaining -= burst
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_rpc_pipeline_parallel(
    count: int,
    clients: int,
    request_size: int,
    response_size: int,
    pipeline: int,
) -> None:
    request = b"r" * request_size
    response = b"s" * response_size
    per_client = max(1, count // clients)
    pipeline = max(1, pipeline)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(request_size)
                if data != request:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                writer.write(response)
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> None:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            remaining = per_client
            while remaining:
                burst = min(pipeline, remaining)
                for _ in range(burst):
                    writer.write(request)
                await writer.drain()
                for _ in range(burst):
                    data = await reader.readexactly(response_size)
                    if data != response:
                        raise RuntimeError(f"unexpected payload size={len(data)}")
                remaining -= burst
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client() for _ in range(clients)))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_echo_mixed_parallel(
    count: int,
    clients: int,
    payload_sizes: tuple[int, ...],
) -> None:
    per_client = max(1, count // clients)
    payloads = [bytes([65 + index % 26]) * size for index, size in enumerate(payload_sizes)]

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            index = 0
            while True:
                payload = payloads[index % len(payloads)]
                data = await reader.readexactly(len(payload))
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                writer.write(data)
                await writer.drain()
                index += 1
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> None:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            for index in range(per_client):
                payload = payloads[index % len(payloads)]
                writer.write(payload)
                await writer.drain()
                echoed = await reader.readexactly(len(payload))
                if echoed != payload:
                    raise RuntimeError(f"unexpected payload size={len(echoed)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client() for _ in range(clients)))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_sock_echo(count: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * payload_size

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.setblocking(False)
    host, port = listener.getsockname()[:2]

    server_done = loop.create_future()

    async def server() -> None:
        conn, _addr = await loop.sock_accept(listener)
        conn.setblocking(False)
        try:
            for _ in range(count):
                data = await recv_exact(loop, conn, payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                await loop.sock_sendall(conn, data)
            server_done.set_result(None)
        finally:
            conn.close()

    server_task = asyncio.create_task(server())
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setblocking(False)

    try:
        await loop.sock_connect(client, (host, port))
        for _ in range(count):
            await loop.sock_sendall(client, payload)
            data = await recv_exact(loop, client, payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
        await server_done
    finally:
        client.close()
        listener.close()
        await server_task


async def recv_exact(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    size: int,
) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = await loop.sock_recv(sock, remaining)
        if not chunk:
            raise RuntimeError(f"unexpected EOF while reading {size} bytes")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


BENCHMARKS: dict[str, BenchmarkSpec] = {
    "call_soon": BenchmarkSpec(
        name="call_soon",
        description="Single-thread ready-queue churn via loop.call_soon()",
        category="scheduler",
        default_iterations=100_000,
        unit="callbacks",
        func=bench_call_soon,
    ),
    "call_soon_threadsafe": BenchmarkSpec(
        name="call_soon_threadsafe",
        description="Cross-thread scheduling through loop.call_soon_threadsafe()",
        category="scheduler",
        default_iterations=50_000,
        unit="callbacks",
        func=bench_call_soon_threadsafe,
    ),
    "sleep_zero": BenchmarkSpec(
        name="sleep_zero",
        description="Coroutine yield/resume churn through asyncio.sleep(0)",
        category="scheduler",
        default_iterations=20_000,
        unit="awaits",
        func=bench_sleep_zero,
    ),
    "timer_zero": BenchmarkSpec(
        name="timer_zero",
        description="Immediate timer scheduling through loop.call_later(0, ...)",
        category="scheduler",
        default_iterations=20_000,
        unit="timers",
        func=bench_timer_zero,
    ),
    "socketpair_1b": BenchmarkSpec(
        name="socketpair_1b",
        description="Raw socketpair ping-pong with 1-byte payloads",
        category="raw-socket",
        default_iterations=20_000,
        unit="roundtrips",
        func=bench_socketpair_small,
    ),
    "socketpair_4k": BenchmarkSpec(
        name="socketpair_4k",
        description="Raw socketpair ping-pong with 4 KiB payloads",
        category="raw-socket",
        default_iterations=5_000,
        unit="roundtrips",
        func=bench_socketpair_large,
    ),
    "tcp_echo_1b": BenchmarkSpec(
        name="tcp_echo_1b",
        description="Single TCP stream echo using asyncio streams with 1-byte payloads",
        category="stream",
        default_iterations=10_000,
        unit="roundtrips",
        func=bench_tcp_echo_small,
    ),
    "tcp_echo_4k": BenchmarkSpec(
        name="tcp_echo_4k",
        description="Single TCP stream echo using asyncio streams with 4 KiB payloads",
        category="stream",
        default_iterations=5_000,
        unit="roundtrips",
        func=bench_tcp_echo_large,
    ),
    "tcp_echo_parallel": BenchmarkSpec(
        name="tcp_echo_parallel",
        description="32 concurrent stream clients with 64-byte echo payloads",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_echo_parallel,
    ),
    "tcp_rpc": BenchmarkSpec(
        name="tcp_rpc",
        description="Single TCP request/response stream with 64-byte requests and 256-byte responses",
        category="stream",
        default_iterations=10_000,
        unit="roundtrips",
        func=bench_tcp_rpc,
    ),
    "tcp_rpc_parallel": BenchmarkSpec(
        name="tcp_rpc_parallel",
        description="32 concurrent TCP request/response clients with 64-byte requests and 256-byte responses",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_rpc_parallel,
    ),
    "tcp_rpc_pipeline": BenchmarkSpec(
        name="tcp_rpc_pipeline",
        description="Single TCP request/response stream with pipeline depth 8",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_rpc_pipeline,
    ),
    "tcp_rpc_pipeline_parallel": BenchmarkSpec(
        name="tcp_rpc_pipeline_parallel",
        description="16 concurrent TCP request/response clients with pipeline depth 4",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_rpc_pipeline_parallel,
    ),
    "tcp_echo_mixed_parallel": BenchmarkSpec(
        name="tcp_echo_mixed_parallel",
        description="16 concurrent echo clients cycling mixed 64B-4KiB payloads",
        category="stream",
        default_iterations=1_024,
        unit="messages",
        func=bench_tcp_echo_mixed_parallel,
    ),
    "tcp_bulk": BenchmarkSpec(
        name="tcp_bulk",
        description="8 concurrent stream clients with 64 KiB echo payloads",
        category="stream",
        default_iterations=1_024,
        unit="messages",
        func=bench_tcp_bulk,
    ),
    "tcp_sock_1b": BenchmarkSpec(
        name="tcp_sock_1b",
        description="TCP echo through loop.sock_* helpers with 1-byte payloads",
        category="raw-socket",
        default_iterations=10_000,
        unit="roundtrips",
        func=bench_tcp_sock_small,
    ),
    "tcp_connect": BenchmarkSpec(
        name="tcp_connect",
        description="Connection open/close churn against a local server",
        category="connection",
        default_iterations=64,
        unit="connections",
        func=bench_tcp_connect_close,
    ),
    "tcp_idle_fanout": BenchmarkSpec(
        name="tcp_idle_fanout",
        description="Open many idle TCP connections and tear them down",
        category="connection",
        default_iterations=128,
        unit="connections",
        func=bench_tcp_idle_fanout,
    ),
}


PROFILE_BENCHMARKS = {
    "smoke": ["call_soon", "tcp_echo_1b", "tcp_sock_1b"],
    "default": [
        "call_soon",
        "call_soon_threadsafe",
        "sleep_zero",
        "timer_zero",
        "tcp_echo_1b",
        "tcp_echo_parallel",
        "tcp_rpc",
        "tcp_rpc_parallel",
        "tcp_rpc_pipeline",
        "tcp_rpc_pipeline_parallel",
        "tcp_sock_1b",
        "tcp_connect",
    ],
    "full": list(BENCHMARKS),
}


def run_single(
    loop_name: str,
    spec: BenchmarkSpec,
    iterations: int,
    repeats: int,
    warmups: int,
) -> BenchResult:
    for _ in range(warmups):
        run_on_loop(loop_name, spec.func(iterations))

    samples = []
    for _ in range(repeats):
        started = time.perf_counter()
        run_on_loop(loop_name, spec.func(iterations))
        samples.append(time.perf_counter() - started)

    return BenchResult(
        loop=loop_name,
        benchmark=spec.name,
        category=spec.category,
        description=spec.description,
        iterations=iterations,
        repeats=repeats,
        unit=spec.unit,
        samples=samples,
    )


def run_single_isolated(
    loop_name: str,
    spec: BenchmarkSpec,
    iterations: int,
    repeats: int,
    warmups: int,
    profile_runtime: bool,
    profile_stream: bool,
) -> BenchResult:
    def run_child_once() -> tuple[float, dict[str, Any] | None]:
        cmd = [
            sys.executable,
            __file__,
            "--child-run",
            "--loop",
            loop_name,
            "--benchmark",
            spec.name,
            "--iterations",
            str(iterations),
            "--repeats",
            "1",
            "--warmups",
            "0",
            "--baseline",
            "uvloop",
            "--json",
        ]
        env = os.environ.copy()
        if loop_name == "kioto" and profile_runtime:
            env["KIOTO_PROFILE_SCHED_JSON"] = "1"
        if loop_name == "kioto" and profile_stream:
            env["KIOTO_PROFILE_SCHED_JSON"] = "1"
            env["KIOTO_PROFILE_STREAM_JSON"] = "1"
        completed = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        payload = json.loads(completed.stdout)
        result = bench_result_from_payload(payload["results"][0])
        return result.samples[0], parse_runtime_profile(completed.stderr)

    for _ in range(warmups):
        run_child_once()

    samples = []
    profiles = []
    for _ in range(repeats):
        sample, profile = run_child_once()
        samples.append(sample)
        if profile is not None:
            profiles.append(profile)
    return BenchResult(
        loop=loop_name,
        benchmark=spec.name,
        category=spec.category,
        description=spec.description,
        iterations=iterations,
        repeats=repeats,
        unit=spec.unit,
        samples=samples,
        profiles=profiles or None,
    )


def format_result(result: BenchResult) -> str:
    return (
        f"{result.loop:7} {result.benchmark:18} "
        f"median={result.median:.6f}s mean={result.mean:.6f}s min={result.minimum:.6f}s "
        f"ns/{result.unit}={result.ns_per_unit:,.0f}"
    )


def emit_summary(results: list[BenchResult], baseline: str) -> None:
    by_benchmark: dict[str, dict[str, BenchResult]] = {}
    for result in results:
        by_benchmark.setdefault(result.benchmark, {})[result.loop] = result

    print()
    print(f"{'benchmark':18} {'baseline':>10} {'kioto':>10} {'kioto/x':>10}")
    for benchmark in sorted(by_benchmark):
        row = by_benchmark[benchmark]
        baseline_result = row.get(baseline)
        kioto_result = row.get("kioto")
        if baseline_result is None or kioto_result is None:
            continue
        ratio = kioto_result.median / baseline_result.median
        print(
            f"{benchmark:18} "
            f"{baseline_result.median:10.6f} "
            f"{kioto_result.median:10.6f} "
            f"{ratio:10.3f}x"
        )


def selected_benchmarks(args: argparse.Namespace) -> list[BenchmarkSpec]:
    if args.benchmark != "all":
        return [BENCHMARKS[args.benchmark]]
    if args.profile:
        return [BENCHMARKS[name] for name in PROFILE_BENCHMARKS[args.profile]]
    return [BENCHMARKS[name] for name in PROFILE_BENCHMARKS["full"]]


def environment_snapshot() -> dict[str, Any]:
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "processor": platform.processor(),
        "pid": os.getpid(),
    }


def result_payload(results: list[BenchResult], baseline: str) -> dict[str, Any]:
    payload_results = []
    grouped: dict[str, dict[str, BenchResult]] = {}
    for result in results:
        payload_results.append(
            {
                **asdict(result),
                "median": result.median,
                "mean": result.mean,
                "minimum": result.minimum,
                "ns_per_unit": result.ns_per_unit,
            }
        )
        grouped.setdefault(result.benchmark, {})[result.loop] = result

    comparisons = []
    for benchmark, row in sorted(grouped.items()):
        baseline_result = row.get(baseline)
        if baseline_result is None:
            continue
        for loop_name, result in sorted(row.items()):
            comparisons.append(
                {
                    "benchmark": benchmark,
                    "loop": loop_name,
                    "baseline": baseline,
                    "ratio_to_baseline": result.median / baseline_result.median,
                }
            )

    return {
        "environment": environment_snapshot(),
        "baseline": baseline,
        "results": payload_results,
        "comparisons": comparisons,
    }


def write_output(payload: dict[str, Any], output: str | None) -> None:
    if output is None:
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", choices=["asyncio", "kioto", "uvloop", "all"], default="all")
    parser.add_argument("--benchmark", choices=[*BENCHMARKS.keys(), "all"], default="all")
    parser.add_argument("--profile", choices=sorted(PROFILE_BENCHMARKS), default="default")
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--baseline", choices=["asyncio", "uvloop", "kioto"], default="uvloop")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--list", action="store_true")
    parser.add_argument(
        "--profile-runtime",
        action="store_true",
        help="Capture Kioto scheduler/runtime profile data for isolated child runs.",
    )
    parser.add_argument(
        "--profile-stream",
        action="store_true",
        help="Capture Kioto stream event traces for isolated child runs.",
    )
    parser.add_argument("--child-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-isolate-process",
        action="store_true",
        help="Run all cases in the current process instead of subprocess isolation.",
    )
    args = parser.parse_args()

    if args.list:
        for name, spec in BENCHMARKS.items():
            print(
                f"{name:18} [{spec.category}] default={spec.default_iterations} {spec.unit} "
                f"- {spec.description}"
            )
        return

    loops = ["asyncio", "uvloop", "kioto"] if args.loop == "all" else [args.loop]
    specs = selected_benchmarks(args)

    results: list[BenchResult] = []
    isolate_process = not args.no_isolate_process and not args.child_run
    if (args.profile_runtime or args.profile_stream) and not isolate_process:
        raise SystemExit("runtime profiling requires subprocess isolation")
    for spec in specs:
        iterations = args.iterations or spec.default_iterations
        for loop_name in loops:
            if loop_name == "uvloop" and uvloop is None:
                continue
            if isolate_process:
                result = run_single_isolated(
                    loop_name,
                    spec,
                    iterations,
                    args.repeats,
                    args.warmups,
                    args.profile_runtime,
                    args.profile_stream,
                )
            else:
                result = run_single(loop_name, spec, iterations, args.repeats, args.warmups)
            results.append(result)
            if not args.json:
                print(format_result(result), flush=True)

    payload = result_payload(results, args.baseline)
    write_output(payload, args.output)

    if args.json:
        print(json.dumps(payload, indent=2))
    elif len(loops) > 1:
        emit_summary(results, args.baseline)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONASYNCIODEBUG", "0")
    main()

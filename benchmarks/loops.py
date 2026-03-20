from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib.metadata as importlib_metadata
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


def percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        raise ValueError("percentile requires at least one sample")
    if len(samples) == 1:
        return samples[0]
    ordered = sorted(samples)
    position = max(0.0, min(1.0, fraction)) * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def median_absolute_deviation(samples: list[float]) -> float:
    center = statistics.median(samples)
    deviations = [abs(sample - center) for sample in samples]
    return statistics.median(deviations)


def interquartile_range(samples: list[float]) -> float:
    return percentile(samples, 0.75) - percentile(samples, 0.25)


def safe_stdev(samples: list[float]) -> float:
    if len(samples) < 2:
        return 0.0
    return statistics.stdev(samples)


def maybe_version(dist_name: str) -> str | None:
    try:
        return importlib_metadata.version(dist_name)
    except importlib_metadata.PackageNotFoundError:
        return None


def git_snapshot() -> dict[str, Any]:
    def run_git(*args: str) -> str | None:
        try:
            completed = subprocess.run(
                ["git", *args],
                check=True,
                capture_output=True,
                text=True,
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            return None
        return completed.stdout.strip()

    status = run_git("status", "--short")
    return {
        "commit": run_git("rev-parse", "HEAD"),
        "branch": run_git("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(status),
        "status_short": status.splitlines() if status else [],
    }


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
    def maximum(self) -> float:
        return max(self.samples)

    @property
    def stdev(self) -> float:
        return safe_stdev(self.samples)

    @property
    def mad(self) -> float:
        return median_absolute_deviation(self.samples)

    @property
    def iqr(self) -> float:
        return interquartile_range(self.samples)

    @property
    def coefficient_of_variation(self) -> float:
        if self.mean == 0.0:
            return 0.0
        return self.stdev / self.mean

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


async def bench_tcp_upload_parallel(count: int) -> None:
    await tcp_stream_rpc_parallel(count, clients=16, request_size=16 * 1024, response_size=1)


async def bench_tcp_download_parallel(count: int) -> None:
    await tcp_stream_rpc_parallel(count, clients=16, request_size=1, response_size=16 * 1024)


async def bench_tcp_rpc_pipeline_4k_parallel(count: int) -> None:
    await tcp_stream_rpc_pipeline_parallel(
        count,
        clients=16,
        request_size=64,
        response_size=4096,
        pipeline=8,
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
    "tcp_upload_parallel": BenchmarkSpec(
        name="tcp_upload_parallel",
        description="16 concurrent clients uploading 16 KiB requests with 1-byte acknowledgements",
        category="stream",
        default_iterations=4_096,
        unit="messages",
        func=bench_tcp_upload_parallel,
    ),
    "tcp_download_parallel": BenchmarkSpec(
        name="tcp_download_parallel",
        description="16 concurrent clients issuing 1-byte requests with 16 KiB responses",
        category="stream",
        default_iterations=4_096,
        unit="messages",
        func=bench_tcp_download_parallel,
    ),
    "tcp_rpc_pipeline_4k_parallel": BenchmarkSpec(
        name="tcp_rpc_pipeline_4k_parallel",
        description="16 concurrent pipelined TCP RPC clients with 4 KiB responses and depth 8",
        category="stream",
        default_iterations=4_096,
        unit="messages",
        func=bench_tcp_rpc_pipeline_4k_parallel,
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


def run_once(
    loop_name: str,
    spec: BenchmarkSpec,
    iterations: int,
    profile_runtime: bool,
    profile_stream: bool,
    isolate_process: bool,
    child_retries: int,
) -> tuple[float, dict[str, Any] | None, int]:
    if not isolate_process:
        started = time.perf_counter()
        run_on_loop(loop_name, spec.func(iterations))
        return time.perf_counter() - started, None, 0
    return run_once_isolated(
        loop_name,
        spec,
        iterations,
        profile_runtime,
        profile_stream,
        child_retries,
    )


def run_once_isolated(
    loop_name: str,
    spec: BenchmarkSpec,
    iterations: int,
    profile_runtime: bool,
    profile_stream: bool,
    child_retries: int,
) -> tuple[float, dict[str, Any] | None, int]:
    attempts = 0
    last_error: subprocess.CalledProcessError | None = None
    while attempts <= child_retries:
        attempts += 1
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
        try:
            completed = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempts <= child_retries:
                continue
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout or "<no child output>"
            raise RuntimeError(
                f"benchmark child failed for loop={loop_name} benchmark={spec.name} "
                f"after {attempts} attempt(s): {detail}"
            ) from exc
        payload = json.loads(completed.stdout)
        result = bench_result_from_payload(payload["results"][0])
        return result.samples[0], parse_runtime_profile(completed.stderr), attempts - 1
    assert last_error is not None
    raise RuntimeError("benchmark child retry loop exited unexpectedly") from last_error


def rotated_order(items: list[str], offset: int) -> list[str]:
    if not items:
        return []
    shift = offset % len(items)
    return items[shift:] + items[:shift]


def run_benchmark_group(
    loops: list[str],
    spec: BenchmarkSpec,
    iterations: int,
    repeats: int,
    warmups: int,
    isolate_process: bool,
    profile_runtime: bool,
    profile_stream: bool,
    interleave_loops: bool,
    child_retries: int,
) -> tuple[list[BenchResult], list[dict[str, Any]] | None]:
    sample_map: dict[str, list[float]] = {loop_name: [] for loop_name in loops}
    profile_map: dict[str, list[dict[str, Any]]] = {loop_name: [] for loop_name in loops}
    round_records: list[dict[str, Any]] = []

    if interleave_loops and len(loops) > 1:
        for warmup_index in range(warmups):
            order = rotated_order(loops, warmup_index)
            for loop_name in order:
                run_once(
                    loop_name,
                    spec,
                    iterations,
                    profile_runtime,
                    profile_stream,
                    isolate_process,
                    child_retries,
                )

        for repeat_index in range(repeats):
            order = rotated_order(loops, repeat_index)
            round_record: dict[str, Any] = {
                "index": repeat_index,
                "order": order,
                "samples": {},
            }
            retries_used: dict[str, int] = {}
            for loop_name in order:
                sample, profile, retries_used_count = run_once(
                    loop_name,
                    spec,
                    iterations,
                    profile_runtime,
                    profile_stream,
                    isolate_process,
                    child_retries,
                )
                sample_map[loop_name].append(sample)
                round_record["samples"][loop_name] = sample
                if retries_used_count:
                    retries_used[loop_name] = retries_used_count
                if profile is not None:
                    profile_map[loop_name].append(profile)
            if retries_used:
                round_record["retries"] = retries_used
            round_records.append(round_record)
    else:
        for loop_name in loops:
            for _ in range(warmups):
                run_once(
                    loop_name,
                    spec,
                    iterations,
                    profile_runtime,
                    profile_stream,
                    isolate_process,
                    child_retries,
                )
            for repeat_index in range(repeats):
                sample, profile, retries_used_count = run_once(
                    loop_name,
                    spec,
                    iterations,
                    profile_runtime,
                    profile_stream,
                    isolate_process,
                    child_retries,
                )
                sample_map[loop_name].append(sample)
                if profile is not None:
                    profile_map[loop_name].append(profile)
                round_record = {
                    "index": repeat_index,
                    "order": [loop_name],
                    "samples": {loop_name: sample},
                }
                if retries_used_count:
                    round_record["retries"] = {loop_name: retries_used_count}
                round_records.append(round_record)

    results = [
        BenchResult(
            loop=loop_name,
            benchmark=spec.name,
            category=spec.category,
            description=spec.description,
            iterations=iterations,
            repeats=repeats,
            unit=spec.unit,
            samples=sample_map[loop_name],
            profiles=profile_map[loop_name] or None,
        )
        for loop_name in loops
    ]
    return results, round_records or None


def format_result(result: BenchResult) -> str:
    return (
        f"{result.loop:7} {result.benchmark:18} "
        f"median={result.median:.6f}s mean={result.mean:.6f}s "
        f"min={result.minimum:.6f}s max={result.maximum:.6f}s "
        f"mad={result.mad:.6f}s iqr={result.iqr:.6f}s "
        f"ns/{result.unit}={result.ns_per_unit:,.0f}"
    )


def compute_paired_comparisons(
    results: list[BenchResult],
    baseline: str,
    rounds_by_benchmark: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, BenchResult]] = {}
    for result in results:
        grouped.setdefault(result.benchmark, {})[result.loop] = result

    comparisons: list[dict[str, Any]] = []
    for benchmark, row in sorted(grouped.items()):
        baseline_result = row.get(baseline)
        if baseline_result is None:
            continue
        rounds = rounds_by_benchmark.get(benchmark, [])
        for loop_name, result in sorted(row.items()):
            ratios: list[float] = []
            wins = 0
            for round_record in rounds:
                samples = round_record.get("samples", {})
                if loop_name not in samples or baseline not in samples:
                    continue
                ratio = samples[loop_name] / samples[baseline]
                ratios.append(ratio)
                if loop_name != baseline and samples[loop_name] < samples[baseline]:
                    wins += 1
            comparisons.append(
                {
                    "benchmark": benchmark,
                    "loop": loop_name,
                    "baseline": baseline,
                    "ratio_to_baseline": result.median / baseline_result.median,
                    "paired_ratio_median": statistics.median(ratios) if ratios else None,
                    "paired_ratio_iqr": interquartile_range(ratios) if len(ratios) >= 2 else 0.0,
                    "paired_rounds": len(ratios),
                    "paired_wins": wins if loop_name != baseline else len(ratios),
                }
            )
    return comparisons


def emit_summary(
    results: list[BenchResult],
    baseline: str,
    rounds_by_benchmark: dict[str, list[dict[str, Any]]],
) -> None:
    by_benchmark: dict[str, dict[str, BenchResult]] = {}
    for result in results:
        by_benchmark.setdefault(result.benchmark, {})[result.loop] = result
    paired = {
        (item["benchmark"], item["loop"]): item
        for item in compute_paired_comparisons(results, baseline, rounds_by_benchmark)
    }

    print()
    print(
        f"{'benchmark':24} {'baseline':>10} {'kioto':>10} "
        f"{'kioto/x':>10} {'paired':>10} {'wins':>8}"
    )
    for benchmark in sorted(by_benchmark):
        row = by_benchmark[benchmark]
        baseline_result = row.get(baseline)
        kioto_result = row.get("kioto")
        if baseline_result is None or kioto_result is None:
            continue
        ratio = kioto_result.median / baseline_result.median
        paired_row = paired.get((benchmark, "kioto"))
        paired_ratio = paired_row["paired_ratio_median"] if paired_row else None
        wins = paired_row["paired_wins"] if paired_row else None
        rounds = paired_row["paired_rounds"] if paired_row else None
        print(
            f"{benchmark:24} "
            f"{baseline_result.median:10.6f} "
            f"{kioto_result.median:10.6f} "
            f"{ratio:10.3f}x "
            f"{(f'{paired_ratio:0.3f}x' if paired_ratio is not None else '-'):>10} "
            f"{(f'{wins}/{rounds}' if wins is not None and rounds is not None else '-'):>8}"
        )


def selected_benchmarks(args: argparse.Namespace) -> list[BenchmarkSpec]:
    if args.benchmark != "all":
        return [BENCHMARKS[args.benchmark]]
    if args.profile:
        return [BENCHMARKS[name] for name in PROFILE_BENCHMARKS[args.profile]]
    return [BENCHMARKS[name] for name in PROFILE_BENCHMARKS["full"]]


def environment_snapshot() -> dict[str, Any]:
    return {
        "timestamp_utc": dt.datetime.now(dt.UTC).isoformat(),
        "python": sys.version,
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "cwd": os.getcwd(),
        "pid": os.getpid(),
        "versions": {
            "kioto": maybe_version("kioto"),
            "uvloop": maybe_version("uvloop"),
        },
        "git": git_snapshot(),
    }


def result_payload(
    results: list[BenchResult],
    baseline: str,
    rounds_by_benchmark: dict[str, list[dict[str, Any]]],
    methodology: dict[str, Any],
) -> dict[str, Any]:
    payload_results = []
    for result in results:
        payload_results.append(
            {
                **asdict(result),
                "median": result.median,
                "mean": result.mean,
                "minimum": result.minimum,
                "maximum": result.maximum,
                "stdev": result.stdev,
                "mad": result.mad,
                "iqr": result.iqr,
                "coefficient_of_variation": result.coefficient_of_variation,
                "ns_per_unit": result.ns_per_unit,
            }
        )

    return {
        "environment": environment_snapshot(),
        "methodology": methodology,
        "baseline": baseline,
        "results": payload_results,
        "rounds": rounds_by_benchmark,
        "comparisons": compute_paired_comparisons(results, baseline, rounds_by_benchmark),
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
    parser.add_argument(
        "--no-interleave-loops",
        action="store_true",
        help="Disable round-robin loop interleaving for multi-loop comparisons.",
    )
    parser.add_argument(
        "--child-retries",
        type=int,
        default=1,
        help="Retry isolated child benchmark failures this many times before aborting.",
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
    rounds_by_benchmark: dict[str, list[dict[str, Any]]] = {}
    isolate_process = not args.no_isolate_process and not args.child_run
    interleave_loops = not args.no_interleave_loops and not args.child_run
    if (args.profile_runtime or args.profile_stream) and not isolate_process:
        raise SystemExit("runtime profiling requires subprocess isolation")
    for spec in specs:
        iterations = args.iterations or spec.default_iterations
        selected_loops = [loop_name for loop_name in loops if loop_name != "uvloop" or uvloop is not None]
        group_results, round_records = run_benchmark_group(
            selected_loops,
            spec,
            iterations,
            args.repeats,
            args.warmups,
            isolate_process,
            args.profile_runtime,
            args.profile_stream,
            interleave_loops,
            max(0, args.child_retries),
        )
        results.extend(group_results)
        if round_records is not None:
            rounds_by_benchmark[spec.name] = round_records
        if not args.json:
            for result in group_results:
                print(format_result(result), flush=True)

    payload = result_payload(
        results,
        args.baseline,
        rounds_by_benchmark,
        {
            "isolated_process": isolate_process,
            "interleaved_rounds": interleave_loops and len(loops) > 1,
            "child_retries": max(0, args.child_retries),
            "repeats": args.repeats,
            "warmups": args.warmups,
        },
    )
    write_output(payload, args.output)

    if args.json:
        print(json.dumps(payload, indent=2))
    elif len(loops) > 1:
        emit_summary(results, args.baseline, rounds_by_benchmark)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONASYNCIODEBUG", "0")
    main()

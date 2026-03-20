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
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import rsloop

try:
    import uvloop
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    uvloop = None


LoopFactory = Callable[[], asyncio.AbstractEventLoop] | None
BenchFn = Callable[[int], Awaitable[None]]


class PythonStreamProfiler:
    def __init__(self) -> None:
        self._orig_feed_data = asyncio.StreamReader.feed_data
        self._orig_feed_eof = asyncio.StreamReader.feed_eof
        self._orig_readexactly = asyncio.StreamReader.readexactly
        self._orig_write = asyncio.StreamWriter.write
        self._orig_drain = asyncio.StreamWriter.drain
        self._installed = False
        self._roles: dict[str, dict[str, Any]] = {}

    def _bucket(self, role: str) -> dict[str, Any]:
        bucket = self._roles.get(role)
        if bucket is None:
            bucket = {
                "feed_data_calls": 0,
                "feed_data_bytes": 0,
                "feed_data_sizes": [],
                "feed_eof_calls": 0,
                "readexactly_calls": 0,
                "readexactly_bytes": 0,
                "write_calls": 0,
                "write_bytes": 0,
                "drain_calls": 0,
            }
            self._roles[role] = bucket
        return bucket

    def install(self) -> None:
        if self._installed:
            return
        profiler = self

        def feed_data(reader: asyncio.StreamReader, data: bytes) -> None:
            role = getattr(reader, "_bench_stream_role", "unknown")
            bucket = profiler._bucket(role)
            bucket["feed_data_calls"] += 1
            bucket["feed_data_bytes"] += len(data)
            bucket["feed_data_sizes"].append(len(data))
            return profiler._orig_feed_data(reader, data)

        def feed_eof(reader: asyncio.StreamReader) -> None:
            role = getattr(reader, "_bench_stream_role", "unknown")
            profiler._bucket(role)["feed_eof_calls"] += 1
            return profiler._orig_feed_eof(reader)

        async def readexactly(reader: asyncio.StreamReader, n: int) -> bytes:
            role = getattr(reader, "_bench_stream_role", "unknown")
            bucket = profiler._bucket(role)
            bucket["readexactly_calls"] += 1
            bucket["readexactly_bytes"] += n
            return await profiler._orig_readexactly(reader, n)

        def write(writer: asyncio.StreamWriter, data: bytes) -> None:
            role = STREAM_WRITER_ROLES.get(id(writer), "unknown")
            bucket = profiler._bucket(role)
            bucket["write_calls"] += 1
            bucket["write_bytes"] += len(data)
            return profiler._orig_write(writer, data)

        async def drain(writer: asyncio.StreamWriter) -> None:
            role = STREAM_WRITER_ROLES.get(id(writer), "unknown")
            profiler._bucket(role)["drain_calls"] += 1
            return await profiler._orig_drain(writer)

        asyncio.StreamReader.feed_data = feed_data
        asyncio.StreamReader.feed_eof = feed_eof
        asyncio.StreamReader.readexactly = readexactly
        asyncio.StreamWriter.write = write
        asyncio.StreamWriter.drain = drain
        self._installed = True

    def emit(self) -> None:
        if not self._installed:
            return
        roles: dict[str, Any] = {}
        totals = {
            "feed_data_calls": 0,
            "feed_data_bytes": 0,
            "feed_eof_calls": 0,
            "readexactly_calls": 0,
            "readexactly_bytes": 0,
            "write_calls": 0,
            "write_bytes": 0,
            "drain_calls": 0,
        }
        for role, bucket in sorted(self._roles.items()):
            feed_sizes = list(bucket["feed_data_sizes"])
            role_payload = {
                "feed_data_calls": bucket["feed_data_calls"],
                "feed_data_bytes": bucket["feed_data_bytes"],
                "feed_data_mean": (
                    bucket["feed_data_bytes"] / bucket["feed_data_calls"]
                    if bucket["feed_data_calls"]
                    else 0.0
                ),
                "feed_data_p50": statistics.median(feed_sizes) if feed_sizes else 0.0,
                "feed_data_p95": percentile(feed_sizes, 0.95) if feed_sizes else 0.0,
                "feed_data_max": max(feed_sizes) if feed_sizes else 0,
                "feed_eof_calls": bucket["feed_eof_calls"],
                "readexactly_calls": bucket["readexactly_calls"],
                "readexactly_bytes": bucket["readexactly_bytes"],
                "write_calls": bucket["write_calls"],
                "write_bytes": bucket["write_bytes"],
                "drain_calls": bucket["drain_calls"],
            }
            roles[role] = role_payload
            for key in totals:
                totals[key] += role_payload[key]
        print(
            "BENCH_PROFILE_PY_STREAM_JSON "
            + json.dumps({"roles": roles, "totals": totals}, sort_keys=True),
            file=sys.stderr,
        )


PYTHON_STREAM_PROFILER: PythonStreamProfiler | None = None
STREAM_WRITER_ROLES: dict[int, str] = {}


def install_python_stream_profiler() -> None:
    global PYTHON_STREAM_PROFILER
    if PYTHON_STREAM_PROFILER is None:
        PYTHON_STREAM_PROFILER = PythonStreamProfiler()
        PYTHON_STREAM_PROFILER.install()


def emit_python_stream_profiler() -> None:
    if PYTHON_STREAM_PROFILER is not None:
        PYTHON_STREAM_PROFILER.emit()


def label_stream_endpoint(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, role: str
) -> None:
    setattr(reader, "_bench_stream_role", role)
    STREAM_WRITER_ROLES[id(writer)] = role


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
    python_stream_runs: list[dict[str, Any]] = []
    for line in stderr.splitlines():
        if line.startswith("RSLOOP_PROFILE_SCHED_JSON "):
            scheduler_runs.append(
                json.loads(line.removeprefix("RSLOOP_PROFILE_SCHED_JSON "))
            )
        elif line.startswith("RSLOOP_PROFILE_STREAM_JSON "):
            stream_runs.append(
                json.loads(line.removeprefix("RSLOOP_PROFILE_STREAM_JSON "))
            )
        elif line.startswith("BENCH_PROFILE_PY_STREAM_JSON "):
            python_stream_runs.append(
                json.loads(line.removeprefix("BENCH_PROFILE_PY_STREAM_JSON "))
            )
    if not scheduler_runs and not stream_runs and not python_stream_runs:
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
    if python_stream_runs:
        profile["python_stream_runs"] = python_stream_runs
        profile["python_stream"] = max(
            python_stream_runs,
            key=lambda item: item.get("totals", {}).get("feed_data_calls", 0),
        )
    return profile


def get_loop_factory(loop_name: str) -> LoopFactory:
    if loop_name == "asyncio":
        return None
    if loop_name == "rsloop":
        return rsloop.RsloopEventLoop
    if loop_name == "uvloop":
        if uvloop is None:
            raise RuntimeError("uvloop is not installed in this environment")
        return uvloop.new_event_loop
    raise ValueError(f"unknown loop {loop_name}")


def run_on_loop(loop_name: str, coro: Awaitable[None]) -> None:
    loop_factory = get_loop_factory(loop_name)
    with asyncio.Runner(loop_factory=loop_factory, debug=False) as runner:
        runner.run(coro)


def split_evenly(total: int, buckets: int) -> list[int]:
    if buckets <= 0:
        raise ValueError("buckets must be positive")
    base, remainder = divmod(total, buckets)
    return [base + (1 if index < remainder else 0) for index in range(buckets)]


def split_weighted(total: int, weights: list[int]) -> list[int]:
    if not weights or any(weight < 0 for weight in weights):
        raise ValueError("weights must be non-empty and non-negative")
    weight_total = sum(weights)
    if weight_total == 0:
        return split_evenly(total, len(weights))
    counts = [(total * weight) // weight_total for weight in weights]
    remaining = total - sum(counts)
    ranked = sorted(
        range(len(weights)),
        key=lambda index: (weights[index], -index),
        reverse=True,
    )
    for index in ranked[:remaining]:
        counts[index] += 1
    return counts


async def wait_for_fd(
    loop: asyncio.AbstractEventLoop,
    fd: int,
    *,
    readable: bool,
) -> None:
    future = loop.create_future()

    def on_ready() -> None:
        remover = loop.remove_reader if readable else loop.remove_writer
        remover(fd)
        if not future.done():
            future.set_result(None)

    registrar = loop.add_reader if readable else loop.add_writer
    remover = loop.remove_reader if readable else loop.remove_writer
    registrar(fd, on_ready)
    try:
        await future
    finally:
        remover(fd)


async def recv_exact_into(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    buf: memoryview,
) -> int:
    received = 0
    while received < len(buf):
        chunk = await loop.sock_recv_into(sock, buf[received:])
        if chunk == 0:
            raise RuntimeError(f"unexpected EOF while reading {len(buf)} bytes")
        received += chunk
    return received


async def udp_sendto(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    data: bytes,
    address: Any,
) -> None:
    while True:
        try:
            sent = sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):
            await wait_for_fd(loop, sock.fileno(), readable=False)
            continue
        if sent != len(data):
            raise RuntimeError(f"partial UDP send: {sent} != {len(data)}")
        return


async def udp_recvfrom(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    size: int,
) -> tuple[bytes, Any]:
    while True:
        try:
            return sock.recvfrom(size)
        except (BlockingIOError, InterruptedError):
            await wait_for_fd(loop, sock.fileno(), readable=True)


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


async def bench_call_soon_batched(count: int, batch_size: int = 8) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    scheduled = 0
    completed = 0

    def schedule_batch() -> None:
        nonlocal scheduled
        burst = min(batch_size, count - scheduled)
        for _ in range(burst):
            scheduled += 1
            loop.call_soon(tick)

    def tick() -> None:
        nonlocal completed
        completed += 1
        if completed == count and not done.done():
            done.set_result(None)
            return
        if scheduled < count:
            schedule_batch()

    schedule_batch()
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


async def bench_call_soon_threadsafe_multi(count: int, producers: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = count
    per_thread = split_evenly(count, producers)

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    def launch_threads() -> None:
        barrier = threading.Barrier(producers + 1)
        threads: list[threading.Thread] = []

        def producer(iterations: int) -> None:
            barrier.wait()
            for _ in range(iterations):
                loop.call_soon_threadsafe(tick)

        for iterations in per_thread:
            thread = threading.Thread(target=producer, args=(iterations,))
            thread.start()
            threads.append(thread)
        barrier.wait()
        for thread in threads:
            thread.join()

    await asyncio.to_thread(launch_threads)
    await done


async def bench_call_soon_threadsafe_multi_4(count: int) -> None:
    await bench_call_soon_threadsafe_multi(count, producers=4)


async def bench_call_soon_threadsafe_multi_8(count: int) -> None:
    await bench_call_soon_threadsafe_multi(count, producers=8)


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


async def bench_timer_staggered(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = count
    delays = (0.0, 0.0001, 0.0005, 0.001)

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    for index in range(count):
        loop.call_later(delays[index % len(delays)], tick)

    await done


async def bench_timer_cancel(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = 0
    handles: list[asyncio.Handle] = []
    delays = (0.0005, 0.001, 0.0015, 0.002)

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    for index in range(count):
        handles.append(loop.call_later(delays[index % len(delays)], tick))
    for index, handle in enumerate(handles):
        if index % 8 == 0:
            remaining += 1
        else:
            handle.cancel()

    if remaining == 0:
        return
    await done


async def bench_socketpair_small(count: int) -> None:
    await socketpair_roundtrip(count, payload_size=1)


async def bench_socketpair_large(count: int) -> None:
    await socketpair_roundtrip(count, payload_size=4096)


async def bench_sock_recv_into(count: int) -> None:
    await socketpair_roundtrip_into(count, payload_size=4096)


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


async def socketpair_roundtrip_into(count: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    payload = b"x" * payload_size
    left_buf = bytearray(payload_size)
    right_buf = bytearray(payload_size)
    left_view = memoryview(left_buf)
    right_view = memoryview(right_buf)

    try:
        for _ in range(count):
            await loop.sock_sendall(left, payload)
            await recv_exact_into(loop, right, right_view)
            if right_view[0] != payload[0] or right_view[-1] != payload[-1]:
                raise RuntimeError("unexpected payload contents on right socket")
            await loop.sock_sendall(right, payload)
            await recv_exact_into(loop, left, left_view)
            if left_view[0] != payload[0] or left_view[-1] != payload[-1]:
                raise RuntimeError("unexpected payload contents on left socket")
    finally:
        left_view.release()
        right_view.release()
        left.close()
        right.close()


async def bench_tcp_echo_small(count: int) -> None:
    await tcp_stream_echo(count, payload_size=1)


async def bench_tcp_echo_large(count: int) -> None:
    await tcp_stream_echo(count, payload_size=4096)


async def bench_tcp_echo_parallel(count: int) -> None:
    await tcp_stream_echo_parallel(count, clients=32, payload_size=64)


async def bench_tcp_echo_fragmented(count: int) -> None:
    await tcp_stream_echo_fragmented(count, payload_size=64, fragment_size=1)


async def bench_tcp_echo_burst_no_drain(count: int) -> None:
    await tcp_stream_echo_burst_no_drain(count, payload_size=64, burst=16)


async def bench_tcp_echo_backpressured(count: int) -> None:
    await tcp_stream_echo_backpressured(
        count,
        payload_size=16 * 1024,
        pause_every=8,
        pause_seconds=0.0005,
    )


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


async def bench_tcp_rpc_asymmetric_smallreq_hugeresp(count: int) -> None:
    await tcp_stream_rpc(count, request_size=64, response_size=64 * 1024)


async def bench_tcp_rpc_asymmetric_hugereq_smallresp(count: int) -> None:
    await tcp_stream_rpc(count, request_size=64 * 1024, response_size=64)


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


async def bench_tcp_echo_parallel_128(count: int) -> None:
    await tcp_stream_echo_parallel(count, clients=128, payload_size=64)


async def bench_tcp_rpc_pipeline_deep(count: int) -> None:
    await tcp_stream_rpc_pipeline(count, request_size=64, response_size=256, pipeline=64)


async def bench_tcp_rpc_pipeline_unbalanced(count: int) -> None:
    await tcp_stream_rpc_pipeline_unbalanced(
        count,
        heavy_clients=4,
        light_clients=28,
        request_size=64,
        response_size=256,
        pipeline=8,
    )


async def bench_tcp_stream_half_close(count: int) -> None:
    await tcp_stream_half_close(count, payload_size=4096)


async def bench_tcp_echo_mixed_parallel(count: int) -> None:
    await tcp_stream_echo_mixed_parallel(count, clients=16, payload_sizes=(64, 256, 1024, 4096))


async def bench_tcp_bulk(count: int) -> None:
    await tcp_stream_echo_parallel(count, clients=8, payload_size=65536)


async def bench_tcp_sock_small(count: int) -> None:
    await tcp_sock_echo(count, payload_size=1)


async def bench_tcp_sock_large(count: int) -> None:
    await tcp_sock_echo(count, payload_size=4096)


async def bench_tcp_sock_parallel(count: int) -> None:
    await tcp_sock_echo_parallel(count, clients=32, payload_size=64)


async def bench_udp_pingpong(count: int) -> None:
    await udp_pingpong(count, payload_size=64)


async def bench_udp_fanout(count: int) -> None:
    await udp_fanout(count, clients=32, payload_size=64)


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


async def bench_tcp_connect_parallel(count: int) -> None:
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

    async def client() -> None:
        _reader, writer = await asyncio.open_connection(host, port)
        writer.close()
        await writer.wait_closed()

    try:
        await asyncio.gather(*(client() for _ in range(count)))
        await done
    finally:
        server.close()
        await server.wait_closed()


async def bench_tcp_accept_burst(count: int) -> None:
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

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(256, count))
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> asyncio.StreamWriter:
        _reader, writer = await asyncio.open_connection(host, port)
        return writer

    clients: list[asyncio.StreamWriter] = []
    try:
        clients = list(await asyncio.gather(*(client() for _ in range(count))))
        await done
    finally:
        for writer in clients:
            writer.close()
        for writer in clients:
            await writer.wait_closed()
        for writer in peers:
            writer.close()
        for writer in peers:
            await writer.wait_closed()
        server.close()
        await server.wait_closed()


async def bench_tcp_churn_small_io(count: int) -> None:
    payload = b"x"

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await reader.readexactly(1)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
            writer.write(data)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(256, count))
    host, port = server.sockets[0].getsockname()[:2]

    try:
        for _ in range(count):
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(payload)
            await writer.drain()
            data = await reader.readexactly(1)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
            writer.close()
            await writer.wait_closed()
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


async def bench_tcp_idle_fanout_large(count: int) -> None:
    await bench_tcp_idle_fanout(count)


async def tcp_stream_echo(count: int, payload_size: int) -> None:
    payload = b"x" * payload_size

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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
        label_stream_endpoint(reader, writer, "client")
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


async def tcp_stream_echo_fragmented(
    count: int,
    payload_size: int,
    fragment_size: int,
) -> None:
    payload = b"x" * payload_size
    fragments = [
        payload[offset : offset + fragment_size]
        for offset in range(0, payload_size, fragment_size)
    ]

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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
        label_stream_endpoint(reader, writer, "client")
        for _ in range(count):
            for fragment in fragments:
                writer.write(fragment)
            await writer.drain()
            data = await reader.readexactly(payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_echo_burst_no_drain(
    count: int,
    payload_size: int,
    burst: int,
) -> None:
    payload = b"x" * payload_size
    burst = max(1, burst)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
        pending = 0
        try:
            while True:
                data = await reader.readexactly(payload_size)
                writer.write(data)
                pending += 1
                if pending >= burst:
                    await writer.drain()
                    pending = 0
        except asyncio.IncompleteReadError:
            pass
        finally:
            if pending:
                await writer.drain()
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        remaining = count
        while remaining:
            current = min(burst, remaining)
            for _ in range(current):
                writer.write(payload)
            await writer.drain()
            for _ in range(current):
                data = await reader.readexactly(payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
            remaining -= current
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_echo_backpressured(
    count: int,
    payload_size: int,
    pause_every: int,
    pause_seconds: float,
) -> None:
    payload = b"x" * payload_size

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
        transport = writer.transport
        seen = 0
        try:
            while True:
                if seen and seen % pause_every == 0:
                    transport.pause_reading()
                    await asyncio.sleep(pause_seconds)
                    transport.resume_reading()
                data = await reader.readexactly(payload_size)
                writer.write(data)
                await writer.drain()
                seen += 1
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
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
    per_client_counts = split_evenly(count, clients)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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

    async def client(iterations: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        try:
            for _ in range(iterations):
                writer.write(payload)
                await writer.drain()
                data = await reader.readexactly(payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_rpc(count: int, request_size: int, response_size: int) -> None:
    request = b"r" * request_size
    response = b"s" * response_size

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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
        label_stream_endpoint(reader, writer, "client")
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
    per_client_counts = split_evenly(count, clients)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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

    async def client(iterations: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        try:
            for _ in range(iterations):
                writer.write(request)
                await writer.drain()
                data = await reader.readexactly(response_size)
                if data != response:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
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
        label_stream_endpoint(reader, writer, "server")
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
        label_stream_endpoint(reader, writer, "client")
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
    per_client_counts = split_evenly(count, clients)
    pipeline = max(1, pipeline)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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

    async def client(iterations: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        try:
            remaining = iterations
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
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_rpc_pipeline_unbalanced(
    count: int,
    heavy_clients: int,
    light_clients: int,
    request_size: int,
    response_size: int,
    pipeline: int,
) -> None:
    request = b"r" * request_size
    response = b"s" * response_size
    pipeline = max(1, pipeline)
    weights = [8] * heavy_clients + [1] * light_clients
    per_client_counts = split_weighted(count, weights)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(128, len(weights)))
    host, port = server.sockets[0].getsockname()[:2]

    async def client(iterations: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        try:
            remaining = iterations
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
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_echo_mixed_parallel(
    count: int,
    clients: int,
    payload_sizes: tuple[int, ...],
) -> None:
    per_client_counts = split_evenly(count, clients)
    payloads = [bytes([65 + index % 26]) * size for index, size in enumerate(payload_sizes)]

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        label_stream_endpoint(reader, writer, "server")
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

    async def client(iterations: int) -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        try:
            for index in range(iterations):
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
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
    finally:
        server.close()
        await server.wait_closed()


async def tcp_stream_half_close(count: int, payload_size: int) -> None:
    payload = b"x" * payload_size
    expected = count * payload_size
    flush_every = 16

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        total = 0
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                total += len(data)
            writer.write(total.to_bytes(8, "big"))
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        pending = 0
        for _ in range(count):
            writer.write(payload)
            pending += 1
            if pending >= flush_every:
                await writer.drain()
                pending = 0
        if pending:
            await writer.drain()
        if not writer.can_write_eof():
            raise RuntimeError("transport does not support write_eof()")
        writer.write_eof()
        total = int.from_bytes(await reader.readexactly(8), "big")
        if total != expected:
            raise RuntimeError(f"unexpected byte count={total}")
        writer.close()
        await writer.wait_closed()
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


async def tcp_sock_echo_parallel(count: int, clients: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * payload_size
    per_client_counts = split_evenly(count, clients)

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(max(128, clients))
    listener.setblocking(False)
    host, port = listener.getsockname()[:2]
    server_tasks: list[asyncio.Task[None]] = []

    async def handle(conn: socket.socket, iterations: int) -> None:
        try:
            for _ in range(iterations):
                data = await recv_exact(loop, conn, payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
                await loop.sock_sendall(conn, data)
        finally:
            conn.close()

    async def acceptor() -> None:
        for iterations in per_client_counts:
            conn, _addr = await loop.sock_accept(listener)
            conn.setblocking(False)
            server_tasks.append(asyncio.create_task(handle(conn, iterations)))

    async def client(iterations: int) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        try:
            await loop.sock_connect(sock, (host, port))
            for _ in range(iterations):
                await loop.sock_sendall(sock, payload)
                data = await recv_exact(loop, sock, payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
        finally:
            sock.close()

    accept_task = asyncio.create_task(acceptor())
    try:
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
        await accept_task
        if server_tasks:
            await asyncio.gather(*server_tasks)
    finally:
        listener.close()


async def udp_pingpong(count: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * payload_size
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    client.bind(("127.0.0.1", 0))
    server.setblocking(False)
    client.setblocking(False)
    server_addr = server.getsockname()

    async def server_loop() -> None:
        for _ in range(count):
            data, addr = await udp_recvfrom(loop, server, payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
            await udp_sendto(loop, server, data, addr)

    server_task = asyncio.create_task(server_loop())
    try:
        for _ in range(count):
            await udp_sendto(loop, client, payload, server_addr)
            data, _addr = await udp_recvfrom(loop, client, payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
        await server_task
    finally:
        client.close()
        server.close()


async def udp_fanout(count: int, clients: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * payload_size
    per_client_counts = split_evenly(count, clients)
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    server.setblocking(False)
    server_addr = server.getsockname()

    async def server_loop() -> None:
        for _ in range(count):
            data, addr = await udp_recvfrom(loop, server, payload_size)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
            await udp_sendto(loop, server, data, addr)

    async def client(iterations: int) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        sock.setblocking(False)
        try:
            for _ in range(iterations):
                await udp_sendto(loop, sock, payload, server_addr)
                data, _addr = await udp_recvfrom(loop, sock, payload_size)
                if data != payload:
                    raise RuntimeError(f"unexpected payload size={len(data)}")
        finally:
            sock.close()

    server_task = asyncio.create_task(server_loop())
    try:
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
        await server_task
    finally:
        server.close()


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
    "call_soon_batched": BenchmarkSpec(
        name="call_soon_batched",
        description="Ready-queue refill churn where callbacks enqueue more callbacks",
        category="scheduler",
        default_iterations=100_000,
        unit="callbacks",
        func=bench_call_soon_batched,
    ),
    "call_soon_threadsafe_multi_4": BenchmarkSpec(
        name="call_soon_threadsafe_multi_4",
        description="Cross-thread scheduling from 4 concurrent producer threads",
        category="scheduler",
        default_iterations=50_000,
        unit="callbacks",
        func=bench_call_soon_threadsafe_multi_4,
    ),
    "call_soon_threadsafe_multi_8": BenchmarkSpec(
        name="call_soon_threadsafe_multi_8",
        description="Cross-thread scheduling from 8 concurrent producer threads",
        category="scheduler",
        default_iterations=50_000,
        unit="callbacks",
        func=bench_call_soon_threadsafe_multi_8,
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
    "timer_staggered": BenchmarkSpec(
        name="timer_staggered",
        description="Mixed near-future timers with staggered deadlines",
        category="scheduler",
        default_iterations=20_000,
        unit="timers",
        func=bench_timer_staggered,
    ),
    "timer_cancel": BenchmarkSpec(
        name="timer_cancel",
        description="Timer scheduling churn with most timers cancelled before firing",
        category="scheduler",
        default_iterations=20_000,
        unit="timers",
        func=bench_timer_cancel,
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
    "sock_recv_into": BenchmarkSpec(
        name="sock_recv_into",
        description="Raw socketpair ping-pong using loop.sock_recv_into() with reusable 4 KiB buffers",
        category="raw-socket",
        default_iterations=5_000,
        unit="roundtrips",
        func=bench_sock_recv_into,
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
    "tcp_echo_fragmented": BenchmarkSpec(
        name="tcp_echo_fragmented",
        description="Single TCP stream echo where each 64-byte message is written 1 byte at a time",
        category="stream",
        default_iterations=8_192,
        unit="messages",
        func=bench_tcp_echo_fragmented,
    ),
    "tcp_echo_burst_no_drain": BenchmarkSpec(
        name="tcp_echo_burst_no_drain",
        description="Single TCP stream echo with bursts of writes before each drain()",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_echo_burst_no_drain,
    ),
    "tcp_echo_backpressured": BenchmarkSpec(
        name="tcp_echo_backpressured",
        description="Single TCP stream echo with periodic server-side read pauses to force backpressure",
        category="stream",
        default_iterations=1_024,
        unit="messages",
        func=bench_tcp_echo_backpressured,
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
    "tcp_rpc_asymmetric_smallreq_hugeresp": BenchmarkSpec(
        name="tcp_rpc_asymmetric_smallreq_hugeresp",
        description="Single TCP request/response stream with 64-byte requests and 64 KiB responses",
        category="stream",
        default_iterations=512,
        unit="roundtrips",
        func=bench_tcp_rpc_asymmetric_smallreq_hugeresp,
    ),
    "tcp_rpc_asymmetric_hugereq_smallresp": BenchmarkSpec(
        name="tcp_rpc_asymmetric_hugereq_smallresp",
        description="Single TCP request/response stream with 64 KiB requests and 64-byte responses",
        category="stream",
        default_iterations=512,
        unit="roundtrips",
        func=bench_tcp_rpc_asymmetric_hugereq_smallresp,
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
    "tcp_echo_parallel_128": BenchmarkSpec(
        name="tcp_echo_parallel_128",
        description="128 concurrent stream clients with 64-byte echo payloads",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_echo_parallel_128,
    ),
    "tcp_rpc_pipeline_deep": BenchmarkSpec(
        name="tcp_rpc_pipeline_deep",
        description="Single TCP request/response stream with pipeline depth 64",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_rpc_pipeline_deep,
    ),
    "tcp_rpc_pipeline_unbalanced": BenchmarkSpec(
        name="tcp_rpc_pipeline_unbalanced",
        description="32 concurrent pipelined TCP RPC clients with heavily skewed per-client message counts",
        category="stream",
        default_iterations=16_384,
        unit="messages",
        func=bench_tcp_rpc_pipeline_unbalanced,
    ),
    "tcp_stream_half_close": BenchmarkSpec(
        name="tcp_stream_half_close",
        description="Client writes a stream, half-closes with write_eof(), then reads the server summary",
        category="stream",
        default_iterations=512,
        unit="messages",
        func=bench_tcp_stream_half_close,
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
    "tcp_sock_4k": BenchmarkSpec(
        name="tcp_sock_4k",
        description="TCP echo through loop.sock_* helpers with 4 KiB payloads",
        category="raw-socket",
        default_iterations=5_000,
        unit="roundtrips",
        func=bench_tcp_sock_large,
    ),
    "tcp_sock_parallel": BenchmarkSpec(
        name="tcp_sock_parallel",
        description="32 concurrent TCP echo clients through loop.sock_* helpers with 64-byte payloads",
        category="raw-socket",
        default_iterations=8_192,
        unit="messages",
        func=bench_tcp_sock_parallel,
    ),
    "udp_pingpong": BenchmarkSpec(
        name="udp_pingpong",
        description="Single UDP ping-pong using nonblocking sockets and loop readiness callbacks",
        category="raw-socket",
        default_iterations=20_000,
        unit="roundtrips",
        func=bench_udp_pingpong,
    ),
    "udp_fanout": BenchmarkSpec(
        name="udp_fanout",
        description="32 concurrent UDP clients ping-ponging through one local server socket",
        category="raw-socket",
        default_iterations=8_192,
        unit="messages",
        func=bench_udp_fanout,
    ),
    "tcp_connect": BenchmarkSpec(
        name="tcp_connect",
        description="Connection open/close churn against a local server",
        category="connection",
        default_iterations=64,
        unit="connections",
        func=bench_tcp_connect_close,
    ),
    "tcp_connect_parallel": BenchmarkSpec(
        name="tcp_connect_parallel",
        description="Many TCP connections opened and closed concurrently against a local server",
        category="connection",
        default_iterations=256,
        unit="connections",
        func=bench_tcp_connect_parallel,
    ),
    "tcp_accept_burst": BenchmarkSpec(
        name="tcp_accept_burst",
        description="Burst open many TCP connections concurrently and hold them until the server accepts all of them",
        category="connection",
        default_iterations=256,
        unit="connections",
        func=bench_tcp_accept_burst,
    ),
    "tcp_churn_small_io": BenchmarkSpec(
        name="tcp_churn_small_io",
        description="Connect, exchange a 1-byte request/response, then close",
        category="connection",
        default_iterations=128,
        unit="connections",
        func=bench_tcp_churn_small_io,
    ),
    "tcp_idle_fanout": BenchmarkSpec(
        name="tcp_idle_fanout",
        description="Open many idle TCP connections and tear them down",
        category="connection",
        default_iterations=128,
        unit="connections",
        func=bench_tcp_idle_fanout,
    ),
    "tcp_idle_fanout_large": BenchmarkSpec(
        name="tcp_idle_fanout_large",
        description="Open a larger set of idle TCP connections and tear them down",
        category="connection",
        default_iterations=512,
        unit="connections",
        func=bench_tcp_idle_fanout_large,
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
    profile_python_streams: bool,
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
        profile_python_streams,
        child_retries,
    )


def run_once_isolated(
    loop_name: str,
    spec: BenchmarkSpec,
    iterations: int,
    profile_runtime: bool,
    profile_stream: bool,
    profile_python_streams: bool,
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
        if loop_name == "rsloop" and profile_runtime:
            env["RSLOOP_PROFILE_SCHED_JSON"] = "1"
        if loop_name == "rsloop" and profile_stream:
            env["RSLOOP_PROFILE_SCHED_JSON"] = "1"
            env["RSLOOP_PROFILE_STREAM_JSON"] = "1"
        if profile_python_streams:
            env["BENCH_PROFILE_PY_STREAM_JSON"] = "1"
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
    profile_python_streams: bool,
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
                    profile_python_streams,
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
                    profile_python_streams,
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
                    profile_python_streams,
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
                    profile_python_streams,
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
        f"{'benchmark':24} {'baseline':>10} {'rsloop':>10} "
        f"{'rsloop/x':>10} {'paired':>10} {'wins':>8}"
    )
    for benchmark in sorted(by_benchmark):
        row = by_benchmark[benchmark]
        baseline_result = row.get(baseline)
        rsloop_result = row.get("rsloop")
        if baseline_result is None or rsloop_result is None:
            continue
        ratio = rsloop_result.median / baseline_result.median
        paired_row = paired.get((benchmark, "rsloop"))
        paired_ratio = paired_row["paired_ratio_median"] if paired_row else None
        wins = paired_row["paired_wins"] if paired_row else None
        rounds = paired_row["paired_rounds"] if paired_row else None
        print(
            f"{benchmark:24} "
            f"{baseline_result.median:10.6f} "
            f"{rsloop_result.median:10.6f} "
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
            "rsloop": maybe_version("rsloop"),
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
    parser.add_argument("--loop", choices=["asyncio", "rsloop", "uvloop", "all"], default="all")
    parser.add_argument("--benchmark", choices=[*BENCHMARKS.keys(), "all"], default="all")
    parser.add_argument("--profile", choices=sorted(PROFILE_BENCHMARKS), default="default")
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--baseline", choices=["asyncio", "uvloop", "rsloop"], default="uvloop")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--list", action="store_true")
    parser.add_argument(
        "--profile-runtime",
        action="store_true",
        help="Capture Rsloop scheduler/runtime profile data for isolated child runs.",
    )
    parser.add_argument(
        "--profile-stream",
        action="store_true",
        help="Capture Rsloop stream event traces for isolated child runs.",
    )
    parser.add_argument(
        "--profile-python-streams",
        action="store_true",
        help="Capture cross-loop Python stream delivery/write/drain counters for isolated child runs.",
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
    profile_python_streams = args.profile_python_streams or (
        os.environ.get("BENCH_PROFILE_PY_STREAM_JSON") == "1"
    )

    if args.list:
        for name, spec in BENCHMARKS.items():
            print(
                f"{name:18} [{spec.category}] default={spec.default_iterations} {spec.unit} "
                f"- {spec.description}"
            )
        return

    loops = ["asyncio", "uvloop", "rsloop"] if args.loop == "all" else [args.loop]
    specs = selected_benchmarks(args)

    results: list[BenchResult] = []
    rounds_by_benchmark: dict[str, list[dict[str, Any]]] = {}
    isolate_process = not args.no_isolate_process and not args.child_run
    interleave_loops = not args.no_interleave_loops and not args.child_run
    if (args.profile_runtime or args.profile_stream or profile_python_streams) and not isolate_process and not args.child_run:
        raise SystemExit("profiling requires subprocess isolation")
    if profile_python_streams:
        install_python_stream_profiler()
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
            profile_python_streams,
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
    if profile_python_streams:
        emit_python_stream_profiler()


if __name__ == "__main__":
    os.environ.setdefault("PYTHONASYNCIODEBUG", "0")
    main()

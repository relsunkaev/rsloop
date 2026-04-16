from __future__ import annotations

import argparse
import asyncio
import base64
import datetime as dt
import errno
import hashlib
import importlib.metadata as importlib_metadata
import json
import os
import platform
import shlex
import signal
import ssl
import struct
import subprocess
import socket
import statistics
import sys
import tempfile
import threading
import time
from asyncio import sslproto as _sslproto
from contextlib import contextmanager
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
ABORTIVE_LINGER = struct.pack("ii", 1, 0)
ISOLATED_CONNECTION_COOLDOWN = 0.1
CONNECTION_BENCHMARK_COOLDOWN = 0.5


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
SSLPROTO_PROFILER: SslProtoProfiler | None = None
PYTHON_CPU_PROFILER: PythonCpuProfiler | None = None
APP_PHASE_PROFILER: AppPhaseProfiler | None = None
STREAM_WRITER_ROLES: dict[int, str] = {}


def install_python_stream_profiler() -> None:
    global PYTHON_STREAM_PROFILER
    if PYTHON_STREAM_PROFILER is None:
        PYTHON_STREAM_PROFILER = PythonStreamProfiler()
        PYTHON_STREAM_PROFILER.install()


def emit_python_stream_profiler() -> None:
    if PYTHON_STREAM_PROFILER is not None:
        PYTHON_STREAM_PROFILER.emit()


class SslProtoProfiler:
    _METHODS = (
        "buffer_updated",
        "_do_read",
        "_do_read__copied",
        "_do_read__buffered",
        "_process_outgoing",
        "_write_appdata",
        "_control_ssl_reading",
        "_on_handshake_complete",
    )

    def __init__(self) -> None:
        self._installed = False
        self._originals: dict[str, Any] = {}
        self._methods: dict[str, dict[str, Any]] = {}

    def _bucket(self, method: str, state: str, mode: str) -> dict[str, Any]:
        key = f"{method}|{state}|{mode}"
        bucket = self._methods.get(key)
        if bucket is None:
            bucket = {
                "method": method,
                "state": state,
                "mode": mode,
                "count": 0,
                "total_seconds": 0.0,
                "max_seconds": 0.0,
                "exceptions": 0,
                "payload_bytes": 0,
            }
            self._methods[key] = bucket
        return bucket

    def install(self) -> None:
        if self._installed:
            return
        profiler = self
        protocol_types = [_sslproto.SSLProtocol]
        try:
            from rsloop.sslproto import RsloopSSLProtocol
        except Exception:
            RsloopSSLProtocol = None
        if RsloopSSLProtocol is not None:
            protocol_types.append(RsloopSSLProtocol)

        def wrap_method(ssl_protocol: type[Any], name: str) -> None:
            if name not in ssl_protocol.__dict__:
                return
            key = f"{ssl_protocol.__module__}.{ssl_protocol.__name__}.{name}"
            original = getattr(ssl_protocol, name)
            profiler._originals[key] = original

            def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
                state_obj = getattr(self, "_state", None)
                state = getattr(state_obj, "name", str(state_obj))
                mode = (
                    "buffered"
                    if getattr(self, "_app_protocol_is_buffer", False)
                    else "copied"
                )
                payload_bytes = 0
                if name == "buffer_updated" and args:
                    payload_bytes = int(args[0])
                elif name == "_write_appdata" and args:
                    payload_bytes = sum(len(chunk) for chunk in args[0])
                bucket = profiler._bucket(name, state, mode)
                bucket["owner"] = ssl_protocol.__name__
                started = time.perf_counter()
                try:
                    return original(self, *args, **kwargs)
                except Exception:
                    bucket["exceptions"] += 1
                    raise
                finally:
                    elapsed = time.perf_counter() - started
                    bucket["count"] += 1
                    bucket["total_seconds"] += elapsed
                    bucket["max_seconds"] = max(bucket["max_seconds"], elapsed)
                    bucket["payload_bytes"] += payload_bytes

            setattr(ssl_protocol, name, wrapped)

        for ssl_protocol in protocol_types:
            for name in self._METHODS:
                wrap_method(ssl_protocol, name)
        self._installed = True

    def emit(self) -> None:
        if not self._installed:
            return
        methods = sorted(
            self._methods.values(),
            key=lambda bucket: (
                bucket["total_seconds"],
                bucket["count"],
                bucket["method"],
            ),
            reverse=True,
        )
        print(
            "BENCH_PROFILE_SSLPROTO_JSON "
            + json.dumps({"methods": methods}, sort_keys=True),
            file=sys.stderr,
        )


def install_sslproto_profiler() -> None:
    global SSLPROTO_PROFILER
    if SSLPROTO_PROFILER is None:
        SSLPROTO_PROFILER = SslProtoProfiler()
        SSLPROTO_PROFILER.install()


def emit_sslproto_profiler() -> None:
    if SSLPROTO_PROFILER is not None:
        SSLPROTO_PROFILER.emit()


class PythonCpuProfiler:
    def __init__(self, interval: float = 0.001) -> None:
        from pyinstrument import Profiler
        from pyinstrument.renderers import JSONRenderer

        self._profiler = Profiler(interval=interval)
        self._renderer = JSONRenderer()
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._profiler.start()
        self._started = True

    def emit(self) -> None:
        if not self._started:
            return
        self._profiler.stop()
        payload = json.loads(self._profiler.output(self._renderer))
        print(
            "BENCH_PROFILE_CPU_JSON " + json.dumps(payload, sort_keys=True),
            file=sys.stderr,
        )
        self._started = False


@dataclass
class AppPhaseBucket:
    count: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0
    total_bytes: int = 0


class AppPhaseProfiler:
    def __init__(self) -> None:
        self._buckets: dict[str, AppPhaseBucket] = {}

    def record(self, phase: str, elapsed: float, *, payload_bytes: int = 0) -> None:
        bucket = self._buckets.get(phase)
        if bucket is None:
            bucket = AppPhaseBucket()
            self._buckets[phase] = bucket
        bucket.count += 1
        bucket.total_seconds += elapsed
        bucket.max_seconds = max(bucket.max_seconds, elapsed)
        bucket.total_bytes += payload_bytes

    def emit(self) -> None:
        phases: dict[str, Any] = {}
        for phase, bucket in sorted(self._buckets.items()):
            phases[phase] = {
                "count": bucket.count,
                "total_seconds": bucket.total_seconds,
                "mean_seconds": (
                    bucket.total_seconds / bucket.count if bucket.count else 0.0
                ),
                "max_seconds": bucket.max_seconds,
                "total_bytes": bucket.total_bytes,
            }
        print(
            "BENCH_PROFILE_APP_PHASE_JSON "
            + json.dumps({"phases": phases}, sort_keys=True),
            file=sys.stderr,
        )


def install_python_cpu_profiler() -> None:
    global PYTHON_CPU_PROFILER
    if PYTHON_CPU_PROFILER is None:
        interval = float(os.environ.get("BENCH_PROFILE_CPU_INTERVAL", "0.001"))
        PYTHON_CPU_PROFILER = PythonCpuProfiler(interval)


def emit_python_cpu_profiler() -> None:
    if PYTHON_CPU_PROFILER is not None:
        PYTHON_CPU_PROFILER.emit()


def install_app_phase_profiler() -> None:
    global APP_PHASE_PROFILER
    if APP_PHASE_PROFILER is None:
        APP_PHASE_PROFILER = AppPhaseProfiler()


def emit_app_phase_profiler() -> None:
    if APP_PHASE_PROFILER is not None:
        APP_PHASE_PROFILER.emit()


@contextmanager
def app_phase_scope(phase: str, *, payload_bytes: int = 0) -> Any:
    if APP_PHASE_PROFILER is None:
        yield
        return
    started = time.perf_counter()
    try:
        yield
    finally:
        APP_PHASE_PROFILER.record(
            phase,
            time.perf_counter() - started,
            payload_bytes=payload_bytes,
        )


def label_stream_endpoint(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, role: str
) -> None:
    setattr(reader, "_bench_stream_role", role)
    STREAM_WRITER_ROLES[id(writer)] = role


def enable_abortive_close(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, ABORTIVE_LINGER)
    except (AttributeError, OSError, ValueError):
        return


async def close_writer(writer: asyncio.StreamWriter, *, abortive: bool = False) -> None:
    if abortive:
        enable_abortive_close(writer)
    writer.close()
    try:
        await writer.wait_closed()
    except (BrokenPipeError, ConnectionResetError, OSError):
        if not abortive:
            raise


async def close_writers(
    writers: list[asyncio.StreamWriter], *, abortive: bool = False
) -> None:
    for writer in writers:
        if abortive:
            enable_abortive_close(writer)
        writer.close()
    for writer in writers:
        try:
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, OSError):
            if not abortive:
                raise


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


class BenchmarkSkipped(RuntimeError):
    pass


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
    onearg_runs: list[dict[str, Any]] = []
    python_stream_runs: list[dict[str, Any]] = []
    sslproto_runs: list[dict[str, Any]] = []
    cpu_runs: list[dict[str, Any]] = []
    app_phase_runs: list[dict[str, Any]] = []
    for line in stderr.splitlines():
        if line.startswith("RSLOOP_PROFILE_SCHED_JSON "):
            scheduler_runs.append(
                json.loads(line.removeprefix("RSLOOP_PROFILE_SCHED_JSON "))
            )
        elif line.startswith("RSLOOP_PROFILE_STREAM_JSON "):
            stream_runs.append(
                json.loads(line.removeprefix("RSLOOP_PROFILE_STREAM_JSON "))
            )
        elif line.startswith("RSLOOP_PROFILE_ONEARG_JSON "):
            onearg_runs.append(
                json.loads(line.removeprefix("RSLOOP_PROFILE_ONEARG_JSON "))
            )
        elif line.startswith("BENCH_PROFILE_PY_STREAM_JSON "):
            python_stream_runs.append(
                json.loads(line.removeprefix("BENCH_PROFILE_PY_STREAM_JSON "))
            )
        elif line.startswith("BENCH_PROFILE_SSLPROTO_JSON "):
            sslproto_runs.append(
                json.loads(line.removeprefix("BENCH_PROFILE_SSLPROTO_JSON "))
            )
        elif line.startswith("BENCH_PROFILE_CPU_JSON "):
            cpu_runs.append(json.loads(line.removeprefix("BENCH_PROFILE_CPU_JSON ")))
        elif line.startswith("BENCH_PROFILE_APP_PHASE_JSON "):
            app_phase_runs.append(
                json.loads(line.removeprefix("BENCH_PROFILE_APP_PHASE_JSON "))
            )
    if (
        not scheduler_runs
        and not stream_runs
        and not onearg_runs
        and not python_stream_runs
        and not sslproto_runs
        and not cpu_runs
        and not app_phase_runs
    ):
        return None
    profile: dict[str, Any] = {}
    if scheduler_runs:
        profile["scheduler_runs"] = scheduler_runs
    if stream_runs:
        profile["stream_runs"] = stream_runs
    if onearg_runs:
        profile["onearg_runs"] = onearg_runs
    if python_stream_runs:
        profile["python_stream_runs"] = python_stream_runs
    if sslproto_runs:
        profile["sslproto_runs"] = sslproto_runs
    if cpu_runs:
        profile["cpu_runs"] = cpu_runs
    if app_phase_runs:
        profile["app_phase_runs"] = app_phase_runs
    return profile


def get_loop_factory(loop_name: str) -> LoopFactory:
    if loop_name == "asyncio":
        return None
    if loop_name == "rsloop":
        return rsloop.new_event_loop
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
    await tcp_stream_rpc_parallel(
        count, clients=16, request_size=16 * 1024, response_size=1
    )


async def bench_tcp_download_parallel(count: int) -> None:
    await tcp_stream_rpc_parallel(
        count, clients=16, request_size=1, response_size=16 * 1024
    )


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
    await tcp_stream_rpc_pipeline(
        count, request_size=64, response_size=256, pipeline=64
    )


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
    await tcp_stream_echo_mixed_parallel(
        count, clients=16, payload_sizes=(64, 256, 1024, 4096)
    )


async def bench_tcp_bulk(count: int) -> None:
    await tcp_stream_echo_parallel(count, clients=8, payload_size=65536)


async def bench_tcp_sock_small(count: int) -> None:
    await tcp_sock_echo(count, payload_size=1)


async def bench_tcp_sock_large(count: int) -> None:
    await tcp_sock_echo(count, payload_size=4096)


async def bench_tcp_sock_parallel(count: int) -> None:
    await tcp_sock_echo_parallel(count, clients=32, payload_size=64)


async def bench_tcp_protocol_echo(count: int) -> None:
    await tcp_protocol_echo(count, payload_size=64)


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
        await close_writer(writer)
        if accepted == count and not done.done():
            done.set_result(None)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(256, count))
    host, port = server.sockets[0].getsockname()[:2]

    try:
        for _ in range(count):
            reader, writer = await asyncio.open_connection(host, port)
            await close_writer(writer)
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
        await close_writer(writer)
        if accepted == count and not done.done():
            done.set_result(None)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=max(256, count))
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> None:
        _reader, writer = await asyncio.open_connection(host, port)
        await close_writer(writer)

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
        while True:
            try:
                _reader, writer = await asyncio.open_connection(host, port)
            except TimeoutError:
                await asyncio.sleep(0)
                continue
            except OSError as exc:
                if exc.errno in {errno.ETIMEDOUT, errno.ECONNREFUSED, 60}:
                    await asyncio.sleep(0)
                    continue
                raise
            return writer

    clients: list[asyncio.StreamWriter] = []
    try:
        batch_size = 8
        for start in range(0, count, batch_size):
            clients.extend(
                await asyncio.gather(
                    *(client() for _ in range(min(batch_size, count - start)))
                )
            )
            await asyncio.sleep(0)
        await done
    finally:
        await close_writers(clients)
        await close_writers(peers)
        server.close()
        await server.wait_closed()


async def bench_tcp_churn_small_io(count: int) -> None:
    payload = b"x"

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            data = await reader.readexactly(1)
            if data != payload:
                raise RuntimeError(f"unexpected payload size={len(data)}")
            writer.write(data)
            await writer.drain()
        finally:
            await close_writer(writer)

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
            await close_writer(writer)
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
        await close_writers(writers, abortive=True)
        await close_writers(peers, abortive=True)
        server.close()
        await server.wait_closed()


async def bench_tcp_idle_fanout_large(count: int) -> None:
    await bench_tcp_idle_fanout(count)


async def tcp_stream_echo(count: int, payload_size: int) -> None:
    payload = b"x" * payload_size

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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


async def tcp_protocol_echo(count: int, payload_size: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * payload_size
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
            self.pending = bytearray()
            self.completed = 0

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport  # type: ignore[assignment]
            self.transport.write(payload)

        def data_received(self, data: bytes) -> None:
            self.pending.extend(data)
            while len(self.pending) >= payload_size:
                chunk = bytes(self.pending[:payload_size])
                del self.pending[:payload_size]
                if chunk != payload:
                    if not done.done():
                        done.set_exception(
                            RuntimeError(f"unexpected payload size={len(chunk)}")
                        )
                    assert self.transport is not None
                    self.transport.close()
                    return
                self.completed += 1
                if self.completed == count:
                    if not done.done():
                        done.set_result(None)
                    assert self.transport is not None
                    self.transport.close()
                    return
                assert self.transport is not None
                self.transport.write(payload)

        def connection_lost(self, exc: BaseException | None) -> None:
            if exc is not None and not done.done():
                done.set_exception(exc)

    server = await loop.create_server(EchoServer, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        transport, _protocol = await loop.create_connection(Client, host, port)
        try:
            await done
        finally:
            transport.close()
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    server = await asyncio.start_server(
        handle, "127.0.0.1", 0, backlog=max(128, len(weights))
    )
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
    payloads = [
        bytes([65 + index % 26]) * size for index, size in enumerate(payload_sizes)
    ]

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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


def build_http1_request_head(
    method: bytes,
    path: bytes,
    body_size: int,
    *,
    keep_alive: bool = True,
    content_type: bytes = b"application/octet-stream",
) -> bytes:
    connection = b"keep-alive" if keep_alive else b"close"
    return (
        method
        + b" "
        + path
        + b" HTTP/1.1\r\nHost: localhost\r\nConnection: "
        + connection
        + b"\r\nContent-Length: "
        + str(body_size).encode()
        + b"\r\nContent-Type: "
        + content_type
        + b"\r\n\r\n"
    )


def build_http1_response_head(
    body_size: int,
    *,
    keep_alive: bool = True,
    content_type: bytes = b"application/octet-stream",
) -> bytes:
    connection = b"keep-alive" if keep_alive else b"close"
    return (
        b"HTTP/1.1 200 OK\r\nConnection: "
        + connection
        + b"\r\nContent-Length: "
        + str(body_size).encode()
        + b"\r\nContent-Type: "
        + content_type
        + b"\r\n\r\n"
    )


async def read_http_request_headers(
    reader: asyncio.StreamReader,
) -> tuple[bytes, bytes]:
    headers = await reader.readuntil(b"\r\n\r\n")
    request_line = headers.split(b"\r\n", 1)[0]
    parts = request_line.split(b" ", 2)
    if len(parts) != 3:
        raise RuntimeError(f"malformed HTTP request line: {request_line!r}")
    return headers, parts[1]


def http_header_value(headers: bytes, name: bytes) -> bytes:
    prefix = name.lower() + b": "
    for line in headers.split(b"\r\n")[1:]:
        lower = line.lower()
        if lower.startswith(prefix):
            return line[len(prefix) :]
    raise RuntimeError(f"missing HTTP header: {name!r}")


async def read_fixed_body(
    reader: asyncio.StreamReader,
    size: int,
    chunk_size: int | None = None,
) -> bytes:
    if size == 0:
        return b""
    if chunk_size is None or chunk_size >= size:
        return await reader.readexactly(size)
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        current = min(chunk_size, remaining)
        chunk = await reader.readexactly(current)
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


async def write_fixed_body(
    writer: asyncio.StreamWriter,
    body: bytes,
    chunk_size: int | None = None,
    *,
    flush: bool = False,
) -> None:
    if not body:
        return
    if chunk_size is None or chunk_size >= len(body):
        writer.write(body)
        if flush:
            await writer.drain()
        return
    for offset in range(0, len(body), chunk_size):
        writer.write(body[offset : offset + chunk_size])
        if flush:
            await writer.drain()


_TLS_CONTEXTS: dict[bool, tuple[Any, Any]] = {}


def tls_contexts(*, native_rsloop: bool = True) -> tuple[Any, Any]:
    """Return (server_context, client_context) for TLS benchmarks.

    When running under rsloop, returns ``RsloopTLSContext`` instances backed
    by the native rustls engine.  For asyncio and uvloop, returns standard
    ``ssl.SSLContext`` objects.
    """
    global _TLS_CONTEXTS
    cached = _TLS_CONTEXTS.get(native_rsloop)
    if cached is not None:
        return cached

    tempdir = Path(tempfile.mkdtemp(prefix="rsloop-bench-tls-"))
    certfile = tempdir / "cert.pem"
    keyfile = tempdir / "key.pem"
    completed = subprocess.run(
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
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError("failed to generate TLS test certificate")

    # For rsloop, use the native rustls TLS engine.
    if native_rsloop and os.environ.get("RSLOOP_BENCH_LOOP") == "rsloop":
        RsloopTLSContext = getattr(rsloop, "RsloopTLSContext", None)
        if RsloopTLSContext is not None:
            server_context = RsloopTLSContext.server(
                certfile.read_bytes(), keyfile.read_bytes()
            )
            client_context = RsloopTLSContext.client(verify=False)
            _TLS_CONTEXTS[native_rsloop] = (server_context, client_context)
            return _TLS_CONTEXTS[native_rsloop]

    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certfile=str(certfile), keyfile=str(keyfile))
    client_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    client_context.check_hostname = False
    client_context.verify_mode = ssl.CERT_NONE
    _TLS_CONTEXTS[native_rsloop] = (server_context, client_context)
    return _TLS_CONTEXTS[native_rsloop]


WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
WS_MASK_KEY = b"rswp"


def build_websocket_handshake_request(path: bytes) -> tuple[bytes, bytes]:
    key = base64.b64encode(path + b":rsloop").rstrip(b"=")
    request = (
        b"GET "
        + path
        + b" HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: "
        + key
        + b"\r\nSec-WebSocket-Version: 13\r\n\r\n"
    )
    return request, key


def build_websocket_accept(key: bytes) -> bytes:
    return base64.b64encode(hashlib.sha1(key + WS_GUID).digest())


def encode_websocket_frame(payload: bytes, *, masked: bool) -> bytes:
    opcode = 0x1
    first = 0x80 | opcode
    length = len(payload)
    mask_bit = 0x80 if masked else 0x00
    header = bytearray([first])
    if length < 126:
        header.append(mask_bit | length)
    elif length < 65536:
        header.append(mask_bit | 126)
        header.extend(length.to_bytes(2, "big"))
    else:
        header.append(mask_bit | 127)
        header.extend(length.to_bytes(8, "big"))
    if not masked:
        return bytes(header) + payload
    masked_payload = bytes(
        byte ^ WS_MASK_KEY[index % len(WS_MASK_KEY)]
        for index, byte in enumerate(payload)
    )
    return bytes(header) + WS_MASK_KEY + masked_payload


async def read_websocket_frame(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readexactly(2)
    masked = bool(header[1] & 0x80)
    length = header[1] & 0x7F
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), "big")
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), "big")
    mask_key = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(length)
    if masked:
        payload = bytes(
            byte ^ mask_key[index % len(mask_key)] for index, byte in enumerate(payload)
        )
    return payload


async def http1_roundtrip(
    count: int,
    *,
    clients: int = 1,
    request_method: bytes = b"POST",
    request_path: bytes = b"/",
    request_body_size: int,
    response_body_size: int,
    request_chunk_size: int | None = None,
    response_chunk_size: int | None = None,
    request_content_type: bytes = b"application/octet-stream",
    response_content_type: bytes = b"application/octet-stream",
    close_each_request: bool = False,
    server_sslcontext: ssl.SSLContext | None = None,
    client_sslcontext: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
) -> None:
    request_body = b"r" * request_body_size
    response_body = b"s" * response_body_size
    request_head = build_http1_request_head(
        request_method,
        request_path,
        request_body_size,
        keep_alive=not close_each_request,
        content_type=request_content_type,
    )
    response_head = build_http1_response_head(
        response_body_size,
        keep_alive=not close_each_request,
        content_type=response_content_type,
    )
    per_client_counts = split_evenly(count, clients)

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        label_stream_endpoint(reader, writer, "server")
        try:
            if close_each_request:
                with app_phase_scope("http.server.read_headers"):
                    await read_http_request_headers(reader)
                if request_body_size:
                    with app_phase_scope(
                        "http.server.read_body", payload_bytes=request_body_size
                    ):
                        body = await read_fixed_body(
                            reader, request_body_size, request_chunk_size
                        )
                    if body != request_body:
                        raise RuntimeError(f"unexpected request body size={len(body)}")
                with app_phase_scope(
                    "http.server.write_response_head",
                    payload_bytes=len(response_head),
                ):
                    writer.write(response_head)
                if response_chunk_size is None:
                    with app_phase_scope(
                        "http.server.write_response_body",
                        payload_bytes=response_body_size,
                    ):
                        writer.write(response_body)
                        await writer.drain()
                else:
                    with app_phase_scope(
                        "http.server.write_response_body",
                        payload_bytes=response_body_size,
                    ):
                        await write_fixed_body(
                            writer,
                            response_body,
                            response_chunk_size,
                            flush=True,
                        )
                return
            while True:
                with app_phase_scope("http.server.read_headers"):
                    await read_http_request_headers(reader)
                if request_body_size:
                    with app_phase_scope(
                        "http.server.read_body", payload_bytes=request_body_size
                    ):
                        body = await read_fixed_body(
                            reader, request_body_size, request_chunk_size
                        )
                    if body != request_body:
                        raise RuntimeError(f"unexpected request body size={len(body)}")
                with app_phase_scope(
                    "http.server.write_response_head",
                    payload_bytes=len(response_head),
                ):
                    writer.write(response_head)
                if response_chunk_size is None:
                    with app_phase_scope(
                        "http.server.write_response_body",
                        payload_bytes=response_body_size,
                    ):
                        writer.write(response_body)
                        await writer.drain()
                else:
                    with app_phase_scope(
                        "http.server.write_response_body",
                        payload_bytes=response_body_size,
                    ):
                        await write_fixed_body(
                            writer,
                            response_body,
                            response_chunk_size,
                            flush=True,
                        )
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(
        handle,
        "127.0.0.1",
        0,
        backlog=max(256, count),
        ssl=server_sslcontext,
    )
    host, port = server.sockets[0].getsockname()[:2]

    async def open_client_connection() -> tuple[
        asyncio.StreamReader, asyncio.StreamWriter
    ]:
        while True:
            try:
                with app_phase_scope("http.client.connect"):
                    return await asyncio.open_connection(
                        host,
                        port,
                        ssl=client_sslcontext,
                        server_hostname=server_hostname,
                    )
            except TimeoutError:
                await asyncio.sleep(0)
                continue
            except OSError as exc:
                if exc.errno in {errno.ETIMEDOUT, errno.ECONNREFUSED, 60}:
                    await asyncio.sleep(0)
                    continue
                raise

    async def client(iterations: int) -> None:
        if close_each_request:
            for index in range(iterations):
                reader, writer = await open_client_connection()
                label_stream_endpoint(reader, writer, "client")
                try:
                    with app_phase_scope(
                        "http.client.write_request_head",
                        payload_bytes=len(request_head),
                    ):
                        writer.write(request_head)
                    with app_phase_scope(
                        "http.client.write_request_body",
                        payload_bytes=request_body_size,
                    ):
                        await write_fixed_body(
                            writer,
                            request_body,
                            request_chunk_size,
                            flush=request_chunk_size is not None,
                        )
                    with app_phase_scope("http.client.drain"):
                        await writer.drain()
                    with app_phase_scope("http.client.read_response_head"):
                        await read_http_request_headers(reader)
                    with app_phase_scope(
                        "http.client.read_response_body",
                        payload_bytes=response_body_size,
                    ):
                        body = await read_fixed_body(
                            reader,
                            response_body_size,
                            response_chunk_size,
                        )
                    if body != response_body:
                        raise RuntimeError(f"unexpected response body size={len(body)}")
                finally:
                    writer.close()
                    await writer.wait_closed()
                if index % 4 == 0:
                    await asyncio.sleep(0)
            return

        reader, writer = await open_client_connection()
        label_stream_endpoint(reader, writer, "client")
        try:
            for _ in range(iterations):
                with app_phase_scope(
                    "http.client.write_request_head",
                    payload_bytes=len(request_head),
                ):
                    writer.write(request_head)
                with app_phase_scope(
                    "http.client.write_request_body",
                    payload_bytes=request_body_size,
                ):
                    await write_fixed_body(
                        writer,
                        request_body,
                        request_chunk_size,
                        flush=request_chunk_size is not None,
                    )
                with app_phase_scope("http.client.drain"):
                    await writer.drain()
                with app_phase_scope("http.client.read_response_head"):
                    await read_http_request_headers(reader)
                with app_phase_scope(
                    "http.client.read_response_body",
                    payload_bytes=response_body_size,
                ):
                    body = await read_fixed_body(
                        reader,
                        response_body_size,
                        response_chunk_size,
                    )
                if body != response_body:
                    raise RuntimeError(f"unexpected response body size={len(body)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        if clients == 1:
            await client(count)
        else:
            await asyncio.gather(
                *(client(iterations) for iterations in per_client_counts)
            )
    finally:
        server.close()
        await server.wait_closed()


async def websocket_echo_roundtrip(
    count: int,
    *,
    clients: int = 1,
    payload_size: int,
    path: bytes = b"/ws",
) -> None:
    payload = b"x" * payload_size
    per_client_counts = split_evenly(count, clients)

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        label_stream_endpoint(reader, writer, "server")
        try:
            headers, request_path = await read_http_request_headers(reader)
            if request_path != path:
                raise RuntimeError(f"unexpected websocket path: {request_path!r}")
            key = http_header_value(headers, b"Sec-WebSocket-Key")
            writer.write(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                + build_websocket_accept(key)
                + b"\r\n\r\n"
            )
            await writer.drain()
            while True:
                frame = await read_websocket_frame(reader)
                if frame != payload:
                    raise RuntimeError(
                        f"unexpected websocket payload size={len(frame)}"
                    )
                writer.write(encode_websocket_frame(frame, masked=False))
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
        request, key = build_websocket_handshake_request(path)
        try:
            writer.write(request)
            await writer.drain()
            response_headers, _response_path = await read_http_request_headers(reader)
            accept = http_header_value(response_headers, b"Sec-WebSocket-Accept")
            if accept != build_websocket_accept(key):
                raise RuntimeError("unexpected websocket handshake response")
            for _ in range(iterations):
                writer.write(encode_websocket_frame(payload, masked=True))
                await writer.drain()
                echoed = await read_websocket_frame(reader)
                if echoed != payload:
                    raise RuntimeError(
                        f"unexpected websocket payload size={len(echoed)}"
                    )
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
    finally:
        server.close()
        await server.wait_closed()


async def websocket_broadcast(
    count: int,
    *,
    clients: int,
    payload_size: int,
    subscriber_path: bytes = b"/sub",
    producer_path: bytes = b"/pub",
) -> None:
    payload = b"x" * payload_size
    subscribers: list[asyncio.StreamWriter] = []
    ready = asyncio.get_running_loop().create_future()
    done = asyncio.get_running_loop().create_future()
    connected = 0

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal connected
        label_stream_endpoint(reader, writer, "server")
        try:
            headers, request_path = await read_http_request_headers(reader)
            key = http_header_value(headers, b"Sec-WebSocket-Key")
            writer.write(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                + build_websocket_accept(key)
                + b"\r\n\r\n"
            )
            await writer.drain()
            if request_path == subscriber_path:
                subscribers.append(writer)
                connected += 1
                if connected == clients and not ready.done():
                    ready.set_result(None)
                try:
                    await done
                finally:
                    if writer in subscribers:
                        subscribers.remove(writer)
            elif request_path == producer_path:
                await ready
                for _ in range(count):
                    frame = await read_websocket_frame(reader)
                    if frame != payload:
                        raise RuntimeError(
                            f"unexpected websocket payload size={len(frame)}"
                        )
                    encoded = encode_websocket_frame(frame, masked=False)
                    for subscriber in subscribers:
                        subscriber.write(encoded)
                    for subscriber in subscribers:
                        await subscriber.drain()
                if not done.done():
                    done.set_result(None)
            else:
                raise RuntimeError(f"unexpected websocket path: {request_path!r}")
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    async def subscriber() -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        request, key = build_websocket_handshake_request(subscriber_path)
        try:
            writer.write(request)
            await writer.drain()
            response_headers, _ = await read_http_request_headers(reader)
            accept = http_header_value(response_headers, b"Sec-WebSocket-Accept")
            if accept != build_websocket_accept(key):
                raise RuntimeError("unexpected websocket handshake response")
            for _ in range(count):
                frame = await read_websocket_frame(reader)
                if frame != payload:
                    raise RuntimeError(
                        f"unexpected websocket payload size={len(frame)}"
                    )
        finally:
            writer.close()
            await writer.wait_closed()

    async def producer() -> None:
        reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        request, key = build_websocket_handshake_request(producer_path)
        try:
            writer.write(request)
            await writer.drain()
            response_headers, _ = await read_http_request_headers(reader)
            accept = http_header_value(response_headers, b"Sec-WebSocket-Accept")
            if accept != build_websocket_accept(key):
                raise RuntimeError("unexpected websocket handshake response")
            await ready
            for _ in range(count):
                writer.write(encode_websocket_frame(payload, masked=True))
                await writer.drain()
            await done
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(
            *(subscriber() for _ in range(clients)),
            producer(),
        )
    finally:
        server.close()
        await server.wait_closed()


async def framed_rpc_roundtrip(
    count: int,
    *,
    clients: int = 1,
    request_size: int,
    response_size: int,
) -> None:
    request = b"r" * request_size
    response = b"s" * response_size
    per_client_counts = split_evenly(count, clients)

    def encode_message(body: bytes) -> bytes:
        return b"\x00" + len(body).to_bytes(4, "big") + body

    async def read_message(reader: asyncio.StreamReader) -> bytes:
        header = await reader.readexactly(5)
        size = int.from_bytes(header[1:], "big")
        body = await reader.readexactly(size)
        return body

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        label_stream_endpoint(reader, writer, "server")
        try:
            while True:
                with app_phase_scope(
                    "grpc.server.read_message", payload_bytes=request_size
                ):
                    body = await read_message(reader)
                if body != request:
                    raise RuntimeError(f"unexpected framed body size={len(body)}")
                with app_phase_scope(
                    "grpc.server.write_message", payload_bytes=5 + response_size
                ):
                    writer.write(encode_message(response))
                    await writer.drain()
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    async def client(iterations: int) -> None:
        with app_phase_scope("grpc.client.connect"):
            reader, writer = await asyncio.open_connection(host, port)
        label_stream_endpoint(reader, writer, "client")
        try:
            for _ in range(iterations):
                with app_phase_scope(
                    "grpc.client.write_message", payload_bytes=5 + request_size
                ):
                    writer.write(encode_message(request))
                    await writer.drain()
                with app_phase_scope(
                    "grpc.client.read_message", payload_bytes=response_size
                ):
                    body = await read_message(reader)
                if body != response:
                    raise RuntimeError(f"unexpected framed body size={len(body)}")
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await asyncio.gather(*(client(iterations) for iterations in per_client_counts))
    finally:
        server.close()
        await server.wait_closed()


async def task_create_gather(count: int) -> None:
    batch_size = 64

    async def worker() -> None:
        await asyncio.sleep(0)

    remaining = count
    while remaining:
        current = min(batch_size, remaining)
        tasks = [asyncio.create_task(worker()) for _ in range(current)]
        await asyncio.gather(*tasks)
        remaining -= current


async def task_cancel_storm(count: int) -> None:
    batch_size = 64

    async def waiter(gate: asyncio.Event) -> None:
        await gate.wait()

    remaining = count
    while remaining:
        current = min(batch_size, remaining)
        gate = asyncio.Event()
        tasks = [asyncio.create_task(waiter(gate)) for _ in range(current)]
        for index, task in enumerate(tasks):
            if index % 4 != 0:
                task.cancel()
        gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        remaining -= current


async def future_completion_storm(count: int) -> None:
    batch_size = 128
    loop = asyncio.get_running_loop()
    remaining = count
    while remaining:
        current = min(batch_size, remaining)
        futures = [loop.create_future() for _ in range(current)]
        waiter = asyncio.gather(*futures)
        for future in futures:
            future.set_result(None)
        await waiter
        remaining -= current


async def timer_heap_heavy(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = 0

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    for index in range(count):
        delay = ((index * 2654435761) % 4096) / 1_000_000
        handle = loop.call_later(delay, tick)
        if index % 4 == 0:
            remaining += 1
        else:
            handle.cancel()

    if remaining:
        await done


async def mixed_ready_and_timers(count: int) -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = count

    def tick() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0 and not done.done():
            done.set_result(None)

    for index in range(count):
        if index % 2 == 0:
            loop.call_soon(tick)
        else:
            loop.call_later(0, tick)
        if index % 64 == 0:
            await asyncio.sleep(0)

    await done


async def bench_uds_connect_close(count: int) -> None:
    with tempfile.TemporaryDirectory(prefix="rsloop-uds-") as tmpdir:
        path = Path(tmpdir) / "sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(max(128, count))
        listener.setblocking(False)
        done = asyncio.get_running_loop().create_future()
        accepted = 0

        async def server() -> None:
            nonlocal accepted
            try:
                while accepted < count:
                    conn, _ = await asyncio.get_running_loop().sock_accept(listener)
                    accepted += 1
                    conn.close()
                done.set_result(None)
            finally:
                listener.close()

        server_task = asyncio.create_task(server())
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.setblocking(False)

        try:
            for index in range(count):
                await asyncio.get_running_loop().sock_connect(client, str(path))
                client.close()
                client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                client.setblocking(False)
                if index % 4 == 0:
                    await asyncio.sleep(0)
            await done
        finally:
            client.close()
            await server_task


async def bench_uds_connect_parallel(count: int) -> None:
    with tempfile.TemporaryDirectory(prefix="rsloop-uds-") as tmpdir:
        path = Path(tmpdir) / "sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(max(128, count))
        listener.setblocking(False)
        accepted = 0
        done = asyncio.get_running_loop().create_future()

        async def server() -> None:
            nonlocal accepted
            try:
                while accepted < count:
                    conn, _ = await asyncio.get_running_loop().sock_accept(listener)
                    accepted += 1
                    conn.close()
                if not done.done():
                    done.set_result(None)
            finally:
                listener.close()

        async def client() -> None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                await asyncio.get_running_loop().sock_connect(sock, str(path))
            finally:
                sock.close()

        server_task = asyncio.create_task(server())
        try:
            batch_size = 16
            for start in range(0, count, batch_size):
                await asyncio.gather(
                    *(client() for _ in range(min(batch_size, count - start)))
                )
                await asyncio.sleep(0)
            await done
        finally:
            await server_task


async def bench_uds_rpc_parallel(count: int) -> None:
    with tempfile.TemporaryDirectory(prefix="rsloop-uds-") as tmpdir:
        path = Path(tmpdir) / "sock"
        payload = b"x" * 64
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(max(128, count))
        listener.setblocking(False)
        per_client_counts = split_evenly(count, 16)
        server_tasks: list[asyncio.Task[None]] = []

        async def handle(conn: socket.socket, iterations: int) -> None:
            try:
                for _ in range(iterations):
                    data = await recv_exact(
                        asyncio.get_running_loop(), conn, len(payload)
                    )
                    if data != payload:
                        raise RuntimeError(f"unexpected payload size={len(data)}")
                    await asyncio.get_running_loop().sock_sendall(conn, data)
            finally:
                conn.close()

        async def acceptor() -> None:
            for iterations in per_client_counts:
                conn, _ = await asyncio.get_running_loop().sock_accept(listener)
                conn.setblocking(False)
                server_tasks.append(asyncio.create_task(handle(conn, iterations)))

        async def client(iterations: int) -> None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                await asyncio.get_running_loop().sock_connect(sock, str(path))
                for _ in range(iterations):
                    await asyncio.get_running_loop().sock_sendall(sock, payload)
                    data = await recv_exact(
                        asyncio.get_running_loop(), sock, len(payload)
                    )
                    if data != payload:
                        raise RuntimeError(f"unexpected payload size={len(data)}")
            finally:
                sock.close()

        accept_task = asyncio.create_task(acceptor())
        try:
            await asyncio.gather(
                *(client(iterations) for iterations in per_client_counts)
            )
            await accept_task
            if server_tasks:
                await asyncio.gather(*server_tasks)
        finally:
            listener.close()


async def bench_uds_idle_fanout(count: int) -> None:
    with tempfile.TemporaryDirectory(prefix="rsloop-uds-") as tmpdir:
        path = Path(tmpdir) / "sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(max(128, count))
        listener.setblocking(False)
        accepted = 0
        done = asyncio.get_running_loop().create_future()
        clients: list[socket.socket] = []
        peers: list[socket.socket] = []

        async def server() -> None:
            nonlocal accepted
            try:
                while accepted < count:
                    conn, _ = await asyncio.get_running_loop().sock_accept(listener)
                    peers.append(conn)
                    accepted += 1
                if not done.done():
                    done.set_result(None)
            finally:
                listener.close()

        async def client() -> socket.socket:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            await asyncio.get_running_loop().sock_connect(sock, str(path))
            return sock

        server_task = asyncio.create_task(server())
        try:
            clients = list(await asyncio.gather(*(client() for _ in range(count))))
            await done
        finally:
            for sock in clients:
                sock.close()
            for sock in peers:
                sock.close()
            await server_task


async def bench_http1_connect_per_request(count: int) -> None:
    await http1_roundtrip(
        count,
        request_method=b"GET",
        request_path=b"/connect",
        request_body_size=0,
        response_body_size=64,
        close_each_request=True,
    )


async def bench_http1_keepalive_small(count: int) -> None:
    await http1_roundtrip(
        count,
        request_method=b"POST",
        request_path=b"/keepalive",
        request_body_size=64,
        response_body_size=64,
        request_content_type=b"application/json",
        response_content_type=b"application/json",
    )


async def bench_http1_streaming_response(count: int) -> None:
    await http1_roundtrip(
        count,
        request_method=b"GET",
        request_path=b"/stream-response",
        request_body_size=0,
        response_body_size=16 * 1024,
        response_chunk_size=1024,
        close_each_request=False,
    )


async def bench_http1_streaming_upload(count: int) -> None:
    await http1_roundtrip(
        count,
        request_method=b"POST",
        request_path=b"/stream-upload",
        request_body_size=16 * 1024,
        response_body_size=1,
        request_chunk_size=1024,
    )


async def bench_tls_handshake_parallel(count: int) -> None:
    server_context, client_context = tls_contexts()
    accepted = 0
    done = asyncio.get_running_loop().create_future()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal accepted
        label_stream_endpoint(reader, writer, "server")
        accepted += 1
        if accepted == count and not done.done():
            done.set_result(None)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(
        handle, "127.0.0.1", 0, backlog=max(256, count), ssl=server_context
    )
    host, port = server.sockets[0].getsockname()[:2]

    async def client() -> None:
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    host,
                    port,
                    ssl=client_context,
                    server_hostname="localhost",
                )
                break
            except TimeoutError:
                await asyncio.sleep(0)
            except OSError as exc:
                if exc.errno in {errno.ETIMEDOUT, errno.ECONNREFUSED, 60}:
                    await asyncio.sleep(0)
                    continue
                raise
        label_stream_endpoint(reader, writer, "client")
        writer.close()
        await writer.wait_closed()

    try:
        batch_size = 8
        for start in range(0, count, batch_size):
            await asyncio.gather(
                *(client() for _ in range(min(batch_size, count - start)))
            )
            await asyncio.sleep(0)
        await done
    finally:
        server.close()
        await server.wait_closed()


async def bench_tls_http1_keepalive(count: int) -> None:
    server_context, client_context = tls_contexts()
    await http1_roundtrip(
        count,
        clients=8,
        request_method=b"POST",
        request_path=b"/tls-json",
        request_body_size=128,
        response_body_size=128,
        request_content_type=b"application/json",
        response_content_type=b"application/json",
        server_sslcontext=server_context,
        client_sslcontext=client_context,
        server_hostname="localhost",
    )


async def bench_websocket_echo_parallel(count: int) -> None:
    await websocket_echo_roundtrip(count, clients=32, payload_size=64)


async def bench_websocket_broadcast(count: int) -> None:
    await websocket_broadcast(count, clients=16, payload_size=64)


async def bench_asgi_json_echo(count: int) -> None:
    await http1_roundtrip(
        count,
        request_method=b"POST",
        request_path=b"/asgi-json",
        request_body_size=256,
        response_body_size=256,
        request_content_type=b"application/json",
        response_content_type=b"application/json",
    )


async def bench_asgi_keepalive(count: int) -> None:
    await http1_roundtrip(
        count,
        clients=16,
        request_method=b"POST",
        request_path=b"/asgi-keepalive",
        request_body_size=256,
        response_body_size=256,
        request_content_type=b"application/json",
        response_content_type=b"application/json",
    )


async def bench_asgi_streaming(count: int) -> None:
    await http1_roundtrip(
        count,
        request_method=b"POST",
        request_path=b"/asgi-streaming",
        request_body_size=256,
        response_body_size=16 * 1024,
        response_chunk_size=1024,
        request_content_type=b"application/json",
        response_content_type=b"application/json",
    )


async def bench_grpc_like_unary(count: int) -> None:
    await framed_rpc_roundtrip(
        count,
        clients=16,
        request_size=64,
        response_size=128,
    )


async def bench_udp_datagram_endpoint(count: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * 64
    completed = 0
    done = loop.create_future()
    server_transport: asyncio.DatagramTransport | None = None

    class ServerProtocol(asyncio.DatagramProtocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            nonlocal server_transport
            server_transport = transport  # type: ignore[assignment]

        def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
            nonlocal completed
            assert server_transport is not None
            server_transport.sendto(data, addr)
            completed += 1
            if completed == count and not done.done():
                done.set_result(None)

    class ClientProtocol(asyncio.DatagramProtocol):
        transport: asyncio.DatagramTransport | None = None
        pending: asyncio.Future[bytes] | None = None

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport  # type: ignore[assignment]

        def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
            if self.pending is not None and not self.pending.done():
                self.pending.set_result(data)

    server_transport_obj, _server_protocol = await loop.create_datagram_endpoint(
        ServerProtocol, local_addr=("127.0.0.1", 0)
    )
    server_addr = server_transport_obj.get_extra_info("sockname")
    assert server_addr is not None
    client_transport, client_protocol = await loop.create_datagram_endpoint(
        ClientProtocol, remote_addr=server_addr
    )

    try:
        for _ in range(count):
            response = loop.create_future()
            client_protocol.pending = response
            assert client_transport is not None
            client_transport.sendto(payload)
            data = await response
            if data != payload:
                raise RuntimeError(f"unexpected datagram size={len(data)}")
        await done
    finally:
        client_transport.close()
        server_transport_obj.close()


async def bench_udp_sock_sendto_recvfrom(count: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * 64
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    client.bind(("127.0.0.1", 0))
    server.setblocking(False)
    client.setblocking(False)
    server_addr = server.getsockname()

    async def server_loop() -> None:
        for _ in range(count):
            data, addr = await loop.sock_recvfrom(server, 1024)
            if data != payload:
                raise RuntimeError(f"unexpected datagram size={len(data)}")
            await loop.sock_sendto(server, data, addr)

    server_task = asyncio.create_task(server_loop())
    try:
        for _ in range(count):
            await loop.sock_sendto(client, payload, server_addr)
            data, addr = await loop.sock_recvfrom(client, 1024)
            if data != payload or addr != server_addr:
                raise RuntimeError(f"unexpected datagram size={len(data)}")
        await server_task
    finally:
        server.close()
        client.close()
        await server_task


async def bench_udp_sock_sendto_recvfrom_into(count: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * 64
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    client.bind(("127.0.0.1", 0))
    server.setblocking(False)
    client.setblocking(False)
    server_addr = server.getsockname()
    server_buf = bytearray(1024)
    client_buf = bytearray(1024)

    async def server_loop() -> None:
        for _ in range(count):
            nbytes, addr = await loop.sock_recvfrom_into(server, server_buf)
            if bytes(server_buf[:nbytes]) != payload:
                raise RuntimeError(f"unexpected datagram size={nbytes}")
            await loop.sock_sendto(server, memoryview(server_buf)[:nbytes], addr)

    server_task = asyncio.create_task(server_loop())
    try:
        for _ in range(count):
            await loop.sock_sendto(client, payload, server_addr)
            nbytes, addr = await loop.sock_recvfrom_into(client, client_buf)
            if bytes(client_buf[:nbytes]) != payload or addr != server_addr:
                raise RuntimeError(f"unexpected datagram size={nbytes}")
        await server_task
    finally:
        server.close()
        client.close()
        await server_task


async def bench_pipe_read(count: int) -> None:
    loop = asyncio.get_running_loop()
    payload = b"x" * 64
    expected = len(payload) * count
    received = 0
    done = loop.create_future()

    class ReaderProtocol(asyncio.Protocol):
        def data_received(self, data: bytes) -> None:
            nonlocal received
            received += len(data)
            if received >= expected and not done.done():
                done.set_result(None)

    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb", buffering=0)

    def writer() -> None:
        with os.fdopen(write_fd, "wb", buffering=0) as write_file:
            for _ in range(count):
                write_file.write(payload)

    writer_task = asyncio.create_task(asyncio.to_thread(writer))
    transport, _protocol = await loop.connect_read_pipe(
        lambda: ReaderProtocol(), read_file
    )
    try:
        await done
    finally:
        transport.close()
        await writer_task
    if received != expected:
        raise RuntimeError(f"unexpected pipe byte count={received}")


async def bench_pipe_write(count: int) -> None:
    payload = b"x" * 64
    expected = len(payload) * count
    received = 0
    read_fd, write_fd = os.pipe()

    def reader() -> None:
        nonlocal received
        with os.fdopen(read_fd, "rb", buffering=0) as read_file:
            while True:
                chunk = read_file.read(8192)
                if not chunk:
                    break
                received += len(chunk)

    reader_task = asyncio.create_task(asyncio.to_thread(reader))
    write_file = os.fdopen(write_fd, "wb", buffering=0)
    transport, _protocol = await asyncio.get_running_loop().connect_write_pipe(
        asyncio.Protocol,
        write_file,
    )
    try:
        for index in range(count):
            transport.write(payload)
            if index % 64 == 0:
                await asyncio.sleep(0)
        transport.close()
        await reader_task
    finally:
        transport.close()
        await reader_task
    if received != expected:
        raise RuntimeError(f"unexpected pipe byte count={received}")


async def bench_subprocess_exec(count: int) -> None:
    payload = b"subprocess-exec"
    script = (
        "import sys; "
        "data = sys.stdin.buffer.read(); "
        "sys.stdout.buffer.write(data[::-1])"
    )
    for _ in range(count):
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            script,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await proc.communicate(payload)
        if stdout != payload[::-1]:
            raise RuntimeError(f"unexpected subprocess stdout size={len(stdout)}")
        if await proc.wait() != 0:
            raise RuntimeError("subprocess_exec exited non-zero")


async def bench_subprocess_shell(count: int) -> None:
    payload = b"subprocess-shell"
    script = (
        "import sys; "
        "data = sys.stdin.buffer.read(); "
        "sys.stdout.buffer.write(data[::-1])"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
    for _ in range(count):
        proc = await asyncio.create_subprocess_shell(
            command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await proc.communicate(payload)
        if stdout != payload[::-1]:
            raise RuntimeError(f"unexpected subprocess stdout size={len(stdout)}")
        if await proc.wait() != 0:
            raise RuntimeError("subprocess_shell exited non-zero")


async def bench_signal_handler_churn(count: int) -> None:
    loop = asyncio.get_running_loop()
    sig = signal.SIGUSR1
    for _ in range(count):
        done = loop.create_future()
        loop.add_signal_handler(sig, done.set_result, None)
        try:
            os.kill(os.getpid(), sig)
            await done
        finally:
            loop.remove_signal_handler(sig)


async def bench_start_tls_upgrade(count: int) -> None:
    # start_tls() still routes through the loop-level stdlib upgrade path,
    # which expects ssl.SSLContext rather than RsloopTLSContext.
    server_context, client_context = tls_contexts(native_rsloop=False)

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            with app_phase_scope("start_tls.server.read_banner", payload_bytes=9):
                await reader.readexactly(9)
            with app_phase_scope("start_tls.server.write_ready", payload_bytes=6):
                writer.write(b"READY\n")
                await writer.drain()
            with app_phase_scope("start_tls.server.handshake"):
                await writer.start_tls(server_context)
            with app_phase_scope("start_tls.server.read_payload", payload_bytes=4):
                await reader.readexactly(4)
            with app_phase_scope("start_tls.server.write_payload", payload_bytes=4):
                writer.write(b"pong")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    try:
        for _ in range(count):
            with app_phase_scope("start_tls.client.connect"):
                reader, writer = await asyncio.open_connection(host, port)
            try:
                with app_phase_scope("start_tls.client.write_banner", payload_bytes=9):
                    writer.write(b"STARTTLS\n")
                    await writer.drain()
                with app_phase_scope("start_tls.client.read_ready", payload_bytes=6):
                    ready = await reader.readexactly(6)
                if ready != b"READY\n":
                    raise RuntimeError("unexpected start_tls readiness banner")
                with app_phase_scope("start_tls.client.handshake"):
                    await writer.start_tls(client_context, server_hostname="localhost")
                with app_phase_scope("start_tls.client.write_payload", payload_bytes=4):
                    writer.write(b"ping")
                    await writer.drain()
                with app_phase_scope("start_tls.client.read_payload", payload_bytes=4):
                    response = await reader.readexactly(4)
                if response != b"pong":
                    raise RuntimeError("unexpected start_tls response")
            finally:
                writer.close()
                await writer.wait_closed()

    finally:
        server.close()
        await server.wait_closed()


async def bench_sock_sendfile(count: int) -> None:
    payload = (b"sendfile-native-" * 4096) + b"tail"
    expected = payload
    loop = asyncio.get_running_loop()

    with tempfile.TemporaryDirectory(prefix="rsloop-sendfile-") as tmpdir:
        path = Path(tmpdir) / "payload.bin"
        path.write_bytes(payload)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(max(128, count))
        listener.setblocking(False)
        host, port = listener.getsockname()[:2]

        async def server() -> None:
            try:
                for _ in range(count):
                    conn, _ = await loop.sock_accept(listener)
                    conn.setblocking(False)
                    try:
                        with path.open("rb") as file:
                            sent = await loop.sock_sendfile(conn, file)
                        if sent != len(expected):
                            raise RuntimeError(f"unexpected sendfile byte count={sent}")
                    finally:
                        conn.close()
            finally:
                listener.close()

        server_task = asyncio.create_task(server())
        try:
            for _ in range(count):
                reader, writer = await asyncio.open_connection(host, port)
                received = bytearray()
                while len(received) < len(expected):
                    chunk = await reader.read(len(expected) - len(received))
                    if not chunk:
                        break
                    received.extend(chunk)
                if bytes(received) != expected:
                    raise RuntimeError(
                        f"unexpected sendfile payload size={len(received)}"
                    )
                writer.close()
                await writer.wait_closed()
            await server_task
        finally:
            listener.close()
            await server_task


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
    "tcp_protocol_echo": BenchmarkSpec(
        name="tcp_protocol_echo",
        description="TCP echo through create_connection()/create_server() with Protocol",
        category="protocol",
        default_iterations=8_192,
        unit="roundtrips",
        func=bench_tcp_protocol_echo,
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
    "udp_datagram_endpoint": BenchmarkSpec(
        name="udp_datagram_endpoint",
        description="Datagram endpoint echo using create_datagram_endpoint()",
        category="datagram",
        default_iterations=8_192,
        unit="messages",
        func=bench_udp_datagram_endpoint,
    ),
    "udp_sock_sendto_recvfrom": BenchmarkSpec(
        name="udp_sock_sendto_recvfrom",
        description="UDP echo using loop.sock_sendto() and loop.sock_recvfrom()",
        category="datagram",
        default_iterations=8_192,
        unit="messages",
        func=bench_udp_sock_sendto_recvfrom,
    ),
    "udp_sock_sendto_recvfrom_into": BenchmarkSpec(
        name="udp_sock_sendto_recvfrom_into",
        description="UDP echo using loop.sock_sendto() and loop.sock_recvfrom_into()",
        category="datagram",
        default_iterations=8_192,
        unit="messages",
        func=bench_udp_sock_sendto_recvfrom_into,
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
    "task_create_gather": BenchmarkSpec(
        name="task_create_gather",
        description="Create batches of tasks, yield once, and gather them back",
        category="scheduler",
        default_iterations=50_000,
        unit="tasks",
        func=task_create_gather,
    ),
    "task_cancel_storm": BenchmarkSpec(
        name="task_cancel_storm",
        description="Spawn tasks, cancel most of them, and drain cancellation cleanup",
        category="scheduler",
        default_iterations=50_000,
        unit="tasks",
        func=task_cancel_storm,
    ),
    "future_completion_storm": BenchmarkSpec(
        name="future_completion_storm",
        description="Complete batches of futures and fan out wakeups through gather()",
        category="scheduler",
        default_iterations=50_000,
        unit="futures",
        func=future_completion_storm,
    ),
    "timer_heap_heavy": BenchmarkSpec(
        name="timer_heap_heavy",
        description="Schedule many distinct timers and cancel most of them before they fire",
        category="scheduler",
        default_iterations=20_000,
        unit="timers",
        func=timer_heap_heavy,
    ),
    "mixed_ready_and_timers": BenchmarkSpec(
        name="mixed_ready_and_timers",
        description="Mixed ready callbacks and zero-delay timers under interleaved yielding",
        category="scheduler",
        default_iterations=20_000,
        unit="events",
        func=mixed_ready_and_timers,
    ),
    "uds_connect": BenchmarkSpec(
        name="uds_connect",
        description="Sequential AF_UNIX connect/close churn against a local listener",
        category="raw-socket",
        default_iterations=64,
        unit="connections",
        func=bench_uds_connect_close,
    ),
    "uds_connect_parallel": BenchmarkSpec(
        name="uds_connect_parallel",
        description="Many AF_UNIX connections opened and closed concurrently",
        category="raw-socket",
        default_iterations=64,
        unit="connections",
        func=bench_uds_connect_parallel,
    ),
    "uds_rpc_parallel": BenchmarkSpec(
        name="uds_rpc_parallel",
        description="AF_UNIX echo clients with 64-byte payloads through loop.sock_* helpers",
        category="raw-socket",
        default_iterations=8_192,
        unit="messages",
        func=bench_uds_rpc_parallel,
    ),
    "uds_idle_fanout": BenchmarkSpec(
        name="uds_idle_fanout",
        description="Open many idle AF_UNIX connections and tear them down",
        category="raw-socket",
        default_iterations=64,
        unit="connections",
        func=bench_uds_idle_fanout,
    ),
    "pipe_read": BenchmarkSpec(
        name="pipe_read",
        description="Read through connect_read_pipe() from an in-memory pipe producer",
        category="ipc",
        default_iterations=8_192,
        unit="chunks",
        func=bench_pipe_read,
    ),
    "pipe_write": BenchmarkSpec(
        name="pipe_write",
        description="Write through connect_write_pipe() into a draining pipe reader",
        category="ipc",
        default_iterations=8_192,
        unit="chunks",
        func=bench_pipe_write,
    ),
    "subprocess_exec": BenchmarkSpec(
        name="subprocess_exec",
        description="Spawn short-lived subprocesses with create_subprocess_exec()",
        category="ipc",
        default_iterations=64,
        unit="processes",
        func=bench_subprocess_exec,
    ),
    "subprocess_shell": BenchmarkSpec(
        name="subprocess_shell",
        description="Spawn short-lived subprocesses with create_subprocess_shell()",
        category="ipc",
        default_iterations=64,
        unit="processes",
        func=bench_subprocess_shell,
    ),
    "signal_handler_churn": BenchmarkSpec(
        name="signal_handler_churn",
        description="Register, deliver, and remove a signal handler each iteration",
        category="signals",
        default_iterations=256,
        unit="signals",
        func=bench_signal_handler_churn,
    ),
    "http1_connect_per_request": BenchmarkSpec(
        name="http1_connect_per_request",
        description="HTTP/1.1 request-per-connection churn with small responses",
        category="stream",
        default_iterations=256,
        unit="requests",
        func=bench_http1_connect_per_request,
    ),
    "http1_keepalive_small": BenchmarkSpec(
        name="http1_keepalive_small",
        description="Small HTTP/1.1 request/response exchanges over one keep-alive connection",
        category="stream",
        default_iterations=8_192,
        unit="requests",
        func=bench_http1_keepalive_small,
    ),
    "http1_streaming_response": BenchmarkSpec(
        name="http1_streaming_response",
        description="HTTP/1.1 response streaming with chunked writes on the server side",
        category="stream",
        default_iterations=2_048,
        unit="requests",
        func=bench_http1_streaming_response,
    ),
    "http1_streaming_upload": BenchmarkSpec(
        name="http1_streaming_upload",
        description="HTTP/1.1 upload streaming with chunked client writes",
        category="stream",
        default_iterations=2_048,
        unit="requests",
        func=bench_http1_streaming_upload,
    ),
    "tls_handshake_parallel": BenchmarkSpec(
        name="tls_handshake_parallel",
        description="Concurrent TLS handshakes and closes against a local server",
        category="stream",
        default_iterations=256,
        unit="connections",
        func=bench_tls_handshake_parallel,
    ),
    "tls_http1_keepalive": BenchmarkSpec(
        name="tls_http1_keepalive",
        description="Small HTTP/1.1 keep-alive exchanges over TLS",
        category="stream",
        default_iterations=2_048,
        unit="requests",
        func=bench_tls_http1_keepalive,
    ),
    "start_tls_upgrade": BenchmarkSpec(
        name="start_tls_upgrade",
        description="Plaintext to TLS connection upgrades via StreamWriter.start_tls()",
        category="stream",
        default_iterations=1,
        unit="connections",
        func=bench_start_tls_upgrade,
    ),
    "sock_sendfile": BenchmarkSpec(
        name="sock_sendfile",
        description="TCP file transfer through loop.sock_sendfile()",
        category="stream",
        default_iterations=64,
        unit="transfers",
        func=bench_sock_sendfile,
    ),
    "websocket_echo_parallel": BenchmarkSpec(
        name="websocket_echo_parallel",
        description="Many websocket clients echoing 64-byte messages in parallel",
        category="application",
        default_iterations=4_096,
        unit="messages",
        func=bench_websocket_echo_parallel,
    ),
    "websocket_broadcast": BenchmarkSpec(
        name="websocket_broadcast",
        description="One websocket producer broadcasting to many subscribers",
        category="application",
        default_iterations=256,
        unit="messages",
        func=bench_websocket_broadcast,
    ),
    "asgi_json_echo": BenchmarkSpec(
        name="asgi_json_echo",
        description="JSON request/response traffic shaped like a small ASGI endpoint",
        category="application",
        default_iterations=4_096,
        unit="requests",
        func=bench_asgi_json_echo,
    ),
    "asgi_keepalive": BenchmarkSpec(
        name="asgi_keepalive",
        description="Keep-alive JSON request/response traffic shaped like an ASGI app",
        category="application",
        default_iterations=8_192,
        unit="requests",
        func=bench_asgi_keepalive,
    ),
    "asgi_streaming": BenchmarkSpec(
        name="asgi_streaming",
        description="Streaming JSON response traffic shaped like an ASGI app",
        category="application",
        default_iterations=2_048,
        unit="requests",
        func=bench_asgi_streaming,
    ),
    "grpc_like_unary": BenchmarkSpec(
        name="grpc_like_unary",
        description="Length-prefixed unary RPC traffic shaped like a small gRPC call",
        category="application",
        default_iterations=8_192,
        unit="messages",
        func=bench_grpc_like_unary,
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
    "real": [
        "http1_keepalive_small",
        "http1_streaming_response",
        "asgi_json_echo",
        "asgi_streaming",
        "grpc_like_unary",
        "tls_http1_keepalive",
        "http1_connect_per_request",
        "udp_datagram_endpoint",
        "pipe_write",
        "subprocess_exec",
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
    profile_sslproto: bool,
    profile_python_cpu: bool,
    profile_app_phases: bool,
    isolate_process: bool,
    child_retries: int,
) -> tuple[float, dict[str, Any] | None, int]:
    if not isolate_process:
        if profile_python_cpu:
            install_python_cpu_profiler()
            assert PYTHON_CPU_PROFILER is not None
            PYTHON_CPU_PROFILER.start()
        started = time.perf_counter()
        try:
            run_on_loop(loop_name, spec.func(iterations))
        except BenchmarkSkipped:
            raise
        finally:
            if profile_python_cpu:
                emit_python_cpu_profiler()
        return time.perf_counter() - started, None, 0
    return run_once_isolated(
        loop_name,
        spec,
        iterations,
        profile_runtime,
        profile_stream,
        profile_python_streams,
        profile_sslproto,
        profile_python_cpu,
        profile_app_phases,
        child_retries,
    )


def run_once_isolated(
    loop_name: str,
    spec: BenchmarkSpec,
    iterations: int,
    profile_runtime: bool,
    profile_stream: bool,
    profile_python_streams: bool,
    profile_sslproto: bool,
    profile_python_cpu: bool,
    profile_app_phases: bool,
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
        env["RSLOOP_BENCH_LOOP"] = loop_name
        if loop_name == "rsloop" and profile_runtime:
            env["RSLOOP_PROFILE_SCHED_JSON"] = "1"
            env["RSLOOP_PROFILE_ONEARG_JSON"] = "1"
        if loop_name == "rsloop" and profile_stream:
            env["RSLOOP_PROFILE_SCHED_JSON"] = "1"
            env["RSLOOP_PROFILE_STREAM_JSON"] = "1"
            env["RSLOOP_PROFILE_ONEARG_JSON"] = "1"
        if profile_python_streams:
            env["BENCH_PROFILE_PY_STREAM_JSON"] = "1"
        if profile_sslproto:
            env["BENCH_PROFILE_SSLPROTO_JSON"] = "1"
        if profile_python_cpu:
            env["BENCH_PROFILE_CPU_JSON"] = "1"
        if profile_app_phases:
            env["BENCH_PROFILE_APP_PHASE_JSON"] = "1"
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
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout or "<no child output>"
            if "NotImplementedError" in detail or "NotImplemented" in detail:
                raise BenchmarkSkipped(
                    f"benchmark unsupported for loop={loop_name} benchmark={spec.name}: {detail}"
                ) from exc
            if attempts <= child_retries:
                continue
            raise RuntimeError(
                f"benchmark child failed for loop={loop_name} benchmark={spec.name} "
                f"after {attempts} attempt(s): {detail}"
            ) from exc
        payload = json.loads(completed.stdout)
        result = bench_result_from_payload(payload["results"][0])
        if spec.category == "connection":
            time.sleep(ISOLATED_CONNECTION_COOLDOWN)
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
    profile_sslproto: bool,
    profile_python_cpu: bool,
    profile_app_phases: bool,
    interleave_loops: bool,
    child_retries: int,
) -> tuple[list[BenchResult], list[dict[str, Any]] | None]:
    sample_map: dict[str, list[float]] = {loop_name: [] for loop_name in loops}
    profile_map: dict[str, list[dict[str, Any]]] = {
        loop_name: [] for loop_name in loops
    }
    round_records: list[dict[str, Any]] = []

    if interleave_loops and len(loops) > 1:
        for warmup_index in range(warmups):
            order = rotated_order(loops, warmup_index)
            for loop_name in order:
                try:
                    run_once(
                        loop_name,
                        spec,
                        iterations,
                        profile_runtime,
                        profile_stream,
                        profile_python_streams,
                        profile_sslproto,
                        profile_python_cpu,
                        profile_app_phases,
                        isolate_process,
                        child_retries,
                    )
                except BenchmarkSkipped:
                    continue

        for repeat_index in range(repeats):
            order = rotated_order(loops, repeat_index)
            round_record: dict[str, Any] = {
                "index": repeat_index,
                "order": order,
                "samples": {},
            }
            retries_used: dict[str, int] = {}
            for loop_name in order:
                try:
                    sample, profile, retries_used_count = run_once(
                        loop_name,
                        spec,
                        iterations,
                        profile_runtime,
                        profile_stream,
                        profile_python_streams,
                        profile_sslproto,
                        profile_python_cpu,
                        profile_app_phases,
                        isolate_process,
                        child_retries,
                    )
                except BenchmarkSkipped:
                    round_record["samples"][loop_name] = None
                    retries_used[loop_name] = -1
                    continue
                sample_index = len(sample_map[loop_name])
                sample_map[loop_name].append(sample)
                round_record["samples"][loop_name] = sample
                if retries_used_count:
                    retries_used[loop_name] = retries_used_count
                if profile is not None:
                    profile_map[loop_name].append(
                        {
                            "sample_index": sample_index,
                            "round_index": repeat_index,
                            "profile": profile,
                        }
                    )
            if retries_used:
                round_record["retries"] = retries_used
            round_records.append(round_record)
    else:
        for loop_name in loops:
            for _ in range(warmups):
                try:
                    run_once(
                        loop_name,
                        spec,
                        iterations,
                        profile_runtime,
                        profile_stream,
                        profile_python_streams,
                        profile_sslproto,
                        profile_python_cpu,
                        profile_app_phases,
                        isolate_process,
                        child_retries,
                    )
                except BenchmarkSkipped:
                    continue
            for repeat_index in range(repeats):
                try:
                    sample, profile, retries_used_count = run_once(
                        loop_name,
                        spec,
                        iterations,
                        profile_runtime,
                        profile_stream,
                        profile_python_streams,
                        profile_sslproto,
                        profile_python_cpu,
                        profile_app_phases,
                        isolate_process,
                        child_retries,
                    )
                except BenchmarkSkipped:
                    continue
                sample_index = len(sample_map[loop_name])
                sample_map[loop_name].append(sample)
                if profile is not None:
                    profile_map[loop_name].append(
                        {
                            "sample_index": sample_index,
                            "round_index": repeat_index,
                            "profile": profile,
                        }
                    )
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
            samples=samples,
            profiles=profile_map[loop_name] or None,
        )
        for loop_name, samples in sample_map.items()
        if samples
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
                    "paired_ratio_median": statistics.median(ratios)
                    if ratios
                    else None,
                    "paired_ratio_iqr": interquartile_range(ratios)
                    if len(ratios) >= 2
                    else 0.0,
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
        "comparisons": compute_paired_comparisons(
            results, baseline, rounds_by_benchmark
        ),
    }


def write_output(payload: dict[str, Any], output: str | None) -> None:
    if output is None:
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--loop", choices=["asyncio", "rsloop", "uvloop", "all"], default="all"
    )
    parser.add_argument(
        "--benchmark", choices=[*BENCHMARKS.keys(), "all"], default="all"
    )
    parser.add_argument(
        "--profile", choices=sorted(PROFILE_BENCHMARKS), default="default"
    )
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument(
        "--baseline", choices=["asyncio", "uvloop", "rsloop"], default="uvloop"
    )
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
    parser.add_argument(
        "--profile-python-cpu",
        action="store_true",
        help="Capture Python CPU samples for isolated child runs with pyinstrument.",
    )
    parser.add_argument(
        "--profile-sslproto",
        action="store_true",
        help="Capture stdlib asyncio.sslproto.SSLProtocol method timings for isolated child runs.",
    )
    parser.add_argument(
        "--profile-app-phases",
        action="store_true",
        help="Capture benchmark-level HTTP/ASGI/TLS phase markers for isolated child runs.",
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
    profile_sslproto = args.profile_sslproto or (
        os.environ.get("BENCH_PROFILE_SSLPROTO_JSON") == "1"
    )
    profile_python_cpu = args.profile_python_cpu or (
        os.environ.get("BENCH_PROFILE_CPU_JSON") == "1"
    )
    profile_app_phases = args.profile_app_phases or (
        os.environ.get("BENCH_PROFILE_APP_PHASE_JSON") == "1"
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
    if (
        (
            args.profile_runtime
            or args.profile_stream
            or profile_python_streams
            or profile_sslproto
            or profile_python_cpu
            or profile_app_phases
        )
        and not isolate_process
        and not args.child_run
    ):
        raise SystemExit("profiling requires subprocess isolation")
    enable_local_profilers = args.child_run or not isolate_process
    if profile_python_streams and enable_local_profilers:
        install_python_stream_profiler()
    if profile_sslproto and enable_local_profilers:
        install_sslproto_profiler()
    if profile_app_phases and enable_local_profilers:
        install_app_phase_profiler()
    for spec in specs:
        iterations = args.iterations or spec.default_iterations
        selected_loops = [
            loop_name
            for loop_name in loops
            if loop_name != "uvloop" or uvloop is not None
        ]
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
            profile_sslproto,
            profile_python_cpu,
            profile_app_phases,
            interleave_loops,
            max(0, args.child_retries),
        )
        results.extend(group_results)
        if round_records is not None:
            rounds_by_benchmark[spec.name] = round_records
        if not args.json:
            for result in group_results:
                print(format_result(result), flush=True)
        if isolate_process and spec.category == "connection":
            time.sleep(CONNECTION_BENCHMARK_COOLDOWN)

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
            "profile_runtime": args.profile_runtime,
            "profile_stream": args.profile_stream,
            "profile_python_streams": profile_python_streams,
            "profile_sslproto": profile_sslproto,
            "profile_python_cpu": profile_python_cpu,
            "profile_app_phases": profile_app_phases,
        },
    )
    write_output(payload, args.output)

    if args.json:
        print(json.dumps(payload, indent=2))
    elif len(loops) > 1:
        emit_summary(results, args.baseline, rounds_by_benchmark)
    if profile_python_streams and enable_local_profilers:
        emit_python_stream_profiler()
    if profile_sslproto and enable_local_profilers:
        emit_sslproto_profiler()
    if profile_app_phases and enable_local_profilers:
        emit_app_phase_profiler()


if __name__ == "__main__":
    os.environ.setdefault("PYTHONASYNCIODEBUG", "0")
    main()

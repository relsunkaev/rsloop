from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import loops


def parse_sample_top(path: Path) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    top: list[dict[str, Any]] = []
    in_section = False
    for line in lines:
        if line.startswith("Sort by top of stack, same collapsed"):
            in_section = True
            continue
        if not in_section:
            continue
        if not line.strip():
            if top:
                break
            continue
        if line.startswith("Binary Images:"):
            break
        stripped = line.strip()
        count, _, frame = stripped.partition(" ")
        if not count.isdigit() or not frame:
            continue
        top.append({"count": int(count), "frame": frame.strip()})
    return top


def parse_sample_frames(path: Path, *, limit: int = 64) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    in_graph = False
    pattern = re.compile(r"^\s*[+!:| ]*\s*(\d+)\s+(.+?)\s+\(in ")
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("Call graph:"):
            in_graph = True
            continue
        if not in_graph:
            continue
        if line.startswith("Total number in stack"):
            break
        match = pattern.match(line)
        if not match:
            continue
        count = int(match.group(1))
        frame = match.group(2).strip()
        counts[frame] = counts.get(frame, 0) + count
    frames = [
        {"count": count, "frame": frame}
        for frame, count in sorted(
            counts.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )[:limit]
    ]
    return frames


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", choices=["asyncio", "rsloop", "uvloop"], required=True)
    parser.add_argument("--benchmark", choices=sorted(loops.BENCHMARKS), required=True)
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--interval-ms", type=int, default=1)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--profile-runtime", action="store_true")
    parser.add_argument("--profile-stream", action="store_true")
    parser.add_argument("--profile-python-streams", action="store_true")
    parser.add_argument("--profile-sslproto", action="store_true")
    parser.add_argument("--profile-app-phases", action="store_true")
    args = parser.parse_args()

    spec = loops.BENCHMARKS[args.benchmark]
    iterations = args.iterations or spec.default_iterations
    prefix = Path(args.output_prefix)
    prefix.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(Path(__file__).with_name("loops.py")),
        "--child-run",
        "--loop",
        args.loop,
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
    if args.loop == "rsloop" and args.profile_runtime:
        env["RSLOOP_PROFILE_SCHED_JSON"] = "1"
        env["RSLOOP_PROFILE_ONEARG_JSON"] = "1"
    if args.loop == "rsloop" and args.profile_stream:
        env["RSLOOP_PROFILE_SCHED_JSON"] = "1"
        env["RSLOOP_PROFILE_STREAM_JSON"] = "1"
        env["RSLOOP_PROFILE_ONEARG_JSON"] = "1"
    if args.profile_python_streams:
        env["BENCH_PROFILE_PY_STREAM_JSON"] = "1"
    if args.profile_sslproto:
        env["BENCH_PROFILE_SSLPROTO_JSON"] = "1"
    if args.profile_app_phases:
        env["BENCH_PROFILE_APP_PHASE_JSON"] = "1"

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    sample_txt = prefix.with_suffix(".sample.txt")
    sample_cmd = [
        "/usr/bin/sample",
        str(process.pid),
        str(args.duration),
        str(args.interval_ms),
        "-mayDie",
        "-file",
        str(sample_txt),
    ]
    sample_started = time.perf_counter()
    sample_completed = subprocess.run(sample_cmd, capture_output=True, text=True)
    stdout, stderr = process.communicate()
    sample_elapsed = time.perf_counter() - sample_started

    payload = json.loads(stdout) if stdout else None
    profile = loops.parse_runtime_profile(stderr)
    summary = {
        "command": cmd,
        "loop": args.loop,
        "benchmark": spec.name,
        "iterations": iterations,
        "duration": args.duration,
        "interval_ms": args.interval_ms,
        "sample_command": sample_cmd,
        "sample_elapsed_seconds": sample_elapsed,
        "sample_returncode": sample_completed.returncode,
        "sample_stdout": sample_completed.stdout,
        "sample_stderr": sample_completed.stderr,
        "sample_path": str(sample_txt),
        "process_returncode": process.returncode,
        "result_payload": payload,
        "runtime_profile": profile,
        "sample_top_frames": parse_sample_top(sample_txt) if sample_txt.exists() else [],
        "sample_frame_counts": parse_sample_frames(sample_txt) if sample_txt.exists() else [],
    }
    prefix.with_suffix(".json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()

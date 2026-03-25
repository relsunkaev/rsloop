from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import loops as harness


def run(cmd: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def build_editable(worktree: Path, *, venv: Path) -> None:
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(venv)
    env["PATH"] = f"{venv / 'bin'}:{env['PATH']}"
    run([str(venv / "bin" / "maturin"), "develop", "--release"], cwd=worktree, env=env)


def benchmark_once(
    worktree: Path,
    *,
    benchmark: str,
    iterations: int,
    rsloop_mode: str,
    profile_runtime: bool,
    profile_stream: bool,
    profile_python_streams: bool,
    profile_sslproto: bool,
    profile_python_cpu: bool,
    profile_app_phases: bool,
    child_retries: int,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(worktree / "benchmarks" / "loops.py"),
        "--loop",
        "rsloop",
        "--benchmark",
        benchmark,
        "--iterations",
        str(iterations),
        "--repeats",
        "1",
        "--warmups",
        "0",
        "--baseline",
        "uvloop",
        "--child-retries",
        str(child_retries),
        "--json",
    ]
    if profile_runtime:
        cmd.append("--profile-runtime")
    if profile_stream:
        cmd.append("--profile-stream")
    if profile_python_streams:
        cmd.append("--profile-python-streams")
    if profile_sslproto:
        cmd.append("--profile-sslproto")
    if profile_python_cpu:
        cmd.append("--profile-python-cpu")
    if profile_app_phases:
        cmd.append("--profile-app-phases")
    env = os.environ.copy()
    env["RSLOOP_MODE"] = rsloop_mode
    payload = json.loads(run(cmd, cwd=worktree, env=env).stdout)
    result = payload["results"][0]
    return {
        "sample": result["samples"][0],
        "profiles": result.get("profiles"),
        "payload": result,
    }


def revision_payload(
    *,
    baseline_label: str,
    candidate_label: str,
    baseline_rev: str,
    methodology: dict[str, Any],
    benchmark_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "environment": harness.environment_snapshot(),
        "methodology": methodology,
        "revisions": {
            "baseline_label": baseline_label,
            "baseline_rev": baseline_rev,
            "candidate_label": candidate_label,
        },
        "benchmarks": benchmark_rows,
    }


def emit_summary(payload: dict[str, Any]) -> None:
    print(
        f"{'benchmark':24} {'baseline':>10} {'candidate':>10} {'delta':>10}",
        flush=True,
    )
    for row in payload["benchmarks"]:
        print(
            f"{row['benchmark']:24} "
            f"{row['baseline_median']:10.6f} "
            f"{row['candidate_median']:10.6f} "
            f"{row['delta_pct']:9.2f}%",
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", choices=[*harness.BENCHMARKS.keys(), "all"], default="all")
    parser.add_argument("--profile", choices=sorted(harness.PROFILE_BENCHMARKS), default="default")
    parser.add_argument("--rsloop-mode", choices=["fast", "compat"], default=None)
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--baseline-rev", type=str, default="HEAD")
    parser.add_argument("--baseline-label", type=str, default="baseline")
    parser.add_argument("--candidate-label", type=str, default="current")
    parser.add_argument("--profile-runtime", action="store_true")
    parser.add_argument("--profile-stream", action="store_true")
    parser.add_argument("--profile-python-streams", action="store_true")
    parser.add_argument("--profile-sslproto", action="store_true")
    parser.add_argument("--profile-python-cpu", action="store_true")
    parser.add_argument("--profile-app-phases", action="store_true")
    parser.add_argument("--child-retries", type=int, default=1)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    venv = Path(os.environ.get("VIRTUAL_ENV", sys.prefix)).resolve()
    maturin_bin = shutil.which("maturin") or str(venv / "bin" / "maturin")
    if not Path(maturin_bin).exists():
        raise SystemExit("revision_ab.py requires maturin in the active virtualenv")

    selected = harness.selected_benchmarks(args)
    tmpdir = Path(tempfile.mkdtemp(prefix="rsloop-revision-ab-"))
    baseline_worktree = tmpdir / "baseline"
    run(
        ["git", "worktree", "add", "--detach", str(baseline_worktree), args.baseline_rev],
        cwd=root,
    )

    try:
        benchmark_rows: list[dict[str, Any]] = []
        for spec in selected:
            iterations = args.iterations or spec.default_iterations
            baseline_samples: list[float] = []
            candidate_samples: list[float] = []
            baseline_profiles: list[dict[str, Any]] = []
            candidate_profiles: list[dict[str, Any]] = []
            rounds: list[dict[str, Any]] = []

            for _ in range(args.warmups):
                for worktree in (baseline_worktree, root):
                    build_editable(worktree, venv=venv)
                    benchmark_once(
                        worktree,
                        benchmark=spec.name,
                        iterations=iterations,
                        rsloop_mode=args.rsloop_mode or os.environ.get("RSLOOP_MODE", "fast"),
                        profile_runtime=args.profile_runtime,
                        profile_stream=args.profile_stream,
                        profile_python_streams=args.profile_python_streams,
                        profile_sslproto=args.profile_sslproto,
                        profile_python_cpu=args.profile_python_cpu,
                        profile_app_phases=args.profile_app_phases,
                        child_retries=max(0, args.child_retries),
                    )

            for repeat_index in range(args.repeats):
                order = (
                    [("baseline", baseline_worktree), ("candidate", root)]
                    if repeat_index % 2 == 0
                    else [("candidate", root), ("baseline", baseline_worktree)]
                )
                round_record: dict[str, Any] = {
                    "index": repeat_index,
                    "order": [label for label, _ in order],
                    "samples": {},
                }
                for label, worktree in order:
                    build_editable(worktree, venv=venv)
                    sample = benchmark_once(
                        worktree,
                        benchmark=spec.name,
                        iterations=iterations,
                        rsloop_mode=args.rsloop_mode or os.environ.get("RSLOOP_MODE", "fast"),
                        profile_runtime=args.profile_runtime,
                        profile_stream=args.profile_stream,
                        profile_python_streams=args.profile_python_streams,
                        profile_sslproto=args.profile_sslproto,
                        profile_python_cpu=args.profile_python_cpu,
                        profile_app_phases=args.profile_app_phases,
                        child_retries=max(0, args.child_retries),
                    )
                    round_record["samples"][label] = sample["sample"]
                    if label == "baseline":
                        baseline_samples.append(sample["sample"])
                        if sample["profiles"]:
                            baseline_profiles.extend(sample["profiles"])
                    else:
                        candidate_samples.append(sample["sample"])
                        if sample["profiles"]:
                            candidate_profiles.extend(sample["profiles"])
                rounds.append(round_record)

            baseline_median = statistics.median(baseline_samples)
            candidate_median = statistics.median(candidate_samples)
            benchmark_rows.append(
                {
                    "benchmark": spec.name,
                    "category": spec.category,
                    "description": spec.description,
                    "iterations": iterations,
                    "unit": spec.unit,
                    "baseline_samples": baseline_samples,
                    "candidate_samples": candidate_samples,
                    "baseline_median": baseline_median,
                    "candidate_median": candidate_median,
                    "delta_pct": ((candidate_median / baseline_median) - 1.0) * 100.0,
                    "baseline_profiles": baseline_profiles or None,
                    "candidate_profiles": candidate_profiles or None,
                    "rounds": rounds,
                }
            )

        payload = revision_payload(
            baseline_label=args.baseline_label,
            candidate_label=args.candidate_label,
            baseline_rev=args.baseline_rev,
            methodology={
                "repeats": args.repeats,
                "warmups": args.warmups,
                "child_retries": max(0, args.child_retries),
                "interleaved_revisions": True,
                "rsloop_mode": args.rsloop_mode or os.environ.get("RSLOOP_MODE", "fast"),
                "profile_runtime": args.profile_runtime,
                "profile_stream": args.profile_stream,
                "profile_python_streams": args.profile_python_streams,
                "profile_sslproto": args.profile_sslproto,
                "profile_python_cpu": args.profile_python_cpu,
                "profile_app_phases": args.profile_app_phases,
            },
            benchmark_rows=benchmark_rows,
        )
        harness.write_output(payload, args.output)
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            emit_summary(payload)
    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(baseline_worktree)],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()

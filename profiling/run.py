from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VENV = ROOT / ".venv"
DEFAULT_PYTHON = DEFAULT_VENV / "bin" / "python"
DEFAULT_OUTPUT_DIR = ROOT / "profiling" / "out"


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def resolve_python(python: str | None) -> Path:
    if python:
        return Path(python)
    if DEFAULT_PYTHON.exists():
        return DEFAULT_PYTHON
    return Path(sys.executable)


def build_benchmark_args(args: argparse.Namespace) -> list[str]:
    return [
        "benchmarks/loops.py",
        "--loop",
        args.loop,
        "--benchmark",
        args.benchmark,
        "--iterations",
        str(args.iterations),
        "--repeats",
        str(args.repeats),
        "--warmups",
        str(args.warmups),
    ]


def output_basename(args: argparse.Namespace) -> str:
    return f"{timestamp()}-{args.tool}-{args.loop}-{args.benchmark}"


def ensure_tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise SystemExit(f"required tool not found on PATH: {name}")
    return path


def run_subprocess(cmd: list[str], env: dict[str, str]) -> None:
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile rsloop benchmark workloads")
    parser.add_argument("--tool", choices=("cprofile", "pyinstrument", "samply"), required=True)
    parser.add_argument("--loop", choices=("asyncio", "rsloop", "uvloop"), default="rsloop")
    parser.add_argument(
        "--benchmark",
        choices=("call_soon", "sleep_zero", "socketpair", "tcp_echo", "tcp_sock", "tcp_connect"),
        default="call_soon",
    )
    parser.add_argument("--iterations", type=int, default=100_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--python", help="Python interpreter to use")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--rate", type=int, default=1000, help="Sampling rate for samply")
    parser.add_argument(
        "--address",
        default="127.0.0.1",
        help="Address for samply's local web server when opening profiles",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=43000,
        help="Port for samply's local web server when opening profiles",
    )
    parser.add_argument(
        "--main-thread-only",
        action="store_true",
        help="Pass --main-thread-only to samply to reduce profile size",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the generated profile after recording",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    python = resolve_python(args.python)
    env = os.environ.copy()
    env["PATH"] = f"{python.parent}:{env.get('PATH', '')}"
    if python.parent.parent.name == ".venv":
        env["VIRTUAL_ENV"] = str(python.parent.parent)

    benchmark_args = build_benchmark_args(args)
    base = output_dir / output_basename(args)

    if args.tool == "cprofile":
        out = base.with_suffix(".pstats")
        cmd = [str(python), "-m", "cProfile", "-o", str(out), *benchmark_args]
        run_subprocess(cmd, env)
        print(f"wrote {out}")
        return

    if args.tool == "pyinstrument":
        tool = ensure_tool("pyinstrument")
        out = base.with_suffix(".html")
        cmd = [tool, "-r", "html", "-o", str(out), *benchmark_args]
        run_subprocess(cmd, env)
        print(f"wrote {out}")
        return

    tool = ensure_tool("samply")
    out = base.with_suffix(".json.gz")
    cmd = [
        tool,
        "record",
        "--rate",
        str(args.rate),
        "--address",
        args.address,
        "--port",
        str(args.port),
        "-o",
        str(out),
    ]
    if not args.open:
        cmd.append("--save-only")
    if args.main_thread_only:
        cmd.append("--main-thread-only")
    cmd.extend(["--", str(python), *benchmark_args])
    run_subprocess(cmd, env)
    print(f"wrote {out}")
    if args.open:
        load_cmd = [
            tool,
            "load",
            "--address",
            args.address,
            "--port",
            str(args.port),
            str(out),
        ]
        run_subprocess(load_cmd, env)


if __name__ == "__main__":
    main()

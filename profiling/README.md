# Profiling

This repo now has a repeatable profiling entrypoint for the benchmark workloads in
[benchmarks/loops.py](/Users/ramazan/Code/oss/kioto/benchmarks/loops.py).

## Tools

- `cProfile`: deterministic Python profiler, good for exact Python call counts and cumulative time.
- `pyinstrument`: sampled Python timeline, easier to read than raw `cProfile`.
- `samply`: mixed native profile for Python + Rust stacks, exported as Firefox Profiler JSON.

## Prerequisites

Python-side profiler:

```bash
.venv/bin/python -m pip install pyinstrument
```

Native mixed-stack profiler:

```bash
cargo install samply
```

## Usage

Run from the repo root:

```bash
.venv/bin/python profiling/run.py --tool cprofile --loop kioto --benchmark call_soon --iterations 300000 --repeats 3 --warmups 1
.venv/bin/python profiling/run.py --tool pyinstrument --loop kioto --benchmark tcp_echo --iterations 10000 --repeats 3 --warmups 1
.venv/bin/python profiling/run.py --tool samply --loop kioto --benchmark tcp_sock --iterations 10000 --repeats 3 --warmups 1
```

Artifacts are written to `profiling/out/` with timestamped names.
`samply` records to disk by default and does not open a browser.
Pass `--open` if you want the Firefox Profiler UI to launch after capture.
When opening, the runner defaults to `127.0.0.1:43000` and you can override that
with `--address` / `--port`.

## Outputs

- `cProfile` writes `.pstats`

Inspect with:

```bash
.venv/bin/python - <<'PY'
import pstats
p = pstats.Stats("profiling/out/<file>.pstats")
p.sort_stats("cumtime").print_stats(40)
PY
```

- `pyinstrument` writes `.html`

Open directly in a browser.

- `samply` writes `.json.gz`

Open with the Firefox Profiler UI:

```bash
samply load profiling/out/<file>.json.gz
```

Or upload manually at https://profiler.firefox.com/

Open directly from the runner:

```bash
.venv/bin/python profiling/run.py --tool samply --loop kioto --benchmark tcp_echo --open
.venv/bin/python profiling/run.py --tool samply --loop kioto --benchmark tcp_echo --open --port 43123
```

## Recommended workflow

1. Use `cprofile` on `call_soon` to answer whether a scheduler bottleneck is still Python-heavy.
2. Use `pyinstrument` on `tcp_echo` to see high-level event loop and stream transport timelines.
3. Use `samply` on the same workload to see where time is split across Python, Rust, libc, and Tokio.

## Suggested baselines

- Scheduler microbenchmark:

```bash
.venv/bin/python profiling/run.py --tool cprofile --loop kioto --benchmark call_soon --iterations 300000 --repeats 3 --warmups 1
```

- Stream transport workload:

```bash
.venv/bin/python profiling/run.py --tool pyinstrument --loop kioto --benchmark tcp_echo --iterations 10000 --repeats 3 --warmups 1
.venv/bin/python profiling/run.py --tool samply --loop kioto --benchmark tcp_echo --iterations 10000 --repeats 3 --warmups 1
```

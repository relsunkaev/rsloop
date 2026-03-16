# kioto

`kioto` is a `pyo3` + `maturin` project scaffold for a Tokio-backed asyncio replacement.

What is in place now:

- A Rust extension module built with `pyo3` and packaged with `maturin`
- A Tokio-backed selector implementation wired into `asyncio.SelectorEventLoop`
- A Python event-loop policy that installs the Kioto loop cleanly through the stdlib contract
- A Rust-side polling core that drives fd readiness through Tokio `AsyncFd`
- A loop-owned completion port that lets Tokio workers resolve Python futures on the loop thread

## Layout

- `src/lib.rs`: Rust extension entrypoint
- `src/completion.rs`: pipe-backed completion queue for Tokio-to-Python wakeups
- `src/poller.rs`: Tokio-backed readiness polling core
- `python/kioto/loop.py`: selector adapter and event-loop policy
- `pyproject.toml`: `maturin` build configuration

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
maturin develop
python examples/bridge.py
```

## Scope

Kioto now implements the event-loop contract by plugging a Tokio-backed selector into the stdlib selector event loop. This gives you task scheduling, timers, thread-safe wakeups, fd readers/writers, raw socket helpers, and the transport/server/client APIs that already exist in `asyncio`.

The Rust/Python future bridge APIs now resolve through a loop-owned completion port instead of calling back into `asyncio` directly from Tokio worker threads.

For Python 3.14+ the preferred entrypoint is `kioto.run(...)`, which uses `asyncio.Runner(loop_factory=KiotoEventLoop)` instead of the deprecated global policy API.

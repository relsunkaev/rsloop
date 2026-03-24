# Performance Experiment Log

Last updated: 2026-03-24

This file tracks the optimization experiments tried during the current rsloop
performance pass, including changes that were reverted.

## Measurement Notes

- Unless noted otherwise, measurements came from `benchmarks/loops.py`.
- Earlier experiments were mostly measured as sequential A/B runs or
  `rsloop` vs `uvloop` comparisons.
- Later experiments used a stricter interleaved A/B method: baseline and
  current were alternated one sample at a time with `PYTHONPATH` pinned to
  per-worktree builds. Prefer those results when they conflict with earlier
  sequential runs.
- A change was kept only if it improved app-shaped workloads or meaningfully
  improved an important path without causing meaningful regressions elsewhere.

## Kept Experiments

### 1. Cache buffered protocol detection once

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change: cache whether a protocol implements the buffered protocol hooks at
  transport construction time instead of repeating Python `hasattr()` checks on
  hot read/write paths.
- Result:
  - `tcp_rpc_parallel` median `0.213446s -> 0.200620s` (`-6.0%`)
  - `tcp_echo_parallel` median `0.199205s -> 0.192231s` (`-3.5%`)
  - profiled `tcp_rpc_parallel`: `fd_dispatch 0.099s -> 0.081s`,
    `writes 0.063s -> 0.055s`
- Decision: kept
- Commit: `4531670` `perf(stream): cache buffered protocol detection`

### 2. Tighten native `readuntil()` scanning

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`tests/test_loop.py`](../tests/test_loop.py)
- Change: carry a rolling search offset for `readuntil()`, use `memmem`
  instead of rescanning from byte 0, and fix `LimitOverrunError` behavior to
  match asyncio.
- Result:
  - `http1_keepalive_small` `0.278939s -> 0.211885s` (`-24.0%`)
  - `http1_streaming_response` `0.107469s -> 0.105118s` (`-2.2%`)
  - `websocket_echo_parallel` `0.090407s -> 0.089733s` (`-0.7%`)
  - `tls_http1_keepalive` `0.189513s -> 0.182814s` (`-3.5%`)
  - vs `uvloop`:
    - `http1_keepalive_small` `1.235x -> 1.055x`
    - `asgi_json_echo` `1.145x -> 1.074x`
    - `tls_http1_keepalive` `1.523x -> 1.174x`
- Decision: kept
- Commit: `3898d49` `perf(stream): tighten native readuntil scanning`

### 3. Move connection setup into native `activate()`

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change: move reader method rebinding and `protocol.connection_made()` into
  native transport activation, reducing Python work in connection setup.
- Result:
  - accepted based on improved connection-heavy and app-shaped paths
  - recorded post-change ratios vs `uvloop`:
    - `http1_connect_per_request` `1.130x`
    - `http1_keepalive_small` `1.021x`
    - `tcp_rpc_parallel` `1.095x`
    - `tls_http1_keepalive` `1.303x`
    - `grpc_like_unary` `1.118x`
- Decision: kept
- Commit: `3b2ff71` `perf(stream): move connection setup into native activate`

### 21. Reuse cached buffered protocol state during readable dispatch

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - use the transport's cached `buffered_protocol` flag in
    `StreamCore::on_readable()` instead of re-running Python
    `hasattr("get_buffer")` / `hasattr("buffer_updated")` checks on every
    readable event
  - refresh the cached flag inside `set_protocol()` so `start_tls()` and other
    protocol swaps keep working
- Result from interleaved A/B against clean `HEAD`:
  - `tls_http1_keepalive` `0.157336s -> 0.156412s` (`-0.59%`)
  - `http1_keepalive_small` `0.205492s -> 0.211748s` (`+3.04%`)
  - `asgi_json_echo` `0.114812s -> 0.111509s` (`-2.88%`)
  - `asgi_keepalive` `0.127271s -> 0.112124s` (`-11.90%`)
  - `grpc_like_unary` `0.115801s -> 0.111559s` (`-3.66%`)
  - `start_tls_upgrade` `0.044751s -> 0.034159s` (`-23.67%`)
- Result vs `uvloop` after the patch:
  - `http1_keepalive_small` `1.058x`
  - `asgi_json_echo` `1.079x`
  - `asgi_keepalive` `1.056x`
  - `grpc_like_unary` `0.998x` raw median, `1.040x` paired
  - `tls_http1_keepalive` `1.668x`
  - `start_tls_upgrade` `0.900x` raw median, `1.264x` paired
- Notes:
  - the first draft of this change broke `test_native_start_tls` because
    `set_protocol()` did not refresh the cached flag; that fix is part of the
    final kept version
  - `start_tls_upgrade` remains noisy, so the interleaved A/B result is more
    trustworthy than the cross-loop paired ratio there
- Decision: kept; broad app-shaped wins outweighed the one small regression on
  `http1_keepalive_small`
- Artifact:
  [`benchmarks/out/ab-interleaved-buffered-flag-fixed/summary.json`](out/ab-interleaved-buffered-flag-fixed/summary.json)

## Reverted Experiments

### 4. Immediate small direct writes up to 256 bytes

- Area: stream write path
- Change: try immediate direct writes for very small payloads.
- Result:
  - `tcp_rpc_parallel` regressed to `0.215717s`
- Decision: reverted

### 5. Combined pipe and TLS patch

- Area: pipe/TLS transport paths
- Result:
  - `start_tls_upgrade` `1.619x -> 1.301x` vs `uvloop`
  - `tls_handshake_parallel` `1.108x -> 1.196x`
  - `tls_http1_keepalive` `1.226x -> 1.876x`
  - `pipe_write` `1.365x -> 1.345x`
  - `subprocess_exec` `1.128x -> 1.112x`
  - `subprocess_shell` `1.089x -> 1.063x`
- Decision: reverted because the TLS keepalive regression dominated

### 6. `set_protocol()` cache fix alone

- Area: TLS / protocol switching
- Result:
  - `start_tls_upgrade` `1.619x -> 0.941x`
  - `tls_handshake_parallel` `1.108x -> 0.997x`
  - `tls_http1_keepalive` `1.226x -> 1.738x`
- Decision: reverted because TLS keepalive regressed badly

### 7. `drain()` ready-awaitable fast path

- Area: stream/drain scheduling
- Result:
  - improved some RPC numbers
  - regressed HTTP/ASGI/TLS cases in tighter A/B
- Decision: reverted

### 8. Native ready-handle / task-wakeup path cleanup

- Area: scheduler/ready queue
- Result:
  - mixed in loose runs
  - collapsed to noise or regressions in tighter A/B
- Decision: reverted

### 9. Broader 1 KB write coalescing and larger `writev` batches

- Area: stream write batching
- Result:
  - looked good in loose runs
  - did not hold up in tighter A/B and produced regressions/noise
- Decision: reverted

### 10. Direct native transport setup path

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change: push more client transport setup directly through native code.
- Result:
  - `http1_connect_per_request` `-21.97%`
  - `http1_keepalive_small` `+11.03%`
  - `asgi_json_echo` `+20.12%`
  - `grpc_like_unary` `+3.85%`
  - `tls_http1_keepalive` `+3.28%`
- Decision: reverted because steady-state app traffic regressed

### 11. Native server accept driver using existing `SocketState`

- Area: accept path / server lifecycle
- Result:
  - `http1_connect_per_request` `-15.91%`
  - `asgi_json_echo` `+9.62%`
  - `grpc_like_unary` `+6.40%`
  - `tls_http1_keepalive` `+10.29%`
- Decision: reverted

### 12. Inline first-callback storage

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Result:
  - `http1_keepalive_small` `+10.74%`
  - `asgi_json_echo` `+15.94%`
  - `grpc_like_unary` `+5.07%`
  - `tls_http1_keepalive` `+27.51%`
- Decision: reverted

### 13. Connection-setup-only cleanup

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Result:
  - looked safe on paper
  - hung on `http1_connect_per_request`
- Decision: reverted immediately

### 14. Direct waiter/future wakeups instead of the completion queue

- Area: stream waiters / scheduler
- Result:
  - `http1_connect_per_request` `-7.49%`
  - `asgi_streaming` `-2.73%`
  - `http1_streaming_response` `-1.79%`
  - `asgi_json_echo` `+8.97%`
  - `asgi_keepalive` `+6.49%`
  - `http1_keepalive_small` `+2.50%`
  - `grpc_like_unary` `+2.12%`
- Decision: reverted

### 15. Low-risk `readexactly()` hot-path cleanup

- Area: native buffered reads
- Result:
  - `http1_connect_per_request` `-13.92%`
  - `tls_http1_keepalive` `-3.93%`
  - `asgi_keepalive` `-2.06%`
  - `http1_streaming_response` `-3.68%`
  - `asgi_json_echo` `+14.66%`
  - `grpc_like_unary` `+7.51%`
  - `http1_keepalive_small` `+4.78%`
- Decision: reverted

### 16. TLS-only native pending waiters for `drain()` / `wait_closed()`

- Area: TLS waiter path
- Result:
  - `tls_http1_keepalive` `0.181289s -> 0.163063s` (`-10.05%`)
  - `asgi_json_echo` `+14.21%`
  - `http1_keepalive_small` `+8.34%`
  - `tcp_rpc_parallel` `+2.93%`
  - `grpc_like_unary` `+2.84%`
- Decision: reverted

### 17. Cache `_scheduler` per transport for native read waiters

- Area: stream waiter scheduling
- Result:
  - `tls_http1_keepalive` `0.181289s -> 0.163438s` (`-9.85%`)
  - `asgi_json_echo` `+4.06%`
  - `grpc_like_unary` `+3.04%`
  - `asgi_streaming` `+1.92%`
  - `http1_keepalive_small` `+1.51%`
- Decision: reverted

### 18. Guarded native fast path for plain `StreamReaderProtocol`

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change: bypass parts of stdlib `StreamReaderProtocol` setup/teardown for the
  simple non-buffered case.
- Result:
  - initial 7-repeat sweep looked mixed-positive
  - tighter 10-repeat confirmation failed:
    - `http1_connect_per_request` `0.044737s -> 0.046040s` (`+2.9%`)
    - `asgi_json_echo` `0.107037s -> 0.108342s` (`+1.2%`)
- Decision: reverted

### 19. Scheduler hot-loop handle classification reuse

- Area: [`src/scheduler.rs`](../src/scheduler.rs)
- Change: compute native handle kind once and reuse it instead of reclassifying
  immediately before execution.
- Result:
  - `asgi_json_echo` `-2.1%`
  - `grpc_like_unary` `+3.3%`
  - `http1_keepalive_small` `+4.1%`
  - `future_completion_storm` `+0.1%`
  - `task_create_gather` `-0.4%`
  - `task_cancel_storm` `-0.3%`
- Decision: reverted

### 20. Direct `_asyncio.Task.task_wakeup` fast path

- Area: [`src/handles.rs`](../src/handles.rs)
- Change: cache a direct C callback target for `Task.task_wakeup` inside
  `OneArgHandle` and bypass the generic bound-method call path.
- Early sequential A/B result:
  - `asgi_json_echo` `-3.12%`
  - `asgi_keepalive` `+0.18%`
  - `asgi_streaming` `-2.22%`
  - `grpc_like_unary` `-4.08%`
  - `http1_keepalive_small` `-5.17%`
  - `tls_http1_keepalive` `-19.00%`
  - `future_completion_storm` `+2.04%`
  - `task_create_gather` `+3.09%`
  - `tcp_rpc_parallel` `+0.92%`
- Later interleaved A/B result:
  - `asgi_json_echo` `+1.90%`
  - `asgi_keepalive` `-2.62%`
  - `asgi_streaming` `+5.47%`
  - `grpc_like_unary` `-3.15%`
  - `http1_keepalive_small` `+5.99%`
  - `tls_http1_keepalive` `+5.67%`
  - `tcp_rpc_parallel` `+6.64%`
  - `future_completion_storm` `-4.75%`
  - `task_create_gather` `-2.87%`
- Decision: reverted; the stricter interleaved A/B showed too many app-shaped
  regressions
- Artifact: [`benchmarks/out/ab-interleaved-taskwakeup/summary.json`](out/ab-interleaved-taskwakeup/summary.json)

### 21. Task wakeup fast path with smaller handle state

- Area: [`src/handles.rs`](../src/handles.rs)
- Change: cache a direct C callback target for `Task.task_wakeup` inside
  `OneArgHandle`, then shrink the stored direct-call state to reduce handle
  footprint.
- Full-profile result:
  - `asgi_json_echo` `1.062x`
  - `asgi_keepalive` `1.060x`
  - `asgi_streaming` `1.121x`
  - `grpc_like_unary` `0.955x`
  - `http1_keepalive_small` `0.997x`
  - `http1_streaming_response` `1.149x`
  - `tcp_rpc_parallel` `1.072x`
  - `future_completion_storm` `1.329x`
  - `task_create_gather` `1.196x`
  - `tls_http1_keepalive` `1.623x`
  - `start_tls_upgrade` `0.884x`
- Decision: reverted; the broader full profile still showed the TLS and
  task-heavy regressions were not worth the app-side wins
- Artifact: [`benchmarks/out/perf-full-taskwakeup.json`](out/perf-full-taskwakeup.json)

## Open Direction

The main conclusion so far is that callback-level micro-optimizations are not
reliably translating into user-visible wins. The remaining credible directions
are:

- deeper connection/setup lifecycle changes
- buffered-read / stream lifecycle work
- TLS path work that improves real app traffic instead of just handshake or
  microbenchmarks

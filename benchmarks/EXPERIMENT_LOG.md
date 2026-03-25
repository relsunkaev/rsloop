# Performance Experiment Log

Last updated: 2026-03-25

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

### 22. Logical read cursor with raw-pointer access

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - keep unread bytes in the Python `bytearray` behind a logical cursor instead
    of front-shifting on every partial consume
  - switch the hot read helpers to raw bytearray pointers so the cursor does
    not pay extra `clone_ref()` / `bind()` churn
  - compact lazily only when the consumed prefix grows large
- Result:
  - `asgi_streaming` `0.109066s` vs `0.134885s` (`0.809x` vs `uvloop`)
  - `start_tls_upgrade` `0.043223s` vs `0.073634s` (`0.587x` vs `uvloop`)
  - `http1_keepalive_small` `0.223702s` vs `0.214296s` (`1.044x` vs `uvloop`)
  - `asgi_keepalive` landed around parity in the cleaner targeted runs
  - remaining gaps stayed in the TLS / JSON / gRPC-style paths:
    `tls_http1_keepalive`, `asgi_json_echo`, `grpc_like_unary`,
    `http1_streaming_response`, `tcp_accept_burst`
- Decision: kept
- Notes:
  - this is the first buffer experiment that clearly helped `start_tls_upgrade`
    and the streaming keepalive-style paths at the same time
  - the TLS keepalive regression is not explained by the stream-buffer profile
    alone; see Open Direction

### 23. Single-mode runtime boundary and real-workload perf gates

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py),
  [`python/rsloop/__init__.py`](../python/rsloop/__init__.py),
  [`benchmarks/loops.py`](../benchmarks/loops.py),
  [`benchmarks/revision_ab.py`](../benchmarks/revision_ab.py)
- Change:
  - add a `real` profile with the app-shaped gate scenarios used for later
    subsystem rewrites
  - simplify the public runtime surface back down to one default/native path
    while keeping the new A/B measurement gates
- Result:
  - the boundary scaffolding itself is not a meaningful speedup; it is kept
    only where it improved measurement discipline without changing steady-state
    behavior
  - interleaved A/B paired medians vs clean `HEAD`:
    - `http1_keepalive_small` `0.940x`
    - `asgi_json_echo` `0.931x`
    - `tls_http1_keepalive` `0.987x` after a stricter 7-repeat confirmation
  - the stricter TLS run had a worse raw median (`+6.56%`) but still a better
    paired median, so it was treated as noise rather than a real regression
- Decision: kept only as benchmark/profile infrastructure; dropped the public
  mode split so the native path stays the only mode

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

### 23. Tighten read-buffer compaction to 8 KiB

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change: compact the logical read cursor sooner.
- Result:
  - `asgi_keepalive` `1.100x`
  - `tls_http1_keepalive` `1.355x`
  - `asgi_json_echo` `1.642x`
  - `grpc_like_unary` `1.066x`
  - `start_tls_upgrade` `1.130x`
- Decision: reverted

### 24. Tighten read-buffer compaction to 32 KiB

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change: try a middle-ground compaction threshold between 8 KiB and 64 KiB.
- Result:
  - `asgi_keepalive` `1.014x`
  - `tls_http1_keepalive` `1.545x`
- Decision: reverted

### 25. Cache buffered TLS callbacks and skip the scheduled `_on_readable` hop

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change: cache `get_buffer` / `buffer_updated` bound methods for buffered
  protocols, and drive buffered readable handling directly from the native fd
  callback instead of scheduling a separate `_on_readable` handle.
- Result:
  - `http1_keepalive_small` `1.047x` paired vs `uvloop` on the guard run
  - `tls_http1_keepalive` `1.544x` paired vs `uvloop`, down from `1.670x` in
    the prior stored run
  - `start_tls_upgrade` `0.975x` paired vs `uvloop`
  - `tls_handshake_parallel` `1.063x` paired vs `uvloop`
  - runtime profile `ready` dropped from about `0.0607s` to `0.0500s` on the
    256-request sample
- Decision: kept
- Artifacts:
  - [`benchmarks/out/ab-current-tls-cached-hooks-tls.json`](out/ab-current-tls-cached-hooks-tls.json)
  - [`benchmarks/out/ab-current-tls-cached-hooks-starttls.json`](out/ab-current-tls-cached-hooks-starttls.json)
  - [`benchmarks/out/ab-current-tls-cached-hooks-handshake.json`](out/ab-current-tls-cached-hooks-handshake.json)
  - [`benchmarks/out/tls-http1-keepalive-runtime-tls-cached-hooks.json`](out/tls-http1-keepalive-runtime-tls-cached-hooks.json)

### 26. Native TLS app-data / callback handling pass

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py),
  [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`src/stream_registry.rs`](../src/stream_registry.rs)
- Change: attempt a deeper native TLS path to shift wrapped read/write handling
  and callback ordering out of stdlib `SSLProtocol`.
- Result:
  - direct TLS HTTP repro failed with
    `ssl.SSLError: [SSL: RECORD_LAYER_FAILURE] record layer failure`
  - the server-side plaintext read path broke before benchmark validation
- Decision: reverted
- Notes:
  - the experiment did not produce stable measurements, so it should not be
    treated as a benchmark result
  - the failure points at remaining correctness work around TLS record-layer
    handling and read ordering

### 27. Native stream reader/writer state with no per-instance rebinding

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py),
  [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`tests/test_loop.py`](../tests/test_loop.py)
- Change:
  - add native-aware `RsloopStreamReader` / `RsloopStreamWriter` classes
  - stop mutating per-connection reader and writer methods
  - move plain-stream read waiter, drain, and close-wait bookkeeping behind
    cached native state objects
- Result versus clean `HEAD` on targeted `rsloop`-only A/B:
  - `http1_keepalive_small` `0.244230791s -> 0.243691208s` (`-0.22%`)
  - `asgi_json_echo` `0.124061125s -> 0.126072584s` (`+1.62%`)
  - `asgi_streaming` `0.106495458s -> 0.117260292s` (`+10.11%`)
  - `http1_streaming_response` `0.113535000s -> 0.113882292s` (`+0.31%`)
  - `grpc_like_unary` `0.115282458s -> 0.118118167s` (`+2.46%`)
  - `tls_http1_keepalive` `0.170259417s -> 0.162731625s` (`-4.42%`)
  - `http1_connect_per_request` `0.040000667s -> 0.035883792s` (`-10.29%`)
- Decision: reverted; the app-shaped regressions were too broad, especially
  `asgi_streaming`

### 28. Writer-only native state split

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py),
  [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`tests/test_loop.py`](../tests/test_loop.py)
- Change:
  - keep only the native writer state and class-level writer methods
  - restore the existing reader fast path and per-instance reader rebinding
- Result versus clean `HEAD` on targeted `rsloop`-only A/B:
  - `http1_keepalive_small` `0.244230791s -> 0.250213667s` (`+2.45%`)
  - `asgi_json_echo` `0.124061125s -> 0.122171458s` (`-1.52%`)
  - `asgi_streaming` `0.106495458s -> 0.114242084s` (`+7.27%`)
  - `http1_streaming_response` `0.113535000s -> 0.114681833s` (`+1.01%`)
  - `grpc_like_unary` `0.115282458s -> 0.114760541s` (`-0.45%`)
  - `http1_connect_per_request` `0.040000667s -> 0.043870834s` (`+9.68%`)
- Additional result:
  - `tls_http1_keepalive` benchmark failed after a constructor-arity regression
    in the SSL transport call site
- Decision: reverted; too many regressions and a TLS correctness break

### 29. Stream waiter context-copy C-API fast path

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - replace Python-level `contextvars.copy_context()` in
    `StreamWaiter.add_done_callback()` with the C-API
    `PyContext_CopyCurrent()`
- Initial targeted `rsloop`-only A/B versus clean `HEAD`:
  - `http1_keepalive_small` `0.228305542s -> 0.219377458s` (`-3.91%`)
  - `http1_streaming_response` `0.106581542s -> 0.105338125s` (`-1.17%`)
  - `asgi_json_echo` `0.118730375s -> 0.114386000s` (`-3.66%`)
  - `asgi_keepalive` `0.117113292s -> 0.122719792s` (`+4.79%`)
  - `asgi_streaming` `0.107081042s -> 0.105759334s` (`-1.23%`)
  - `grpc_like_unary` `0.107249625s -> 0.108508166s` (`+1.17%`)
  - `tls_http1_keepalive` `0.154162250s -> 0.151561750s` (`-1.69%`)
  - `start_tls_upgrade` `0.049731292s -> 0.040155833s` (`-19.25%`)
- Tightened interleaved A/B:
  - `http1_keepalive_small` `+1.90%`
  - `asgi_json_echo` `+3.01%`
  - `asgi_keepalive` `+5.62%`
  - `asgi_streaming` `-2.11%`
  - `grpc_like_unary` `+5.36%`
  - `tls_http1_keepalive` `-4.17%`
- Decision: reverted; the tighter run showed broader real-workload regressions
  than wins
- Artifacts:
  - [`benchmarks/out/ab-contextcopy-baseline-summary.json`](out/ab-contextcopy-baseline-summary.json)
  - [`benchmarks/out/ab-contextcopy-current-summary.json`](out/ab-contextcopy-current-summary.json)
  - [`benchmarks/out/ab-contextcopy-interleaved-summary.json`](out/ab-contextcopy-interleaved-summary.json)

### 30. Env-gated Python task with direct `StreamWaiter` resume

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py),
  [`python/rsloop/task.py`](../python/rsloop/task.py),
  [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - prototype an env-gated custom task factory based on `asyncio.tasks._PyTask`
  - when a task awaited a native `StreamWaiter`, register the task directly on
    the waiter and resume it through a zero-arg handle instead of scheduling
    the usual one-arg `Task.task_wakeup` callback
- Functional result:
  - the first prototype needed extra compatibility shims for
    `asyncio.current_task()` / `asyncio.all_tasks()` in Python 3.14 because the
    default C builtins do not see `_PyTask`
  - after those shims, targeted loop tests passed under the env flag
- Targeted `rsloop`-only A/B with the env flag off vs on:
  - `http1_keepalive_small` `0.298071458s -> 0.328320958s` (`+10.15%`)
  - `http1_streaming_response` `0.117935292s -> 0.118046542s` (`+0.09%`)
  - `asgi_json_echo` `0.148487083s -> 0.152993709s` (`+3.04%`)
  - `asgi_keepalive` `0.170888833s -> 0.178208625s` (`+4.28%`)
  - `asgi_streaming` `0.117828708s -> 0.124516708s` (`+5.68%`)
  - `grpc_like_unary` `0.136319208s -> 0.143473292s` (`+5.25%`)
  - `tls_http1_keepalive` `0.196055500s -> 0.173776875s` (`-11.36%`)
- Runtime-profile result:
  - on `http1_keepalive_small`, the direct waiter path worked mechanically:
    `ready_zero_arg` rose to `16389` and `ready_one_arg` fell to `4`
  - on `tls_http1_keepalive`, the direct waiter path did **not** apply because
    the hot TLS waits still go through stdlib SSL/Future machinery; the trace
    still showed `RsloopTask.__wakeup` dominating one-arg callbacks
- Additional conclusion:
  - subclassing the C `_asyncio.Task` does not expose an overrideable
    `task_wakeup` hook, so a C-task version of this prototype is not reachable
    from Python alone
- Decision: reverted; the pure-Python task needed to prototype the direct
  waiter resume path regressed too many non-TLS workloads even though the
  mechanism itself worked for plain stream waiters

### 31. TLS buffered-read batching for stdlib `SSLProtocol`

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - for buffered/TLS protocols, try to fill more of the writable buffer before
    calling `buffer_updated()`
  - batch multiple raw `recv_into()` reads into one dispatch and carry EOF
    through the same buffered callback path
- Rationale:
  - runtime traces showed TLS steady-state traffic still dominated by
    scheduler `ready` time and stdlib `SSLProtocol` callbacks
  - the working hypothesis was that rsloop was delivering overly fragmented
    encrypted reads, leading to too many `buffer_updated()` /
    `_do_read__buffered()` / `feed_data()` hops
- Functional result:
  - the first implementation had a compile break in `on_readable()` because the
    new EOF-after-buffered path referenced a missing `protocol` binding; fixed
    during the experiment
  - focused smokes passed after the fix:
    `test_native_start_tls` and `test_tcp_socket_helpers_repeated_roundtrip`
- Initial interleaved A/B versus clean `HEAD` (3 repeats):
  - `tls_http1_keepalive` `0.240535459s -> 0.191891250s` (`-20.22%`)
  - `start_tls_upgrade` `0.046506250s -> 0.047563000s` (`+2.27%`)
  - `http1_keepalive_small` `0.320607125s -> 0.315441917s` (`-1.61%`)
  - `asgi_keepalive` `0.169853167s -> 0.165371792s` (`-2.64%`)
  - `grpc_like_unary` (3 repeats) initially looked worse at `+4.40%`
- Follow-up checks:
  - `grpc_like_unary` did **not** hold the regression on a tighter 5-repeat A/B:
    `0.141363s -> 0.139675s` (`-1.19%`)
  - direct current-vs-`uvloop` spot checks only showed a small absolute change:
    - `tls_http1_keepalive` `1.528x`
    - `http1_keepalive_small` `1.053x`
    - `grpc_like_unary` `1.029x`
  - a tighter 5-repeat interleaved A/B on the main target collapsed the win to
    noise:
    - `tls_http1_keepalive` `0.181930s -> 0.180706s` (`-0.67%`)
  - `asgi_json_echo` produced wildly inconsistent A/B rounds during this pass
    (`0.166s`, `1.216s`, `1.296s` baseline samples), so it was discarded as a
    decision signal for this experiment
- Decision: reverted; the promising 3-repeat TLS keepalive result was a false
  positive and did not survive a tighter 5-repeat A/B
- Artifacts:
  - [`benchmarks/out/ab-tls-http1-keepalive-batch.json`](out/ab-tls-http1-keepalive-batch.json)
  - [`benchmarks/out/ab-tls-http1-keepalive-batch-r5.json`](out/ab-tls-http1-keepalive-batch-r5.json)
  - [`benchmarks/out/ab-start-tls-upgrade-batch.json`](out/ab-start-tls-upgrade-batch.json)
  - [`benchmarks/out/ab-http1-keepalive-small-batch.json`](out/ab-http1-keepalive-small-batch.json)
  - [`benchmarks/out/ab-asgi-keepalive-batch.json`](out/ab-asgi-keepalive-batch.json)
  - [`benchmarks/out/ab-grpc-like-unary-batch-r5.json`](out/ab-grpc-like-unary-batch-r5.json)
  - [`benchmarks/out/ab-asgi-json-echo-batch.json`](out/ab-asgi-json-echo-batch.json)

### 32. TLS app-protocol coalescing above stdlib `SSLProtocol`

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change:
  - wrap TLS `StreamReaderProtocol` instances in a small protocol proxy that
    buffers multiple `data_received()` calls and flushes them once with
    `loop.call_soon()`
- Rationale:
  - fresh TLS traces showed rsloop still delivering roughly `8070`
    `feed_data()` calls for `968704` bytes where `uvloop` delivered `4096`
  - the goal was to cut `reader.feed_data()` wakeups without rewriting
    `SSLProtocol`
- Functional result:
  - focused smokes passed:
    `test_native_start_tls` and `test_tcp_socket_helpers_repeated_roundtrip`
- Runtime trace result on `tls_http1_keepalive`:
  - `feed_data_calls` dropped `8070 -> 6479`
  - but the benchmark got slower `0.178044125s -> 0.239805041s`
  - scheduler `ready` time rose `0.099186s -> 0.163925s`
  - `Task.task_wakeup` total rose `62.392ms -> 71.437ms`
  - header/body phases moved the wrong way overall:
    - `http.client.read_response_head` `972.888ms -> 1069.220ms`
    - `http.server.read_headers` `969.741ms -> 1060.850ms`
- Conclusion:
  - delaying the first decrypted chunk to coalesce later chunks hurt latency
    more than it helped wakeup count
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/tls_http1_keepalive-rsloop-runtime-coalesced.json`](out/tls_http1_keepalive-rsloop-runtime-coalesced.json)

### 33. Native TLS decrypted-buffer reader bridge

- Area: [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`src/lib.rs`](../src/lib.rs),
  [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change:
  - prototype a native `StreamReaderBridge` for TLS streams
  - keep stdlib `SSLProtocol`, but patch TLS `StreamReader` instances so
    `readexactly()` / `readuntil()` use rsloop-native waiters and scans on top
    of the decrypted `reader._buffer`
  - wrap the TLS app protocol so the bridge gets immediate
    `after_data_received()` / `after_eof()` notifications with no deferred
    coalescing
- Rationale:
  - the coalescing pass proved that reducing `feed_data()` count by delaying
    delivery was the wrong trade
  - the next idea was to leave delivery immediate but cut the Python work in
    stdlib `readuntil()` / `readexactly()` after each TLS chunk
- Functional result:
  - compile issues around Rust borrowing were fixed during the experiment
  - focused smokes passed:
    `test_native_start_tls` and `test_tcp_socket_helpers_repeated_roundtrip`
- Runtime trace result on `tls_http1_keepalive`:
  - headers improved slightly:
    - `http.client.read_response_head` `972.888ms -> 965.646ms`
    - `http.server.read_headers` `969.741ms -> 966.728ms`
  - but the overall path still regressed:
    - sample `0.178044125s -> 0.193317375s`
    - scheduler `ready` time `0.099186s -> 0.115499s`
    - `Task.task_wakeup` total `62.392ms -> 66.483ms`
- Interleaved A/B versus clean `HEAD`:
  - `tls_http1_keepalive` `0.160595s -> 0.177139s` (`+10.30%`)
  - `http1_keepalive_small` `0.304442s -> 0.326810s` (`+7.35%`)
- Conclusion:
  - moving TLS `readuntil()` / `readexactly()` onto native waiters without a
    deeper TLS protocol change still added enough extra machinery around the
    existing stdlib path to lose overall
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/tls_http1_keepalive-rsloop-runtime-reader-bridge.json`](out/tls_http1_keepalive-rsloop-runtime-reader-bridge.json)
  - [`benchmarks/out/ab-tls-http1-keepalive-reader-bridge.json`](out/ab-tls-http1-keepalive-reader-bridge.json)
  - [`benchmarks/out/ab-http1-keepalive-small-reader-bridge.json`](out/ab-http1-keepalive-small-reader-bridge.json)

## Open Direction

### 34. Native datagram transport replacement

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change:
  - replace stdlib `_SelectorDatagramTransport` with an rsloop-owned datagram
    transport using rsloop `_add_reader()` / `_add_writer()` directly
  - activate `connection_made()`, reader registration, and the setup waiter
    immediately instead of deferring them with `call_soon()`
- Rationale:
  - `udp_datagram_endpoint` was still stdlib-backed and behind `uvloop`
  - the first low-risk cut was to own the transport without changing raw UDP
    socket helpers or scheduler internals
- Functional result:
  - focused datagram and policy tests passed
- Interleaved A/B versus clean `HEAD`:
  - `udp_datagram_endpoint` `0.220391s -> 0.218247s` (`-0.97%`)
- Conclusion:
  - the transport swap was effectively neutral; the remaining UDP gap is not
    in the thin Python transport wrapper
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-udp-datagram-native.json`](out/revision-ab-udp-datagram-native.json)

### 35. Native write-pipe transport with writev batching

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change:
  - replace stdlib `_UnixWritePipeTransport` with an rsloop-owned write-pipe
    transport
  - keep connection lifecycle inline and replace stdlib's growing `bytearray`
    buffer with queued chunks plus `os.writev()` when draining
  - test a second variant that coalesced small writes into the queue before
    attempting an immediate `os.write()`
- Rationale:
  - `pipe_write` remained one of the worst full-profile laggards
  - write-side copy and flush behavior looked like the most promising lever
- Functional result:
  - focused subprocess/pipe tests passed for both variants
- Interleaved A/B versus clean `HEAD`:
  - writev queue variant:
    `pipe_write` `0.011642s -> 0.011454s` (`-1.62%`)
  - small-write coalescing variant:
    `pipe_write` `0.014417s -> 0.014891s` (`+3.29%`)
- Conclusion:
  - the writev queue variant helped slightly but did not clear the keep bar
  - small-write coalescing actively regressed the benchmark
  - the real `pipe_write` gap is not going to close with a Python-only
    transport swap
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-pipe-write-native.json`](out/revision-ab-pipe-write-native.json)
  - [`benchmarks/out/revision-ab-pipe-write-native-v2.json`](out/revision-ab-pipe-write-native-v2.json)

### 36. Native generic socket transport fallback

- Area:
  [`python/rsloop/loop.py`](../python/rsloop/loop.py),
  [`src/stream_transport.rs`](../src/stream_transport.rs),
  [`src/stream_registry.rs`](../src/stream_registry.rs),
  [`tests/test_loop.py`](../tests/test_loop.py),
  [`benchmarks/loops.py`](../benchmarks/loops.py)
- Change:
  - route all socket protocols through native `StreamTransport`, not just
    `StreamReaderProtocol`
  - keep stream-specific reader/writer rebinding only for
    `StreamReaderProtocol`
  - fix reentrant generic protocol control paths so `transport.close()` and
    `pause_reading()` can be called safely from protocol callbacks
  - add explicit tests for plain `Protocol` and `BufferedProtocol`
  - add a focused `tcp_protocol_echo` benchmark
- Rationale:
  - stdlib `_SelectorSocketTransport` was still the generic fallback
  - the Rust transport already had `reader is None` and buffered-protocol
    paths, so the smallest architectural step was to use that core for
    generic protocols too
- Functional result:
  - focused protocol tests passed after fixing reentrant close/registry borrows
  - targeted unittest slice passed with benchmark parser coverage
- Same-codebase interleaved A/B against forced selector fallback on
  `tcp_protocol_echo`:
  - first run: `0.188133s -> 0.149791s` (`-20.38%`)
  - strict confirmation: `0.180764s -> 0.153646s` (`-15.00%`), 6/7 wins
- Cross-loop result on the kept version:
  - `tcp_protocol_echo`: `0.973x` raw median and `0.983x` paired vs `uvloop`
- Guard checks on existing app-shaped cases:
  - `http1_keepalive_small`: `1.088x` vs `uvloop`
  - `asgi_json_echo`: `1.031x`
  - `tls_http1_keepalive`: `1.643x`
  - these stayed in the expected range for the existing codebase; no new
    regression signal from this slice
- Runtime trace note:
  - after routing the benchmark through native transport dispatch, scheduler
    `generic_fd_hits` dropped from `16386` to `1` and `stream_fd_hits` rose to
    `16385`
- Decision: kept
- Artifacts:
  - [`benchmarks/out/ab-tcp-protocol-echo-selector-vs-native.json`](out/ab-tcp-protocol-echo-selector-vs-native.json)
  - [`benchmarks/out/ab-tcp-protocol-echo-selector-vs-native-strict.json`](out/ab-tcp-protocol-echo-selector-vs-native-strict.json)
  - [`benchmarks/out/tcp-protocol-echo-all-final.json`](out/tcp-protocol-echo-all-final.json)
  - [`benchmarks/out/tcp-protocol-echo-selector-runtime.json`](out/tcp-protocol-echo-selector-runtime.json)
  - [`benchmarks/out/tcp-protocol-echo-native-runtime.json`](out/tcp-protocol-echo-native-runtime.json)

### 37. Native `StreamReaderProtocol.connection_made` bind

- Area: [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change:
  - replace stdlib `StreamReaderProtocol.connection_made()` with an rsloop
    helper for exact stdlib `StreamReaderProtocol` instances
  - use `RsloopStreamWriter` directly for server-side `client_connected_cb`
    callbacks
  - first try it on both plain streams and TLS, then narrow it to the TLS
    app-protocol path only
- Rationale:
  - this was the smallest shared slice that touched both the stream-helper path
    and the TLS app-protocol handoff without attempting a full SSLProtocol
    rewrite
- Functional result:
  - focused stream and `start_tls` tests passed
- Mixed plain-stream + TLS A/B against the old path:
  - `http1_keepalive_small`: `+7.60%`
  - `http1_connect_per_request`: `+4.57%`
  - `tls_http1_keepalive`: `-5.88%`
- Narrowed TLS-only A/B:
  - `tls_http1_keepalive`: `+0.80%`
  - `start_tls_upgrade`: `-36.74%` on the first 5-round run
  - `http1_keepalive_small`: `-1.22%`
- Strict confirmation on `start_tls_upgrade` did not hold:
  - `0.036657s -> 0.044439s` (`+21.23%`)
- Conclusion:
  - the combined stream/TLS bind regressed the plain stream workloads we care
    about
  - the TLS-only version looked promising at first but collapsed under a
    stricter confirmation run
  - the lifecycle handoff is too noisy to keep in its current form
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/ab-native-stream-reader-bind.json`](out/ab-native-stream-reader-bind.json)
  - [`benchmarks/out/ab-native-stream-reader-bind-tls-only.json`](out/ab-native-stream-reader-bind-tls-only.json)
  - [`benchmarks/out/ab-start-tls-upgrade-stream-bind-strict.json`](out/ab-start-tls-upgrade-stream-bind-strict.json)
  - [`benchmarks/out/start-tls-upgrade-stream-bind-final.json`](out/start-tls-upgrade-stream-bind-final.json)
  - [`benchmarks/out/tls-http1-keepalive-stream-bind-final.json`](out/tls-http1-keepalive-stream-bind-final.json)
  - [`benchmarks/out/http1-keepalive-small-stream-bind-final.json`](out/http1-keepalive-small-stream-bind-final.json)

The main conclusion so far is that callback-level micro-optimizations are not
reliably translating into user-visible wins unless they remove a real callback
hop or a repeated protocol lookup. The remaining credible directions are:

- deeper connection/setup lifecycle changes
- buffered-read / stream lifecycle work
- TLS path work that improves real app traffic instead of just handshake or
  microbenchmarks
- TLS keepalive profiling still shows scheduler `ready` time as the big bucket,
  but the latest TLS callback pass moved it from about `0.0607s` to `0.0500s`
  on the 256-request runtime sample; the next credible step is a deeper native
  SSLProtocol-style path or more callback specialization on top of it

### 38. Rsloop-owned TLS protocol replacement

- Area:
  - [`python/rsloop/loop.py`](../python/rsloop/loop.py)
  - `python/rsloop/sslproto.py` (reverted)
- Change:
  - add an rsloop-owned `RsloopSSLProtocol` subclass of stdlib
    `asyncio.sslproto.SSLProtocol`
  - override the app-transport path and copied-read path to special-case exact
    stdlib `StreamReaderProtocol` instances
  - wire `_make_ssl_transport()` to use the new protocol, with a temporary
    `RSLOOP_FORCE_STDLIB_SSLPROTO=1` override for same-codebase A/B
- Rationale:
  - this was the narrowest full-protocol replacement step that could attack the
    wrapped TLS steady-state path directly instead of continuing to optimize
    around stdlib `SSLProtocol`
- Functional result:
  - the first version broke `tls_http1_keepalive` with premature EOF because
    the copied-read path treated `SSLWantRead` as EOF
  - after fixing that sentinel bug, focused `start_tls` tests passed
- Same-codebase interleaved A/B against forced stdlib `SSLProtocol`:
  - `tls_http1_keepalive`: `0.208705s -> 0.218243s` (`+4.57%`)
  - `start_tls_upgrade`: `0.064489s -> 0.049020s` (`-23.99%`)
- Conclusion:
  - the replacement helped upgrade/handshake behavior but regressed the main
    wrapped steady-state target
  - the design still inherits too much stdlib `SSLProtocol` behavior to change
    the copied-read cost structure in the right direction
  - if rsloop replaces TLS for performance, it likely needs a truly native
    protocol/state machine instead of a stdlib subclass
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/ab-rsloop-sslproto-replacement.json`](out/ab-rsloop-sslproto-replacement.json)

### 39. Tuned rsloop TLS protocol replacement

- Area:
  - [`python/rsloop/loop.py`](../python/rsloop/loop.py)
  - [`python/rsloop/sslproto.py`](../python/rsloop/sslproto.py)
  - [`benchmarks/loops.py`](../benchmarks/loops.py)
- Change:
  - keep an rsloop-owned `RsloopSSLProtocol` as the default TLS protocol
  - cache exact stdlib `StreamReaderProtocol` reader hooks once in
    `_set_app_protocol()`
  - specialize copied-mode wrapped reads for the stream-helper case so TLS data
    goes straight to the cached reader `feed_data()` / `feed_eof()` hooks
  - cheapen `_process_outgoing()` by skipping empty BIO reads
  - extend the benchmark `sslproto` profiler to wrap rsloop-owned TLS methods
    without double-wrapping inherited stdlib methods
- Rationale:
  - the first TLS replacement draft was still paying too much stdlib-shaped
    overhead in the wrapped read path and the profiler itself became blind once
    rsloop owned more of the class
- Functional result:
  - focused TLS tests and benchmark parser coverage passed
  - the profiler fix added coverage for rsloop-owned TLS methods and a test for
    the subclass wrapping behavior
- Same-codebase interleaved A/B against forced stdlib `SSLProtocol`
  (no profiler):
  - `tls_http1_keepalive`: `0.207242s -> 0.182067s` (`-12.15%`), 4/5 wins
  - `start_tls_upgrade`: `0.057689s -> 0.034670s` (`-39.90%`), 4/5 wins
- Strict non-TLS guards:
  - `http1_keepalive_small`: `-1.10%`
  - `asgi_json_echo`: `+0.33%`
- Cross-loop spot checks against `uvloop`:
  - `tls_http1_keepalive`: `1.588x`
  - `start_tls_upgrade`: `0.962x`
  - `http1_keepalive_small`: `1.026x`
  - `asgi_json_echo`: `1.041x`
- Profiler note:
  - on the candidate run, wrapped `_process_outgoing` time dropped from about
    `0.0288s` to `0.0255s` and `_write_appdata` also dropped in the non-outlier
    samples; the tuned path moved the hot TLS buckets in the right direction
- Conclusion:
  - a Python-level rsloop-owned TLS path can improve the wrapped steady-state
    case if it owns enough of the copied read path and avoids extra profiler and
    empty-BIO overhead
  - this is still not a full native TLS state machine, but it is the first TLS
    replacement pass that held up on the main keepalive target
- Decision: kept
- Artifacts:
  - [`benchmarks/out/ab-rsloop-sslproto-prototype-v3-noprofile.json`](out/ab-rsloop-sslproto-prototype-v3-noprofile.json)
  - [`benchmarks/out/ab-rsloop-sslproto-guards-strict.json`](out/ab-rsloop-sslproto-guards-strict.json)
  - [`benchmarks/out/ab-rsloop-sslproto-prototype-v3.json`](out/ab-rsloop-sslproto-prototype-v3.json)
  - [`benchmarks/out/tls-http1-keepalive-all-candidate.json`](out/tls-http1-keepalive-all-candidate.json)
  - [`benchmarks/out/start-tls-upgrade-all-candidate.json`](out/start-tls-upgrade-all-candidate.json)
  - [`benchmarks/out/http1-keepalive-small-all-candidate.json`](out/http1-keepalive-small-all-candidate.json)
  - [`benchmarks/out/asgi-json-echo-all-candidate.json`](out/asgi-json-echo-all-candidate.json)

### 40. TLS multi-chunk native StreamReader append

- Area:
  - [`python/rsloop/sslproto.py`](../python/rsloop/sslproto.py)
  - [`src/stream_transport.rs`](../src/stream_transport.rs)
  - [`src/lib.rs`](../src/lib.rs)
- Change:
  - add `_rsloop.feed_stream_reader_data()` and `_rsloop.feed_stream_reader_eof()`
    so TLS can reuse native `StreamReader` buffer/waiter/backpressure updates
  - first tried routing every TLS `feed_data()` call through the new helper
  - narrowed that to a hybrid policy:
    - single-chunk copied reads keep using cached Python `reader.feed_data()`
    - two-chunk reads keep using Python `b''.join(...)` plus `feed_data()`
    - only 3+ chunk copied reads use the native append helper
- Rationale:
  - the tuned TLS protocol still paid Python allocation and `StreamReader`
    method overhead when a copied-mode TLS read produced multiple decrypted
    fragments
- Functional result:
  - full unittest suite passed after wiring the helper into the extension and
    rebuilding with `maturin develop --release`
- Revision A/B against the previous kept TLS revision (`HEAD`):
  - first fully-native handoff draft regressed `tls_http1_keepalive` by
    `+17.72%`
  - hybrid single-chunk path regressed by `+2.98%`
  - final 3+-chunk-only helper:
    - `tls_http1_keepalive`: `0.192875s -> 0.181689s` (`-5.80%`)
    - `start_tls_upgrade`: `0.039380s -> 0.036604s` (`-7.05%`)
- Guard checks:
  - `http1_keepalive_small`: `+0.45%` on a stricter 7-repeat / 2-warmup A/B
  - `asgi_json_echo`: `-2.79%`
- Cross-loop spot checks were noisy on macOS and not used as the keep/revert
  gate for this slice; the interleaved revision A/Bs were materially more
  stable for this change
- Conclusion:
  - native `StreamReader` mutation can help TLS steady-state traffic, but only
    when it replaces a meaningfully expensive multi-fragment path
  - applying the helper to every decrypted chunk was a regression; the final
    win came from using it only when TLS produced 3+ copied chunks
- Decision: kept
- Artifacts:
  - [`benchmarks/out/revision-ab-tls-reader-helper-v3.json`](out/revision-ab-tls-reader-helper-v3.json)
  - [`benchmarks/out/revision-ab-start-tls-reader-helper-v3.json`](out/revision-ab-start-tls-reader-helper-v3.json)
  - [`benchmarks/out/revision-ab-http1-reader-helper-v3-strict.json`](out/revision-ab-http1-reader-helper-v3-strict.json)
  - [`benchmarks/out/revision-ab-asgi-reader-helper-v3.json`](out/revision-ab-asgi-reader-helper-v3.json)

### 41. Native TLS copied-read hot path

- Area:
  - [`python/rsloop/sslproto.py`](../python/rsloop/sslproto.py)
  - [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - move the exact `RsloopSSLProtocol` copied-mode wrapped-read path out of
    Python and into `StreamTransport`
  - bypass Python `buffer_updated()` / `_do_read()` / `_do_read__copied()` on
    the hot path
  - keep handshake, buffered app protocols, and non-rsloop TLS shapes on the
    existing fallback path
  - cache bound TLS helpers once on the protocol object:
    - incoming `MemoryBIO.write`
    - `SSLObject.read`
    - outgoing `MemoryBIO.read`
    - transport `write`
    - stream reader buffer and limit
- Rationale:
  - the remaining steady-state TLS gap was still dominated by Python wrapped
    read dispatch after the earlier TLS wins
  - the goal of this pass was to remove Python control flow from the stream
    reader copied-read path instead of only making the existing Python code
    cheaper
- Tuning notes:
  - first Rust cut regressed `tls_http1_keepalive` by `+6.61%`
  - switching from generic helper/list delivery to direct native reader-buffer
    append reduced that to `-0.39%`
  - caching the bound TLS methods and reader buffer on the protocol made the
    pass hold up materially
- Functional result:
  - `cargo test -q` passed
  - `./.venv/bin/python -m unittest tests.test_loop tests.test_benchmarks -q`
    passed
- Revision A/B against `HEAD`:
  - `tls_http1_keepalive`: `0.200402s -> 0.178998s` (`-10.68%`)
  - `start_tls_upgrade`: `0.062691s -> 0.041565s` (`-33.70%`)
- Strict guards:
  - `http1_keepalive_small`: `0.322324s -> 0.321069s` (`-0.39%`)
  - `asgi_json_echo`: `0.169061s -> 0.168592s` (`-0.28%`)
- Cross-loop spot checks against `uvloop`:
  - `tls_http1_keepalive`: `1.458x`
  - `start_tls_upgrade`: `1.041x`
  - `http1_keepalive_small`: `1.049x`
  - `asgi_json_echo`: `1.042x`
- Conclusion:
  - moving the copied-read wrapped TLS path into Rust is a real same-codebase
    win once the Rust side stops rebinding Python methods and appending through
    generic Python iterables
  - the main remaining TLS gap vs `uvloop` is now lower in the wrapped path
    than the Python `SSLProtocol` control loop we started from
- Decision: kept
- Artifacts:
  - [`benchmarks/out/revision-ab-tls-native-hotpath-v3.json`](out/revision-ab-tls-native-hotpath-v3.json)
  - [`benchmarks/out/revision-ab-start-tls-native-hotpath-v3.json`](out/revision-ab-start-tls-native-hotpath-v3.json)
  - [`benchmarks/out/revision-ab-http1-native-hotpath-v3-strict.json`](out/revision-ab-http1-native-hotpath-v3-strict.json)
  - [`benchmarks/out/revision-ab-asgi-native-hotpath-v3.json`](out/revision-ab-asgi-native-hotpath-v3.json)
  - [`benchmarks/out/tls-http1-keepalive-all-native-hotpath-v3.json`](out/tls-http1-keepalive-all-native-hotpath-v3.json)
  - [`benchmarks/out/start-tls-upgrade-all-native-hotpath-v3.json`](out/start-tls-upgrade-all-native-hotpath-v3.json)
  - [`benchmarks/out/http1-keepalive-small-all-native-hotpath-v3.json`](out/http1-keepalive-small-all-native-hotpath-v3.json)
  - [`benchmarks/out/asgi-json-echo-all-native-hotpath-v3.json`](out/asgi-json-echo-all-native-hotpath-v3.json)

### 42. Native TLS app-write fast path

- Area:
  - [`python/rsloop/sslproto.py`](../python/rsloop/sslproto.py)
  - [`src/stream_transport.rs`](../src/stream_transport.rs)
  - [`src/lib.rs`](../src/lib.rs)
- Change:
  - first tried moving `_write_appdata()` and `_process_outgoing()` wholesale into
    Rust, including direct `SSLObject.write()` and backlog draining
  - after that regressed, narrowed it to a fast path only:
    - native direct single-write attempt in `WRAPPED` state
    - stdlib fallback for all other write shapes
- Rationale:
  - after the copied-read hot path moved native, the next visible TLS cost was
    `_write_appdata` on small keepalive writes
- Functional result:
  - targeted TLS tests still passed through both variants
- Revision A/B against the kept copied-read baseline:
  - broad native write pass:
    - `tls_http1_keepalive`: `0.179620s -> 0.192421s` (`+7.13%`)
  - narrowed fast-path-only pass:
    - `tls_http1_keepalive`: `0.197297s -> 0.182058s` (`-7.72%`)
    - `start_tls_upgrade`: `0.043939s -> 0.043161s` (`-1.77%`)
- Guard checks on the narrowed pass:
  - `http1_keepalive_small`: `0.305060s -> 0.322161s` (`+5.61%`)
  - `asgi_json_echo`: `0.168220s -> 0.173484s` (`+3.13%`)
- Conclusion:
  - the TLS write-side fast path can help the isolated TLS keepalive case
  - but it regressed the plain HTTP and ASGI guards too much, so it is not a
    keeper in its current shape
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-tls-native-write-v1.json`](out/revision-ab-tls-native-write-v1.json)
  - [`benchmarks/out/revision-ab-tls-native-write-v2.json`](out/revision-ab-tls-native-write-v2.json)
  - [`benchmarks/out/revision-ab-start-tls-native-write-v2.json`](out/revision-ab-start-tls-native-write-v2.json)
  - [`benchmarks/out/revision-ab-http1-native-write-v2-strict.json`](out/revision-ab-http1-native-write-v2-strict.json)
  - [`benchmarks/out/revision-ab-asgi-native-write-v2.json`](out/revision-ab-asgi-native-write-v2.json)

### 43. StreamReader `_paused` cache in native stream path

- Area:
  - [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - cache `StreamReader._paused` in `StreamCore` and use that cached flag in
    the native read path instead of reading the Python attribute on every chunk
    and every resume check
- Rationale:
  - the plain-stream hot loop still hit `reader.getattr("_paused")` several
    times per read / resume path
- Functional result:
  - build passed
  - focused TCP stream tests passed
- Revision A/B against `HEAD`:
  - `http1_keepalive_small`: `0.316714s -> 0.314416s` (`-0.73%`)
  - `asgi_streaming`: `0.133377s -> 0.135299s` (`+1.44%`)
  - `asgi_json_echo`: `0.159218s -> 0.163348s` (`+2.59%`)
- Conclusion:
  - the saved Python attribute lookups were too small to matter
  - the cached-state version regressed the app-shaped stream cases we care
    about most, so it is not worth keeping
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-http1-reader-paused-cache.json`](out/revision-ab-http1-reader-paused-cache.json)
  - [`benchmarks/out/revision-ab-asgi-streaming-reader-paused-cache.json`](out/revision-ab-asgi-streaming-reader-paused-cache.json)
  - [`benchmarks/out/revision-ab-asgi-json-reader-paused-cache.json`](out/revision-ab-asgi-json-reader-paused-cache.json)

### 44. Socket helper optimistic future completion

- Area:
  - [`src/socket_state.rs`](../src/socket_state.rs)
- Change:
  - remove repeated `Future.done()` polling from the native socket helper loop
  - switch completions to optimistic `set_result` / `set_exception` /
    `cancel()` with `InvalidStateError` ignored on cancellation races
- Rationale:
  - the native socket helper path still called back into Python just to ask if
    the future was already done before every completion
- Functional result:
  - focused socket-helper / datagram tests passed
- Revision A/B against `HEAD`:
  - `tcp_connect_parallel`: `0.043621s -> 0.043415s` (`-0.47%`)
  - `tcp_churn_small_io`: `0.026208s -> 0.027484s` (`+4.87%`)
- Conclusion:
  - removing `done()` polling is directionally sensible, but the current change
    did not produce a meaningful win and regressed churn-heavy connection traffic
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-tcp-connect-parallel-socket-future-fastpath.json`](out/revision-ab-tcp-connect-parallel-socket-future-fastpath.json)
  - [`benchmarks/out/revision-ab-tcp-churn-small-io-socket-future-fastpath.json`](out/revision-ab-tcp-churn-small-io-socket-future-fastpath.json)

### 45. Native TLS cache handoff for wrapped reads

- Area:
  - [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - cache stable `RsloopSSLProtocol` objects once at transport activation /
    protocol switch:
    - `_ssl_buffer_view`
    - `_rsloop_incoming_write`
    - `_rsloop_sslobj_read`
    - `_rsloop_outgoing_read`
    - `_rsloop_transport_write`
    - copied-mode `StreamReader` objects / buffer / limit
  - use that cached state in the native wrapped-read path instead of repeated
    `getattr()` lookups on every TLS fragment
- Rationale:
  - after moving copied wrapped reads into native code, the hot path still
    re-fetched several Python attributes per readable event and per copied-read
    drain
  - this was measurable boundary churn on the remaining steady-state TLS path
- Functional result:
  - `cargo test -q` passed
  - `./.venv/bin/python -m unittest tests.test_loop tests.test_benchmarks -q`
    passed
- Revision A/B against `HEAD`:
  - `tls_http1_keepalive`: `0.179753s -> 0.173030s` (`-3.74%`)
  - `start_tls_upgrade`: `0.043507s -> 0.040906s` (`-5.98%`)
- Guard checks:
  - `http1_keepalive_small`: `0.314166s -> 0.311057s` (`-0.99%`)
  - `asgi_json_echo`: `0.162751s -> 0.162911s` (`+0.10%`)
- Cross-loop spot checks vs `uvloop`:
  - `tls_http1_keepalive`: `1.376x`
  - `start_tls_upgrade`: `0.776x`
  - `http1_keepalive_small`: `1.049x`
- Conclusion:
  - caching the stable TLS protocol handles was worth keeping
  - it improved the TLS target and upgrade path while keeping plain HTTP / ASGI
    effectively flat
- Decision: kept
- Artifacts:
  - [`benchmarks/out/revision-ab-tls-cache-handoff.json`](out/revision-ab-tls-cache-handoff.json)
  - [`benchmarks/out/revision-ab-starttls-cache-handoff.json`](out/revision-ab-starttls-cache-handoff.json)
  - [`benchmarks/out/revision-ab-http1-cache-handoff-strict.json`](out/revision-ab-http1-cache-handoff-strict.json)
  - [`benchmarks/out/revision-ab-asgi-cache-handoff.json`](out/revision-ab-asgi-cache-handoff.json)
  - [`benchmarks/out/tls-http1-all-cache-handoff.json`](out/tls-http1-all-cache-handoff.json)
  - [`benchmarks/out/start-tls-all-cache-handoff.json`](out/start-tls-all-cache-handoff.json)
  - [`benchmarks/out/http1-all-cache-handoff.json`](out/http1-all-cache-handoff.json)

### 46. Cached `StreamReader._wakeup_waiter` on native TLS copied reads

- Area:
  - [`src/stream_transport.rs`](../src/stream_transport.rs)
- Change:
  - cache the exact `StreamReader._wakeup_waiter` bound method alongside the
    other copied-mode TLS reader state
  - use that cached method from the native copied-read path instead of
    reconstructing the wakeup sequence through separate `_waiter` /
    `cancelled()` / `set_result(None)` attribute lookups
- Rationale:
  - after caching the TLS protocol-side handles, the next repeated Python
    control-plane step in copied-mode wrapped reads was the `StreamReader`
    waiter wakeup path
- Functional result:
  - `cargo test -q` passed
  - `./.venv/bin/maturin develop --release` passed
- Revision A/B against `HEAD`:
  - `tls_http1_keepalive`: `0.171501s -> 0.167197s` (`-2.51%`)
  - `start_tls_upgrade`: `0.052311s -> 0.037720s` (`-27.89%`)
- Guard checks:
  - `asgi_json_echo`: `0.185090s -> 0.185558s` (`+0.25%`)
  - `http1_keepalive_small` initial strict run:
    `0.338179s -> 0.341734s` (`+1.05%`)
  - `http1_keepalive_small` tighter confirmation:
    `0.313483s -> 0.313122s` (`-0.12%`)
- Cross-loop spot check vs `uvloop`:
  - `tls_http1_keepalive`: `1.157x`
- Conclusion:
  - caching the exact wakeup method is worth keeping
  - the target TLS path improved again, and the tighter plain-HTTP confirmation
    removed the initial guard concern
- Decision: kept
- Artifacts:
  - [`benchmarks/out/revision-ab-tls-wakeup-cache.json`](out/revision-ab-tls-wakeup-cache.json)
  - [`benchmarks/out/revision-ab-starttls-wakeup-cache.json`](out/revision-ab-starttls-wakeup-cache.json)
  - [`benchmarks/out/revision-ab-asgi-wakeup-cache.json`](out/revision-ab-asgi-wakeup-cache.json)
  - [`benchmarks/out/revision-ab-http1-wakeup-cache-strict.json`](out/revision-ab-http1-wakeup-cache-strict.json)
  - [`benchmarks/out/revision-ab-http1-wakeup-cache-confirm.json`](out/revision-ab-http1-wakeup-cache-confirm.json)
  - [`benchmarks/out/tls-http1-all-wakeup-cache.json`](out/tls-http1-all-wakeup-cache.json)

### 47. Native Unix write-pipe transport

- Area:
  - [`python/rsloop/loop.py`](../python/rsloop/loop.py)
  - `src/pipe_transport.rs` (reverted)
- Change:
  - replace stdlib `_UnixWritePipeTransport` with an rsloop-owned Rust
    transport for `connect_write_pipe()` and subprocess stdin
  - native hot path used direct `libc::write`, a Rust-side pending buffer, and
    native `_on_writable` callbacks
  - a second tuning pass added `bytes` / `bytearray` fast paths to avoid the
    generic Python buffer-protocol overhead on every `write()`
- Rationale:
  - focused CPU profiling showed stdlib
    `asyncio.unix_events._UnixWritePipeTransport.write()` dominating
    `pipe_write`, with `os.write` itself only about half the cost
- Functional result:
  - targeted pipe/subprocess tests passed on both drafts
- Revision A/B against `HEAD`:
  - first strict sequential run:
    - `pipe_write`: `0.006082s -> 0.006402s` (`+5.26%`)
  - tuned `bytes` fast path:
    - `pipe_write`: `0.005662s -> 0.005714s` (`+0.93%`)
- Notes:
  - an earlier parallel A/B run falsely showed `-3.24%`; that result was
    discarded after rerunning sequentially
  - the transport likely removed some Python overhead, but the remaining
    Python-to-Rust crossing plus buffer coercion kept it below the keep bar
- Conclusion:
  - a real native write-pipe transport is plausible, but this version was not
    a win and did not justify more surface area
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-pipe-write-native-transport-seq.json`](out/revision-ab-pipe-write-native-transport-seq.json)
  - [`benchmarks/out/revision-ab-pipe-write-native-transport-bytesfast.json`](out/revision-ab-pipe-write-native-transport-bytesfast.json)

### 48. Cached subprocess exit callback

- Area:
  - [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- Change:
  - cache `transport._process_exited` once during subprocess transport creation
  - use the cached bound method from `_child_watcher_callback()` instead of
    rebuilding the bound method for every child exit
- Rationale:
  - `subprocess_exec` is already close to parity, so the cheapest remaining
    hypothesis was to remove a small Python-bound-method allocation on the
    child-watcher thread
- Functional result:
  - subprocess tests passed
- Revision A/B against `HEAD`:
  - `subprocess_exec`: `0.936902s -> 1.016443s` (`+8.49%`)
- Conclusion:
  - this callback-level tweak is the wrong layer; the subprocess gap is not the
    bound-method lookup in `_child_watcher_callback()`
- Decision: reverted
- Artifacts:
  - [`benchmarks/out/revision-ab-subprocess-exit-cache-seq.json`](out/revision-ab-subprocess-exit-cache-seq.json)

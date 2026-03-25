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

## Open Direction

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

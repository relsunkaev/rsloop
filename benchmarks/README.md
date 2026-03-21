## Benchmark Suite

`benchmarks/loops.py` is the primary performance harness for comparing `asyncio`, `uvloop`, and `rsloop`.

It now covers eight workload classes:

- `scheduler`: callback, timer, cancellation, and cross-thread scheduling churn
- `raw-socket`: `loop.sock_*` helpers, socketpair/TCP/AF_UNIX raw I/O, reusable recv-into buffers, and UDP datagrams
- `datagram`: `create_datagram_endpoint()`, `sock_sendto()`, `sock_recvfrom()`, and `sock_recvfrom_into()` UDP echo paths
- `ipc`: `connect_read_pipe()`, `connect_write_pipe()`, and subprocess spawn/communicate churn
- `signals`: `add_signal_handler()` / `remove_signal_handler()` registration and delivery churn
- `stream`: asyncio streams on small, large, fragmented, bursty, concurrent, asymmetric upload/download, request/response, pipelined, half-close, backpressured, mixed-payload, HTTP/1.1, TLS, `start_tls()`, and `sock_sendfile()` traffic
- `application`: websocket, JSON request/response, and framed unary RPC workloads
- `connection`: sequential and parallel connect/close churn, burst accept, small-I/O churn, and idle fanout

### Methodology

The harness is designed to make comparisons harder to dismiss:

- each measured sample runs in a fresh child process by default
- multi-loop comparisons are interleaved round-robin, so `asyncio`, `uvloop`, and `rsloop` each see the same thermal/drift conditions per round
- JSON artifacts include paired per-round samples, not just aggregate medians
- comparisons include both median ratio and paired round data
- environment metadata includes Python/platform details plus git commit/dirty state
- child benchmark failures can be retried before the suite aborts, and retries are recorded in round data when they occur

### Useful commands

Run the default profile across all loops:

```bash
.venv/bin/python benchmarks/loops.py --loop all --profile default --repeats 5 --warmups 1
```

Run the full suite and save structured output:

```bash
.venv/bin/python benchmarks/loops.py \
  --loop all \
  --profile full \
  --repeats 5 \
  --warmups 1 \
  --output benchmarks/out/full.json
```

Run a stricter, paired comparison with explicit retry policy:

```bash
.venv/bin/python benchmarks/loops.py \
  --loop all \
  --profile full \
  --repeats 7 \
  --warmups 2 \
  --child-retries 1 \
  --output benchmarks/out/full-paired.json
```

List available scenarios:

```bash
.venv/bin/python benchmarks/loops.py --list
```

Capture Rsloop scheduler shape data and stream events for an isolated run:

```bash
.venv/bin/python benchmarks/loops.py \
  --loop rsloop \
  --benchmark tcp_echo_parallel \
  --repeats 1 \
  --warmups 0 \
  --profile-runtime \
  --profile-stream \
  --output benchmarks/out/tcp_echo_parallel-profile.json \
  --json
```

Capture cross-loop Python stream delivery/write shape for an isolated comparison:

```bash
.venv/bin/python benchmarks/loops.py \
  --loop all \
  --benchmark tcp_rpc_pipeline_parallel \
  --repeats 1 \
  --warmups 0 \
  --profile-python-streams \
  --output benchmarks/out/tcp_rpc_pipeline_parallel-python-stream-profile.json \
  --json
```

### Profiles

- `smoke`: fast sanity checks
- `default`: balanced comparison set for routine iteration
- `full`: all runnable scenarios, including heavier mixed-payload stream cases
- The expanded `full` profile also includes control-plane task/future/timer storms, AF_UNIX raw-socket cases, datagram endpoint and `sock_sendto` / `sock_recvfrom` coverage, pipe and subprocess churn, signal handler churn, HTTP/1.1 request/response cases, TLS handshakes, `sock_sendfile()` transfers, websocket workloads, and application-shaped request/response cases.
- `start_tls_upgrade` is available as a standalone benchmark for targeted debugging. It is currently excluded from `full` because the rsloop path hangs in the current build.

### Notes

- Default output prints per-loop lines plus a compact summary table for Rsloop vs the selected baseline, including paired round wins.
- JSON output includes environment metadata, git state, per-sample timings, dispersion stats, paired round samples, and baseline ratios.
- `--profile-runtime` captures Rsloop scheduler per-tick summaries into the JSON artifact.
- `--profile-stream` captures a bounded Rsloop stream event trace into the JSON artifact.
- `--profile-python-streams` captures cross-loop Python stream counters for `feed_data`, `readexactly`, `write`, and `drain`.
- `--iterations` overrides the default iteration count for the selected scenarios.
- `--no-interleave-loops` disables round-robin pairing if you need the old run shape.
- `--child-retries` retries transient child failures before aborting the suite.
- Runtime profile capture requires subprocess isolation; leave `--no-isolate-process` off when using it.
- On Rsloop’s native stream fast path, Python stream counters can legitimately stay near zero. That means the workload stayed below `StreamReader`/`StreamWriter`, not that the profiler failed.

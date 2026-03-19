## Benchmark Suite

`benchmarks/loops.py` is the primary performance harness for comparing `asyncio`, `uvloop`, and `kioto`.

It now covers four workload classes:

- `scheduler`: callback, timer, and cross-thread scheduling churn
- `raw-socket`: `loop.sock_*` helpers and socketpair/TCP raw I/O
- `stream`: asyncio streams on small, large, concurrent, request/response, pipelined, and mixed-payload traffic
- `connection`: connect/close churn and idle fanout

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

List available scenarios:

```bash
.venv/bin/python benchmarks/loops.py --list
```

Capture Kioto scheduler shape data and stream events for an isolated run:

```bash
.venv/bin/python benchmarks/loops.py \
  --loop kioto \
  --benchmark tcp_echo_parallel \
  --repeats 1 \
  --warmups 0 \
  --profile-runtime \
  --profile-stream \
  --output benchmarks/out/tcp_echo_parallel-profile.json \
  --json
```

### Profiles

- `smoke`: fast sanity checks
- `default`: balanced comparison set for routine iteration
- `full`: all scenarios, including heavier mixed-payload stream cases

### Notes

- Default output prints per-loop lines plus a compact summary table for Kioto vs the selected baseline.
- JSON output includes environment metadata, per-sample timings, medians, and baseline ratios.
- `--profile-runtime` captures Kioto scheduler per-tick summaries into the JSON artifact.
- `--profile-stream` captures a bounded Kioto stream event trace into the JSON artifact.
- `--iterations` overrides the default iteration count for the selected scenarios.
- Runtime profile capture requires subprocess isolation; leave `--no-isolate-process` off when using it.

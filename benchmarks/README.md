## Benchmark Suite

`benchmarks/loops.py` is the primary performance harness for comparing `asyncio`, `uvloop`, and `kioto`.

It now covers four workload classes:

- `scheduler`: callback, timer, and cross-thread scheduling churn
- `raw-socket`: `loop.sock_*` helpers and socketpair/TCP raw I/O
- `stream`: asyncio streams on small, large, and concurrent payloads
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

### Profiles

- `smoke`: fast sanity checks
- `default`: balanced comparison set for routine iteration
- `full`: all scenarios

### Notes

- Default output prints per-loop lines plus a compact summary table for Kioto vs the selected baseline.
- JSON output includes environment metadata, per-sample timings, medians, and baseline ratios.
- `--iterations` overrides the default iteration count for the selected scenarios.

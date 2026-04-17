# Native Loop-Owned I/O Engine

## Goal

Reduce the loop's control-plane cost per unit of useful I/O by moving the
high-volume socket, pipe, and subprocess-stdio paths onto one native,
loop-owned operation model.

This is the anti-Goodhart move for `rsloop`: stop teaching isolated benchmark
surfaces to win locally, and instead reduce the shared overhead that shows up
across `raw-socket`, `ipc`, `subprocess_*`, `connection`, and parts of
`stream`.

## Invariants

- A single `RsloopEventLoop` has one owning loop thread.
- Worker and watcher threads may only enqueue notifications.
- `Future` resolution and transport state transitions stay on the loop thread.
- Native I/O helpers may perform syscalls off the hot Python path, but they do
  not complete Python futures or mutate transport/protocol state directly from
  foreign threads.

## Current Repo Truth

The repo is already partway toward this architecture, but the control plane is
split across three different shapes.

### 1. Streams already use a native registry model

- [`src/stream_registry.rs`](../src/stream_registry.rs) keeps segmented per-fd
  state, a completion queue, and a deferred write queue.
- [`src/scheduler.rs`](../src/scheduler.rs) dispatches stream fds before
  generic fd callbacks, drains stream completions, then flushes the deferred
  write phase.
- This is the cleanest existing example of "syscall/event work now, Python
  completion later on the loop thread".

### 2. Raw sockets are partly native already

- [`src/socket_state.rs`](../src/socket_state.rs) and
  [`src/socket_registry.rs`](../src/socket_registry.rs) already back
  `sock_recv`, `sock_recv_into`, `sock_sendall`, `sock_accept`, and
  `sock_connect`.
- [`python/rsloop/loop.py`](../python/rsloop/loop.py) routes those operations
  through `_rsloop.SocketState(...)` in `_rsloop_socket_state()`.
- The old Python [`_SocketState`](../python/rsloop/loop.py) class is still in
  the file, but it is no longer the active path for those operations.

The gap is that this socket path still resolves futures inline in
`on_readable()` / `on_writable()` rather than using a completion queue and
deferred finish phase like streams do.

### 3. Datagrams, pipes, and subprocess stdio still use heavier Python control planes

- `sock_recvfrom`, `sock_recvfrom_into`, and `sock_sendto` still go through
  generic `_add_reader()` / `_add_writer()` handle registration in
  [`python/rsloop/loop.py`](../python/rsloop/loop.py).
- [`python/rsloop/pipe_transport.py`](../python/rsloop/pipe_transport.py) is
  still callback-heavy Python transport code with direct `_add_reader()` /
  `_remove_reader()` and `_add_writer()` / `_remove_writer()` churn.
- Native subprocess creation in
  [`src/native_subprocess.rs`](../src/native_subprocess.rs) already exists, but
  [`python/rsloop/loop.py`](../python/rsloop/loop.py) still wires stdin,
  stdout, and stderr through the Python pipe transport layer after spawn.

### 4. Generic fd readiness still pays Python-handle overhead

- [`src/fd_callbacks.rs`](../src/fd_callbacks.rs) clones Python handles and
  checks cancellation one-by-one for generic reader/writer callbacks.
- [`src/scheduler.rs`](../src/scheduler.rs) already records
  `stream_fd_hits`, `socket_fd_hits`, and `generic_fd_hits`, which gives us a
  measurement seam for proving that hot paths are moving off the generic
  callback registry.

### 5. The experiment log already rejects local fast-path tricks

- [`benchmarks/EXPERIMENT_LOG.md`](../benchmarks/EXPERIMENT_LOG.md) documents
  repeated "small local fast path" experiments that improved a narrow surface
  but regressed broader app-shaped workloads.
- That log is evidence for a shared-control-plane thesis rather than another
  round of benchmark-specific tweaks.

## Problem Statement

Today the loop still has multiple overlapping fd control planes:

- stream registry
- socket registry
- generic fd callback registry
- Python pipe transports layered over generic fd callbacks

That split creates duplicated overhead:

- per-request `Future` allocation and completion
- repeated add/remove reader-writer registration churn
- callback/handle cloning and cancellation checks
- Python transport/protocol hops for pipe and subprocess stdio
- direct inline completion work in the fd-dispatch phase instead of a batched
  finish phase

Those costs do not stay confined to one benchmark. They surface in different
forms across pipe/subprocess, raw-socket churn, accept bursts, future-heavy
control-plane tests, and some RPC/app paths.

## Proposal

Build a loop-owned native I/O operation engine that extends the existing native
registry model to all high-volume fd-driven surfaces:

- stream
- connected socket ops
- datagram socket ops
- read pipe / write pipe
- subprocess stdio endpoints

The key design rule is not "one giant transport type". The key rule is:

> one native per-fd state machine shape, one loop-thread completion model, one
> place to measure shared control-plane cost.

## Recommended Architecture

### A. Converge on a shared native endpoint pattern

Each active fd-backed endpoint should have:

- segmented registry lookup by fd
- native per-fd queued operations
- explicit readable/writable interest sync
- batched completion queue drained on the loop thread
- optional deferred write phase when write flushing can be separated from fd
  dispatch

Streams already match this pattern. Sockets should be moved closer to it.
Pipes and subprocess stdio should be added to it.

### B. Treat streams as the model, not necessarily the base class

The stream path in [`src/stream_registry.rs`](../src/stream_registry.rs) is the
best behavioral template:

- dispatch syscall readiness work first
- queue completions
- drain completions on the loop thread
- flush deferred writes after ready/timer work

The first implementation should reuse this pattern, not force stream, socket,
and pipe code into a single shared struct too early.

### C. Promote sockets from "native syscall path" to "native completion path"

[`src/socket_state.rs`](../src/socket_state.rs) already removes Python from the
syscall loop for connected sockets, but it still completes futures inline.

The next step is to:

- queue socket completion records instead of calling `future.set_result()` /
  `future.set_exception()` directly inside `on_readable()` and `on_writable()`
- optionally add a socket write queue / deferred flush phase if batching helps
  connected writes
- extend the same machinery to datagram `recvfrom`, `recvfrom_into`, and
  `sendto`

That keeps the socket path aligned with the stream invariant and makes the
control-plane cost measurable in the same units.

### D. Move pipe and subprocess stdio onto native endpoint state machines

Pipe transports are still structurally expensive because they are implemented as
Python transports driven by generic fd callbacks.

The desired end state is:

- native read-pipe endpoint state
- native write-pipe endpoint state
- native completion records for `data_received`, EOF, and close/error delivery
- subprocess stdio endpoints reusing those native pipe endpoints directly

Process creation can stay where it is today in
[`src/native_subprocess.rs`](../src/native_subprocess.rs). The anti-Goodhart
win comes from eliminating the Python stdio control plane after spawn.

### E. Shrink the generic fd callback registry by moving real workloads off it

[`src/fd_callbacks.rs`](../src/fd_callbacks.rs) will still matter for truly
generic `add_reader()` / `add_writer()` users, but it should stop being the hot
path for:

- datagram sockets
- pipes
- subprocess stdio

Only after those migrations should we decide whether it is worth redesigning
the generic callback registry itself.

## Metrics That Matter

Do not judge this work by a single benchmark name.

Judge it by shared control-plane ratios captured in scheduler/runtime profiles:

- ready handles per fd event
- generic fd hits vs native socket/stream/pipe hits
- loop wakeups per batch of useful completions
- completion items per queued request
- deferred write flushes per request
- Python object creations per connection or per subprocess stdio setup

The existing scheduler profile in [`src/scheduler.rs`](../src/scheduler.rs)
already records several useful ingredients:

- `fd_events`
- `stream_fd_hits`
- `socket_fd_hits`
- `generic_fd_hits`
- `ready_handles`
- `completion_items`
- `writes_flushed`

The next instrumentation pass should add endpoint-specific request/completion
counts so we can compute ratios directly from one run artifact.

## First-Cut Refactor Plan

### Phase 0: Instrument the shared thesis

Before moving code, add counters that answer:

- how many socket requests become completion items?
- how many pipe/subprocess fd events still become generic ready handles?
- how many loop wakeups are consumed by control-plane churn rather than useful
  I/O completions?

Deliverables:

- extend scheduler/runtime JSON with per-endpoint request/completion counters
- add explicit pipe endpoint hit counters alongside stream/socket/generic
- record object creation counts in Python wrappers where native counts are not
  visible yet

### Phase 1: Finish the native socket engine

Target:

- connected sockets plus datagram sockets use one native socket registry path

Work:

- extend [`src/socket_state.rs`](../src/socket_state.rs) for
  `recvfrom` / `recvfrom_into` / `sendto`
- move socket completions onto a queue drained after fd dispatch
- stop using generic `_add_reader()` / `_add_writer()` for datagram ops in
  [`python/rsloop/loop.py`](../python/rsloop/loop.py)
- remove the stale Python `_SocketState` definition once no callers depend on
  it

Why this first:

- the socket registry already exists
- it touches `raw-socket`, `connection`, and parts of app traffic immediately
- it is the lowest-risk place to prove the completion-queue thesis outside
  streams

### Phase 2: Introduce a native pipe endpoint registry

Target:

- `connect_read_pipe()` / `connect_write_pipe()` no longer depend on Python pipe
  transports for the hot fd path

Work:

- add native per-fd pipe state and a registry parallel to streams/sockets
- model read data, EOF, and write completion as loop-thread completion records
- preserve asyncio transport/protocol behavior at the API edge
- keep the watcher-thread invariant: only notifications cross threads

Why this second:

- it attacks `ipc`, `pipe_*`, and subprocess stdio overhead together
- it removes one of the last heavy generic fd users

### Phase 3: Rewire subprocess stdio to native pipe endpoints

Target:

- subprocess spawn remains native
- subprocess stdio no longer climbs back into Python pipe transports

Work:

- make `_make_subprocess_transport()` attach stdin/stdout/stderr through the
  native pipe endpoint layer
- keep existing process lifecycle and watcher logic
- preserve protocol callbacks via loop-thread completion delivery

### Phase 4: Re-evaluate the generic fd callback registry

Only after phases 1-3:

- measure whether `fd_callbacks.rs` is still a material cost center
- if yes, redesign it around lower-clone dispatch or endpoint-owned callbacks
- if no, leave it as the compatibility path for true `add_reader()` /
  `add_writer()` users

## Validation Plan

Use the `real` profile as the primary gate, not isolated microbenchmarks.

Required comparisons after each phase:

- `benchmarks/loops.py --loop all --profile real`
- targeted reruns for the affected category:
  - raw socket / datagram cases for phase 1
  - pipe and subprocess cases for phases 2-3
  - connection-heavy/app-shaped cases after every phase

Also inspect scheduler/runtime profile ratios, not just medians:

- native endpoint hit mix vs generic fd hits
- ready handles per fd event
- completion items per request
- writes flushed per request batch

## Risks

### 1. Regressing transport/protocol fidelity

This repo has already shown that broad "native rewrite" attempts can preserve
the syscall path while subtly regressing behavior or app-shaped performance.

Mitigation:

- stage by endpoint family
- preserve asyncio-visible semantics first
- keep the completion delivery model explicit

### 2. Collapsing stream-specific machinery into the wrong abstraction

Streams have protocol-specific buffering and TLS integration that sockets and
pipes do not.

Mitigation:

- share patterns and queueing discipline first
- unify structs only when the shared shape becomes obvious in code

### 3. Measuring the wrong thing again

If we only watch `pipe_write` or `future_completion_storm`, we can Goodhart the
rewrite just as easily as the earlier local fast paths.

Mitigation:

- gate on `real`
- track internal control-plane ratios
- require wins across multiple endpoint families before calling the design a
  success

## Recommended First Bet

The first concrete bet should be:

1. instrument shared socket/pipe/generic-fd ratios in the scheduler profile
2. finish the native socket registry by absorbing datagram ops and queued
   completions
3. only then move pipes/subprocess stdio onto the same completion discipline

That path reuses the strongest existing seams in the repo:

- stream registry as the behavioral model
- socket registry as the nearest expansion point
- scheduler counters as the proof surface

It is ambitious, but it is still incremental and falsifiable.

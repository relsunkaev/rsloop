# AGENTS.md

## Execution model invariant

- A single `RsloopEventLoop` has one owning loop thread.
- Python and Rust futures share the same runtime boundary through that loop.
- Never resolve Python futures directly from worker or watcher threads.
- Cross-thread completions must hop to the loop thread first (`call_soon_threadsafe` or loop-owned completion queue).
- Subprocess watcher callbacks must only notify; subprocess transport state transitions happen on the loop thread.

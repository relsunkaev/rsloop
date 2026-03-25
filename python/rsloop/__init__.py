import asyncio
from collections.abc import Awaitable
from typing import Any

from ._rsloop import backend_name, run_in_tokio, sleep, wrap_future
from .loop import RsloopEventLoop, RsloopEventLoopPolicy, install, new_event_loop

__all__ = [
    "backend_name",
    "RsloopEventLoop",
    "RsloopEventLoopPolicy",
    "install",
    "new_event_loop",
    "run",
    "run_in_tokio",
    "sleep",
    "wrap_future",
]


def run(
    awaitable: Awaitable[Any],
    *,
    debug: bool | None = None,
) -> Any:
    with asyncio.Runner(
        loop_factory=new_event_loop,
        debug=debug,
    ) as runner:
        return runner.run(awaitable)

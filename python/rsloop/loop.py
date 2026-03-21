from __future__ import annotations

import asyncio
import collections
import errno
import functools
import math
import os
import selectors
import signal
import stat
import socket
import threading
import sys
import ssl
from asyncio import base_events as _base_events
from asyncio import base_subprocess as _base_subprocess
from asyncio import constants as _constants
from asyncio import coroutines as _coroutines
from asyncio import exceptions as _exceptions
from asyncio import events as _events
from asyncio import futures as _futures
from asyncio import selector_events as _selector_events
from asyncio import sslproto as _sslproto
from asyncio import streams as _streams
from asyncio import tasks as _tasks
from asyncio import unix_events as _unix_events
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from . import _rsloop


logger = _selector_events.logger
_ORIGINAL_STREAMWRITER = _streams.StreamWriter
_TIMER_QUANTUM = 0.001 if sys.platform == "darwin" else 0.0
_TIMER_IMMEDIATE_CUTOFF = _TIMER_QUANTUM * 0.75


def _signal_noop(_sig: int, _frame: Any = None) -> None:
    return None


class RsloopStreamWriter:
    __slots__ = (
        "_transport",
        "_protocol",
        "_reader",
        "_loop",
        "write",
        "drain",
        "close",
        "wait_closed",
        "sendfile_static",
    )

    def __init__(self, transport: Any, protocol: Any, reader: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._transport = transport
        self._protocol = protocol
        self._reader = reader
        self._loop = loop

        bind_write = getattr(transport, "bind_write", None)
        self.write = bind_write() if bind_write is not None else self._write
        bind_drain = getattr(transport, "bind_drain", None)
        self.drain = bind_drain(self._drain_fallback) if bind_drain is not None else self._drain_fallback
        bind_close = getattr(transport, "bind_close", None)
        self.close = bind_close() if bind_close is not None else self._close
        bind_wait_closed = getattr(transport, "bind_wait_closed", None)
        self.wait_closed = bind_wait_closed() if bind_wait_closed is not None else self._wait_closed
        self.sendfile_static = self._sendfile_static_fallback

    def __repr__(self) -> str:
        info = [self.__class__.__name__, f"transport={self._transport!r}"]
        if self._reader is not None:
            info.append(f"reader={self._reader!r}")
        return f"<{' '.join(info)}>"

    @property
    def transport(self) -> Any:
        return self._transport

    def _write(self, data: Any) -> None:
        self._transport.write(data)

    def writelines(self, data: Any) -> None:
        self._transport.writelines(data)

    def write_eof(self) -> Any:
        return self._transport.write_eof()

    def can_write_eof(self) -> bool:
        return self._transport.can_write_eof()

    def _close(self) -> Any:
        return self._transport.close()

    def is_closing(self) -> bool:
        return self._transport.is_closing()

    async def _wait_closed(self) -> None:
        await self._protocol._get_close_waiter(self)

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._transport.get_extra_info(name, default)

    async def _drain_fallback(self) -> None:
        if self._reader is not None:
            exc = self._reader.exception()
            if exc is not None:
                raise exc
        if self._transport.is_closing():
            await asyncio.sleep(0)
        await self._protocol._drain_helper()

    async def _sendfile_static_fallback(
        self,
        file: Any,
        offset: int = 0,
        count: int | None = None,
    ) -> int:
        return await _sendfile_static_impl(
            self.write,
            self.drain,
            file,
            offset=offset,
            count=count,
        )

    async def start_tls(
        self,
        sslcontext: ssl.SSLContext,
        *,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
    ) -> None:
        server_side = self._protocol._client_connected_cb is not None
        protocol = self._protocol
        await self.drain()
        new_transport = await self._loop.start_tls(
            self._transport,
            protocol,
            sslcontext,
            server_side=server_side,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        self._transport = new_transport
        protocol._replace_transport(new_transport)


def _patch_stream_writer() -> None:
    if _streams.StreamWriter is not RsloopStreamWriter:
        _streams.StreamWriter = RsloopStreamWriter


async def _sendfile_static_impl(
    write: Callable[[bytes], None],
    drain: Callable[[], Awaitable[None]],
    file: Any,
    offset: int = 0,
    count: int | None = None,
) -> int:
    if offset:
        await asyncio.to_thread(file.seek, offset)

    total_sent = 0
    remaining = count
    block_size = 16 * 1024

    while True:
        if remaining is not None:
            if remaining <= 0:
                break
            current = min(block_size, remaining)
        else:
            current = block_size
        chunk = await asyncio.to_thread(file.read, current)
        if not chunk:
            break
        write(chunk)
        await drain()
        sent = len(chunk)
        total_sent += sent
        if remaining is not None:
            remaining -= sent

    return total_sent


@dataclass(slots=True)
class _RecvRequest:
    future: asyncio.Future[bytes]
    size: int


@dataclass(slots=True)
class _RecvIntoRequest:
    future: asyncio.Future[int]
    buffer: Any


@dataclass(slots=True)
class _AcceptRequest:
    future: asyncio.Future[tuple[socket.socket, Any]]


@dataclass(slots=True)
class _SendRequest:
    future: asyncio.Future[None]
    view: memoryview
    sent: int


@dataclass(slots=True)
class _ConnectRequest:
    future: asyncio.Future[None]
    address: Any


class _SocketState:
    __slots__ = (
        "_loop",
        "_sock",
        "fd",
        "_read_ops",
        "_write_ops",
        "_reader_registered",
        "_writer_registered",
        "_reader_disarm",
        "_writer_disarm",
        "_closed",
    )

    def __init__(self, loop: "RsloopEventLoop", sock: socket.socket) -> None:
        self._loop = loop
        self._sock = sock
        self.fd = sock.fileno()
        self._read_ops: collections.deque[_RecvRequest | _RecvIntoRequest | _AcceptRequest] = (
            collections.deque()
        )
        self._write_ops: collections.deque[_SendRequest | _ConnectRequest] = (
            collections.deque()
        )
        self._reader_registered = False
        self._writer_registered = False
        self._reader_disarm: asyncio.Handle | None = None
        self._writer_disarm: asyncio.Handle | None = None
        self._closed = False

    @property
    def sock(self) -> socket.socket:
        return self._sock

    def enqueue_recv(self, future: asyncio.Future[bytes], size: int) -> None:
        self._read_ops.append(_RecvRequest(future, size))
        future.add_done_callback(self._wake_reader)
        self._ensure_reader()

    def enqueue_recv_into(self, future: asyncio.Future[int], buffer: Any) -> None:
        self._read_ops.append(_RecvIntoRequest(future, buffer))
        future.add_done_callback(self._wake_reader)
        self._ensure_reader()

    def enqueue_accept(
        self, future: asyncio.Future[tuple[socket.socket, Any]]
    ) -> None:
        self._read_ops.append(_AcceptRequest(future))
        future.add_done_callback(self._wake_reader)
        self._ensure_reader()

    def enqueue_sendall(
        self, future: asyncio.Future[None], view: memoryview, sent: int
    ) -> None:
        self._write_ops.append(_SendRequest(future, view, sent))
        future.add_done_callback(self._wake_writer)
        self._ensure_writer()

    def enqueue_connect(self, future: asyncio.Future[None], address: Any) -> None:
        self._write_ops.append(_ConnectRequest(future, address))
        future.add_done_callback(self._wake_writer)
        self._ensure_writer()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._reader_disarm is not None:
            self._reader_disarm.cancel()
            self._reader_disarm = None
        if self._writer_disarm is not None:
            self._writer_disarm.cancel()
            self._writer_disarm = None
        if self._reader_registered:
            self._loop._remove_reader(self.fd)
            self._reader_registered = False
        if self._writer_registered:
            self._loop._remove_writer(self.fd)
            self._writer_registered = False
        for request in self._read_ops:
            if not request.future.done():
                request.future.cancel()
        self._read_ops.clear()
        for request in self._write_ops:
            if not request.future.done():
                request.future.cancel()
        self._write_ops.clear()

    def _ensure_reader(self) -> None:
        if self._closed:
            return
        if self._reader_disarm is not None:
            self._reader_disarm.cancel()
            self._reader_disarm = None
        if self._reader_registered:
            return
        self._loop._add_reader(self.fd, self._on_readable)
        self._reader_registered = True

    def _ensure_writer(self) -> None:
        if self._closed:
            return
        if self._writer_disarm is not None:
            self._writer_disarm.cancel()
            self._writer_disarm = None
        if self._writer_registered:
            return
        self._loop._add_writer(self.fd, self._on_writable)
        self._writer_registered = True

    def _wake_reader(self, _future: asyncio.Future[Any]) -> None:
        if self._closed or self._loop.is_closed():
            return
        if self._read_ops:
            self._on_readable()
        else:
            self._schedule_reader_disarm()

    def _wake_writer(self, _future: asyncio.Future[Any]) -> None:
        if self._closed or self._loop.is_closed():
            return
        if self._write_ops:
            self._on_writable()
        else:
            self._schedule_writer_disarm()

    def _schedule_reader_disarm(self) -> None:
        if self._closed or not self._reader_registered:
            return
        if self._reader_disarm is None:
            self._reader_disarm = self._loop.call_soon(self._maybe_disarm_reader)

    def _schedule_writer_disarm(self) -> None:
        if self._closed or not self._writer_registered:
            return
        if self._writer_disarm is None:
            self._writer_disarm = self._loop.call_soon(self._maybe_disarm_writer)

    def _maybe_disarm_reader(self) -> None:
        self._reader_disarm = None
        if self._closed or self._read_ops:
            return
        if self._reader_registered:
            self._loop._remove_reader(self.fd)
            self._reader_registered = False

    def _maybe_disarm_writer(self) -> None:
        self._writer_disarm = None
        if self._closed or self._write_ops:
            return
        if self._writer_registered:
            self._loop._remove_writer(self.fd)
            self._writer_registered = False

    def _on_readable(self) -> None:
        if self._closed:
            return
        while self._read_ops:
            request = self._read_ops[0]
            future = request.future
            if future.done():
                self._read_ops.popleft()
                continue
            try:
                if isinstance(request, _RecvRequest):
                    result = self._sock.recv(request.size)
                elif isinstance(request, _RecvIntoRequest):
                    result = self._sock.recv_into(request.buffer)
                else:
                    conn, address = self._sock.accept()
                    conn.setblocking(False)
                    result = (conn, address)
            except (BlockingIOError, InterruptedError):
                break
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._read_ops.popleft()
                future.set_exception(exc)
            else:
                self._read_ops.popleft()
                future.set_result(result)
        if not self._read_ops:
            self._schedule_reader_disarm()

    def _on_writable(self) -> None:
        if self._closed:
            return
        while self._write_ops:
            request = self._write_ops[0]
            future = request.future
            if future.done():
                self._write_ops.popleft()
                continue
            try:
                if isinstance(request, _SendRequest):
                    while request.sent < len(request.view):
                        sent = self._sock.send(request.view[request.sent :])
                        if sent == 0:
                            raise BlockingIOError
                        request.sent += sent
                    result: None = None
                else:
                    err = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                    if err != 0:
                        raise OSError(err, f"Connect call failed {request.address}")
                    result = None
            except (BlockingIOError, InterruptedError):
                break
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._write_ops.popleft()
                future.set_exception(exc)
            else:
                self._write_ops.popleft()
                future.set_result(result)
        if not self._write_ops:
            self._schedule_writer_disarm()


class RsloopEventLoop(_base_events.BaseEventLoop):
    """Tokio-backed asyncio loop with a native scheduling core."""

    def __init__(self) -> None:
        _patch_stream_writer()
        super().__init__()
        self._poller = _rsloop.TokioPoller()
        self._scheduler = _rsloop.Scheduler()
        self._native_api = _rsloop.LoopApi(self, self._scheduler, self._debug)
        self._fd_registry = _rsloop.FdCallbackRegistry()
        self._stream_registry = _rsloop.StreamTransportRegistry()
        self._socket_registry = _rsloop.SocketStateRegistry()
        self._signal_handlers: dict[int, _events.Handle] = {}
        self._unix_server_sockets: dict[socket.socket, int] = {}
        self._watcher = (
            _unix_events._PidfdChildWatcher()
            if hasattr(os, "pidfd_open") and _unix_events.can_use_pidfd()
            else _unix_events._ThreadedChildWatcher()
        )
        self._native_run_forever = getattr(self._scheduler, "run_forever_native", None)
        self._install_native_bindings()
        self._transports: dict[int, Any] = {}
        self._rsloop_socket_states: dict[int, Any] = {}
        self._rsloop_completion_port = _rsloop.CompletionPort()
        self._ssock: socket.socket | None = None
        self._csock: socket.socket | None = None
        self._make_self_pipe()
        self._poller.set_interest(self._rsloop_completion_port.fileno(), True, False)

    def _bind_native_callable(self, binder_name: str, fallback: Any) -> Any:
        binder = getattr(self._native_api, binder_name, None)
        if binder is None:
            return fallback
        return binder()

    def _install_native_bindings(self) -> None:
        self.call_soon = self._bind_native_callable(
            "bind_call_soon", self._compat_call_soon
        )
        self._call_soon = getattr(self._native_api, "_call_soon", self._compat__call_soon)
        self.call_soon_threadsafe = self._bind_native_callable(
            "bind_call_soon_threadsafe", self._compat_call_soon_threadsafe
        )
        self.call_at = self._bind_native_callable(
            "bind_call_at", self._compat_call_at
        )
        self.call_later = self._bind_native_callable(
            "bind_call_later", self._compat_call_later
        )

    def _normalize_fd(self, fd: Any) -> int:
        fileno = fd
        if not isinstance(fileno, int):
            try:
                fileno = int(fileno.fileno())
            except (AttributeError, TypeError, ValueError):
                raise ValueError(f"Invalid file object: {fd!r}") from None
        return fileno

    def _sync_fd_state(self, fd: int) -> None:
        readable, writable = self._fd_registry.interest(fd)
        self._poller.set_interest(fd, readable, writable)

    def _ensure_fd_no_transport(self, fd: Any) -> None:
        fileno = self._normalize_fd(fd)
        transport = self._transports.get(fileno)
        if transport and not transport.is_closing():
            raise RuntimeError(
                f"File descriptor {fd!r} is used by transport {transport!r}"
            )

    def _take_fd_for_transport(self, sock: socket.socket) -> int:
        fileno = sock.fileno()
        state = self._rsloop_socket_states.pop(fileno, None)
        if state is not None:
            state.close()
        self._remove_reader(fileno)
        self._remove_writer(fileno)
        return fileno

    def _make_self_pipe(self) -> None:
        self._ssock, self._csock = socket.socketpair()
        self._ssock.setblocking(False)
        self._csock.setblocking(False)
        self._internal_fds += 1
        native_api = getattr(self, "_native_api", None)
        if native_api is not None:
            native_api.set_wakeup_fd(self._csock.fileno())
        self._add_reader(self._ssock.fileno(), self._read_from_self)

    def _close_self_pipe(self) -> None:
        if self._ssock is None or self._csock is None:
            return
        native_api = getattr(self, "_native_api", None)
        if native_api is not None:
            native_api.set_wakeup_fd(-1)
        self._remove_reader(self._ssock.fileno())
        self._ssock.close()
        self._ssock = None
        self._csock.close()
        self._csock = None
        self._internal_fds -= 1

    def _process_self_data(self, _data: bytes) -> None:
        for signum in _data:
            if not signum:
                continue
            self._handle_signal(signum)

    def _check_signal(self, sig: int) -> None:
        if not isinstance(sig, int):
            raise TypeError(f"sig must be an int, not {sig!r}")
        if sig not in signal.valid_signals():
            raise ValueError(f"invalid signal number {sig}")

    def _handle_signal(self, sig: int) -> None:
        handle = self._signal_handlers.get(sig)
        if handle is None:
            return
        if handle._cancelled:
            self.remove_signal_handler(sig)
        else:
            self._add_callback_signalsafe(handle)

    def _read_from_self(self) -> None:
        if self._ssock is None:
            return
        while True:
            try:
                data = self._ssock.recv(4096)
                if not data:
                    break
                self._process_self_data(data)
            except InterruptedError:
                continue
            except BlockingIOError:
                break
        native_api = getattr(self, "_native_api", None)
        if native_api is not None:
            native_api.clear_wakeup()

    def _write_to_self(self) -> None:
        if self._csock is None:
            return
        try:
            self._csock.send(b"\0")
        except OSError:
            if self._debug:
                logger.debug("Fail to write a null byte into the self-pipe", exc_info=True)

    def add_reader(self, fd: Any, callback: Any, *args: Any) -> None:
        self._ensure_fd_no_transport(fd)
        self._add_reader(fd, callback, *args)

    def remove_reader(self, fd: Any) -> bool:
        self._ensure_fd_no_transport(fd)
        return self._remove_reader(fd)

    def _add_reader(self, fd: Any, callback: Any, *args: Any) -> _events.Handle:
        self._check_closed()
        fileno = self._normalize_fd(fd)
        handle = _rsloop.make_handle(callback, args, self, None, None)
        previous, readable, writable = self._fd_registry.add_reader(fileno, handle)
        if previous is not None:
            previous.cancel()
        self._poller.set_interest(fileno, readable, writable)
        return handle

    def _remove_reader(self, fd: Any) -> bool:
        if self.is_closed():
            return False
        fileno = self._normalize_fd(fd)
        handle, readable, writable = self._fd_registry.remove_reader(fileno)
        self._poller.set_interest(fileno, readable, writable)
        if handle is not None:
            handle.cancel()
            return True
        return False

    def add_writer(self, fd: Any, callback: Any, *args: Any) -> None:
        self._ensure_fd_no_transport(fd)
        self._add_writer(fd, callback, *args)

    def remove_writer(self, fd: Any) -> bool:
        self._ensure_fd_no_transport(fd)
        return self._remove_writer(fd)

    def add_signal_handler(self, sig: int, callback: Any, *args: Any) -> None:
        if _coroutines.iscoroutine(callback) or _coroutines._iscoroutinefunction(callback):
            raise TypeError("coroutines cannot be used with add_signal_handler()")
        self._check_signal(sig)
        self._check_closed()
        try:
            signal.set_wakeup_fd(self._csock.fileno() if self._csock is not None else -1)
        except (ValueError, OSError) as exc:
            raise RuntimeError(str(exc))

        handle = _events.Handle(callback, args, self, None)
        self._signal_handlers[sig] = handle
        try:
            signal.signal(sig, _signal_noop)
            signal.siginterrupt(sig, False)
        except OSError as exc:
            del self._signal_handlers[sig]
            if not self._signal_handlers:
                try:
                    signal.set_wakeup_fd(-1)
                except (ValueError, OSError) as wake_exc:
                    logger.info("set_wakeup_fd(-1) failed: %s", wake_exc)
            if exc.errno == errno.EINVAL:
                raise RuntimeError(f"sig {sig} cannot be caught") from None
            raise

    def remove_signal_handler(self, sig: int) -> bool:
        self._check_signal(sig)
        try:
            del self._signal_handlers[sig]
        except KeyError:
            return False

        handler = signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
        try:
            signal.signal(sig, handler)
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise RuntimeError(f"sig {sig} cannot be caught") from None
            raise

        if not self._signal_handlers:
            try:
                signal.set_wakeup_fd(-1)
            except (ValueError, OSError) as exc:
                logger.info("set_wakeup_fd(-1) failed: %s", exc)
        return True

    def _add_writer(self, fd: Any, callback: Any, *args: Any) -> _events.Handle:
        self._check_closed()
        fileno = self._normalize_fd(fd)
        handle = _rsloop.make_handle(callback, args, self, None, None)
        previous, readable, writable = self._fd_registry.add_writer(fileno, handle)
        if previous is not None:
            previous.cancel()
        self._poller.set_interest(fileno, readable, writable)
        return handle

    def _remove_writer(self, fd: Any) -> bool:
        if self.is_closed():
            return False
        fileno = self._normalize_fd(fd)
        handle, readable, writable = self._fd_registry.remove_writer(fileno)
        self._poller.set_interest(fileno, readable, writable)
        if handle is not None:
            handle.cancel()
            return True
        return False

    def _process_fd_events(self, events: list[tuple[int, int]]) -> None:
        for handle in self._fd_registry.dispatch(events):
            self._add_callback(handle)

    def _add_callback(self, handle: _events.Handle) -> None:
        if not handle._cancelled:
            try:
                self._scheduler.push_ready(handle)
            except RuntimeError as exc:
                if "borrow" not in str(exc):
                    raise
                native_api = getattr(self, "_native_api", None)
                if native_api is None:
                    raise
                native_api.push_ready_fallback(handle)

    def _add_callback_signalsafe(self, handle: _events.Handle) -> None:
        self._add_callback(handle)
        self._write_to_self()

    def _compat_call_soon(
        self, callback: Any, *args: Any, context: Any = None
    ) -> _events.Handle:
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, "call_soon")
        handle = _rsloop.make_handle(callback, args, self, context, None)
        try:
            self._scheduler.push_ready(handle)
        except RuntimeError as exc:
            if "borrow" not in str(exc):
                raise
            self._native_api.push_ready_fallback(handle)
        return handle

    def _compat__call_soon(
        self,
        callback: Any,
        args: tuple[Any, ...],
        context: Any,
    ) -> _events.Handle:
        handle = _rsloop.make_handle(callback, args, self, context, None)
        self._scheduler.push_ready(handle)
        return handle

    def _compat_call_later(
        self, delay: float, callback: Any, *args: Any, context: Any = None
    ) -> _events.TimerHandle:
        if delay is None:
            raise TypeError("delay must not be None")
        delay = self._quantize_timer_delay(delay)
        if delay <= 0:
            return self.call_soon(callback, *args, context=context)
        return self.call_at(self.time() + delay, callback, *args, context=context)

    def _compat_call_soon_threadsafe(
        self, callback: Any, *args: Any, context: Any = None
    ) -> _events.Handle:
        self._check_closed()
        if self._debug:
            self._check_callback(callback, "call_soon_threadsafe")
        handle = _rsloop.make_handle(callback, args, self, context, None)
        try:
            self._scheduler.push_ready(handle)
        except RuntimeError as exc:
            if "borrow" not in str(exc):
                raise
            self._native_api.push_ready_fallback(handle)
        self._write_to_self()
        return handle

    def _compat_call_at(
        self,
        when: float,
        callback: Any,
        *args: Any,
        context: Any = None,
    ) -> _events.TimerHandle:
        if when is None:
            raise TypeError("when cannot be None")
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, "call_at")
        timer = _rsloop.make_handle(callback, args, self, context, when)
        self._scheduler.push_timer(timer, when)
        return timer

    def _quantize_timer_delay(self, delay: float) -> float:
        if _TIMER_QUANTUM <= 0.0:
            return delay
        if delay < _TIMER_IMMEDIATE_CUTOFF:
            return 0.0
        return math.ceil(delay / _TIMER_QUANTUM) * _TIMER_QUANTUM

    def _timer_handle_cancelled(self, handle: _events.TimerHandle) -> None:
        return

    def _run_once(self) -> None:
        self._scheduler.run_once(
            self,
            self._native_api,
            self._poller,
            self._rsloop_completion_port,
            self._fd_registry,
            self._stream_registry,
            self._socket_registry,
            self._stopping,
            self._clock_resolution,
            self._debug,
            self.slow_callback_duration,
        )

    def run_forever(self) -> None:
        self._run_forever_setup()
        self._native_api.set_stopping(False)
        try:
            if self._native_run_forever is None:
                while True:
                    self._run_once()
                    if self._stopping:
                        break
            else:
                self._native_run_forever(
                    self,
                    self._native_api,
                    self._poller,
                    self._rsloop_completion_port,
                    self._fd_registry,
                    self._stream_registry,
                    self._socket_registry,
                    self._clock_resolution,
                    self._debug,
                    self.slow_callback_duration,
                )
        finally:
            self._native_api.set_stopping(False)
            self._run_forever_cleanup()

    def stop(self) -> None:
        self._stopping = True
        self._native_api.set_stopping(True)

    def _drain_completions(self) -> None:
        for kind, target, payload, is_err in self._rsloop_completion_port.drain():
            if kind == 0:
                if target.cancelled():
                    continue
                if is_err:
                    target.set_exception(payload)
                else:
                    target.set_result(payload)
                continue
            if kind == 2:
                future, result = payload
                target(future, result, is_err)
                continue
            target(payload)

    def _rsloop_socket_state(self, sock: socket.socket) -> _SocketState:
        fd = sock.fileno()
        state = self._rsloop_socket_states.get(fd)
        if state is not None and state.sock is not sock:
            state.close()
            self._rsloop_socket_states.pop(fd, None)
            state = None
        if state is None:
            self._ensure_fd_no_transport(fd)
            state = _rsloop.SocketState(sock, self)
            self._socket_registry.register(fd, state)
            self._rsloop_socket_states[fd] = state
        return state

    def _check_socket(self, sock: socket.socket) -> None:
        _base_events._check_ssl_socket(sock)
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

    def _byte_view(self, buffer: Any, *, writable: bool = False) -> memoryview:
        view = memoryview(buffer)
        if writable and view.readonly:
            raise TypeError("a writable bytes-like object is required")
        if view.itemsize != 1 or view.ndim != 1 or not view.c_contiguous:
            try:
                view = view.cast("B")
            except TypeError as exc:
                mode = "writable" if writable else "bytes-like"
                raise TypeError(f"a C-contiguous {mode} object is required") from exc
        if writable and view.readonly:
            raise TypeError("a writable bytes-like object is required")
        if not view.c_contiguous:
            mode = "writable" if writable else "bytes-like"
            raise TypeError(f"a C-contiguous {mode} object is required")
        return view

    async def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        self._check_socket(sock)
        try:
            return sock.recv(n)
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        self._rsloop_socket_state(sock).enqueue_recv(future, n)
        try:
            return await future
        finally:
            future = None

    async def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        self._check_socket(sock)
        view = self._byte_view(buf, writable=True)
        try:
            return sock.recv_into(view)
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        self._rsloop_socket_state(sock).enqueue_recv_into(future, view)
        try:
            return await future
        finally:
            future = None
            view = None

    async def sock_sendall(
        self, sock: socket.socket, data: bytes | bytearray | memoryview
    ) -> None:
        self._check_socket(sock)
        view = self._byte_view(data)
        try:
            sent = sock.send(view)
        except (BlockingIOError, InterruptedError):
            sent = 0
        if sent == len(view):
            return
        future = self.create_future()
        self._rsloop_socket_state(sock).enqueue_sendall(future, view, sent=sent)
        try:
            return await future
        finally:
            future = None
            view = None

    async def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        self._check_socket(sock)
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
            return conn, address
        except (BlockingIOError, InterruptedError):
            pass
        state = self._rsloop_socket_state(sock)
        enqueue_accept = getattr(state, "enqueue_accept", None)
        if enqueue_accept is not None:
            future = self.create_future()
            enqueue_accept(future)
            try:
                return await future
            finally:
                future = None
        future = self.create_future()

        def on_readable() -> None:
            if future.done():
                self._remove_reader(sock.fileno())
                return
            try:
                conn, address = sock.accept()
                conn.setblocking(False)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._remove_reader(sock.fileno())
                future.set_exception(exc)
            else:
                self._remove_reader(sock.fileno())
                future.set_result((conn, address))

        self._add_reader(sock.fileno(), on_readable)
        try:
            return await future
        finally:
            self._remove_reader(sock.fileno())

    async def sock_connect(self, sock: socket.socket, address: Any) -> None:
        self._check_socket(sock)
        if sock.family == socket.AF_INET or (
            _base_events._HAS_IPv6 and sock.family == socket.AF_INET6
        ):
            host, port = address[:2]
            info = _base_events._ipaddr_info(
                host,
                port,
                sock.family,
                sock.type,
                sock.proto,
                *address[2:],
            )
            if info is None:
                resolved = await self._ensure_resolved(
                    address,
                    family=sock.family,
                    type=sock.type,
                    proto=sock.proto,
                    loop=self,
                )
                _, _, _, _, address = resolved[0]
            else:
                _, _, _, _, address = info
        try:
            sock.connect(address)
        except (BlockingIOError, InterruptedError):
            pass
        except (SystemExit, KeyboardInterrupt):
            raise
        else:
            return None
        future = self.create_future()
        self._rsloop_socket_state(sock).enqueue_connect(future, address)
        try:
            return await future
        finally:
            future = None

    async def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        _base_events._check_ssl_socket(sock)
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            return sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_reader(fd, self._sock_recvfrom, future, sock, bufsize)
        future.add_done_callback(
            functools.partial(self._sock_read_done, fd, handle=handle)
        )
        return await future

    async def sock_recvfrom_into(
        self, sock: socket.socket, buf: Any, nbytes: int = 0
    ) -> tuple[int, Any]:
        _base_events._check_ssl_socket(sock)
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        if not nbytes:
            nbytes = len(buf)
        try:
            return sock.recvfrom_into(buf, nbytes)
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_reader(fd, self._sock_recvfrom_into, future, sock, buf, nbytes)
        future.add_done_callback(
            functools.partial(self._sock_read_done, fd, handle=handle)
        )
        return await future

    async def sock_sendto(
        self, sock: socket.socket, data: bytes | bytearray | memoryview, address: Any
    ) -> int:
        _base_events._check_ssl_socket(sock)
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        try:
            return sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        fd = sock.fileno()
        self._ensure_fd_no_transport(fd)
        handle = self._add_writer(fd, self._sock_sendto, future, sock, data, address)
        future.add_done_callback(
            functools.partial(self._sock_write_done, fd, handle=handle)
        )
        return await future

    def _sock_read_done(
        self, fd: int, fut: asyncio.Future[Any], handle: _events.Handle | None = None
    ) -> None:
        if handle is None or not handle.cancelled():
            self.remove_reader(fd)

    def _sock_write_done(
        self, fd: int, fut: asyncio.Future[Any], handle: _events.Handle | None = None
    ) -> None:
        if handle is None or not handle.cancelled():
            self.remove_writer(fd)

    def _sock_recvfrom(
        self, fut: asyncio.Future[Any], sock: socket.socket, bufsize: int
    ) -> None:
        if fut.done():
            return
        try:
            result = sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

    def _sock_recvfrom_into(
        self, fut: asyncio.Future[Any], sock: socket.socket, buf: Any, bufsize: int
    ) -> None:
        if fut.done():
            return
        try:
            result = sock.recvfrom_into(buf, bufsize)
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

    def _sock_sendto(
        self,
        fut: asyncio.Future[Any],
        sock: socket.socket,
        data: bytes | bytearray | memoryview,
        address: Any,
    ) -> None:
        if fut.done():
            return
        try:
            n = sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(n)

    def _sock_add_cancellation_callback(self, fut: asyncio.Future[Any], sock: socket.socket) -> None:
        def cb(done: asyncio.Future[Any]) -> None:
            if done.cancelled():
                fd = sock.fileno()
                if fd != -1:
                    self.remove_writer(fd)

        fut.add_done_callback(cb)

    def _sock_sendfile_update_filepos(self, fileno: int, offset: int, total_sent: int) -> None:
        if total_sent > 0:
            os.lseek(fileno, offset, os.SEEK_SET)

    def _sock_sendfile_native_impl(
        self,
        fut: asyncio.Future[Any],
        registered_fd: int | None,
        sock: socket.socket,
        fileno: int,
        offset: int,
        count: int | None,
        blocksize: int,
        total_sent: int,
    ) -> None:
        fd = sock.fileno()
        if registered_fd is not None:
            self.remove_writer(registered_fd)
        if fut.cancelled():
            self._sock_sendfile_update_filepos(fileno, offset, total_sent)
            return
        if count:
            blocksize = count - total_sent
            if blocksize <= 0:
                self._sock_sendfile_update_filepos(fileno, offset, total_sent)
                fut.set_result(total_sent)
                return
        blocksize = min(blocksize, sys.maxsize // 2 + 1)
        try:
            sent = os.sendfile(fd, fileno, offset, blocksize)
        except (BlockingIOError, InterruptedError):
            if registered_fd is None:
                self._sock_add_cancellation_callback(fut, sock)
            self.add_writer(
                fd,
                self._sock_sendfile_native_impl,
                fut,
                fd,
                sock,
                fileno,
                offset,
                count,
                blocksize,
                total_sent,
            )
        except OSError as exc:
            if (
                registered_fd is not None
                and exc.errno == errno.ENOTCONN
                and type(exc) is not ConnectionError
            ):
                new_exc = ConnectionError("socket is not connected", errno.ENOTCONN)
                new_exc.__cause__ = exc
                exc = new_exc
            if total_sent == 0:
                err = _exceptions.SendfileNotAvailableError("os.sendfile call failed")
                self._sock_sendfile_update_filepos(fileno, offset, total_sent)
                fut.set_exception(err)
            else:
                self._sock_sendfile_update_filepos(fileno, offset, total_sent)
                fut.set_exception(exc)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._sock_sendfile_update_filepos(fileno, offset, total_sent)
            fut.set_exception(exc)
        else:
            if sent == 0:
                self._sock_sendfile_update_filepos(fileno, offset, total_sent)
                fut.set_result(total_sent)
            else:
                offset += sent
                total_sent += sent
                if registered_fd is None:
                    self._sock_add_cancellation_callback(fut, sock)
                self.add_writer(
                    fd,
                    self._sock_sendfile_native_impl,
                    fut,
                    fd,
                    sock,
                    fileno,
                    offset,
                    count,
                    blocksize,
                    total_sent,
                )

    async def _sock_sendfile_native(
        self, sock: socket.socket, file: Any, offset: int, count: int | None
    ) -> int:
        try:
            os.sendfile
        except AttributeError:
            raise _exceptions.SendfileNotAvailableError("os.sendfile() is not available")
        try:
            fileno = file.fileno()
        except (AttributeError, OSError):
            raise _exceptions.SendfileNotAvailableError("not a regular file")
        try:
            fsize = os.fstat(fileno).st_size
        except OSError:
            raise _exceptions.SendfileNotAvailableError("not a regular file")
        blocksize = count if count else fsize
        if not blocksize:
            return 0
        fut = self.create_future()
        self._sock_sendfile_native_impl(fut, None, sock, fileno, offset, count, blocksize, 0)
        return await fut

    def _make_socket_transport(
        self,
        sock: socket.socket,
        protocol: asyncio.Protocol,
        waiter: asyncio.Future[Any] | None = None,
        *,
        extra: dict[str, Any] | None = None,
        server: Any = None,
    ) -> asyncio.Transport:
        self._ensure_fd_no_transport(sock)
        self._take_fd_for_transport(sock)
        stream_transport_type = getattr(_rsloop, "StreamTransport", None)
        if (
            stream_transport_type is not None
            and isinstance(protocol, _streams.StreamReaderProtocol)
        ):
            extra = {} if extra is None else dict(extra)
            reader = protocol._stream_reader
            _base_events._set_nodelay(sock)
            transport = stream_transport_type(
                sock,
                protocol,
                self,
                self._poller,
                self._stream_registry,
                self._transports,
                extra,
                reader,
                reader._limit if reader is not None else 65536,
            )
            if reader is not None:
                bind_readexactly = getattr(transport, "bind_readexactly", None)
                if bind_readexactly is not None:
                    reader.readexactly = bind_readexactly()
            protocol.connection_made(transport)
            stream_writer = getattr(protocol, "_stream_writer", None)
            if stream_writer is not None and not isinstance(
                stream_writer, RsloopStreamWriter
            ):
                bind_write = getattr(transport, "bind_write", None)
                if bind_write is not None:
                    stream_writer.write = bind_write()
                bind_close = getattr(transport, "bind_close", None)
                if bind_close is not None:
                    stream_writer.close = bind_close()
                bind_drain = getattr(transport, "bind_drain", None)
                if bind_drain is not None:
                    stream_writer.drain = bind_drain(stream_writer.drain)
                bind_wait_closed = getattr(transport, "bind_wait_closed", None)
                if bind_wait_closed is not None:
                    stream_writer.wait_closed = bind_wait_closed()
            activate = getattr(transport, "activate", None)
            if activate is not None:
                activate()
            if waiter is not None and not waiter.cancelled():
                waiter.set_result(None)
            return transport
        return _selector_events._SelectorSocketTransport(
            self, sock, protocol, waiter, extra, server
        )

    def _make_datagram_transport(
        self,
        sock: socket.socket,
        protocol: asyncio.Protocol,
        address: Any = None,
        waiter: asyncio.Future[Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> asyncio.Transport:
        self._ensure_fd_no_transport(sock)
        return _selector_events._SelectorDatagramTransport(
            self,
            sock,
            protocol,
            address,
            waiter,
            extra,
        )

    def _make_read_pipe_transport(
        self,
        pipe: Any,
        protocol: asyncio.Protocol,
        waiter: asyncio.Future[Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> asyncio.Transport:
        return _unix_events._UnixReadPipeTransport(self, pipe, protocol, waiter, extra)

    def _make_write_pipe_transport(
        self,
        pipe: Any,
        protocol: asyncio.Protocol,
        waiter: asyncio.Future[Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> asyncio.Transport:
        return _unix_events._UnixWritePipeTransport(self, pipe, protocol, waiter, extra)

    async def _make_subprocess_transport(
        self,
        protocol: asyncio.BaseProtocol,
        args: Any,
        shell: bool,
        stdin: Any,
        stdout: Any,
        stderr: Any,
        bufsize: int,
        extra: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> asyncio.Transport:
        waiter = self.create_future()
        transport = _unix_events._UnixSubprocessTransport(
            self,
            protocol,
            args,
            shell,
            stdin,
            stdout,
            stderr,
            bufsize,
            waiter=waiter,
            extra=extra,
            **kwargs,
        )
        self._watcher.add_child_handler(
            transport.get_pid(),
            self._child_watcher_callback,
            transport,
        )
        try:
            await waiter
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            transport.close()
            await transport._wait()
            raise
        return transport

    def _child_watcher_callback(
        self, pid: int, returncode: int, transp: Any
    ) -> None:
        self.call_soon_threadsafe(transp._process_exited, returncode)

    def _make_ssl_transport(
        self,
        rawsock: socket.socket,
        protocol: asyncio.Protocol,
        sslcontext: Any,
        waiter: asyncio.Future[Any] | None = None,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
        extra: dict[str, Any] | None = None,
        server: Any = None,
        ssl_handshake_timeout: float = _constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout: float = _constants.SSL_SHUTDOWN_TIMEOUT,
    ) -> asyncio.Transport:
        self._ensure_fd_no_transport(rawsock)
        self._take_fd_for_transport(rawsock)
        ssl_protocol = _sslproto.SSLProtocol(
            self,
            protocol,
            sslcontext,
            waiter,
            server_side,
            server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        _selector_events._SelectorSocketTransport(
            self, rawsock, ssl_protocol, None, extra, server
        )
        return ssl_protocol._app_transport

    async def _create_connection_transport(
        self,
        sock: socket.socket,
        protocol_factory: Any,
        ssl: Any,
        server_hostname: str | None,
        server_side: bool = False,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
    ) -> tuple[asyncio.BaseTransport, asyncio.Protocol]:
        sock.setblocking(False)

        protocol = protocol_factory()
        if ssl:
            waiter = self.create_future()
            sslcontext = None if isinstance(ssl, bool) else ssl
            transport = self._make_ssl_transport(
                sock,
                protocol,
                sslcontext,
                waiter,
                server_side=server_side,
                server_hostname=server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout,
                ssl_shutdown_timeout=ssl_shutdown_timeout,
            )
            try:
                await waiter
            except BaseException:
                transport.close()
                raise
            return transport, protocol

        transport = self._make_socket_transport(sock, protocol, waiter=None)
        return transport, protocol

    def _start_serving(
        self,
        protocol_factory: Any,
        sock: socket.socket,
        sslcontext: Any = None,
        server: Any = None,
        backlog: int = 100,
        ssl_handshake_timeout: float = _constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout: float = _constants.SSL_SHUTDOWN_TIMEOUT,
    ) -> None:
        self._add_reader(
            sock.fileno(),
            self._accept_connection_ready,
            protocol_factory,
            sock,
            sslcontext,
            server,
            ssl_handshake_timeout,
            ssl_shutdown_timeout,
        )

    def _stop_serving(self, sock: socket.socket) -> None:
        self._remove_reader(sock.fileno())
        sock.close()

    def _accept_connection_ready(
        self,
        protocol_factory: Any,
        sock: socket.socket,
        sslcontext: Any = None,
        server: Any = None,
        ssl_handshake_timeout: float = _constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout: float = _constants.SSL_SHUTDOWN_TIMEOUT,
        accepted: tuple[int, Any] | None = None,
    ) -> None:
        while True:
            if accepted is None:
                try:
                    conn, addr = sock.accept()
                    conn.setblocking(False)
                except (BlockingIOError, InterruptedError):
                    break
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    self._accept_connection_error(sock, protocol_factory, server, exc)
                    break
            else:
                fd, addr = accepted
                conn = socket.socket(sock.family, sock.type, sock.proto, fileno=fd)
                conn.setblocking(False)
            if self._debug:
                logger.debug("%r got a new connection from %r: %r", server, addr, conn)
            extra = {"peername": addr}
            if sslcontext is None:
                self._accept_connection_sync(protocol_factory, conn, extra, server)
            else:
                accept = self._accept_connection2(
                    protocol_factory,
                    conn,
                    extra,
                    sslcontext,
                    server,
                    ssl_handshake_timeout,
                    ssl_shutdown_timeout,
                )
                self.create_task(accept)
            if accepted is not None:
                break

    def _accept_connection_error(
        self,
        sock: socket.socket,
        protocol_factory: Any,
        server: Any,
        exc: BaseException,
    ) -> None:
        if not isinstance(exc, OSError):
            raise exc
        err_no = getattr(exc, "errno", None)
        if err_no == errno.EBADF or sock.fileno() < 0 or "Bad file descriptor" in str(exc):
            return
        if err_no in (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM):
            self.call_exception_handler(
                {
                    "message": "socket.accept() out of system resource",
                    "exception": exc,
                    "socket": _selector_events.trsock.TransportSocket(sock),
                }
            )
            self.call_later(
                _constants.ACCEPT_RETRY_DELAY,
                self._start_serving,
                protocol_factory,
                sock,
                None,
                server,
                100,
                _constants.SSL_HANDSHAKE_TIMEOUT,
                _constants.SSL_SHUTDOWN_TIMEOUT,
            )
            return
        raise exc

    async def _accept_connection2(
        self,
        protocol_factory: Any,
        conn: socket.socket,
        extra: dict[str, Any],
        sslcontext: Any = None,
        server: Any = None,
        ssl_handshake_timeout: float = _constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout: float = _constants.SSL_SHUTDOWN_TIMEOUT,
    ) -> None:
        protocol = None
        transport = None
        try:
            protocol = protocol_factory()
            waiter = self.create_future()
            if sslcontext:
                transport = self._make_ssl_transport(
                    conn,
                    protocol,
                    sslcontext,
                    waiter=waiter,
                    server_side=True,
                    extra=extra,
                    server=server,
                    ssl_handshake_timeout=ssl_handshake_timeout,
                    ssl_shutdown_timeout=ssl_shutdown_timeout,
                )
            else:
                transport = self._make_socket_transport(
                    conn, protocol, waiter=waiter, extra=extra, server=server
                )
            try:
                await waiter
            except BaseException:
                transport.close()
                waiter = None
                raise
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            if self._debug:
                context = {
                    "message": "Error on transport creation for incoming connection",
                    "exception": exc,
                }
                if protocol is not None:
                    context["protocol"] = protocol
                if transport is not None:
                    context["transport"] = transport
                self.call_exception_handler(context)

    def _accept_connection_sync(
        self,
        protocol_factory: Any,
        conn: socket.socket,
        extra: dict[str, Any],
        server: Any = None,
    ) -> None:
        protocol = None
        transport = None
        try:
            protocol = protocol_factory()
            transport = self._make_socket_transport(
                conn, protocol, waiter=None, extra=extra, server=server
            )
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            if transport is not None:
                transport.close()
            else:
                conn.close()
            if self._debug:
                context = {
                    "message": "Error on transport creation for incoming connection",
                    "exception": exc,
                }
                if protocol is not None:
                    context["protocol"] = protocol
                if transport is not None:
                    context["transport"] = transport
                self.call_exception_handler(context)

    def create_tokio_future(self, awaitable: Awaitable[Any]) -> Awaitable[Any]:
        return _rsloop.run_in_tokio(awaitable)

    def wrap_tokio_future(self, awaitable: Awaitable[Any]) -> Awaitable[Any]:
        return _rsloop.wrap_future(awaitable)

    async def sleep(self, delay: float) -> None:
        await _rsloop.sleep(delay)

    async def sendfile(
        self,
        transport: Any,
        file: Any,
        offset: int = 0,
        count: int | None = None,
        *,
        fallback: bool = True,
    ) -> int:
        sendfile_static = getattr(transport, "sendfile_static", None)
        if sendfile_static is not None:
            return await sendfile_static(file, offset=offset, count=count)
        return await super().sendfile(
            transport,
            file,
            offset=offset,
            count=count,
            fallback=fallback,
        )

    def set_debug(self, enabled: bool) -> None:
        super().set_debug(enabled)
        native_api = getattr(self, "_native_api", None)
        if native_api is not None:
            native_api.set_debug(enabled)

    def close(self) -> None:
        if self.is_running():
            raise RuntimeError("Cannot close a running event loop")
        if self.is_closed():
            return
        native_api = getattr(self, "_native_api", None)
        if native_api is not None:
            native_api.set_closed(True)
        scheduler = getattr(self, "_scheduler", None)
        if scheduler is not None:
            scheduler.clear()
        socket_states = getattr(self, "_rsloop_socket_states", None)
        if socket_states is not None:
            for state in socket_states.values():
                state.close()
            socket_states.clear()
        completion_port = getattr(self, "_rsloop_completion_port", None)
        if completion_port is not None:
            self._poller.set_interest(completion_port.fileno(), False, False)
            completion_port.close()
        if getattr(self, "_ssock", None) is not None or getattr(self, "_csock", None) is not None:
            self._close_self_pipe()
        if self._signal_handlers:
            for sig in list(self._signal_handlers):
                self.remove_signal_handler(sig)
        poller = getattr(self, "_poller", None)
        if poller is not None:
            poller.close()
        fd_registry = getattr(self, "_fd_registry", None)
        if fd_registry is not None:
            for handle in fd_registry.clear():
                handle.cancel()
        transports = getattr(self, "_transports", None)
        if transports is not None:
            transports.clear()
        super().close()


class RsloopEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """Event-loop policy that creates Rsloop event loops."""

    def new_event_loop(self) -> RsloopEventLoop:
        return RsloopEventLoop()


def install() -> RsloopEventLoopPolicy:
    """Install Rsloop as the active asyncio policy."""

    policy = RsloopEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
    return policy

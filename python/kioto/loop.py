from __future__ import annotations

import asyncio
import collections
import errno
import functools
import selectors
import socket
from asyncio import base_events as _base_events
from asyncio import constants as _constants
from asyncio import events as _events
from asyncio import selector_events as _selector_events
from asyncio import sslproto as _sslproto
from asyncio import streams as _streams
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any

from . import _kioto


logger = _selector_events.logger


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

    def __init__(self, loop: "KiotoEventLoop", sock: socket.socket) -> None:
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


class KiotoSocketTransport(_selector_events._SelectorTransport):
    _start_tls_compatible = True
    _sendfile_compatible = _constants._SendfileMode.TRY_NATIVE

    def __init__(
        self,
        loop: "KiotoEventLoop",
        sock: socket.socket,
        protocol: asyncio.Protocol,
        waiter: asyncio.Future[Any] | None = None,
        extra: dict[str, Any] | None = None,
        server: Any = None,
    ) -> None:
        self._empty_waiter: asyncio.Future[None] | None = None
        self._driver: Any = None
        self._tcp_core: Any = None
        self._write_core: Any = None
        self._python_write_fallback: Any = None
        self._native_try_write_bytes: Any = None
        self._native_write_bytes: Any = None
        self._stream_reader_bridge: Any = None
        self._stream_reader: _streams.StreamReader | None = None
        self._stream_reader_protocol: _streams.StreamReaderProtocol | None = None
        self._write_buffer = bytearray()
        self._write_notify_requested = False
        self._eof = False
        self._eof_sent = False
        self._native_started = False
        self._native_reading = False
        super().__init__(loop, sock, protocol, extra, server)
        self._sync_protocol_fast_path()
        _base_events._set_nodelay(self._sock)
        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.call_soon(self._activate_native_io, waiter)

    def _native_transport_core(self) -> Any:
        return self._tcp_core if self._tcp_core is not None else self._write_core

    def _native_reader_driver(self) -> Any:
        return self._tcp_core if self._tcp_core is not None else self._driver

    def __repr__(self) -> str:
        info = [self.__class__.__name__]
        if self._sock is None:
            info.append("closed")
        elif self._closing:
            info.append("closing")
        info.append(f"fd={self._sock_fd}")
        info.append("read=polling" if self.is_reading() else "read=idle")
        info.append(
            "write=<"
            f"{'polling' if self.get_write_buffer_size() else 'idle'}, "
            f"bufsize={self.get_write_buffer_size()}>"
        )
        return "<{}>".format(" ".join(info))

    def set_protocol(self, protocol: asyncio.Protocol) -> None:
        super().set_protocol(protocol)
        self._sync_protocol_fast_path()

    def _sync_protocol_fast_path(self) -> None:
        if isinstance(self._protocol, _streams.StreamReaderProtocol):
            self._stream_reader_protocol = self._protocol
            self._stream_reader = self._protocol._stream_reader
            if self._stream_reader is not None:
                bridge_type = getattr(_kioto, "StreamReaderBridge", None)
                if bridge_type is not None:
                    self._stream_reader_bridge = bridge_type(self._stream_reader, self)
                    bind_readexactly = getattr(
                        self._stream_reader_bridge, "bind_readexactly", None
                    )
                    if bind_readexactly is not None:
                        self._stream_reader.readexactly = bind_readexactly()
                else:
                    self._stream_reader_bridge = None
            else:
                self._stream_reader_bridge = None
        else:
            self._stream_reader_protocol = None
            self._stream_reader = None
            self._stream_reader_bridge = None

    def pause_reading(self) -> None:
        if not self.is_reading():
            return
        self._paused = True
        if self._native_started:
            self._stop_native_reading()
        if self._loop.get_debug():
            logger.debug("%r pauses reading", self)

    def resume_reading(self) -> None:
        if self._closing or not self._paused:
            return
        self._paused = False
        if self._native_started:
            self._start_native_reading()
        if self._loop.get_debug():
            logger.debug("%r resumes reading", self)

    def _activate_native_io(self, waiter: asyncio.Future[Any] | None) -> None:
        if self._native_started or self._closing or self._sock is None:
            return
        self._native_started = True
        if self._use_tokio_tcp_core():
            self._tcp_core = _kioto.TokioTcpTransportCore(self._sock.fileno())
            if self._stream_reader_bridge is not None:
                self._stream_reader_bridge.attach_tcp_core(self._tcp_core)
            self._python_write_fallback = type(self).write.__get__(self, type(self))
            bind_transport_write = getattr(self._tcp_core, "bind_transport_write", None)
            if bind_transport_write is not None:
                self.write = bind_transport_write(self)
            self._native_try_write_bytes = self._tcp_core.bind_try_write_bytes()
            self._native_write_bytes = self._tcp_core.bind_write_bytes()
        else:
            self._driver = self._loop._kioto_socket_driver(self._sock)
            self._write_core = _kioto.TransportWriteCore(self._sock.fileno())
            self._native_try_write_bytes = self._write_core.bind_try_write_bytes()
            self._native_write_bytes = self._write_core.bind_write_bytes()
        if not self._paused:
            self._start_native_reading()
        if self._write_buffer:
            self._start_native_write()
        if waiter is not None and not waiter.cancelled():
            waiter.set_result(None)

    def _use_tokio_tcp_core(self) -> bool:
        return hasattr(_kioto, "TokioTcpTransportCore")

    def _start_native_reading(self) -> None:
        driver = self._native_reader_driver()
        if self._native_reading or self._sock is None or driver is None:
            return
        if self._tcp_core is not None:
            if self._stream_reader_bridge is not None:
                driver.start_stream_reader_bridge_reading(
                    self._stream_reader_bridge,
                    self._on_native_read_error,
                    self.max_size,
                )
            else:
                driver.start_reading(
                    self._on_native_read,
                    self._on_native_eof,
                    self._on_native_read_error,
                    self.max_size,
                )
        else:
            if self._stream_reader_bridge is not None:
                driver.start_stream_reader_bridge_reading(
                    self._stream_reader_bridge,
                    self._on_native_read_error,
                    self.max_size,
                )
            else:
                driver.start_reading(
                    self._on_native_read,
                    self._on_native_eof,
                    self._on_native_read_error,
                    self.max_size,
                )
        self._native_reading = True

    def _stop_native_reading(self) -> None:
        if not self._native_reading:
            return
        try:
            driver = self._native_reader_driver()
            if driver is not None:
                driver.stop_reading()
        except OSError:
            pass
        self._native_reading = False

    def _close_driver(self) -> None:
        self._native_reading = False
        if self._tcp_core is not None:
            self._tcp_core.close()
            self._tcp_core = None
        if self._write_core is not None:
            self._write_core.close()
        self._write_core = None
        self._python_write_fallback = None
        self._native_try_write_bytes = None
        self._native_write_bytes = None
        if self._driver is None:
            return
        fd = self._sock_fd
        entry = self._loop._kioto_socket_drivers.get(fd)
        if entry is not None and entry[1] is self._driver:
            self._loop._kioto_socket_drivers.pop(fd, None)
        self._driver.close()
        self._driver = None

    def _deliver_buffered_data(self, data: bytes) -> None:
        try:
            buf = self._protocol.get_buffer(len(data))
            if not len(buf):
                raise RuntimeError("get_buffer() returned an empty buffer")
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._native_fatal_error(exc, "Fatal error: protocol.get_buffer() call failed.")
            return
        if len(buf) < len(data):
            self._native_fatal_error(
                RuntimeError("get_buffer() returned a buffer that is too small"),
                "Fatal error: protocol.get_buffer() call failed.",
            )
            return
        memoryview(buf)[: len(data)] = data
        try:
            self._protocol.buffer_updated(len(data))
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._native_fatal_error(
                exc, "Fatal error: protocol.buffer_updated() call failed."
            )

    def _deliver_read_data(self, data: bytes) -> None:
        if self._conn_lost:
            return
        if self._stream_reader is not None:
            self._feed_stream_reader(data)
            return
        if isinstance(self._protocol, asyncio.BufferedProtocol):
            self._deliver_buffered_data(data)
            return
        try:
            self._protocol.data_received(data)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._native_fatal_error(
                exc, "Fatal error: protocol.data_received() call failed."
            )

    def _deliver_eof(self) -> None:
        if self._loop.get_debug():
            logger.debug("%r received EOF", self)
        if self._stream_reader_protocol is not None:
            self._feed_stream_reader_eof()
            if self._stream_reader_protocol._over_ssl:
                self.close()
            else:
                self._stop_native_reading()
            return
        try:
            keep_open = self._protocol.eof_received()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._native_fatal_error(exc, "Fatal error: protocol.eof_received() call failed.")
            return

        if keep_open:
            self._stop_native_reading()
        else:
            self.close()

    def _native_fatal_error(self, exc: BaseException, message: str) -> None:
        if isinstance(exc, OSError):
            if self._loop.get_debug():
                logger.debug("%r: %s", self, message, exc_info=True)
        else:
            self._loop.call_exception_handler(
                {
                    "message": message,
                    "exception": exc,
                    "transport": self,
                    "protocol": self._protocol,
                }
            )
        self._force_close(exc)

    def _wake_stream_reader_waiter(self) -> None:
        reader = self._stream_reader
        if reader is None:
            return
        waiter = reader._waiter
        if waiter is None:
            return
        reader._waiter = None
        if not waiter.cancelled():
            waiter.set_result(None)

    def _feed_stream_reader(self, data: bytes) -> None:
        reader = self._stream_reader
        if reader is None or not data:
            return
        reader._buffer.extend(data)
        self._wake_stream_reader_waiter()
        if (
            reader._transport is not None
            and not reader._paused
            and len(reader._buffer) > 2 * reader._limit
        ):
            try:
                self.pause_reading()
            except NotImplementedError:
                reader._transport = None
            else:
                reader._paused = True

    def _feed_stream_reader_eof(self) -> None:
        reader = self._stream_reader
        if reader is None:
            return
        reader._eof = True
        self._wake_stream_reader_waiter()

    def _start_native_write(self) -> None:
        core = self._native_transport_core()
        if core is None or not self._write_buffer:
            return
        payload = bytes(self._write_buffer)
        self._write_buffer.clear()
        core.write(payload)
        if self._protocol_paused or self._empty_waiter is not None or self._closing or self._eof:
            self._request_native_write_notify()

    def _request_native_write_notify(self) -> None:
        core = self._native_transport_core()
        if core is None or self._write_notify_requested:
            return
        if not self._write_buffer and core.get_write_buffer_size() == 0:
            self._on_native_write_done()
            return
        self._write_notify_requested = True
        waiter = core.drain_waiter()
        waiter.add_done_callback(self._on_native_write_waiter_done)

    def _on_native_write_waiter_done(self, waiter: asyncio.Future[Any]) -> None:
        if waiter.cancelled():
            self._write_notify_requested = False
            return
        try:
            waiter.result()
        except BaseException as exc:
            self._on_native_write_error(exc)
        else:
            self._on_native_write_done()

    def _on_native_read(self, data: bytes) -> None:
        if not self._closing and not self._paused:
            self._deliver_read_data(data)

    def _on_native_eof(self, _payload: Any = None) -> None:
        if not self._closing:
            self._native_reading = False
            self._deliver_eof()

    def _on_native_read_error(self, exc: BaseException) -> None:
        self._native_reading = False
        if not self._closing and not self._conn_lost:
            self._native_fatal_error(exc, "Fatal read error on socket transport")

    def _on_native_write_done(self, _payload: Any = None) -> None:
        if self._write_buffer:
            self._start_native_write()
        self._write_notify_requested = False
        self._maybe_resume_protocol()
        if self._empty_waiter is not None and not self._empty_waiter.done():
            self._empty_waiter.set_result(None)
        if self._closing:
            self._conn_lost += 1
            self._loop.call_soon(self._call_connection_lost, None)
        elif self._eof and not self._eof_sent:
            self._eof_sent = True

    def _on_native_write_error(self, exc: BaseException) -> None:
        self._write_notify_requested = False
        if self._empty_waiter is not None and not self._empty_waiter.done():
            self._empty_waiter.set_exception(exc)
        if not self._conn_lost:
            self._native_fatal_error(exc, "Fatal write error on socket transport")

    def _kioto_native_readexactly(
        self, future: asyncio.Future[Any], size: int
    ) -> bool:
        if (
            self._tcp_core is None
            or self._stream_reader_bridge is None
            or not self._native_reading
            or self._closing
        ):
            return False
        self._tcp_core.readexactly(future, size)
        return True

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                "data argument must be a bytes, bytearray, or memoryview "
                f"object, not {type(data).__name__!r}"
            )
        if self._eof:
            raise RuntimeError("Cannot call write() after write_eof()")
        if self._empty_waiter is not None:
            raise RuntimeError("unable to write; sendfile is in progress")
        if not data:
            return
        if self._conn_lost:
            if self._conn_lost >= _constants.LOG_THRESHOLD_FOR_CONNLOST_WRITES:
                logger.warning("socket.send() raised exception.")
            self._conn_lost += 1
            return

        core = self._native_transport_core()
        payload = data if isinstance(data, bytes) else None
        if (
            core is not None
            and not self._write_buffer
            and (
                self._tcp_core is not None
                or payload is None
                or len(payload) > 8
            )
            and core.get_write_buffer_size() == 0
            and not self._protocol_paused
            and self._empty_waiter is None
            and not self._closing
        ):
            try:
                sent = (
                    (
                        self._native_try_write_bytes(payload)
                        if self._native_try_write_bytes is not None
                        else core.try_write_bytes(payload)
                    )
                    if payload is not None
                    else core.try_write(data)
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._native_fatal_error(exc, "Fatal write error on socket transport")
                return
            data_len = len(data)
            if sent == data_len:
                return
            if payload is not None:
                payload = payload[sent:]
            else:
                payload = bytes(memoryview(data)[sent:])
        if core is None:
            self._write_buffer.extend(data if payload is None else payload)
        else:
            if self._write_buffer:
                self._start_native_write()
            if payload is not None:
                if self._native_write_bytes is not None:
                    self._native_write_bytes(payload)
                else:
                    core.write_bytes(payload)
            else:
                core.write(data)
        self._maybe_pause_protocol()
        if self._protocol_paused:
            self._request_native_write_notify()

    def writelines(self, list_of_data: list[bytes | bytearray | memoryview]) -> None:
        if self._eof:
            raise RuntimeError("Cannot call writelines() after write_eof()")
        if self._empty_waiter is not None:
            raise RuntimeError("unable to writelines; sendfile is in progress")
        if not list_of_data:
            return
        if self._conn_lost:
            if self._conn_lost >= _constants.LOG_THRESHOLD_FOR_CONNLOST_WRITES:
                logger.warning("socket.send() raised exception.")
            self._conn_lost += 1
            return

        for item in list_of_data:
            core = self._native_transport_core()
            if core is None:
                self._write_buffer.extend(item)
            else:
                if self._write_buffer:
                    self._start_native_write()
                if isinstance(item, bytes):
                    if self._native_write_bytes is not None:
                        self._native_write_bytes(item)
                    else:
                        core.write_bytes(item)
                else:
                    core.write(item)
        if self._write_buffer:
            self._maybe_pause_protocol()
            self._start_native_write()
        elif self._native_transport_core() is not None:
            self._maybe_pause_protocol()
            if self._protocol_paused:
                self._request_native_write_notify()

    def get_write_buffer_size(self) -> int:
        core = self._native_transport_core()
        native_buffer = 0 if core is None else core.get_write_buffer_size()
        return len(self._write_buffer) + native_buffer

    def write_eof(self) -> None:
        if self._closing or self._eof:
            return
        self._eof = True
        if not self.get_write_buffer_size():
            try:
                self._sock.shutdown(socket.SHUT_WR)
            except OSError:
                self._native_fatal_error(
                    ConnectionError("Failed to half-close socket"),
                    "Fatal write error on socket transport",
                )
            else:
                self._eof_sent = True
        else:
            core = self._native_transport_core()
            if core is not None:
                core.write_eof()
            self._request_native_write_notify()

    def can_write_eof(self) -> bool:
        return True

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._stop_native_reading()
        if not self.get_write_buffer_size():
            self._conn_lost += 1
            self._loop.call_soon(self._call_connection_lost, None)
            return
        self._request_native_write_notify()

    def _force_close(self, exc: BaseException | None) -> None:
        if self._conn_lost:
            return
        self._closing = True
        self._stop_native_reading()
        self._write_buffer.clear()
        self._write_notify_requested = False
        self._close_driver()
        self._conn_lost += 1
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc: BaseException | None) -> None:
        try:
            self._close_driver()
            super()._call_connection_lost(exc)
        finally:
            self._stream_reader_bridge = None
            self._stream_reader = None
            self._stream_reader_protocol = None
            self._write_buffer.clear()
            self._write_notify_requested = False
            if self._empty_waiter is not None and not self._empty_waiter.done():
                self._empty_waiter.set_exception(
                    ConnectionError("Connection is closed by peer")
                )

    def _make_empty_waiter(self) -> asyncio.Future[None]:
        if self._empty_waiter is not None:
            raise RuntimeError("Empty waiter is already set")
        self._empty_waiter = self._loop.create_future()
        if not self.get_write_buffer_size():
            self._empty_waiter.set_result(None)
        return self._empty_waiter

    def _reset_empty_waiter(self) -> None:
        self._empty_waiter = None


class KiotoEventLoop(_base_events.BaseEventLoop):
    """Tokio-backed asyncio loop with a native scheduling core."""

    def __init__(self) -> None:
        super().__init__()
        self._poller = _kioto.TokioPoller()
        self._scheduler = _kioto.Scheduler()
        self._native_api = _kioto.LoopApi(self, self._scheduler, self._debug)
        self._fd_registry = _kioto.FdCallbackRegistry()
        self._stream_registry = _kioto.StreamTransportRegistry()
        self._socket_registry = _kioto.SocketStateRegistry()
        self._native_run_forever = getattr(self._scheduler, "run_forever_native", None)
        self._install_native_bindings()
        self._transports: dict[int, Any] = {}
        self._kioto_socket_states: dict[int, Any] = {}
        self._kioto_socket_drivers: dict[int, tuple[socket.socket, Any]] = {}
        self._kioto_server_drivers: dict[int, tuple[socket.socket, Any]] = {}
        self._kioto_completion_port = _kioto.CompletionPort()
        self._ssock: socket.socket | None = None
        self._csock: socket.socket | None = None
        self._make_self_pipe()
        self._poller.set_interest(self._kioto_completion_port.fileno(), True, False)

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
        state = self._kioto_socket_states.pop(fileno, None)
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
        return

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
        handle = _kioto.make_handle(callback, args, self, None, None)
        previous = self._fd_registry.add_reader(fileno, handle)
        if previous is not None:
            previous.cancel()
        self._sync_fd_state(fileno)
        return handle

    def _remove_reader(self, fd: Any) -> bool:
        if self.is_closed():
            return False
        fileno = self._normalize_fd(fd)
        handle = self._fd_registry.remove_reader(fileno)
        self._sync_fd_state(fileno)
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

    def _add_writer(self, fd: Any, callback: Any, *args: Any) -> _events.Handle:
        self._check_closed()
        fileno = self._normalize_fd(fd)
        handle = _kioto.make_handle(callback, args, self, None, None)
        previous = self._fd_registry.add_writer(fileno, handle)
        if previous is not None:
            previous.cancel()
        self._sync_fd_state(fileno)
        return handle

    def _remove_writer(self, fd: Any) -> bool:
        if self.is_closed():
            return False
        fileno = self._normalize_fd(fd)
        handle = self._fd_registry.remove_writer(fileno)
        self._sync_fd_state(fileno)
        if handle is not None:
            handle.cancel()
            return True
        return False

    def _process_fd_events(self, events: list[tuple[int, int]]) -> None:
        for handle in self._fd_registry.dispatch(events):
            self._add_callback(handle)

    def _add_callback(self, handle: _events.Handle) -> None:
        if not handle._cancelled:
            self._scheduler.push_ready(handle)

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
        handle = _kioto.make_handle(callback, args, self, context, None)
        self._scheduler.push_ready(handle)
        return handle

    def _compat__call_soon(
        self,
        callback: Any,
        args: tuple[Any, ...],
        context: Any,
    ) -> _events.Handle:
        handle = _kioto.make_handle(callback, args, self, context, None)
        self._scheduler.push_ready(handle)
        return handle

    def call_later(
        self, delay: float, callback: Any, *args: Any, context: Any = None
    ) -> _events.TimerHandle:
        if delay is None:
            raise TypeError("delay must not be None")
        if delay <= 0:
            return self.call_soon(callback, *args, context=context)
        return self.call_at(self.time() + delay, callback, *args, context=context)

    def _compat_call_soon_threadsafe(
        self, callback: Any, *args: Any, context: Any = None
    ) -> _events.Handle:
        self._check_closed()
        if self._debug:
            self._check_callback(callback, "call_soon_threadsafe")
        handle = _kioto.make_handle(callback, args, self, context, None)
        push_ready_threadsafe = getattr(self._scheduler, "push_ready_threadsafe", None)
        if push_ready_threadsafe is not None:
            push_ready_threadsafe(handle)
        else:
            self._scheduler.push_ready(handle)
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
        timer = _kioto.make_handle(callback, args, self, context, when)
        self._scheduler.push_timer(timer, when)
        return timer

    def _timer_handle_cancelled(self, handle: _events.TimerHandle) -> None:
        return

    def _run_once(self) -> None:
        self._scheduler.run_once(
            self,
            self._poller,
            self._kioto_completion_port,
            self._stopping,
            self._clock_resolution,
            self._debug,
            self.slow_callback_duration,
        )

    def run_forever(self) -> None:
        self._run_forever_setup()
        self._scheduler.set_stopping(False)
        try:
            if self._native_run_forever is None:
                while True:
                    self._run_once()
                    if self._stopping:
                        break
            else:
                self._native_run_forever(
                    self,
                    self._poller,
                    self._kioto_completion_port,
                    self._clock_resolution,
                    self._debug,
                    self.slow_callback_duration,
                )
        finally:
            self._scheduler.set_stopping(False)
            self._run_forever_cleanup()

    def stop(self) -> None:
        self._stopping = True
        self._scheduler.set_stopping(True)

    def _drain_completions(self) -> None:
        for kind, target, payload, is_err in self._kioto_completion_port.drain():
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

    def _kioto_socket_state(self, sock: socket.socket) -> _SocketState:
        fd = sock.fileno()
        state = self._kioto_socket_states.get(fd)
        if state is not None and state.sock is not sock:
            state.close()
            self._kioto_socket_states.pop(fd, None)
            state = None
        if state is None:
            self._ensure_fd_no_transport(fd)
            state = _kioto.SocketState(sock, self)
            self._socket_registry.register(fd, state)
            self._kioto_socket_states[fd] = state
        return state

    def _kioto_socket_driver(self, sock: socket.socket) -> Any:
        fd = sock.fileno()
        state = self._kioto_socket_states.pop(fd, None)
        if state is not None:
            state.close()

        entry = self._kioto_socket_drivers.get(fd)
        if entry is not None:
            current_sock, driver = entry
            if current_sock is sock:
                return driver
            driver.close()
            self._kioto_socket_drivers.pop(fd, None)

        driver = _kioto.SocketDriver(fd)
        self._kioto_socket_drivers[fd] = (sock, driver)
        return driver

    def _kioto_server_driver(self, sock: socket.socket) -> Any:
        fd = sock.fileno()
        entry = self._kioto_server_drivers.get(fd)
        if entry is not None:
            current_sock, driver = entry
            if current_sock is sock:
                return driver
            driver.close()
            self._kioto_server_drivers.pop(fd, None)

        driver = _kioto.ServerDriver(fd)
        self._kioto_server_drivers[fd] = (sock, driver)
        return driver

    def _check_socket(self, sock: socket.socket) -> None:
        _base_events._check_ssl_socket(sock)
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

    async def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        self._check_socket(sock)
        try:
            return sock.recv(n)
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        self._kioto_socket_state(sock).enqueue_recv(future, n)
        try:
            return await future
        finally:
            future = None

    async def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        self._check_socket(sock)
        try:
            return sock.recv_into(buf)
        except (BlockingIOError, InterruptedError):
            pass
        data = await self.sock_recv(sock, len(buf))
        view = memoryview(buf)
        view[: len(data)] = data
        return len(data)

    async def sock_sendall(
        self, sock: socket.socket, data: bytes | bytearray | memoryview
    ) -> None:
        self._check_socket(sock)
        try:
            sent = sock.send(data)
        except (BlockingIOError, InterruptedError):
            sent = 0
        if sent == len(data):
            return
        future = self.create_future()
        self._kioto_socket_state(sock).enqueue_sendall(
            future, memoryview(data)[sent:], sent=0
        )
        try:
            return await future
        finally:
            future = None

    async def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        self._check_socket(sock)
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
            return conn, address
        except (BlockingIOError, InterruptedError):
            pass
        fd, address = await self._kioto_server_driver(sock).accept()
        conn = socket.socket(sock.family, sock.type, sock.proto, fileno=fd)
        conn.setblocking(False)
        return conn, address

    async def sock_connect(self, sock: socket.socket, address: Any) -> None:
        self._check_socket(sock)
        if sock.family == socket.AF_INET or (
            _base_events._HAS_IPv6 and sock.family == socket.AF_INET6
        ):
            resolved = await self._ensure_resolved(
                address,
                family=sock.family,
                type=sock.type,
                proto=sock.proto,
                loop=self,
            )
            _, _, _, _, address = resolved[0]
        try:
            sock.connect(address)
        except (BlockingIOError, InterruptedError):
            pass
        except (SystemExit, KeyboardInterrupt):
            raise
        else:
            return None
        future = self.create_future()
        self._kioto_socket_state(sock).enqueue_connect(future, address)
        try:
            return await future
        finally:
            future = None

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
        stream_transport_type = getattr(_kioto, "StreamTransport", None)
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
            bind_drain = getattr(transport, "bind_drain", None)
            if stream_writer is not None and bind_drain is not None:
                stream_writer.drain = bind_drain(stream_writer.drain)
            activate = getattr(transport, "activate", None)
            if activate is not None:
                activate()
            if waiter is not None and not waiter.cancelled():
                waiter.set_result(None)
            return transport
        return KiotoSocketTransport(self, sock, protocol, waiter, extra, server)

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
        KiotoSocketTransport(self, rawsock, ssl_protocol, extra=extra, server=server)
        return ssl_protocol._app_transport

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
            driver_entry = self._kioto_server_drivers.pop(sock.fileno(), None)
            if driver_entry is not None:
                _current_sock, driver = driver_entry
                driver.stop_serving()
                driver.close()
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
        return _kioto.run_in_tokio(awaitable)

    def wrap_tokio_future(self, awaitable: Awaitable[Any]) -> Awaitable[Any]:
        return _kioto.wrap_future(awaitable)

    async def sleep(self, delay: float) -> None:
        await _kioto.sleep(delay)

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
        socket_drivers = getattr(self, "_kioto_socket_drivers", None)
        if socket_drivers is not None:
            for _sock, driver in socket_drivers.values():
                driver.close()
            socket_drivers.clear()
        server_drivers = getattr(self, "_kioto_server_drivers", None)
        if server_drivers is not None:
            for _sock, driver in server_drivers.values():
                driver.close()
            server_drivers.clear()
        socket_states = getattr(self, "_kioto_socket_states", None)
        if socket_states is not None:
            for state in socket_states.values():
                state.close()
            socket_states.clear()
        completion_port = getattr(self, "_kioto_completion_port", None)
        if completion_port is not None:
            self._poller.set_interest(completion_port.fileno(), False, False)
            completion_port.close()
        if getattr(self, "_ssock", None) is not None or getattr(self, "_csock", None) is not None:
            self._close_self_pipe()
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


class KiotoEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """Event-loop policy that creates Kioto event loops."""

    def new_event_loop(self) -> KiotoEventLoop:
        return KiotoEventLoop()


def install() -> KiotoEventLoopPolicy:
    """Install Kioto as the active asyncio policy."""

    policy = KiotoEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
    return policy

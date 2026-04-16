from __future__ import annotations

import errno
import os
import stat
import sys
import warnings
from asyncio import constants as _constants
from asyncio import futures as _futures
from asyncio import selector_events as _selector_events
from asyncio import transports as _transports
from collections import deque
from itertools import islice
from typing import Any


_LOGGER = _selector_events.logger
_WRITEV_MAX = max(1, min(64, os.sysconf("SC_IOV_MAX")))
_SMALL_WRITE_MERGE_LIMIT = 64 * 1024
_SMALL_WRITE_CHUNK_LIMIT = 1024


class RsloopReadPipeTransport(_transports.ReadTransport):
    max_size = 256 * 1024

    def __init__(
        self,
        loop: Any,
        pipe: Any,
        protocol: Any,
        waiter: Any = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(extra)
        self._extra["pipe"] = pipe
        self._loop = loop
        self._pipe = pipe
        self._fileno = pipe.fileno()
        self._protocol = protocol
        self._closing = False
        self._paused = False

        mode = os.fstat(self._fileno).st_mode
        if not (
            stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode) or stat.S_ISCHR(mode)
        ):
            self._pipe = None
            self._fileno = None
            self._protocol = None
            raise ValueError("Pipe transport is for pipes/sockets only.")

        os.set_blocking(self._fileno, False)
        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.call_soon(self._add_reader)
        if waiter is not None:
            self._loop.call_soon(_futures._set_result_unless_cancelled, waiter, None)

    def _add_reader(self) -> None:
        if self.is_reading():
            self._loop._add_reader(self._fileno, self._read_ready)

    def is_reading(self) -> bool:
        return not self._paused and not self._closing

    def pause_reading(self) -> None:
        if not self.is_reading():
            return
        self._paused = True
        self._loop._remove_reader(self._fileno)

    def resume_reading(self) -> None:
        if self._closing or not self._paused:
            return
        self._paused = False
        self._loop._add_reader(self._fileno, self._read_ready)

    def set_protocol(self, protocol: Any) -> None:
        self._protocol = protocol

    def get_protocol(self) -> Any:
        return self._protocol

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if not self._closing:
            self._close(None)

    def __del__(self, _warn=warnings.warn) -> None:
        if self._pipe is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            self._pipe.close()

    def _read_ready(self) -> None:
        try:
            data = os.read(self._fileno, self.max_size)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            self._fatal_error(exc, "Fatal read error on pipe transport")
            return

        if data:
            self._protocol.data_received(data)
            return

        self._closing = True
        self._loop._remove_reader(self._fileno)
        self._loop.call_soon(self._protocol.eof_received)
        self._loop.call_soon(self._call_connection_lost, None)

    def _fatal_error(self, exc: BaseException, message: str) -> None:
        if isinstance(exc, OSError) and exc.errno == errno.EIO:
            if self._loop.get_debug():
                _LOGGER.debug("%r: %s", self, message, exc_info=True)
        else:
            self._loop.call_exception_handler(
                {
                    "message": message,
                    "exception": exc,
                    "transport": self,
                    "protocol": self._protocol,
                }
            )
        self._close(exc)

    def _close(self, exc: BaseException | None) -> None:
        self._closing = True
        self._loop._remove_reader(self._fileno)
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc: BaseException | None) -> None:
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._pipe.close()
            self._pipe = None
            self._protocol = None
            self._loop = None


class RsloopWritePipeTransport(
    _transports._FlowControlMixin, _transports.WriteTransport
):
    def __init__(
        self,
        loop: Any,
        pipe: Any,
        protocol: Any,
        waiter: Any = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(extra, loop)
        self._extra["pipe"] = pipe
        self._pipe = pipe
        self._fileno = pipe.fileno()
        self._protocol = protocol
        self._buffer: deque[bytes | bytearray | memoryview] = deque()
        self._buffer_size = 0
        self._conn_lost = 0
        self._closing = False
        self._writer_registered = False
        self._writer_disarm: Any | None = None
        self._flushing = False
        self._write_ready_cb = self._write_ready

        mode = os.fstat(self._fileno).st_mode
        self._is_char = stat.S_ISCHR(mode)
        self._is_fifo = stat.S_ISFIFO(mode)
        self._is_socket = stat.S_ISSOCK(mode)
        if not (self._is_char or self._is_fifo or self._is_socket):
            self._pipe = None
            self._fileno = None
            self._protocol = None
            raise ValueError(
                "Pipe transport is only for pipes, sockets and character devices"
            )

        os.set_blocking(self._fileno, False)
        self._loop.call_soon(self._protocol.connection_made, self)

        if self._is_socket or (self._is_fifo and not sys.platform.startswith("aix")):
            self._loop.call_soon(self._add_reader)

        if waiter is not None:
            self._loop.call_soon(_futures._set_result_unless_cancelled, waiter, None)

    def _add_reader(self) -> None:
        if self._pipe is not None and not self._closing:
            self._loop._add_reader(self._fileno, self._read_ready)

    def get_write_buffer_size(self) -> int:
        return self._buffer_size

    def _read_ready(self) -> None:
        if self._buffer:
            self._close(BrokenPipeError())
        else:
            self._close(None)

    def write(self, data: Any) -> None:
        if self._conn_lost or self._closing:
            if self._conn_lost >= _constants.LOG_THRESHOLD_FOR_CONNLOST_WRITES:
                _LOGGER.warning(
                    "pipe closed by peer or os.write(pipe, data) raised exception."
                )
            self._conn_lost += 1
            return

        if type(data) is bytes:
            chunk = data
        elif isinstance(data, bytearray):
            chunk = bytes(data)
        elif isinstance(data, memoryview):
            chunk = data.tobytes()
        else:
            raise TypeError(f"data argument must be bytes-like, got {type(data)!r}")

        if not chunk:
            return

        if self._buffer:
            tail = self._buffer[-1]
            if (
                len(chunk) <= _SMALL_WRITE_CHUNK_LIMIT
                and len(tail) + len(chunk) <= _SMALL_WRITE_MERGE_LIMIT
            ):
                if isinstance(tail, bytearray):
                    tail.extend(chunk)
                else:
                    merged = bytearray(tail)
                    merged.extend(chunk)
                    self._buffer[-1] = merged
                previous_size = self._buffer_size
                self._buffer_size += len(chunk)
                if (
                    not self._protocol_paused
                    and previous_size <= self._high_water < self._buffer_size
                ):
                    self._maybe_pause_protocol()
                if not self._writer_registered:
                    self._schedule_flush()
                return

        if type(chunk) is bytes and len(chunk) <= _SMALL_WRITE_CHUNK_LIMIT:
            self._buffer.append(chunk)
        else:
            self._buffer.append(memoryview(chunk))
        previous_size = self._buffer_size
        self._buffer_size += len(chunk)
        if not self._protocol_paused and previous_size <= self._high_water < self._buffer_size:
            self._maybe_pause_protocol()
        if not self._writer_registered:
            self._schedule_flush()

    def _schedule_flush(self) -> None:
        if self._writer_disarm is not None:
            self._writer_disarm.cancel()
            self._writer_disarm = None
        if self._flushing:
            return
        if self._writer_registered:
            return
        self._loop._add_writer(self._fileno, self._write_ready_cb)
        self._writer_registered = True

    def _schedule_writer_disarm(self) -> None:
        if self._writer_disarm is not None or not self._writer_registered:
            return
        self._writer_disarm = self._loop.call_soon(self._maybe_disarm_writer)

    def _maybe_disarm_writer(self) -> None:
        self._writer_disarm = None
        if self._buffer or self._closing:
            return
        if self._writer_registered:
            self._loop._remove_writer(self._fileno)
            self._writer_registered = False

    def _write_ready(self) -> None:
        if not self._buffer or self._pipe is None:
            return

        self._flushing = True
        try:
            while self._buffer:
                try:
                    written = self._writev()
                except (BlockingIOError, InterruptedError):
                    written = 0
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException as exc:
                    self._buffer.clear()
                    self._buffer_size = 0
                    self._conn_lost += 1
                    if self._writer_registered:
                        self._loop._remove_writer(self._fileno)
                        self._writer_registered = False
                    self._fatal_error(exc, "Fatal write error on pipe transport")
                    return

                if written > 0:
                    self._consume(written)
                    continue
                break

            if self._buffer:
                if not self._writer_registered:
                    self._loop._add_writer(self._fileno, self._write_ready_cb)
                    self._writer_registered = True
                return

            if self._writer_disarm is not None:
                self._writer_disarm.cancel()
                self._writer_disarm = None
            self._schedule_writer_disarm()
            if self._writer_registered:
                if self._closing:
                    self._loop._remove_writer(self._fileno)
                    self._writer_registered = False
            self._maybe_resume_protocol()
            if self._closing:
                self._loop._remove_reader(self._fileno)
                self._call_connection_lost(None)
            return
        finally:
            self._flushing = False

    def _writev(self) -> int:
        if not self._buffer:
            return 0
        if len(self._buffer) == 1:
            return os.write(self._fileno, self._buffer[0])
        chunks = list(islice(self._buffer, _WRITEV_MAX))
        if len(chunks) == 1:
            return os.write(self._fileno, chunks[0])
        return os.writev(self._fileno, chunks)

    def _consume(self, written: int) -> None:
        self._buffer_size -= written
        while written and self._buffer:
            head = self._buffer[0]
            if written >= len(head):
                written -= len(head)
                self._buffer.popleft()
                continue
            if isinstance(head, (bytearray, bytes)):
                self._buffer[0] = memoryview(head)[written:]
            else:
                self._buffer[0] = head[written:]
            written = 0

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        if self._closing:
            return
        self._closing = True
        if not self._buffer:
            self._loop._remove_reader(self._fileno)
            self._loop.call_soon(self._call_connection_lost, None)

    def set_protocol(self, protocol: Any) -> None:
        self._protocol = protocol

    def get_protocol(self) -> Any:
        return self._protocol

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if self._pipe is not None and not self._closing:
            self.write_eof()

    def abort(self) -> None:
        self._close(None)

    def __del__(self, _warn=warnings.warn) -> None:
        if self._pipe is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            self._pipe.close()

    def _fatal_error(self, exc: BaseException, message: str) -> None:
        if isinstance(exc, OSError):
            if self._loop.get_debug():
                _LOGGER.debug("%r: %s", self, message, exc_info=True)
        else:
            self._loop.call_exception_handler(
                {
                    "message": message,
                    "exception": exc,
                    "transport": self,
                    "protocol": self._protocol,
                }
            )
        self._close(exc)

    def _close(self, exc: BaseException | None = None) -> None:
        self._closing = True
        if self._writer_disarm is not None:
            self._writer_disarm.cancel()
            self._writer_disarm = None
        if self._writer_registered:
            self._loop._remove_writer(self._fileno)
            self._writer_registered = False
        self._buffer.clear()
        self._buffer_size = 0
        self._loop._remove_reader(self._fileno)
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc: BaseException | None) -> None:
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._pipe.close()
            self._pipe = None
            self._protocol = None
            self._loop = None

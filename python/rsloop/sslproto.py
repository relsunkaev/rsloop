from __future__ import annotations

from asyncio import sslproto as _sslproto
from asyncio import streams as _streams
from typing import Any


class RsloopSSLProtocol(_sslproto.SSLProtocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._rsloop_stream_reader = None
        self._rsloop_reader_feed_data = None
        self._rsloop_reader_feed_eof = None
        super().__init__(*args, **kwargs)

    def _set_app_protocol(self, app_protocol: Any) -> None:
        super()._set_app_protocol(app_protocol)
        self._rsloop_stream_reader = None
        self._rsloop_reader_feed_data = None
        self._rsloop_reader_feed_eof = None
        if type(app_protocol) is not _streams.StreamReaderProtocol:
            return
        reader = app_protocol._stream_reader
        if reader is None:
            return
        self._rsloop_stream_reader = reader
        self._rsloop_reader_feed_data = reader.feed_data
        self._rsloop_reader_feed_eof = reader.feed_eof

    def _process_outgoing(self) -> None:
        if self._ssl_writing_paused or not self._outgoing.pending:
            return
        data = self._outgoing.read()
        if data:
            self._transport.write(data)

    def _do_read(self) -> None:
        if self._state not in (
            _sslproto.SSLProtocolState.WRAPPED,
            _sslproto.SSLProtocolState.FLUSHING,
        ):
            return
        try:
            if not self._app_reading_paused:
                if self._app_protocol_is_buffer:
                    self._do_read__buffered()
                else:
                    self._do_read__copied()
                if self._write_backlog:
                    self._do_write()
                else:
                    self._process_outgoing()
                    self._control_app_writing()
            self._control_ssl_reading()
        except Exception as ex:
            self._fatal_error(ex, "Fatal error on SSL protocol")

    def _do_read__copied(self) -> None:
        feed_data = self._rsloop_reader_feed_data
        if feed_data is None:
            super()._do_read__copied()
            return

        chunk = b"1"
        zero = True
        one = False
        sslobj_read = self._sslobj.read
        max_size = self.max_size

        try:
            while True:
                chunk = sslobj_read(max_size)
                if not chunk:
                    break
                if zero:
                    zero = False
                    one = True
                    first = chunk
                elif one:
                    one = False
                    data = [first, chunk]
                else:
                    data.append(chunk)
        except _sslproto.SSLAgainErrors:
            pass

        if one:
            feed_data(first)
        elif not zero:
            feed_data(b"".join(data))
        if not chunk:
            self._call_eof_received()
            self._start_shutdown()

    def _call_eof_received(self) -> None:
        feed_eof = self._rsloop_reader_feed_eof
        if (
            feed_eof is not None
            and self._app_state == _sslproto.AppProtocolState.STATE_CON_MADE
        ):
            self._app_state = _sslproto.AppProtocolState.STATE_EOF
            feed_eof()
            return
        super()._call_eof_received()

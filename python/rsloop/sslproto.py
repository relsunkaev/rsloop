from __future__ import annotations

from asyncio import sslproto as _sslproto
from asyncio import streams as _streams
from typing import Any

class RsloopSSLProtocol(_sslproto.SSLProtocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._rsloop_stream_reader = None
        self._rsloop_stream_reader_buffer = None
        self._rsloop_stream_reader_limit = 0
        self._rsloop_incoming_write = None
        self._rsloop_sslobj_read = None
        self._rsloop_outgoing_read = None
        self._rsloop_transport_write = None
        super().__init__(*args, **kwargs)
        self._rsloop_incoming_write = self._incoming.write
        self._rsloop_sslobj_read = self._sslobj.read
        self._rsloop_outgoing_read = self._outgoing.read

    def _set_app_protocol(self, app_protocol: Any) -> None:
        super()._set_app_protocol(app_protocol)
        self._rsloop_stream_reader = None
        self._rsloop_stream_reader_buffer = None
        self._rsloop_stream_reader_limit = 0
        if type(app_protocol) is not _streams.StreamReaderProtocol:
            return
        reader = app_protocol._stream_reader
        if reader is None:
            return
        self._rsloop_stream_reader = reader
        self._rsloop_stream_reader_buffer = reader._buffer
        self._rsloop_stream_reader_limit = reader._limit

    def connection_made(self, transport: Any) -> None:
        super().connection_made(transport)
        self._rsloop_transport_write = self._transport.write

    def _process_outgoing(self) -> None:
        if self._ssl_writing_paused or not self._outgoing.pending:
            return
        data = self._outgoing.read()
        if data:
            self._transport.write(data)

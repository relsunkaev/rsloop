#![cfg(unix)]

use std::collections::VecDeque;
use std::env;
use std::os::fd::RawFd;
use std::ptr;

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict};

const DEFAULT_STREAM_READ_SIZE: usize = 64 * 1024;
const NATIVE_READEXACTLY_LIMIT: usize = 16 * 1024;
const WRITE_HIGH_WATER: usize = 64 * 1024;
const WRITE_LOW_WATER: usize = 16 * 1024;

struct PendingRead {
    size: usize,
    future: Py<PyAny>,
}

struct PendingWrite {
    data: Vec<u8>,
    sent: usize,
}

#[pyclass(module = "kioto._kioto")]
pub struct StreamTransport {
    sock: Py<PyAny>,
    fd: RawFd,
    protocol: Py<PyAny>,
    loop_obj: Py<PyAny>,
    extra: Py<PyAny>,
    reader: Option<Py<PyAny>>,
    reader_readexactly: Option<Py<PyAny>>,
    drain_fallback: Option<Py<PyAny>>,
    ready_drain: Option<Py<PyAny>>,
    buffer: Option<Py<PyByteArray>>,
    limit: usize,
    pending_read: Option<PendingRead>,
    read_size: usize,
    read_buffer: Vec<u8>,
    write_queue: VecDeque<PendingWrite>,
    pending_write_bytes: usize,
    closing: bool,
    closed: bool,
    read_paused: bool,
    reader_registered: bool,
    writer_registered: bool,
    eof_requested: bool,
    eof_sent: bool,
    protocol_paused: bool,
    connection_lost_sent: bool,
}

#[pymethods]
impl StreamTransport {
    #[new]
    #[pyo3(signature = (sock, protocol, loop_obj, extra=None, reader=None, read_size=DEFAULT_STREAM_READ_SIZE))]
    fn new(
        py: Python<'_>,
        sock: Py<PyAny>,
        protocol: Py<PyAny>,
        loop_obj: Py<PyAny>,
        extra: Option<Py<PyAny>>,
        reader: Option<Py<PyAny>>,
        read_size: usize,
    ) -> PyResult<Self> {
        let extra = match extra {
            Some(extra) => extra,
            None => PyDict::new(py).into_any().unbind(),
        };
        let fd = sock.bind(py).call_method0("fileno")?.extract::<i32>()? as RawFd;
        let (buffer, reader_readexactly, limit) = if let Some(reader_ref) = reader.as_ref() {
            let reader_ref = reader_ref.bind(py);
            let buffer = reader_ref
                .getattr("_buffer")?
                .cast_into::<PyByteArray>()?
                .unbind();
            let reader_readexactly = reader_ref.getattr("readexactly")?.unbind();
            let limit = reader_ref.getattr("_limit")?.extract()?;
            (Some(buffer), Some(reader_readexactly), limit)
        } else {
            (None, None, 64 * 1024)
        };
        Ok(Self {
            sock,
            fd,
            protocol,
            loop_obj,
            extra,
            reader,
            reader_readexactly,
            drain_fallback: None,
            ready_drain: None,
            buffer,
            limit,
            pending_read: None,
            read_size: read_size.max(1),
            read_buffer: vec![0_u8; read_size.max(1)],
            write_queue: VecDeque::new(),
            pending_write_bytes: 0,
            closing: false,
            closed: false,
            read_paused: false,
            reader_registered: false,
            writer_registered: false,
            eof_requested: false,
            eof_sent: false,
            protocol_paused: false,
            connection_lost_sent: false,
        })
    }

    fn activate(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        {
            let transport = slf.borrow(py);
            let transports = transport.loop_obj.bind(py).getattr("_transports")?;
            transports.set_item(transport.fd, slf.bind(py))?;
            transport
                .loop_obj
                .bind(py)
                .getattr("_stream_registry")?
                .call_method1("register", (transport.fd, slf.bind(py)))?;
        }
        let mut transport = slf.borrow_mut(py);
        transport.sync_interest(py)
    }

    fn bind_readexactly(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_readexactly(slf.py(), slf.as_any().as_unbound())
    }

    fn bind_drain(slf: Py<Self>, py: Python<'_>, fallback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        slf.borrow_mut(py).drain_fallback = Some(fallback);
        create_bound_drain(py, slf.bind(py).as_any().as_unbound())
    }

    fn write(&mut self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        self.ensure_can_write()?;
        if let Ok(bytes) = data.cast::<PyBytes>() {
            self.write_bytes_inner(py, bytes.as_bytes())?;
        } else {
            let bytes = data.extract::<Vec<u8>>()?;
            self.write_bytes_inner(py, &bytes)?;
        }
        Ok(())
    }

    fn writelines(&mut self, py: Python<'_>, list_of_data: Bound<'_, PyAny>) -> PyResult<()> {
        for item in list_of_data.try_iter()? {
            self.write(py, item?)?;
        }
        Ok(())
    }

    fn write_eof(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut transport = slf.borrow_mut(py);
        if transport.closing || transport.eof_requested {
            return Ok(());
        }
        transport.eof_requested = true;
        if transport.pending_write_bytes == 0 {
            transport.finish_half_close()?;
        } else {
            transport.sync_interest(py)?;
        }
        Ok(())
    }

    fn can_write_eof(&self) -> bool {
        true
    }

    fn close(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut transport = slf.borrow_mut(py);
        if transport.closing || transport.closed {
            return Ok(());
        }
        transport.closing = true;
        transport.remove_reader(py)?;
        if transport.pending_write_bytes == 0 {
            transport.finish_close(py, None)?;
        } else {
            transport.sync_interest(py)?;
        }
        Ok(())
    }

    #[pyo3(signature = (message=None))]
    fn abort(slf: Py<Self>, py: Python<'_>, message: Option<String>) -> PyResult<()> {
        let exc = message.map(|message| PyRuntimeError::new_err(message).into_value(py).into_any());
        let mut transport = slf.borrow_mut(py);
        transport.finish_close(py, exc)
    }

    fn is_closing(&self) -> bool {
        self.closing || self.closed
    }

    fn get_write_buffer_size(&self) -> usize {
        self.pending_write_bytes
    }

    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(
        &self,
        py: Python<'_>,
        name: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if name == "socket" {
            return Ok(self.sock.clone_ref(py));
        }
        let extra = self.extra.bind(py).cast::<PyDict>()?;
        if let Some(value) = extra.get_item(name)? {
            Ok(value.unbind())
        } else if let Some(default) = default {
            Ok(default)
        } else {
            Ok(py.None())
        }
    }

    fn pause_reading(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.read_paused || self.closed {
            return Ok(());
        }
        self.read_paused = true;
        self.remove_reader(py)
    }

    fn resume_reading(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut transport = slf.borrow_mut(py);
        if !transport.read_paused || transport.closed || transport.closing {
            return Ok(());
        }
        transport.read_paused = false;
        transport.sync_interest(py)
    }

    fn _on_readable(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut transport = slf.borrow_mut(py);
        transport.on_readable(py)
    }

    fn _on_writable(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut transport = slf.borrow_mut(py);
        transport.on_writable(py)
    }

    pub(crate) fn on_read_error(&mut self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<()> {
        self.finish_close(py, Some(exc))
    }
}

impl StreamTransport {
    fn ensure_can_write(&self) -> PyResult<()> {
        if self.closing || self.closed {
            return Err(PyRuntimeError::new_err("stream transport is closing"));
        }
        if self.eof_requested {
            return Err(PyRuntimeError::new_err("Cannot call write() after write_eof()"));
        }
        Ok(())
    }

    fn write_bytes_inner(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        if self.write_queue.is_empty() && !self.writer_registered {
            match try_send_bytes(self.fd, data) {
                Ok(sent) if sent == data.len() => return Ok(()),
                Ok(sent) => {
                    self.queue_write(&data[sent..]);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    self.queue_write(data);
                }
                Err(err) => {
                    return self.finish_close(
                        py,
                        Some(PyRuntimeError::new_err(err.to_string()).into_value(py).into_any()),
                    );
                }
            }
        } else {
            self.queue_write(data);
        }

        self.sync_interest(py)?;
        self.maybe_pause_protocol(py)?;
        Ok(())
    }

    fn queue_write(&mut self, data: &[u8]) {
        self.pending_write_bytes += data.len();
        self.write_queue.push_back(PendingWrite {
            data: data.to_vec(),
            sent: 0,
        });
    }

    pub(crate) fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closing || self.closed || self.read_paused {
            return Ok(());
        }
        if env::var_os("KIOTO_TRACE_STREAM").is_some() {
            eprintln!("stream-transport readable fd={} closing={} paused={}", self.fd, self.closing, self.read_paused);
        }
        loop {
            match recv_into(self.fd, &mut self.read_buffer) {
                Ok(0) => {
                    if env::var_os("KIOTO_TRACE_STREAM").is_some() {
                        eprintln!("stream-transport eof fd={}", self.fd);
                    }
                    self.remove_reader(py)?;
                    self.feed_eof_inner(py)?;
                    if self.closing && self.pending_write_bytes == 0 {
                        self.finish_close(py, None)?;
                    }
                    return Ok(());
                }
                Ok(n) => {
                    if env::var_os("KIOTO_TRACE_STREAM").is_some() {
                        eprintln!("stream-transport read fd={} bytes={}", self.fd, n);
                    }
                    self.feed_read_buffer_inner(py, n)?;
                    if self.read_paused {
                        return Ok(());
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) => {
                    return self.finish_close(
                        py,
                        Some(PyRuntimeError::new_err(err.to_string()).into_value(py).into_any()),
                    );
                }
            }
        }
    }

    pub(crate) fn on_writable(&mut self, py: Python<'_>) -> PyResult<()> {
        if env::var_os("KIOTO_TRACE_STREAM").is_some() {
            eprintln!("stream-transport writable fd={} queued={}", self.fd, self.pending_write_bytes);
        }
        while let Some(write) = self.write_queue.front_mut() {
            match try_send_bytes(self.fd, &write.data[write.sent..]) {
                Ok(0) => break,
                Ok(sent) => {
                    if env::var_os("KIOTO_TRACE_STREAM").is_some() {
                        eprintln!("stream-transport wrote fd={} bytes={}", self.fd, sent);
                    }
                    write.sent += sent;
                    self.pending_write_bytes = self.pending_write_bytes.saturating_sub(sent);
                    if write.sent == write.data.len() {
                        self.write_queue.pop_front();
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    return self.finish_close(
                        py,
                        Some(PyRuntimeError::new_err(err.to_string()).into_value(py).into_any()),
                    );
                }
            }
        }

        if self.pending_write_bytes == 0 {
            self.remove_writer(py)?;
            self.maybe_resume_protocol(py)?;
            if self.eof_requested && !self.eof_sent {
                self.finish_half_close()?;
            }
            if self.closing {
                self.finish_close(py, None)?;
            }
        }

        Ok(())
    }

    fn finish_half_close(&mut self) -> PyResult<()> {
        if self.eof_sent {
            return Ok(());
        }
        let rc = unsafe { libc::shutdown(self.fd, libc::SHUT_WR) };
        if rc != 0 {
            return Err(PyRuntimeError::new_err("failed to half-close socket"));
        }
        self.eof_sent = true;
        Ok(())
    }

    fn finish_close(&mut self, py: Python<'_>, exc: Option<Py<PyAny>>) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.closing = true;
        self.loop_obj
            .bind(py)
            .getattr("_stream_registry")?
            .call_method1("unregister", (self.fd,))?;
        self.disable_interest(py)?;
        self.pending_write_bytes = 0;
        self.write_queue.clear();
        self.loop_obj
            .bind(py)
            .getattr("_transports")?
            .call_method1("pop", (self.fd, py.None()))?;
        match exc.as_ref() {
            Some(exc) => self.protocol.bind(py).call_method1("connection_lost", (exc.bind(py),))?,
            None => self.protocol.bind(py).call_method1("connection_lost", (py.None(),))?,
        };
        self.connection_lost_sent = true;
        self.sock.bind(py).call_method0("close")?;
        Ok(())
    }

    fn remove_reader(&mut self, py: Python<'_>) -> PyResult<()> {
        self.reader_registered = false;
        self.sync_interest(py)
    }

    fn remove_writer(&mut self, py: Python<'_>) -> PyResult<()> {
        self.writer_registered = false;
        self.sync_interest(py)
    }

    fn sync_interest(&mut self, py: Python<'_>) -> PyResult<()> {
        let readable = !self.read_paused && !self.closed && !self.closing;
        let writable =
            !self.closed && (self.pending_write_bytes != 0 || (self.closing && !self.closed));
        self.reader_registered = readable;
        self.writer_registered = writable;
        self.loop_obj
            .bind(py)
            .getattr("_poller")?
            .call_method1("set_interest", (self.fd, readable, writable))?;
        Ok(())
    }

    fn disable_interest(&mut self, py: Python<'_>) -> PyResult<()> {
        self.reader_registered = false;
        self.writer_registered = false;
        self.loop_obj
            .bind(py)
            .getattr("_poller")?
            .call_method1("set_interest", (self.fd, false, false))?;
        Ok(())
    }

    fn maybe_pause_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.protocol_paused || self.pending_write_bytes <= WRITE_HIGH_WATER {
            return Ok(());
        }
        self.protocol.bind(py).call_method0("pause_writing")?;
        self.protocol_paused = true;
        Ok(())
    }

    fn maybe_resume_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.protocol_paused || self.pending_write_bytes > WRITE_LOW_WATER {
            return Ok(());
        }
        self.protocol.bind(py).call_method0("resume_writing")?;
        self.protocol_paused = false;
        Ok(())
    }

    pub(crate) fn feed_data_inner(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Ok(());
        };
        if data.is_empty() {
            return Ok(());
        }

        let reader = reader_obj.bind(py);
        let buffer_obj = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .clone_ref(py);
        let buffer = buffer_obj.bind(py);
        append_to_bytearray(buffer.as_ptr(), data)?;
        if !self.try_finish_pending_read(py, &reader)? {
            wake_waiter(py, &reader)?;
        }

        let reader_transport = reader.getattr("_transport")?;
        if reader_transport.is_none() || reader.getattr("_paused")?.is_truthy()? {
            return Ok(());
        }

        let buffered = unsafe { ffi::PyByteArray_Size(buffer.as_ptr()) };
        if buffered <= (2 * self.limit) as isize {
            return Ok(());
        }

        match self.pause_reading(py) {
            Ok(_) => {
                reader.setattr("_paused", true)?;
                Ok(())
            }
            Err(err) if err.is_instance_of::<PyNotImplementedError>(py) => {
                reader.setattr("_transport", py.None())?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn feed_read_buffer_inner(&mut self, py: Python<'_>, size: usize) -> PyResult<()> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Ok(());
        };
        if size == 0 {
            return Ok(());
        }

        let reader = reader_obj.bind(py);
        let buffer_obj = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .clone_ref(py);
        let buffer = buffer_obj.bind(py);
        append_to_bytearray(buffer.as_ptr(), &self.read_buffer[..size])?;
        if !self.try_finish_pending_read(py, &reader)? {
            wake_waiter(py, &reader)?;
        }

        let reader_transport = reader.getattr("_transport")?;
        if reader_transport.is_none() || reader.getattr("_paused")?.is_truthy()? {
            return Ok(());
        }

        let buffered = unsafe { ffi::PyByteArray_Size(buffer.as_ptr()) };
        if buffered <= (2 * self.limit) as isize {
            return Ok(());
        }

        match self.pause_reading(py) {
            Ok(_) => {
                reader.setattr("_paused", true)?;
                Ok(())
            }
            Err(err) if err.is_instance_of::<PyNotImplementedError>(py) => {
                reader.setattr("_transport", py.None())?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn feed_eof_inner(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Ok(());
        };
        let reader = reader_obj.bind(py);
        reader.setattr("_eof", true)?;
        self.fail_pending_read_eof(py, &reader)?;
        wake_waiter(py, &reader)
    }

    pub(crate) fn complete_exact_read_inner(
        &mut self,
        py: Python<'_>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        is_err: bool,
    ) -> PyResult<()> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Ok(());
        };
        let reader = reader_obj.bind(py);
        clear_waiter(&reader, py)?;
        if self
            .pending_read
            .as_ref()
            .map(|pending| pending.future.is(&future))
            .unwrap_or(false)
        {
            self.pending_read = None;
        }

        if future_done(py, &future)? {
            return Ok(());
        }
        if is_err {
            future
                .bind(py)
                .call_method1("set_exception", (payload.bind(py),))?;
            return Ok(());
        }
        future.bind(py).call_method1("set_result", (payload.bind(py),))?;
        maybe_resume_transport(&reader)?;
        Ok(())
    }

    fn readexactly_inner(&mut self, py: Python<'_>, size: usize) -> PyResult<Py<PyAny>> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Err(PyRuntimeError::new_err(
                "stream transport has no StreamReader attached",
            ));
        };
        let reader = reader_obj.bind(py);
        let buffer_obj = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .clone_ref(py);
        let buffer = buffer_obj.bind(py);
        if let Some(exc) = current_exception(&reader)? {
            return ready_future_exception(py, &reader, exc);
        }
        if size == 0 {
            let payload = PyBytes::new(py, b"").into_any().unbind();
            return ready_future_result(py, &reader, payload);
        }

        if size > NATIVE_READEXACTLY_LIMIT {
            if let Some(reader_readexactly) = self.reader_readexactly.as_ref() {
                return Ok(reader_readexactly.bind(py).call1((size,))?.unbind());
            }
        }

        if try_consume_exact(buffer.as_ptr(), size)? {
            let data = consume_exact(py, buffer.as_ptr(), size)?;
            maybe_resume_transport(&reader)?;
            return ready_future_result(py, &reader, data);
        }

        if reader.getattr("_eof")?.is_truthy()? {
            let partial = consume_all(py, buffer.as_ptr())?;
            return ready_future_exception(py, &reader, incomplete_read_error(py, partial, size)?);
        }

        if self.pending_read.is_some() {
            return Err(PyRuntimeError::new_err(
                "readexactly() called while another coroutine is already waiting for incoming data",
            ));
        }

        if reader.getattr("_paused")?.is_truthy()? {
            reader.setattr("_paused", false)?;
            reader.getattr("_transport")?.call_method0("resume_reading")?;
        }

        let future = self.loop_obj.bind(py).call_method0("create_future")?.unbind();
        reader.setattr("_waiter", future.bind(py))?;
        self.pending_read = Some(PendingRead {
            size,
            future: future.clone_ref(py),
        });
        Ok(future)
    }

    fn drain_inner(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) {
            let reader = reader_obj.bind(py);
            if let Some(exc) = current_exception(&reader)? {
                return Err(PyErr::from_value(exc.into_bound(py).into_any()));
            }
        }

        if !self.closing && !self.protocol_paused && self.pending_write_bytes == 0 {
            if let Some(ready) = self.ready_drain.as_ref() {
                return Ok(ready.clone_ref(py));
            }
            let future = self.loop_obj.bind(py).call_method0("create_future")?.unbind();
            future.bind(py).call_method1("set_result", (py.None(),))?;
            self.ready_drain = Some(future.clone_ref(py));
            return Ok(future);
        }

        let Some(fallback) = self.drain_fallback.as_ref() else {
            let future = self.loop_obj.bind(py).call_method0("create_future")?.unbind();
            future.bind(py).call_method1("set_result", (py.None(),))?;
            return Ok(future);
        };
        Ok(fallback.bind(py).call0()?.unbind())
    }

    fn try_finish_pending_read(
        &mut self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let Some(pending) = self.pending_read.as_ref() else {
            return Ok(false);
        };
        let buffered = bytearray_len(
            self.buffer
                .as_ref()
                .expect("stream transport buffer")
                .bind(py)
                .as_ptr(),
        )?;
        if buffered < pending.size {
            return Ok(false);
        }

        let pending = self.pending_read.take().expect("pending read present");
        let data = consume_exact(
            py,
            self.buffer
                .as_ref()
                .expect("stream transport buffer")
                .bind(py)
                .as_ptr(),
            pending.size,
        )?;
        clear_waiter(reader, py)?;
        if !future_done(py, &pending.future)? {
            pending
                .future
                .bind(py)
                .call_method1("set_result", (data.bind(py),))?;
        }
        maybe_resume_transport(reader)?;
        Ok(true)
    }

    fn fail_pending_read_eof(
        &mut self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let Some(pending) = self.pending_read.take() else {
            return Ok(());
        };
        clear_waiter(reader, py)?;
        if future_done(py, &pending.future)? {
            return Ok(());
        }
        let partial = consume_all(
            py,
            self.buffer
                .as_ref()
                .expect("stream transport buffer")
                .bind(py)
                .as_ptr(),
        )?;
        let exc = incomplete_read_error(py, partial, pending.size)?;
        pending
            .future
            .bind(py)
            .call_method1("set_exception", (exc.bind(py),))?;
        Ok(())
    }
}

struct SyncPyMethodDef(ffi::PyMethodDef);

unsafe impl Sync for SyncPyMethodDef {}

static READEXACTLY_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"readexactly".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_readexactly_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native exact-read fast path backed by the Kioto stream transport.".as_ptr(),
});

static DRAIN_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"drain".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_drain_c,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"Native drain fast path backed by the Kioto stream transport.".as_ptr(),
});

fn create_bound_readexactly(py: Python<'_>, slf: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            &READEXACTLY_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
            slf.as_ptr(),
            std::ptr::null_mut(),
        );
        Ok(Bound::<PyAny>::from_owned_ptr_or_err(py, func)?.unbind())
    }
}

fn create_bound_drain(py: Python<'_>, slf: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            &DRAIN_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
            slf.as_ptr(),
            std::ptr::null_mut(),
        );
        Ok(Bound::<PyAny>::from_owned_ptr_or_err(py, func)?.unbind())
    }
}

unsafe extern "C" fn stream_transport_readexactly_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let transport = Bound::from_borrowed_ptr(py, slf)
            .cast_into_unchecked::<StreamTransport>()
            .unbind();
        let mut transport_ref = transport.borrow_mut(py);
        let size = Bound::<PyAny>::from_borrowed_ptr(py, arg).extract::<isize>()?;
        if size < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "readexactly size can not be less than zero",
            ));
        }
        transport_ref.readexactly_inner(py, size as usize)
    })();

    match result {
        Ok(obj) => obj.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn stream_transport_drain_c(
    slf: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let transport = Bound::from_borrowed_ptr(py, slf)
            .cast_into_unchecked::<StreamTransport>()
            .unbind();
        let mut transport_ref = transport.borrow_mut(py);
        transport_ref.drain_inner(py)
    })();

    match result {
        Ok(obj) => obj.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

fn recv_into(fd: RawFd, buffer: &mut [u8]) -> std::io::Result<usize> {
    let result = unsafe { libc::recv(fd, buffer.as_mut_ptr().cast(), buffer.len(), recv_flags()) };
    if result >= 0 {
        return Ok(result as usize);
    }
    Err(std::io::Error::last_os_error())
}

fn try_send_bytes(fd: RawFd, bytes: &[u8]) -> std::io::Result<usize> {
    let result = unsafe { libc::send(fd, bytes.as_ptr().cast(), bytes.len(), send_flags()) };
    if result >= 0 {
        return Ok(result as usize);
    }
    Err(std::io::Error::last_os_error())
}

fn recv_flags() -> i32 {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        libc::MSG_DONTWAIT
    }
}

fn send_flags() -> i32 {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        libc::MSG_DONTWAIT
    }
}

fn append_to_bytearray(bytearray: *mut ffi::PyObject, data: &[u8]) -> PyResult<()> {
    unsafe {
        let old_len = ffi::PyByteArray_Size(bytearray);
        if old_len < 0 {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        let new_len = old_len
            .checked_add(data.len() as isize)
            .ok_or_else(|| pyo3::exceptions::PyOverflowError::new_err("bytearray too large"))?;
        if ffi::PyByteArray_Resize(bytearray, new_len) != 0 {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        let dest = ffi::PyByteArray_AsString(bytearray) as *mut u8;
        if dest.is_null() {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        ptr::copy_nonoverlapping(data.as_ptr(), dest.add(old_len as usize), data.len());
    }
    Ok(())
}

fn bytearray_len(bytearray: *mut ffi::PyObject) -> PyResult<usize> {
    let len = unsafe { ffi::PyByteArray_Size(bytearray) };
    if len < 0 {
        return Err(PyErr::fetch(unsafe { Python::assume_attached() }));
    }
    Ok(len as usize)
}

fn try_consume_exact(bytearray: *mut ffi::PyObject, size: usize) -> PyResult<bool> {
    let len = unsafe { ffi::PyByteArray_Size(bytearray) };
    Ok(len >= size as isize)
}

fn consume_exact(py: Python<'_>, bytearray: *mut ffi::PyObject, size: usize) -> PyResult<Py<PyAny>> {
    unsafe {
        let len = ffi::PyByteArray_Size(bytearray);
        if len < size as isize {
            return Err(PyRuntimeError::new_err("insufficient buffered data"));
        }
        let src = ffi::PyByteArray_AsString(bytearray) as *mut u8;
        if src.is_null() {
            return Err(PyErr::fetch(py));
        }
        let data = PyBytes::new(py, std::slice::from_raw_parts(src.cast_const(), size))
            .into_any()
            .unbind();
        if len == size as isize {
            if ffi::PyByteArray_Resize(bytearray, 0) != 0 {
                return Err(PyErr::fetch(py));
            }
        } else {
            ptr::copy(src.add(size), src, (len as usize) - size);
            if ffi::PyByteArray_Resize(bytearray, len - size as isize) != 0 {
                return Err(PyErr::fetch(py));
            }
        }
        Ok(data)
    }
}

fn consume_all(py: Python<'_>, bytearray: *mut ffi::PyObject) -> PyResult<Py<PyAny>> {
    unsafe {
        let len = ffi::PyByteArray_Size(bytearray);
        if len < 0 {
            return Err(PyErr::fetch(py));
        }
        let src = ffi::PyByteArray_AsString(bytearray) as *mut u8;
        if src.is_null() {
            return Err(PyErr::fetch(py));
        }
        let data = PyBytes::new(py, std::slice::from_raw_parts(src.cast_const(), len as usize))
            .into_any()
            .unbind();
        if ffi::PyByteArray_Resize(bytearray, 0) != 0 {
            return Err(PyErr::fetch(py));
        }
        Ok(data)
    }
}

fn ready_future_result(
    py: Python<'_>,
    reader: &Bound<'_, PyAny>,
    payload: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let future = reader.getattr("_loop")?.call_method0("create_future")?;
    future.call_method1("set_result", (payload.bind(py),))?;
    Ok(future.unbind())
}

fn ready_future_exception(
    py: Python<'_>,
    reader: &Bound<'_, PyAny>,
    exc: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let future = reader.getattr("_loop")?.call_method0("create_future")?;
    future.call_method1("set_exception", (exc.bind(py),))?;
    Ok(future.unbind())
}

fn incomplete_read_error(py: Python<'_>, partial: Py<PyAny>, expected: usize) -> PyResult<Py<PyAny>> {
    static INCOMPLETE_READ_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let cls = INCOMPLETE_READ_ERROR.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.exceptions")?
            .getattr("IncompleteReadError")?
            .unbind())
    })?;
    Ok(cls.bind(py).call1((partial.bind(py), expected))?.unbind())
}

fn current_exception(reader: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    let exc = reader.getattr("_exception")?;
    if exc.is_none() {
        Ok(None)
    } else {
        Ok(Some(exc.unbind()))
    }
}

fn wake_waiter(py: Python<'_>, reader: &Bound<'_, PyAny>) -> PyResult<()> {
    let waiter = reader.getattr("_waiter")?;
    if waiter.is_none() {
        return Ok(());
    }
    clear_waiter(reader, py)?;
    if !waiter.call_method0("cancelled")?.is_truthy()? {
        waiter.call_method1("set_result", (py.None(),))?;
    }
    Ok(())
}

fn clear_waiter(reader: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<()> {
    reader.setattr("_waiter", py.None())
}

fn maybe_resume_transport(reader: &Bound<'_, PyAny>) -> PyResult<()> {
    if reader.getattr("_paused")?.is_truthy()? {
        reader.call_method0("_maybe_resume_transport")?;
    }
    Ok(())
}

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    if future.bind(py).call_method0("cancelled")?.is_truthy()? {
        return Ok(true);
    }
    future.bind(py).call_method0("done")?.is_truthy()
}

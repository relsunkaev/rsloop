#![cfg(unix)]

use std::io;
use std::os::fd::RawFd;
use std::{cmp, collections::VecDeque};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict};
use pyo3::IntoPyObjectExt;

use crate::pipe_registry::PipeTransportRegistry;
use crate::poller::TokioPoller;

const READ_MAX_SIZE: usize = 256 * 1024;
const READ_OPPORTUNISTIC_THRESHOLD: usize = 64 * 1024;
const WRITEV_MAX: usize = 64;
const SMALL_WRITE_MERGE_LIMIT: usize = 64 * 1024;
const SMALL_WRITE_CHUNK_LIMIT: usize = 1024;
const WRITE_HIGH_WATER: usize = 64 * 1024;
const WRITE_LOW_WATER: usize = 16 * 1024;

pub(crate) enum PipeCompletion {
    DataReceived {
        transport: Py<ReadPipeTransport>,
        data: Py<PyAny>,
    },
    ConnectionLost {
        transport: Py<ReadPipeTransport>,
        exception: Option<Py<PyAny>>,
        deliver_eof: bool,
    },
}

struct PendingWrite {
    data: Vec<u8>,
    sent: usize,
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct ReadPipeTransport {
    pipe: Option<Py<PyAny>>,
    protocol: Option<Py<PyAny>>,
    extra: Py<PyDict>,
    fileno: RawFd,
    poller: Py<TokioPoller>,
    registry: Py<PipeTransportRegistry>,
    self_ref: Option<Py<ReadPipeTransport>>,
    closing: bool,
    paused: bool,
    registered: bool,
    read_buf: Vec<u8>,
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct WritePipeTransport {
    pipe: Option<Py<PyAny>>,
    protocol: Option<Py<PyAny>>,
    extra: Py<PyDict>,
    fileno: RawFd,
    poller: Py<TokioPoller>,
    registry: Py<PipeTransportRegistry>,
    self_ref: Option<Py<WritePipeTransport>>,
    is_socket_or_fifo: bool,
    closing: bool,
    reader_registered: bool,
    writer_registered: bool,
    connection_lost_sent: bool,
    protocol_paused: bool,
    conn_lost: usize,
    buffer_size: usize,
    writes: VecDeque<PendingWrite>,
}

#[pymethods]
impl ReadPipeTransport {
    #[new]
    #[pyo3(signature = (loop_obj, pipe, protocol, extra=None))]
    fn new(
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        pipe: Py<PyAny>,
        protocol: Py<PyAny>,
        extra: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let fileno: RawFd = pipe.bind(py).call_method0("fileno")?.extract()?;
        let _ = ensure_pipe_like_fd(fileno)?;
        set_nonblocking(fileno)?;

        let extra_dict = if let Some(extra) = extra {
            extra.bind(py).cast::<PyDict>()?.copy()?
        } else {
            PyDict::new(py)
        };
        extra_dict.set_item("pipe", pipe.clone_ref(py))?;
        let poller = loop_obj
            .bind(py)
            .getattr("_poller")?
            .extract::<Py<TokioPoller>>()?;
        let registry = loop_obj
            .bind(py)
            .getattr("_pipe_registry")?
            .extract::<Py<PipeTransportRegistry>>()?;

        Ok(Self {
            pipe: Some(pipe),
            protocol: Some(protocol),
            extra: extra_dict.unbind(),
            fileno,
            poller,
            registry,
            self_ref: None,
            closing: false,
            paused: false,
            registered: false,
            read_buf: vec![0_u8; READ_MAX_SIZE],
        })
    }

    #[pyo3(signature = (waiter=None))]
    fn activate(slf: Bound<'_, Self>, waiter: Option<Py<PyAny>>) -> PyResult<()> {
        let py = slf.py();
        let protocol = {
            let this = slf.borrow();
            this.protocol.as_ref().map(|value| value.clone_ref(py))
        };
        if let Some(protocol) = protocol {
            protocol
                .bind(py)
                .call_method1("connection_made", (slf.clone().into_any(),))?;
        }
        {
            let mut this = slf.borrow_mut();
            this.self_ref = Some(slf.clone().unbind());
            this.registry
                .bind(py)
                .borrow()
                .register_read_inner(this.fileno, slf.clone().unbind());
            this.sync_interest(py)?;
        }
        if let Some(waiter) = waiter {
            py.import("asyncio.futures")?
                .call_method1("_set_result_unless_cancelled", (waiter, py.None()))?;
        }
        Ok(())
    }

    fn _add_reader(slf: Bound<'_, Self>) -> PyResult<()> {
        slf.borrow_mut().sync_interest(slf.py())
    }

    fn is_reading(&self) -> bool {
        !self.paused && !self.closing
    }

    fn pause_reading(slf: Bound<'_, Self>) -> PyResult<()> {
        let mut this = slf.borrow_mut();
        if this.closing || this.paused {
            return Ok(());
        }
        this.paused = true;
        this.sync_interest(slf.py())
    }

    fn resume_reading(slf: Bound<'_, Self>) -> PyResult<()> {
        let mut this = slf.borrow_mut();
        if this.closing || !this.paused {
            return Ok(());
        }
        this.paused = false;
        this.sync_interest(slf.py())
    }

    fn set_protocol(&mut self, protocol: Py<PyAny>) {
        self.protocol = Some(protocol);
    }

    fn get_protocol(&self, py: Python<'_>) -> Py<PyAny> {
        self.protocol
            .as_ref()
            .map(|protocol| protocol.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn get_extra_info(
        &self,
        py: Python<'_>,
        name: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let value = self.extra.bind(py).get_item(name)?;
        if let Some(value) = value {
            Ok(value.unbind())
        } else {
            Ok(default.unwrap_or_else(|| py.None()))
        }
    }

    fn is_closing(&self) -> bool {
        self.closing
    }

    fn close(slf: Bound<'_, Self>) -> PyResult<()> {
        if slf.borrow().closing {
            return Ok(());
        }
        slf.borrow_mut().close_now(slf.py(), None, false)
    }
}

impl ReadPipeTransport {
    pub(crate) fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closing || self.paused || self.pipe.is_none() {
            return Ok(());
        }

        match read_once(self.fileno, &mut self.read_buf) {
            Ok(Some(read)) => {
                self.queue_data(py, &self.read_buf[..read])?;
                if read >= READ_OPPORTUNISTIC_THRESHOLD {
                    match read_once(self.fileno, &mut self.read_buf) {
                        Ok(Some(extra)) => {
                            self.queue_data(py, &self.read_buf[..extra])?;
                        }
                        Ok(None) => {
                            self.begin_close(py, None, true)?;
                        }
                        Err(err)
                            if err.kind() == io::ErrorKind::WouldBlock
                                || err.kind() == io::ErrorKind::Interrupted => {}
                        Err(err) => {
                            let exc = PyRuntimeError::new_err(err.to_string()).into_py_any(py)?;
                            self.begin_close(py, Some(exc), false)?;
                        }
                    }
                }
            }
            Ok(None) => {
                self.begin_close(py, None, true)?;
            }
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => {
                let exc = PyRuntimeError::new_err(err.to_string()).into_py_any(py)?;
                self.begin_close(py, Some(exc), false)?;
            }
        }

        Ok(())
    }

    pub(crate) fn deliver_data(&mut self, py: Python<'_>, data: Py<PyAny>) -> PyResult<()> {
        let Some(protocol) = self.protocol.as_ref() else {
            return Ok(());
        };
        if self.closing {
            return Ok(());
        }
        protocol.bind(py).call_method1("data_received", (data,))?;
        Ok(())
    }

    pub(crate) fn finish_close(
        &mut self,
        py: Python<'_>,
        exception: Option<Py<PyAny>>,
        deliver_eof: bool,
    ) -> PyResult<()> {
        let protocol = self.protocol.take();
        let pipe = self.pipe.take();
        self.self_ref = None;
        if let Some(protocol) = protocol {
            if deliver_eof && exception.is_none() {
                let _ = protocol.bind(py).call_method0("eof_received");
            }
            let arg = exception.unwrap_or_else(|| py.None());
            let _ = protocol.bind(py).call_method1("connection_lost", (arg,));
        }
        if let Some(pipe) = pipe {
            let _ = pipe.bind(py).call_method0("close");
        }
        Ok(())
    }

    fn queue_data(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        self.registry
            .bind(py)
            .borrow()
            .queue_completion(PipeCompletion::DataReceived {
                transport: self.self_ref(py)?,
                data: PyBytes::new(py, data).unbind().into_any(),
            });
        Ok(())
    }

    fn begin_close(
        &mut self,
        py: Python<'_>,
        exception: Option<Py<PyAny>>,
        deliver_eof: bool,
    ) -> PyResult<()> {
        if self.closing {
            return Ok(());
        }
        self.closing = true;
        self.sync_interest(py)?;
        self.registry
            .bind(py)
            .borrow()
            .unregister_read_inner(self.fileno);
        self.registry
            .bind(py)
            .borrow()
            .queue_completion(PipeCompletion::ConnectionLost {
                transport: self.self_ref(py)?,
                exception,
                deliver_eof,
            });
        Ok(())
    }

    fn close_now(
        &mut self,
        py: Python<'_>,
        exception: Option<Py<PyAny>>,
        deliver_eof: bool,
    ) -> PyResult<()> {
        if self.closing {
            return Ok(());
        }
        self.closing = true;
        self.sync_interest(py)?;
        self.registry
            .bind(py)
            .borrow()
            .unregister_read_inner(self.fileno);
        self.finish_close(py, exception, deliver_eof)
    }

    fn sync_interest(&mut self, py: Python<'_>) -> PyResult<()> {
        let readable = !self.paused && !self.closing && self.pipe.is_some();
        if self.registered == readable {
            return Ok(());
        }
        self.registered = readable;
        self.poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fileno, readable, false)?;
        Ok(())
    }

    fn self_ref(&self, py: Python<'_>) -> PyResult<Py<ReadPipeTransport>> {
        self.self_ref
            .as_ref()
            .map(|value| value.clone_ref(py))
            .ok_or_else(|| PyRuntimeError::new_err("missing pipe transport self reference"))
    }
}

#[pymethods]
impl WritePipeTransport {
    #[new]
    #[pyo3(signature = (loop_obj, pipe, protocol, extra=None))]
    fn new(
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        pipe: Py<PyAny>,
        protocol: Py<PyAny>,
        extra: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let fileno: RawFd = pipe.bind(py).call_method0("fileno")?.extract()?;
        let mode = ensure_pipe_like_fd(fileno)?;
        set_nonblocking(fileno)?;
        let extra_dict = if let Some(extra) = extra {
            extra.bind(py).cast::<PyDict>()?.copy()?
        } else {
            PyDict::new(py)
        };
        extra_dict.set_item("pipe", pipe.clone_ref(py))?;
        let poller = loop_obj
            .bind(py)
            .getattr("_poller")?
            .extract::<Py<TokioPoller>>()?;
        let registry = loop_obj
            .bind(py)
            .getattr("_pipe_registry")?
            .extract::<Py<PipeTransportRegistry>>()?;

        Ok(Self {
            pipe: Some(pipe),
            protocol: Some(protocol),
            extra: extra_dict.unbind(),
            fileno,
            poller,
            registry,
            self_ref: None,
            is_socket_or_fifo: mode == libc::S_IFSOCK as u32 || mode == libc::S_IFIFO as u32,
            closing: false,
            reader_registered: false,
            writer_registered: false,
            connection_lost_sent: false,
            protocol_paused: false,
            conn_lost: 0,
            buffer_size: 0,
            writes: VecDeque::new(),
        })
    }

    #[pyo3(signature = (waiter=None))]
    fn activate(slf: Bound<'_, Self>, waiter: Option<Py<PyAny>>) -> PyResult<()> {
        let py = slf.py();
        let protocol = {
            let this = slf.borrow();
            this.protocol.as_ref().map(|value| value.clone_ref(py))
        };
        if let Some(protocol) = protocol {
            protocol
                .bind(py)
                .call_method1("connection_made", (slf.clone().into_any(),))?;
        }
        {
            let mut this = slf.borrow_mut();
            this.self_ref = Some(slf.clone().unbind());
            this.registry
                .bind(py)
                .borrow()
                .register_write_inner(this.fileno, slf.clone().unbind());
            this.sync_interest(py)?;
        }
        if let Some(waiter) = waiter {
            py.import("asyncio.futures")?
                .call_method1("_set_result_unless_cancelled", (waiter, py.None()))?;
        }
        Ok(())
    }

    fn write(slf: Bound<'_, Self>, data: Bound<'_, PyAny>) -> PyResult<()> {
        slf.borrow_mut().write_inner(slf.py(), data)
    }

    fn writelines(slf: Bound<'_, Self>, seq: Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        for item in seq.try_iter()? {
            slf.borrow_mut().write_inner(py, item?)?;
        }
        Ok(())
    }

    fn get_write_buffer_size(&self) -> usize {
        self.buffer_size
    }

    fn can_write_eof(&self) -> bool {
        true
    }

    fn write_eof(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let mut this = slf.borrow_mut();
        if this.closing {
            return Ok(());
        }
        this.closing = true;
        this.sync_interest(py)?;
        if this.writes.is_empty() {
            this.finish_close(py, None)?;
        }
        Ok(())
    }

    fn set_protocol(&mut self, protocol: Py<PyAny>) {
        self.protocol = Some(protocol);
    }

    fn get_protocol(&self, py: Python<'_>) -> Py<PyAny> {
        self.protocol
            .as_ref()
            .map(|protocol| protocol.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn get_extra_info(
        &self,
        py: Python<'_>,
        name: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let value = self.extra.bind(py).get_item(name)?;
        if let Some(value) = value {
            Ok(value.unbind())
        } else {
            Ok(default.unwrap_or_else(|| py.None()))
        }
    }

    fn is_closing(&self) -> bool {
        self.closing
    }

    fn close(slf: Bound<'_, Self>) -> PyResult<()> {
        slf.call_method0("write_eof")?;
        Ok(())
    }

    fn abort(slf: Bound<'_, Self>) -> PyResult<()> {
        slf.borrow_mut().force_close(slf.py(), None)
    }
}

impl WritePipeTransport {
    pub(crate) fn on_events(&mut self, py: Python<'_>, mask: u8) -> PyResult<bool> {
        let mut handled = false;
        if mask & 0x01 != 0 && self.reader_registered {
            handled = true;
            self.on_peer_closed(py)?;
        }
        if mask & 0x02 != 0 && self.writer_registered {
            handled = true;
            self.flush_writes(py)?;
        }
        Ok(handled)
    }

    fn write_inner(&mut self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        if self.conn_lost != 0 || self.connection_lost_sent {
            self.conn_lost += 1;
            return Ok(());
        }
        let chunk = bytes_like_to_vec(py, data)?;
        if chunk.is_empty() {
            return Ok(());
        }
        if let Some(last) = self.writes.back_mut() {
            if chunk.len() <= SMALL_WRITE_CHUNK_LIMIT
                && last.data.len() + chunk.len() <= SMALL_WRITE_MERGE_LIMIT
            {
                last.data.extend_from_slice(&chunk);
                self.buffer_size += chunk.len();
                self.maybe_pause_protocol(py)?;
                self.sync_interest(py)?;
                return Ok(());
            }
        }
        self.buffer_size += chunk.len();
        self.writes.push_back(PendingWrite {
            data: chunk,
            sent: 0,
        });
        self.maybe_pause_protocol(py)?;
        self.sync_interest(py)
    }

    fn on_peer_closed(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.writes.is_empty() {
            self.finish_close(py, None)
        } else {
            self.force_close(
                py,
                Some(PyRuntimeError::new_err("pipe closed").into_py_any(py)?),
            )
        }
    }

    fn flush_writes(&mut self, py: Python<'_>) -> PyResult<()> {
        while let Some(front) = self.writes.front() {
            let remaining = front.data.len().saturating_sub(front.sent);
            if remaining == 0 {
                self.writes.pop_front();
                continue;
            }
            match self.writev_once() {
                Ok(0) => break,
                Ok(written) => {
                    self.consume_written(written);
                    self.maybe_resume_protocol(py)?;
                }
                Err(err)
                    if err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::Interrupted =>
                {
                    break;
                }
                Err(err) => {
                    self.conn_lost += 1;
                    return self.force_close(
                        py,
                        Some(PyRuntimeError::new_err(err.to_string()).into_py_any(py)?),
                    );
                }
            }
        }
        self.sync_interest(py)?;
        if self.closing && self.writes.is_empty() {
            self.finish_close(py, None)?;
        }
        Ok(())
    }

    fn writev_once(&self) -> io::Result<usize> {
        if self.writes.is_empty() {
            return Ok(0);
        }
        let mut slices = Vec::with_capacity(cmp::min(self.writes.len(), WRITEV_MAX));
        for pending in self.writes.iter().take(WRITEV_MAX) {
            let slice = &pending.data[pending.sent..];
            if !slice.is_empty() {
                slices.push(std::io::IoSlice::new(slice));
            }
        }
        if slices.is_empty() {
            return Ok(0);
        }
        if slices.len() == 1 {
            return write_all_once(self.fileno, slices[0].as_ref());
        }
        let written =
            unsafe { libc::writev(self.fileno, slices.as_ptr().cast(), slices.len() as i32) };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(written as usize)
    }

    fn consume_written(&mut self, mut written: usize) {
        self.buffer_size = self.buffer_size.saturating_sub(written);
        while written != 0 {
            let Some(front) = self.writes.front_mut() else {
                break;
            };
            let remaining = front.data.len().saturating_sub(front.sent);
            if written >= remaining {
                written -= remaining;
                self.writes.pop_front();
            } else {
                front.sent += written;
                break;
            }
        }
    }

    fn maybe_pause_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.protocol_paused || self.buffer_size <= WRITE_HIGH_WATER {
            return Ok(());
        }
        self.protocol_paused = true;
        if let Some(protocol) = self.protocol.as_ref() {
            let _ = protocol.bind(py).call_method0("pause_writing");
        }
        Ok(())
    }

    fn maybe_resume_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.protocol_paused || self.buffer_size > WRITE_LOW_WATER {
            return Ok(());
        }
        self.protocol_paused = false;
        if let Some(protocol) = self.protocol.as_ref() {
            let _ = protocol.bind(py).call_method0("resume_writing");
        }
        Ok(())
    }

    fn sync_interest(&mut self, py: Python<'_>) -> PyResult<()> {
        let readable = self.is_socket_or_fifo && !self.connection_lost_sent;
        let writable = !self.writes.is_empty();
        if readable == self.reader_registered && writable == self.writer_registered {
            return Ok(());
        }
        self.reader_registered = readable;
        self.writer_registered = writable;
        self.poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fileno, readable, writable)?;
        Ok(())
    }

    fn finish_close(&mut self, py: Python<'_>, exc: Option<Py<PyAny>>) -> PyResult<()> {
        if self.connection_lost_sent {
            return Ok(());
        }
        self.connection_lost_sent = true;
        self.reader_registered = false;
        self.writer_registered = false;
        self.registry
            .bind(py)
            .borrow()
            .unregister_write_inner(self.fileno);
        let _ = self
            .poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fileno, false, false);
        self.self_ref = None;
        let protocol = self.protocol.take();
        let pipe = self.pipe.take();
        if let Some(protocol) = protocol {
            let arg = exc.unwrap_or_else(|| py.None());
            let _ = protocol.bind(py).call_method1("connection_lost", (arg,));
        }
        if let Some(pipe) = pipe {
            let _ = pipe.bind(py).call_method0("close");
        }
        Ok(())
    }

    fn force_close(&mut self, py: Python<'_>, exc: Option<Py<PyAny>>) -> PyResult<()> {
        self.closing = true;
        self.writes.clear();
        self.buffer_size = 0;
        self.finish_close(py, exc)
    }
}

fn ensure_pipe_like_fd(fd: RawFd) -> PyResult<u32> {
    let mut stat_buf = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat_buf.as_mut_ptr()) };
    if rc != 0 {
        return Err(PyRuntimeError::new_err(
            io::Error::last_os_error().to_string(),
        ));
    }
    let mode = unsafe { stat_buf.assume_init().st_mode };
    let kind = mode & libc::S_IFMT;
    let ok = kind == libc::S_IFIFO || kind == libc::S_IFSOCK || kind == libc::S_IFCHR;
    if ok {
        Ok(kind.into())
    } else {
        Err(PyValueError::new_err(
            "Pipe transport is for pipes/sockets only.",
        ))
    }
}

fn set_nonblocking(fd: RawFd) -> PyResult<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(PyRuntimeError::new_err(
            io::Error::last_os_error().to_string(),
        ));
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc < 0 {
        return Err(PyRuntimeError::new_err(
            io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

fn read_once(fd: RawFd, buf: &mut [u8]) -> io::Result<Option<usize>> {
    let read = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if read == 0 {
        return Ok(None);
    }
    if read < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(Some(read as usize))
}

fn write_all_once(fd: RawFd, buf: &[u8]) -> io::Result<usize> {
    let written = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if written < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(written as usize)
}

fn bytes_like_to_vec(py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = data.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if let Ok(bytearray) = data.cast::<PyByteArray>() {
        // SAFETY: access is on the loop thread while Python owns the object.
        return Ok(unsafe { bytearray.as_bytes() }.to_vec());
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<u8>::get(&data) {
        if buffer.as_slice(py).is_some() {
            let bytes_type = py.import("builtins")?.getattr("bytes")?;
            let bytes_obj = bytes_type.call1((data,))?;
            return Ok(bytes_obj.cast::<PyBytes>()?.as_bytes().to_vec());
        }
    }
    Err(PyValueError::new_err(format!(
        "data argument must be bytes-like, got {:?}",
        data.get_type().name()?
    )))
}

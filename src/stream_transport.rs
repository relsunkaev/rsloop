#![cfg(unix)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::env;
use std::fmt::Write as _;
use std::os::fd::RawFd;
use std::ptr;
use std::rc::Rc;
use std::sync::{OnceLock, atomic::{AtomicU64, Ordering}};
use std::time::Instant;

use parking_lot::Mutex;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyStopIteration};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict};

use crate::handles::OneArgHandle;
use crate::poller::TokioPoller;
use crate::scheduler::Scheduler;
use crate::stream_registry::StreamTransportRegistry;

const DEFAULT_STREAM_READ_SIZE: usize = 64 * 1024;
const NATIVE_READEXACTLY_LIMIT: usize = 16 * 1024;
const DIRECT_WRITE_LIMIT: usize = 0;
const WRITEV_BATCH_LIMIT: usize = 8;
const SMALL_WRITE_CHUNK_LIMIT: usize = 256;
const SMALL_WRITE_BUFFER_LIMIT: usize = 4 * 1024;
const WRITE_HIGH_WATER: usize = 64 * 1024;
const WRITE_LOW_WATER: usize = 16 * 1024;
static TRACE_STREAM: OnceLock<bool> = OnceLock::new();
static STREAM_TOKEN: AtomicU64 = AtomicU64::new(1);
static STREAM_PROFILE: OnceLock<Option<Mutex<StreamProfileState>>> = OnceLock::new();

struct StreamProfileEvent {
    at_us: u64,
    kind: &'static str,
    fd: RawFd,
    size: usize,
    aux: usize,
}

struct StreamProfileState {
    started_at: Instant,
    limit: usize,
    dropped: u64,
    events: Vec<StreamProfileEvent>,
}

struct PendingRead {
    size: usize,
    waiter: Py<StreamWaiter>,
}

pub(crate) enum StreamCompletion {
    ReadResult {
        reader: Py<PyAny>,
        waiter: Py<StreamWaiter>,
        payload: Py<PyAny>,
        resume_transport: bool,
    },
    ReadException {
        reader: Py<PyAny>,
        waiter: Py<StreamWaiter>,
        exception: Py<PyAny>,
    },
    FutureResult {
        future: Py<PyAny>,
        value: Py<PyAny>,
    },
    FutureException {
        future: Py<PyAny>,
        exception: Py<PyAny>,
    },
}

enum PendingWriteData {
    PyBytes(Py<PyBytes>),
    Owned(Vec<u8>),
}

struct PendingWrite {
    data: PendingWriteData,
    sent: usize,
}

impl PendingWrite {
    fn len(&self, py: Python<'_>) -> usize {
        match &self.data {
            PendingWriteData::PyBytes(data) => data.bind(py).as_bytes().len(),
            PendingWriteData::Owned(data) => data.len(),
        }
    }
}

pub(crate) type StreamCoreRef = Rc<RefCell<StreamCore>>;

pub(crate) struct StreamCore {
    pub(crate) stream_token: u64,
    sock: Py<PyAny>,
    fd: RawFd,
    protocol: Py<PyAny>,
    loop_obj: Py<PyAny>,
    poller: Py<TokioPoller>,
    registry: Py<StreamTransportRegistry>,
    transports: Py<PyDict>,
    extra: Py<PyAny>,
    reader: Option<Py<PyAny>>,
    reader_readexactly: Option<Py<PyAny>>,
    close_waiter: Option<Py<PyAny>>,
    drain_fallback: Option<Py<PyAny>>,
    ready_drain: Option<Py<PyAny>>,
    buffer: Option<Py<PyByteArray>>,
    limit: usize,
    pending_read: Option<PendingRead>,
    pending_drains: Vec<Py<PyAny>>,
    pending_close_waiters: Vec<Py<PyAny>>,
    read_buffer: Vec<u8>,
    write_queue: VecDeque<PendingWrite>,
    pub(crate) pending_write_bytes: usize,
    closing: bool,
    pub(crate) closed: bool,
    read_paused: bool,
    reader_registered: bool,
    writer_registered: bool,
    write_waiting_for_writable: bool,
    eof_requested: bool,
    eof_sent: bool,
    protocol_paused: bool,
    connection_lost_sent: bool,
    pub(crate) write_queued: bool,
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct StreamTransport {
    pub(crate) core: StreamCoreRef,
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
struct ReadyAwaitable {
    result: Option<Py<PyAny>>,
    exception: Option<Py<PyAny>>,
    done: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamWaiterState {
    Pending,
    Cancelled,
    Finished,
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub(crate) struct StreamWaiter {
    scheduler: Py<Scheduler>,
    loop_obj: Py<PyAny>,
    callbacks: Vec<(Py<PyAny>, Py<PyAny>)>,
    result: Option<Py<PyAny>>,
    exception: Option<Py<PyAny>>,
    cancel_message: Option<Py<PyAny>>,
    state: StreamWaiterState,
    yielded: bool,
    #[pyo3(get, set)]
    _asyncio_future_blocking: bool,
}

#[pymethods]
impl ReadyAwaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if slf.done {
            return Ok(None);
        }
        slf.done = true;
        if let Some(exception) = slf.exception.take() {
            return Err(PyErr::from_value(exception.into_bound(py).into_any()));
        }
        let result = slf.result.take().unwrap_or_else(|| py.None());
        Err(PyStopIteration::new_err((result,)))
    }
}

#[pymethods]
impl StreamWaiter {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(slf: Py<Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let mut waiter = slf.borrow_mut(py);
        if waiter.state == StreamWaiterState::Pending {
            if waiter.yielded {
                return Err(PyRuntimeError::new_err("await wasn't used with future"));
            }
            waiter.yielded = true;
            waiter._asyncio_future_blocking = true;
            return Ok(Some(slf.clone_ref(py).into_any()));
        }
        waiter.yielded = true;
        waiter._asyncio_future_blocking = false;
        if waiter.state == StreamWaiterState::Cancelled {
            return Err(cancelled_error(py, waiter.cancel_message.as_ref())?);
        }
        if let Some(exception) = waiter.exception.as_ref() {
            return Err(PyErr::from_value(exception.clone_ref(py).into_bound(py).into_any()));
        }
        let result = waiter.result.as_ref().map(|value| value.clone_ref(py)).unwrap_or_else(|| py.None());
        Err(PyStopIteration::new_err((result,)))
    }

    #[pyo3(signature = (callback, *, context=None))]
    fn add_done_callback(
        slf: Py<Self>,
        py: Python<'_>,
        callback: Py<PyAny>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let context = match context {
            Some(context) => context,
            None => copy_context_obj(py)?,
        };
        let pending = {
            let mut waiter = slf.borrow_mut(py);
            if waiter.state == StreamWaiterState::Pending {
                waiter.callbacks.push((callback.clone_ref(py), context.clone_ref(py)));
                true
            } else {
                false
            }
        };
        if !pending {
            StreamWaiter::schedule_one(py, &slf, callback, context)?;
        }
        Ok(())
    }

    fn remove_done_callback(&mut self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<usize> {
        let original = self.callbacks.len();
        self.callbacks.retain(|(registered, _)| {
            registered
                .bind(py)
                .eq(callback.bind(py))
                .map(|equal| !equal)
                .unwrap_or(true)
        });
        Ok(original - self.callbacks.len())
    }

    #[pyo3(signature = (msg=None))]
    fn cancel(slf: Py<Self>, py: Python<'_>, msg: Option<Py<PyAny>>) -> PyResult<bool> {
        let callbacks = {
            let mut waiter = slf.borrow_mut(py);
            if waiter.state != StreamWaiterState::Pending {
                return Ok(false);
            }
            waiter.state = StreamWaiterState::Cancelled;
            waiter.cancel_message = msg;
            waiter.result = None;
            waiter.exception = None;
            waiter._asyncio_future_blocking = false;
            std::mem::take(&mut waiter.callbacks)
        };
        StreamWaiter::schedule_callbacks(py, &slf, callbacks)?;
        Ok(true)
    }

    fn cancelled(&self) -> bool {
        self.state == StreamWaiterState::Cancelled
    }

    fn done(&self) -> bool {
        self.state != StreamWaiterState::Pending
    }

    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.state {
            StreamWaiterState::Pending => Err(invalid_state_error(py, "Result is not ready.")?),
            StreamWaiterState::Cancelled => Err(cancelled_error(py, self.cancel_message.as_ref())?),
            StreamWaiterState::Finished => {
                if let Some(exception) = self.exception.as_ref() {
                    Err(PyErr::from_value(exception.clone_ref(py).into_bound(py).into_any()))
                } else {
                    Ok(self.result.as_ref().map(|value| value.clone_ref(py)).unwrap_or_else(|| py.None()))
                }
            }
        }
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.state {
            StreamWaiterState::Pending => Err(invalid_state_error(py, "Exception is not set.")?),
            StreamWaiterState::Cancelled => Err(cancelled_error(py, self.cancel_message.as_ref())?),
            StreamWaiterState::Finished => Ok(self.exception.as_ref().map(|value| value.clone_ref(py))),
        }
    }

    fn set_result(slf: Py<Self>, py: Python<'_>, result: Py<PyAny>) -> PyResult<()> {
        StreamWaiter::finish_result(&slf, py, result)
    }

    fn set_exception(slf: Py<Self>, py: Python<'_>, exception: Py<PyAny>) -> PyResult<()> {
        StreamWaiter::finish_exception(&slf, py, exception)
    }

    fn get_loop(&self, py: Python<'_>) -> Py<PyAny> {
        self.loop_obj.clone_ref(py)
    }
}

impl StreamWaiter {
    fn new(py: Python<'_>, scheduler: Py<Scheduler>, loop_obj: Py<PyAny>) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                scheduler,
                loop_obj,
                callbacks: Vec::new(),
                result: None,
                exception: None,
                cancel_message: None,
                state: StreamWaiterState::Pending,
                yielded: false,
                _asyncio_future_blocking: false,
            },
        )
    }

    pub(crate) fn finish_result(slf: &Py<Self>, py: Python<'_>, result: Py<PyAny>) -> PyResult<()> {
        let callbacks = {
            let mut waiter = slf.borrow_mut(py);
            if waiter.state != StreamWaiterState::Pending {
                return Err(invalid_state_error(py, "Result is already set.")?);
            }
            waiter.result = Some(result);
            waiter.exception = None;
            waiter.state = StreamWaiterState::Finished;
            waiter._asyncio_future_blocking = false;
            std::mem::take(&mut waiter.callbacks)
        };
        Self::schedule_callbacks(py, slf, callbacks)
    }

    pub(crate) fn finish_exception(
        slf: &Py<Self>,
        py: Python<'_>,
        exception: Py<PyAny>,
    ) -> PyResult<()> {
        let exception = normalize_exception(py, exception)?;
        let callbacks = {
            let mut waiter = slf.borrow_mut(py);
            if waiter.state != StreamWaiterState::Pending {
                return Err(invalid_state_error(py, "Exception is already set.")?);
            }
            waiter.result = None;
            waiter.exception = Some(exception);
            waiter.state = StreamWaiterState::Finished;
            waiter._asyncio_future_blocking = false;
            std::mem::take(&mut waiter.callbacks)
        };
        Self::schedule_callbacks(py, slf, callbacks)
    }

    fn schedule_callbacks(
        py: Python<'_>,
        slf: &Py<Self>,
        callbacks: Vec<(Py<PyAny>, Py<PyAny>)>,
    ) -> PyResult<()> {
        for (callback, context) in callbacks {
            Self::schedule_one(py, slf, callback, context)?;
        }
        Ok(())
    }

    fn schedule_one(
        py: Python<'_>,
        slf: &Py<Self>,
        callback: Py<PyAny>,
        context: Py<PyAny>,
    ) -> PyResult<()> {
        let waiter = slf.borrow(py);
        let handle = OneArgHandle::create(
            py,
            callback,
            slf.clone_ref(py).into_any(),
            waiter.loop_obj.clone_ref(py),
            Some(context),
            None,
        )?;
        waiter.scheduler.bind(py).borrow().push_ready_inner(handle);
        Ok(())
    }
}

#[pymethods]
impl StreamTransport {
    #[new]
    #[pyo3(signature = (sock, protocol, loop_obj, poller, registry, transports, extra=None, reader=None, read_size=DEFAULT_STREAM_READ_SIZE))]
    fn new(
        py: Python<'_>,
        sock: Py<PyAny>,
        protocol: Py<PyAny>,
        loop_obj: Py<PyAny>,
        poller: Py<TokioPoller>,
        registry: Py<StreamTransportRegistry>,
        transports: Py<PyDict>,
        extra: Option<Py<PyAny>>,
        reader: Option<Py<PyAny>>,
        read_size: usize,
    ) -> PyResult<Self> {
        let extra = match extra {
            Some(extra) => extra,
            None => PyDict::new(py).into_any().unbind(),
        };
        let fd = sock.bind(py).call_method0("fileno")?.extract::<i32>()? as RawFd;
        let (buffer, reader_readexactly, close_waiter, limit) =
            if let Some(reader_ref) = reader.as_ref() {
                let reader_ref = reader_ref.bind(py);
                let buffer = reader_ref
                    .getattr("_buffer")?
                    .cast_into::<PyByteArray>()?
                    .unbind();
                let reader_readexactly = reader_ref.getattr("readexactly")?.unbind();
                let close_waiter = protocol.bind(py).getattr("_closed").ok().map(Bound::unbind);
                let limit = reader_ref.getattr("_limit")?.extract()?;
                (Some(buffer), Some(reader_readexactly), close_waiter, limit)
            } else {
                (None, None, None, 64 * 1024)
            };
        let core = Rc::new(RefCell::new(StreamCore {
                stream_token: STREAM_TOKEN.fetch_add(1, Ordering::Relaxed),
                sock,
                fd,
                protocol,
                loop_obj,
                poller,
                registry,
                transports,
                extra,
                reader,
                reader_readexactly,
                close_waiter,
                drain_fallback: None,
                ready_drain: None,
                buffer,
                limit,
                pending_read: None,
                pending_drains: Vec::new(),
                pending_close_waiters: Vec::new(),
                read_buffer: vec![0_u8; read_size.max(1)],
                write_queue: VecDeque::new(),
                pending_write_bytes: 0,
                closing: false,
                closed: false,
                read_paused: false,
                reader_registered: false,
                writer_registered: false,
                write_waiting_for_writable: false,
                eof_requested: false,
                eof_sent: false,
                protocol_paused: false,
                connection_lost_sent: false,
                write_queued: false,
            }));
        Ok(Self { core })
    }

    fn activate(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let fd = core.borrow().fd;
        let transports = core.borrow().transports.clone_ref(py);
        let registry = core.borrow().registry.clone_ref(py);
        transports.bind(py).set_item(fd, slf.bind(py))?;
        registry.bind(py).borrow_mut().register_inner(fd, core.clone());
        let result = core.borrow_mut().sync_interest(py);
        result
    }

    fn bind_readexactly(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_readexactly(slf.py(), slf.as_any().as_unbound())
    }

    fn bind_write(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_write(slf.py(), slf.as_any().as_unbound())
    }

    fn bind_close(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_close(slf.py(), slf.as_any().as_unbound())
    }

    fn bind_drain(slf: Py<Self>, py: Python<'_>, fallback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        slf.borrow(py).core.borrow_mut().drain_fallback = Some(fallback);
        create_bound_drain(py, slf.bind(py).as_any().as_unbound())
    }

    fn bind_wait_closed(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_wait_closed(slf.py(), slf.as_any().as_unbound())
    }

    fn drain_fast(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.core.borrow_mut().drain_fast(py)
    }

    fn wait_closed_fast(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.core.borrow_mut().wait_closed_fast(py)
    }

    fn write(&mut self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        self.core.borrow_mut().write(py, data)
    }

    fn writelines(&mut self, py: Python<'_>, list_of_data: Bound<'_, PyAny>) -> PyResult<()> {
        for item in list_of_data.try_iter()? {
            self.write(py, item?)?;
        }
        Ok(())
    }

    fn write_eof(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let mut transport = core.borrow_mut();
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
        let core = slf.borrow(py).core.clone();
        let mut transport = core.borrow_mut();
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
        let core = slf.borrow(py).core.clone();
        let mut transport = core.borrow_mut();
        transport.finish_close(py, exc)
    }

    fn is_closing(&self) -> bool {
        let core = self.core.borrow();
        core.closing || core.closed
    }

    fn get_write_buffer_size(&self) -> usize {
        self.core.borrow().pending_write_bytes
    }

    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(
        &self,
        py: Python<'_>,
        name: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let core = self.core.borrow();
        if name == "socket" {
            return Ok(core.sock.clone_ref(py));
        }
        let extra = core.extra.bind(py).cast::<PyDict>()?;
        if let Some(value) = extra.get_item(name)? {
            Ok(value.unbind())
        } else if let Some(default) = default {
            Ok(default)
        } else {
            Ok(py.None())
        }
    }

    fn pause_reading(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut core = self.core.borrow_mut();
        if core.read_paused || core.closed {
            return Ok(());
        }
        core.read_paused = true;
        core.remove_reader(py)
    }

    fn resume_reading(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let mut transport = core.borrow_mut();
        if !transport.read_paused || transport.closed || transport.closing {
            return Ok(());
        }
        transport.read_paused = false;
        transport.sync_interest(py)?;
        transport.on_readable(py)
    }

    fn _on_readable(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let result = core.borrow_mut().on_readable(py);
        result
    }

    fn _on_writable(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let result = core.borrow_mut().on_writable(py);
        result
    }

    pub(crate) fn on_events(&mut self, py: Python<'_>, mask: u8) -> PyResult<()> {
        self.core.borrow_mut().on_events(py, mask)
    }

    pub(crate) fn on_read_error(&mut self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<()> {
        self.core.borrow_mut().on_read_error(py, exc)
    }
}

impl StreamCore {
    fn pause_reading(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.read_paused || self.closed {
            return Ok(());
        }
        self.read_paused = true;
        self.remove_reader(py)
    }

    pub(crate) fn on_events(&mut self, py: Python<'_>, mask: u8) -> PyResult<()> {
        if mask & 0x01 != 0 {
            self.on_readable(py)?;
        }
        if !self.closed && mask & 0x02 != 0 {
            self.on_writable(py)?;
        }
        Ok(())
    }

    pub(crate) fn on_read_error(&mut self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<()> {
        self.finish_close(py, Some(exc))
    }

    fn drain_fast(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) {
            let reader = reader_obj.bind(py);
            if let Some(exc) = current_exception(&reader)? {
                return Err(PyErr::from_value(exc.into_bound(py).into_any()));
            }
        }

        if !self.closing && !self.protocol_paused {
            if let Some(ready) = self.ready_drain.as_ref() {
                return Ok(Some(ready.clone_ref(py)));
            }
            let future = self
                .loop_obj
                .bind(py)
                .call_method0("create_future")?
                .unbind();
            future.bind(py).call_method1("set_result", (py.None(),))?;
            self.ready_drain = Some(future.clone_ref(py));
            return Ok(Some(future));
        }

        Ok(None)
    }

    fn wait_closed_fast(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closed {
            return ready_awaitable_result(py, py.None());
        }
        let future = self
            .loop_obj
            .bind(py)
            .call_method0("create_future")?
            .unbind();
        self.pending_close_waiters.push(future.clone_ref(py));
        Ok(future)
    }

    fn write(&mut self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        self.ensure_can_write()?;
        if let Ok(bytes) = data.cast::<PyBytes>() {
            self.write_bytes_object_inner(py, &bytes)?;
        } else {
            let bytes = coerce_bytes(py, &data)?;
            self.write_bytes_object_inner(py, &bytes.bind(py))?;
        }
        Ok(())
    }

    fn ensure_can_write(&self) -> PyResult<()> {
        if self.closing || self.closed {
            return Err(PyRuntimeError::new_err("stream transport is closing"));
        }
        if self.eof_requested {
            return Err(PyRuntimeError::new_err(
                "Cannot call write() after write_eof()",
            ));
        }
        Ok(())
    }

    fn write_bytes_object_inner(&mut self, py: Python<'_>, data: &Bound<'_, PyBytes>) -> PyResult<()> {
        let bytes = data.as_bytes();
        if self.write_queue.is_empty() && !self.writer_registered && bytes.len() <= DIRECT_WRITE_LIMIT {
            match try_send_bytes(self.fd, bytes) {
                Ok(sent) if sent == bytes.len() => {
                    record_stream_profile_event("write_direct", self.fd, sent, 0);
                    return Ok(());
                }
                Ok(sent) => {
                    let tail = PyBytes::new(py, &bytes[sent..]).unbind();
                    self.queue_write_bytes(py, tail, sent)?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    record_stream_profile_event("write_direct_would_block", self.fd, bytes.len(), 0);
                    self.queue_write_bytes(py, data.clone().unbind(), 0)?;
                }
                Err(err) => {
                    return self.finish_close(
                        py,
                        Some(
                            PyRuntimeError::new_err(err.to_string())
                                .into_value(py)
                                .into_any(),
                        ),
                    );
                }
            }
        } else {
            self.queue_write_bytes(py, data.clone().unbind(), 0)?;
        }
        self.sync_interest(py)?;
        self.schedule_write_phase(py)?;
        self.maybe_pause_protocol(py)?;
        Ok(())
    }

    fn write_bytes_inner(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        if self.write_queue.is_empty() && !self.writer_registered && data.len() <= DIRECT_WRITE_LIMIT {
            match try_send_bytes(self.fd, data) {
                Ok(sent) if sent == data.len() => return Ok(()),
                Ok(sent) => {
                    self.queue_write_bytes(py, PyBytes::new(py, &data[sent..]).unbind(), sent)?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    record_stream_profile_event("write_direct_would_block", self.fd, data.len(), 0);
                    self.queue_write_bytes(py, PyBytes::new(py, data).unbind(), 0)?;
                }
                Err(err) => {
                    return self.finish_close(
                        py,
                        Some(
                            PyRuntimeError::new_err(err.to_string())
                                .into_value(py)
                                .into_any(),
                        ),
                    );
                }
            }
        } else {
            self.queue_write_bytes(py, PyBytes::new(py, data).unbind(), 0)?;
        }

        self.sync_interest(py)?;
        self.schedule_write_phase(py)?;
        self.maybe_pause_protocol(py)?;
        Ok(())
    }

    fn schedule_write_phase(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.write_queued || self.pending_write_bytes == 0 {
            return Ok(());
        }
        self.write_queued = true;
        self.registry
            .bind(py)
            .borrow()
            .queue_write_phase(self.fd, self.stream_token);
        Ok(())
    }

    fn queue_write_bytes(&mut self, py: Python<'_>, data: Py<PyBytes>, sent: usize) -> PyResult<()> {
        let len = data.bind(py).as_bytes().len();
        let sent = sent.min(len);
        let queued = len.saturating_sub(sent);
        self.pending_write_bytes += queued;
        if queued != 0 && sent == 0 && len <= SMALL_WRITE_CHUNK_LIMIT {
            let bytes = data.bind(py).as_bytes();
            if let Some(last) = self.write_queue.back_mut() {
                if last.sent == 0 {
                    match &mut last.data {
                        PendingWriteData::Owned(existing) => {
                            if existing.len() + len <= SMALL_WRITE_BUFFER_LIMIT {
                                existing.extend_from_slice(bytes);
                                record_stream_profile_event(
                                    "write_queue_small_append",
                                    self.fd,
                                    queued,
                                    self.pending_write_bytes,
                                );
                                return Ok(());
                            }
                        }
                        PendingWriteData::PyBytes(existing) => {
                            let existing_bytes = existing.bind(py).as_bytes();
                            if existing_bytes.len() <= SMALL_WRITE_CHUNK_LIMIT
                                && existing_bytes.len() + len <= SMALL_WRITE_BUFFER_LIMIT
                            {
                                let mut combined =
                                    Vec::with_capacity(existing_bytes.len() + len);
                                combined.extend_from_slice(existing_bytes);
                                combined.extend_from_slice(bytes);
                                last.data = PendingWriteData::Owned(combined);
                                record_stream_profile_event(
                                    "write_queue_small_merge",
                                    self.fd,
                                    queued,
                                    self.pending_write_bytes,
                                );
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
        self.write_queue.push_back(PendingWrite {
            data: PendingWriteData::PyBytes(data),
            sent,
        });
        record_stream_profile_event(
            "write_queue",
            self.fd,
            queued,
            self.pending_write_bytes,
        );
        Ok(())
    }

    pub(crate) fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closing || self.closed || self.read_paused {
            return Ok(());
        }
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport readable fd={} closing={} paused={}",
                self.fd, self.closing, self.read_paused
            );
        }
        loop {
            match recv_into(self.fd, &mut self.read_buffer) {
                Ok(0) => {
                    if trace_stream_enabled() {
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
                    record_stream_profile_event("read", self.fd, n, self.pending_write_bytes);
                    if trace_stream_enabled() {
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
                        Some(
                            PyRuntimeError::new_err(err.to_string())
                                .into_value(py)
                                .into_any(),
                        ),
                    );
                }
            }
        }
    }

    pub(crate) fn on_writable(&mut self, py: Python<'_>) -> PyResult<()> {
        record_stream_profile_event("write_ready", self.fd, self.pending_write_bytes, self.write_queue.len());
        self.write_waiting_for_writable = false;
        self.sync_interest(py)?;
        self.schedule_write_phase(py)
    }

    pub(crate) fn flush_write_phase(&mut self, py: Python<'_>) -> PyResult<()> {
        let queued_before = self.pending_write_bytes;
        let mut phase_sent = 0usize;
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport writable fd={} queued={}",
                self.fd, self.pending_write_bytes
            );
        }
        record_stream_profile_event("write_phase_start", self.fd, queued_before, self.write_queue.len());
        while !self.write_queue.is_empty() {
            match self.flush_write_once(py) {
                Ok(0) => {
                    self.write_waiting_for_writable = true;
                    self.sync_interest(py)?;
                    break;
                }
                Ok(sent) => {
                    phase_sent += sent;
                    record_stream_profile_event("write_flush", self.fd, sent, self.pending_write_bytes);
                    if trace_stream_enabled() {
                        eprintln!("stream-transport wrote fd={} bytes={}", self.fd, sent);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    self.write_waiting_for_writable = true;
                    self.sync_interest(py)?;
                    record_stream_profile_event(
                        "write_would_block",
                        self.fd,
                        self.pending_write_bytes,
                        self.write_queue.len(),
                    );
                    break;
                }
                Err(err) => {
                    return self.finish_close(
                        py,
                        Some(
                            PyRuntimeError::new_err(err.to_string())
                                .into_value(py)
                                .into_any(),
                        ),
                    );
                }
            }
        }
        record_stream_profile_event("write_phase_end", self.fd, phase_sent, self.pending_write_bytes);

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

    fn flush_write_once(&mut self, py: Python<'_>) -> std::io::Result<usize> {
        if self.write_queue.len() <= 1 {
            let Some(write) = self.write_queue.front_mut() else {
                return Ok(0);
            };
            let sent = match &write.data {
                PendingWriteData::PyBytes(data) => {
                    let data = data.bind(py);
                    let bytes = data.as_bytes();
                    try_send_bytes(self.fd, &bytes[write.sent..])?
                }
                PendingWriteData::Owned(data) => try_send_bytes(self.fd, &data[write.sent..])?,
            };
            if sent == 0 {
                return Ok(0);
            }
            let len = write.len(py);
            write.sent += sent;
            self.pending_write_bytes = self.pending_write_bytes.saturating_sub(sent);
            if write.sent == len {
                self.write_queue.pop_front();
            }
            return Ok(sent);
        }

        let mut iovecs = [libc::iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }; WRITEV_BATCH_LIMIT];
        let mut iovec_count = 0usize;
        for pending in self.write_queue.iter().take(WRITEV_BATCH_LIMIT) {
            let slice = match &pending.data {
                PendingWriteData::PyBytes(data) => {
                    let bytes = data.bind(py).as_bytes();
                    &bytes[pending.sent..]
                }
                PendingWriteData::Owned(data) => &data[pending.sent..],
            };
            if slice.is_empty() {
                continue;
            }
            iovecs[iovec_count] = libc::iovec {
                iov_base: slice.as_ptr() as *mut libc::c_void,
                iov_len: slice.len(),
            };
            iovec_count += 1;
        }
        if iovec_count == 0 {
            return Ok(0);
        }

        let sent = unsafe { libc::writev(self.fd, iovecs.as_ptr(), iovec_count as i32) };
        if sent < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let sent = sent as usize;
        if sent == 0 {
            return Ok(0);
        }
        self.consume_written_bytes(py, sent);
        Ok(sent)
    }

    fn consume_written_bytes(&mut self, py: Python<'_>, mut written: usize) {
        while written != 0 {
            let Some(write) = self.write_queue.front_mut() else {
                break;
            };
            let len = write.len(py);
            let remaining = len.saturating_sub(write.sent);
            let consumed = written.min(remaining);
            write.sent += consumed;
            self.pending_write_bytes = self.pending_write_bytes.saturating_sub(consumed);
            written -= consumed;
            if write.sent == len {
                self.write_queue.pop_front();
            }
        }
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
        self.write_queued = false;
        self.write_waiting_for_writable = false;
        self.registry
            .bind(py)
            .borrow()
            .unregister_inner(self.fd);
        self.disable_interest(py)?;
        self.pending_write_bytes = 0;
        self.write_queue.clear();
        self.transports.bind(py).del_item(self.fd).ok();
        match exc.as_ref() {
            Some(exc) => self
                .protocol
                .bind(py)
                .call_method1("connection_lost", (exc.bind(py),))?,
            None => self
                .protocol
                .bind(py)
                .call_method1("connection_lost", (py.None(),))?,
        };
        self.connection_lost_sent = true;
        self.sock.bind(py).call_method0("close")?;
        self.queue_pending_drains(py, exc)?;
        self.queue_close_waiters(py)?;
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
        let writable = !self.closed && self.write_waiting_for_writable;
        if readable == self.reader_registered && writable == self.writer_registered {
            return Ok(());
        }
        if writable != self.writer_registered {
            record_stream_profile_event(
                if writable { "write_interest_on" } else { "write_interest_off" },
                self.fd,
                self.pending_write_bytes,
                self.write_queue.len(),
            );
        }
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport interest fd={} readable={} writable={} prev_r={} prev_w={}",
                self.fd, readable, writable, self.reader_registered, self.writer_registered
            );
        }
        self.reader_registered = readable;
        self.writer_registered = writable;
        self.poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fd, readable, writable)?;
        Ok(())
    }

    fn disable_interest(&mut self, py: Python<'_>) -> PyResult<()> {
        self.reader_registered = false;
        self.writer_registered = false;
        self.poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fd, false, false)?;
        Ok(())
    }

    fn maybe_pause_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.protocol_paused || self.pending_write_bytes <= WRITE_HIGH_WATER {
            return Ok(());
        }
        self.protocol.bind(py).call_method0("pause_writing")?;
        self.protocol_paused = true;
        record_stream_profile_event("pause_writing", self.fd, self.pending_write_bytes, self.write_queue.len());
        Ok(())
    }

    fn maybe_resume_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.protocol_paused || self.pending_write_bytes > WRITE_LOW_WATER {
            return Ok(());
        }
        self.protocol.bind(py).call_method0("resume_writing")?;
        self.protocol_paused = false;
        record_stream_profile_event("resume_writing", self.fd, self.pending_write_bytes, self.write_queue.len());
        self.queue_pending_drains(py, None)?;
        Ok(())
    }

    fn feed_read_buffer_inner(&mut self, py: Python<'_>, size: usize) -> PyResult<()> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Ok(());
        };
        if size == 0 {
            return Ok(());
        }

        let reader = reader_obj.bind(py);
        let had_pending_read = self.pending_read.is_some();
        if self.try_finish_pending_read_from_chunk(py, &reader, size)? {
            return Ok(());
        }
        let buffer_obj = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .clone_ref(py);
        let buffer = buffer_obj.bind(py);
        append_to_bytearray(buffer.as_ptr(), &self.read_buffer[..size])?;
        if !self.try_finish_pending_read(py, &reader)? && !had_pending_read {
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

    fn try_finish_pending_read_from_chunk(
        &mut self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
        size: usize,
    ) -> PyResult<bool> {
        let Some(pending) = self.pending_read.as_ref() else {
            return Ok(false);
        };
        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .bind(py);
        let buffered = bytearray_len(buffer.as_ptr())?;
        if buffered + size < pending.size {
            return Ok(false);
        }

        let pending = self.pending_read.take().expect("pending read present");
        let needed_from_chunk = pending.size.saturating_sub(buffered);
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport finish chunk fd={} needed={} chunk={}",
                self.fd, needed_from_chunk, size
            );
        }
        let payload = if buffered == 0 && needed_from_chunk == size {
            PyBytes::new(py, &self.read_buffer[..size]).into_any().unbind()
        } else {
            join_buffered_prefix_and_chunk(
                py,
                buffer.as_ptr(),
                buffered,
                &self.read_buffer[..needed_from_chunk],
                pending.size,
            )?
        };
        reader.setattr("_waiter", py.None())?;
        self.queue_completion(py, StreamCompletion::ReadResult {
            reader: reader.clone().unbind(),
            waiter: pending.waiter,
            payload,
            resume_transport: true,
        })?;
        record_stream_profile_event("read_wait_done", self.fd, pending.size, buffered);

        if needed_from_chunk != size || buffered != 0 {
            replace_bytearray(buffer.as_ptr(), &self.read_buffer[needed_from_chunk..size])?;
        }
        maybe_resume_transport(reader)?;
        Ok(true)
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
            return ready_awaitable_exception(py, exc);
        }
        if size == 0 {
            let payload = PyBytes::new(py, b"").into_any().unbind();
            return ready_awaitable_result(py, payload);
        }

        if size > NATIVE_READEXACTLY_LIMIT {
            if let Some(reader_readexactly) = self.reader_readexactly.as_ref() {
                return Ok(reader_readexactly.bind(py).call1((size,))?.unbind());
            }
        }

        if try_consume_exact(buffer.as_ptr(), size)? {
            let data = consume_exact(py, buffer.as_ptr(), size)?;
            maybe_resume_transport(&reader)?;
            return ready_awaitable_result(py, data);
        }

        if reader.getattr("_eof")?.is_truthy()? {
            let partial = consume_all(py, buffer.as_ptr())?;
            return ready_awaitable_exception(py, incomplete_read_error(py, partial, size)?);
        }

        if self.pending_read.is_some() {
            return Err(PyRuntimeError::new_err(
                "readexactly() called while another coroutine is already waiting for incoming data",
            ));
        }

        if reader.getattr("_paused")?.is_truthy()? {
            reader.setattr("_paused", false)?;
            reader
                .getattr("_transport")?
                .call_method0("resume_reading")?;
        }

        let scheduler = self
            .loop_obj
            .bind(py)
            .getattr("_scheduler")?
            .cast_into::<Scheduler>()?
            .unbind();
        let waiter = StreamWaiter::new(py, scheduler, self.loop_obj.clone_ref(py))?;
        reader.setattr("_waiter", waiter.bind(py))?;
        self.pending_read = Some(PendingRead {
            size,
            waiter: waiter.clone_ref(py),
        });
        Ok(waiter.into_any())
    }

    fn drain_inner(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(ready) = self.drain_fast(py)? {
            return Ok(ready);
        }

        if let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) {
            let reader = reader_obj.bind(py);
            if let Some(exc) = current_exception(&reader)? {
                return Err(PyErr::from_value(exc.into_bound(py).into_any()));
            }
        }

        let future = self
            .loop_obj
            .bind(py)
            .call_method0("create_future")?
            .unbind();
        self.pending_drains.push(future.clone_ref(py));
        record_stream_profile_event("drain_wait", self.fd, self.pending_write_bytes, self.pending_drains.len());
        Ok(future)
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
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport finish exact fd={} size={}",
                self.fd, pending.size
            );
        }
        let data = consume_exact(
            py,
            self.buffer
                .as_ref()
                .expect("stream transport buffer")
                .bind(py)
                .as_ptr(),
            pending.size,
        )?;
        reader.setattr("_waiter", py.None())?;
        self.queue_completion(py, StreamCompletion::ReadResult {
            reader: reader.clone().unbind(),
            waiter: pending.waiter,
            payload: data,
            resume_transport: true,
        })?;
        record_stream_profile_event("read_wait_done", self.fd, pending.size, buffered);
        Ok(true)
    }

    fn fail_pending_read_eof(
        &mut self,
        py: Python<'_>,
        _reader: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let Some(pending) = self.pending_read.take() else {
            return Ok(());
        };
        let partial = consume_all(
            py,
            self.buffer
                .as_ref()
                .expect("stream transport buffer")
                .bind(py)
                .as_ptr(),
        )?;
        let exc = incomplete_read_error(py, partial, pending.size)?;
        _reader.setattr("_waiter", py.None())?;
        self.queue_completion(py, StreamCompletion::ReadException {
            reader: _reader.clone().unbind(),
            waiter: pending.waiter,
            exception: exc,
        })?;
        Ok(())
    }

    fn queue_completion(&self, py: Python<'_>, completion: StreamCompletion) -> PyResult<()> {
        self.registry
            .bind(py)
            .borrow()
            .queue_completion(completion);
        Ok(())
    }

    fn queue_pending_drains(&mut self, py: Python<'_>, exception: Option<Py<PyAny>>) -> PyResult<()> {
        if self.pending_drains.is_empty() {
            return Ok(());
        }
        record_stream_profile_event(
            if exception.is_some() { "drain_fail" } else { "drain_ready" },
            self.fd,
            self.pending_write_bytes,
            self.pending_drains.len(),
        );
        let completions = std::mem::take(&mut self.pending_drains);
        let value = py.None();
        for future in completions {
            let completion = match exception.as_ref() {
                Some(exception) => StreamCompletion::FutureException {
                    future,
                    exception: exception.clone_ref(py),
                },
                None => StreamCompletion::FutureResult {
                    future,
                    value: value.clone_ref(py),
                },
            };
            self.queue_completion(py, completion)?;
        }
        Ok(())
    }

    fn queue_close_waiters(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.pending_close_waiters.is_empty() {
            return Ok(());
        }
        let waiters = std::mem::take(&mut self.pending_close_waiters);
        let value = py.None();
        for future in waiters {
            self.queue_completion(
                py,
                StreamCompletion::FutureResult {
                    future,
                    value: value.clone_ref(py),
                },
            )?;
        }
        Ok(())
    }
}

fn trace_stream_enabled() -> bool {
    *TRACE_STREAM.get_or_init(|| env::var_os("RSLOOP_TRACE_STREAM").is_some())
}

pub(crate) fn take_profile_stream_json() -> Option<String> {
    let state = stream_profile_state()?;
    let mut state = state.lock();
    let mut out = String::new();
    let _ = write!(out, "{{\"dropped\":{},\"events\":[", state.dropped);
    for (index, event) in state.events.iter().enumerate() {
        if index != 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"at_us\":{},\"kind\":\"{}\",\"fd\":{},\"size\":{},\"aux\":{}}}",
            event.at_us, event.kind, event.fd, event.size, event.aux
        );
    }
    out.push_str("]}");
    state.events.clear();
    state.dropped = 0;
    Some(out)
}

fn stream_profile_state() -> Option<&'static Mutex<StreamProfileState>> {
    STREAM_PROFILE
        .get_or_init(|| {
            if env::var_os("RSLOOP_PROFILE_STREAM_JSON").is_none() {
                return None;
            }
            let limit = env::var("RSLOOP_PROFILE_STREAM_LIMIT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(256);
            Some(Mutex::new(StreamProfileState {
                started_at: Instant::now(),
                limit,
                dropped: 0,
                events: Vec::with_capacity(limit.min(1024)),
            }))
        })
        .as_ref()
}

fn record_stream_profile_event(kind: &'static str, fd: RawFd, size: usize, aux: usize) {
    let Some(state) = stream_profile_state() else {
        return;
    };
    let mut state = state.lock();
    if state.events.len() >= state.limit {
        state.dropped += 1;
        return;
    }
    let at_us = state.started_at.elapsed().as_micros() as u64;
    state.events.push(StreamProfileEvent {
        at_us,
        kind,
        fd,
        size,
        aux,
    });
}

struct SyncPyMethodDef(ffi::PyMethodDef);

unsafe impl Sync for SyncPyMethodDef {}

static READEXACTLY_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"readexactly".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_readexactly_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native exact-read fast path backed by the Rsloop stream transport.".as_ptr(),
});

static DRAIN_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"drain".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_drain_c,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"Native drain fast path backed by the Rsloop stream transport.".as_ptr(),
});

static WRITE_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"write".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_write_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native write fast path backed by the Rsloop stream transport.".as_ptr(),
});

static CLOSE_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"close".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_close_c,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"Native close fast path backed by the Rsloop stream transport.".as_ptr(),
});

static WAIT_CLOSED_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"wait_closed".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_wait_closed_c,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"Native wait_closed fast path backed by the Rsloop stream transport.".as_ptr(),
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

fn create_bound_write(py: Python<'_>, slf: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            &WRITE_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
            slf.as_ptr(),
            std::ptr::null_mut(),
        );
        Ok(Bound::<PyAny>::from_owned_ptr_or_err(py, func)?.unbind())
    }
}

fn create_bound_close(py: Python<'_>, slf: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            &CLOSE_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
            slf.as_ptr(),
            std::ptr::null_mut(),
        );
        Ok(Bound::<PyAny>::from_owned_ptr_or_err(py, func)?.unbind())
    }
}

fn create_bound_wait_closed(py: Python<'_>, slf: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            &WAIT_CLOSED_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
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
        let transport = Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<StreamTransport>();
        let core = transport.borrow().core.clone();
        let size = Bound::<PyAny>::from_borrowed_ptr(py, arg).extract::<isize>()?;
        if size < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "readexactly size can not be less than zero",
            ));
        }
        let result = core.borrow_mut().readexactly_inner(py, size as usize);
        result
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
        let transport = Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<StreamTransport>();
        let core = transport.borrow().core.clone();
        let result = core.borrow_mut().drain_inner(py);
        result
    })();

    match result {
        Ok(obj) => obj.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn stream_transport_write_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let transport = Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<StreamTransport>();
        let core = transport.borrow().core.clone();
        let mut transport_ref = core.borrow_mut();
        if ffi::PyBytes_Check(arg) != 0 {
            transport_ref.ensure_can_write()?;
            let data = Bound::<PyAny>::from_borrowed_ptr(py, arg).cast_into_unchecked::<PyBytes>();
            transport_ref.write_bytes_object_inner(py, &data)?;
            return Ok(py.None());
        }
        let data = Bound::<PyAny>::from_borrowed_ptr(py, arg);
        transport_ref.write(py, data)?;
        Ok(py.None())
    })();

    match result {
        Ok(obj) => obj.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn stream_transport_close_c(
    slf: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let transport = Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<StreamTransport>();
        StreamTransport::close(transport.unbind(), py)?;
        Ok(py.None())
    })();

    match result {
        Ok(obj) => obj.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn stream_transport_wait_closed_c(
    slf: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let transport = Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<StreamTransport>();
        let core = transport.borrow().core.clone();
        let result = core.borrow_mut().wait_closed_fast(py);
        result
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

fn replace_bytearray(bytearray: *mut ffi::PyObject, data: &[u8]) -> PyResult<()> {
    unsafe {
        if ffi::PyByteArray_Resize(bytearray, data.len() as isize) != 0 {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        if data.is_empty() {
            return Ok(());
        }
        let dest = ffi::PyByteArray_AsString(bytearray) as *mut u8;
        if dest.is_null() {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        ptr::copy_nonoverlapping(data.as_ptr(), dest, data.len());
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

fn consume_exact(
    py: Python<'_>,
    bytearray: *mut ffi::PyObject,
    size: usize,
) -> PyResult<Py<PyAny>> {
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
        let data = PyBytes::new(
            py,
            std::slice::from_raw_parts(src.cast_const(), len as usize),
        )
        .into_any()
        .unbind();
        if ffi::PyByteArray_Resize(bytearray, 0) != 0 {
            return Err(PyErr::fetch(py));
        }
        Ok(data)
    }
}

fn join_buffered_prefix_and_chunk(
    py: Python<'_>,
    bytearray: *mut ffi::PyObject,
    buffered: usize,
    chunk: &[u8],
    total: usize,
) -> PyResult<Py<PyAny>> {
    unsafe {
        let object = ffi::PyBytes_FromStringAndSize(std::ptr::null(), total as ffi::Py_ssize_t);
        let bytes =
            Bound::<PyAny>::from_owned_ptr_or_err(py, object)?.cast_into_unchecked::<PyBytes>();
        let dest = ffi::PyBytes_AsString(bytes.as_ptr()) as *mut u8;
        if dest.is_null() {
            return Err(PyErr::fetch(py));
        }
        if buffered != 0 {
            let src = ffi::PyByteArray_AsString(bytearray) as *const u8;
            if src.is_null() {
                return Err(PyErr::fetch(py));
            }
            ptr::copy_nonoverlapping(src, dest, buffered);
        }
        if !chunk.is_empty() {
            ptr::copy_nonoverlapping(chunk.as_ptr(), dest.add(buffered), chunk.len());
        }
        Ok(bytes.into_any().unbind())
    }
}

fn coerce_bytes(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    static BYTES_TYPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let bytes_type = BYTES_TYPE.get_or_try_init(py, || -> PyResult<_> {
        Ok(py.import("builtins")?.getattr("bytes")?.unbind())
    })?;
    Ok(bytes_type
        .bind(py)
        .call1((data,))?
        .cast_into::<PyBytes>()?
        .unbind())
}

fn ready_awaitable_result(py: Python<'_>, payload: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Py::new(
        py,
        ReadyAwaitable {
            result: Some(payload),
            exception: None,
            done: false,
        },
    )
    .map(|obj| obj.into_bound(py).into_any().unbind())
}

fn ready_awaitable_exception(py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Py::new(
        py,
        ReadyAwaitable {
            result: None,
            exception: Some(exc),
            done: false,
        },
    )
    .map(|obj| obj.into_bound(py).into_any().unbind())
}

fn incomplete_read_error(
    py: Python<'_>,
    partial: Py<PyAny>,
    expected: usize,
) -> PyResult<Py<PyAny>> {
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

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    future.bind(py).call_method0("done")?.is_truthy()
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

fn copy_context_obj(py: Python<'_>) -> PyResult<Py<PyAny>> {
    static COPY_CONTEXT: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let copy_context = COPY_CONTEXT.get_or_try_init(py, || -> PyResult<_> {
        Ok(py.import("contextvars")?.getattr("copy_context")?.unbind())
    })?;
    Ok(copy_context.bind(py).call0()?.unbind())
}

fn invalid_state_error(py: Python<'_>, message: &str) -> PyResult<PyErr> {
    static INVALID_STATE_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let cls = INVALID_STATE_ERROR.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.exceptions")?
            .getattr("InvalidStateError")?
            .unbind())
    })?;
    Ok(PyErr::from_value(
        cls.bind(py).call1((message,))?.into_any().unbind().into_bound(py).into_any(),
    ))
}

fn cancelled_error(py: Python<'_>, message: Option<&Py<PyAny>>) -> PyResult<PyErr> {
    static CANCELLED_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let cls = CANCELLED_ERROR.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.exceptions")?
            .getattr("CancelledError")?
            .unbind())
    })?;
    let value = match message {
        Some(message) => cls.bind(py).call1((message.bind(py),))?,
        None => cls.bind(py).call0()?,
    };
    Ok(PyErr::from_value(value.into_any()))
}

fn normalize_exception(py: Python<'_>, exception: Py<PyAny>) -> PyResult<Py<PyAny>> {
    if exception.bind(py).is_instance_of::<pyo3::types::PyType>() {
        Ok(exception.bind(py).call0()?.unbind())
    } else {
        Ok(exception)
    }
}

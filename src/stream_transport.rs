#![cfg(unix)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::env;
use std::fmt::Write as _;
use std::os::fd::RawFd;
use std::ptr;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    OnceLock,
};
use std::time::Instant;

use memchr::memmem;
use parking_lot::Mutex;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyStopIteration};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyModule};

use crate::handles::OneArgHandle;
use crate::poller::TokioPoller;
use crate::scheduler::Scheduler;
use crate::stream_registry::StreamTransportRegistry;

const DEFAULT_STREAM_READ_SIZE: usize = 64 * 1024;
const NATIVE_READEXACTLY_LIMIT: usize = 16 * 1024;
const READ_BUFFER_COMPACT_THRESHOLD: usize = DEFAULT_STREAM_READ_SIZE;
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
    stream_token: u64,
    fd: RawFd,
    size: usize,
    aux: usize,
    read_buffer_len: usize,
    read_buffer_offset: usize,
    pending_write_bytes: usize,
    state_flags: u32,
}

struct StreamProfileState {
    started_at: Instant,
    limit: usize,
    dropped: u64,
    events: Vec<StreamProfileEvent>,
}

struct PendingRead {
    kind: PendingReadKind,
    waiter: Py<StreamWaiter>,
}

enum PendingReadKind {
    Exact(usize),
    Until { separator: Vec<u8>, offset: usize },
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

enum ReadableDispatch {
    WouldBlock,
    Buffered {
        buffer_updated: Py<PyAny>,
        nbytes: usize,
    },
    Eof {
        protocol: Py<PyAny>,
    },
    Error {
        exc: Py<PyAny>,
    },
}

fn buffered_protocol_methods(
    protocol: &Bound<'_, PyAny>,
) -> PyResult<(bool, Option<Py<PyAny>>, Option<Py<PyAny>>)> {
    if protocol.hasattr("get_buffer")? && protocol.hasattr("buffer_updated")? {
        return Ok((
            true,
            Some(protocol.getattr("get_buffer")?.unbind()),
            Some(protocol.getattr("buffer_updated")?.unbind()),
        ));
    }
    Ok((false, None, None))
}

fn defer_transport_method(py: Python<'_>, transport: &Bound<'_, StreamTransport>, method: &str) -> PyResult<()> {
    let loop_obj = PyModule::import(py, "asyncio")?
        .getattr("get_running_loop")?
        .call0()?;
    let callback = transport.getattr(method)?;
    loop_obj.call_method1("call_soon", (callback,))?;
    Ok(())
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
    buffered_protocol: bool,
    buffered_get_buffer: Option<Py<PyAny>>,
    buffered_buffer_updated: Option<Py<PyAny>>,
    loop_obj: Py<PyAny>,
    poller: Py<TokioPoller>,
    registry: Py<StreamTransportRegistry>,
    transports: Py<PyDict>,
    extra: Py<PyAny>,
    reader: Option<Py<PyAny>>,
    reader_readexactly: Option<Py<PyAny>>,
    reader_readuntil: Option<Py<PyAny>>,
    close_waiter: Option<Py<PyAny>>,
    drain_fallback: Option<Py<PyAny>>,
    ready_drain: Option<Py<PyAny>>,
    buffer: Option<Py<PyByteArray>>,
    limit: usize,
    pending_read: Option<PendingRead>,
    pending_drains: Vec<Py<PyAny>>,
    pending_close_waiters: Vec<Py<PyAny>>,
    read_buffer: Vec<u8>,
    read_buffer_offset: usize,
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
    #[pyo3(get)]
    _start_tls_compatible: bool,
    fd: RawFd,
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
            return Err(PyErr::from_value(
                exception.clone_ref(py).into_bound(py).into_any(),
            ));
        }
        let result = waiter
            .result
            .as_ref()
            .map(|value| value.clone_ref(py))
            .unwrap_or_else(|| py.None());
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
                waiter
                    .callbacks
                    .push((callback.clone_ref(py), context.clone_ref(py)));
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
                    Err(PyErr::from_value(
                        exception.clone_ref(py).into_bound(py).into_any(),
                    ))
                } else {
                    Ok(self
                        .result
                        .as_ref()
                        .map(|value| value.clone_ref(py))
                        .unwrap_or_else(|| py.None()))
                }
            }
        }
    }

    fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.state {
            StreamWaiterState::Pending => Err(invalid_state_error(py, "Exception is not set.")?),
            StreamWaiterState::Cancelled => Err(cancelled_error(py, self.cancel_message.as_ref())?),
            StreamWaiterState::Finished => {
                Ok(self.exception.as_ref().map(|value| value.clone_ref(py)))
            }
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
        let protocol_ref = protocol.bind(py);
        let (buffered_protocol, buffered_get_buffer, buffered_buffer_updated) =
            buffered_protocol_methods(&protocol_ref)?;
        let fd = sock.bind(py).call_method0("fileno")?.extract::<i32>()? as RawFd;
        let (buffer, reader_readexactly, reader_readuntil, close_waiter, limit) =
            if let Some(reader_ref) = reader.as_ref() {
                let reader_ref = reader_ref.bind(py);
                let buffer = reader_ref
                    .getattr("_buffer")?
                    .cast_into::<PyByteArray>()?
                    .unbind();
                let reader_readexactly = reader_ref.getattr("readexactly")?.unbind();
                let reader_readuntil = reader_ref.getattr("readuntil")?.unbind();
                let close_waiter = protocol.bind(py).getattr("_closed").ok().map(Bound::unbind);
                let limit = reader_ref.getattr("_limit")?.extract()?;
                (
                    Some(buffer),
                    Some(reader_readexactly),
                    Some(reader_readuntil),
                    close_waiter,
                    limit,
                )
            } else {
                (None, None, None, None, 64 * 1024)
            };
        let core = Rc::new(RefCell::new(StreamCore {
            stream_token: STREAM_TOKEN.fetch_add(1, Ordering::Relaxed),
            sock,
            fd,
            protocol,
            buffered_protocol,
            buffered_get_buffer,
            buffered_buffer_updated,
            loop_obj,
            poller,
            registry,
            transports,
            extra,
            reader,
            reader_readexactly,
            reader_readuntil,
            close_waiter,
            drain_fallback: None,
            ready_drain: None,
            buffer,
            limit,
            pending_read: None,
            pending_drains: Vec::new(),
            pending_close_waiters: Vec::new(),
            read_buffer: vec![0_u8; read_size.max(1)],
            read_buffer_offset: 0,
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
        Ok(Self {
            _start_tls_compatible: true,
            fd,
            core,
        })
    }

    fn activate(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let (fd, transports, registry, reader, protocol) = {
            let core = core.borrow();
            (
                core.fd,
                core.transports.clone_ref(py),
                core.registry.clone_ref(py),
                core.reader.as_ref().map(|reader| reader.clone_ref(py)),
                core.protocol.clone_ref(py),
            )
        };
        let transport = slf.bind(py);
        let transport_any = transport.clone().into_any().unbind();

        if let Some(reader) = reader {
            reader
                .bind(py)
                .setattr("readexactly", create_bound_readexactly(py, &transport_any)?)?;
            reader
                .bind(py)
                .setattr("readuntil", create_bound_readuntil(py, &transport_any)?)?;
        }

        protocol
            .bind(py)
            .call_method1("connection_made", (transport.clone().into_any(),))?;

        transports.bind(py).set_item(fd, transport)?;
        registry
            .bind(py)
            .borrow_mut()
            .register_inner(fd, core.clone());
        let result = core.borrow_mut().sync_interest(py);
        result
    }

    fn bind_readexactly(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_readexactly(slf.py(), slf.as_any().as_unbound())
    }

    fn bind_readuntil(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_readuntil(slf.py(), slf.as_any().as_unbound())
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

    fn set_protocol(slf: Py<Self>, py: Python<'_>, protocol: Py<PyAny>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let protocol_ref = protocol.bind(py);
        let (buffered_protocol, buffered_get_buffer, buffered_buffer_updated) =
            buffered_protocol_methods(&protocol_ref)?;
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport set_protocol fd={} type={}",
                core.borrow().fd,
                protocol_ref.get_type().name()?
            );
        }
        let mut core = core.borrow_mut();
        core.protocol = protocol;
        core.buffered_protocol = buffered_protocol;
        core.buffered_get_buffer = buffered_get_buffer;
        core.buffered_buffer_updated = buffered_buffer_updated;
        Ok(())
    }

    fn get_protocol(&self, py: Python<'_>) -> Py<PyAny> {
        self.core.borrow().protocol.clone_ref(py)
    }

    fn drain(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = slf.borrow(py).core.clone();
        let result = core.borrow_mut().drain_inner(py);
        result
    }

    #[pyo3(signature = (file, offset=0, count=None))]
    fn sendfile_static(
        slf: Py<Self>,
        py: Python<'_>,
        file: Py<PyAny>,
        offset: usize,
        count: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let helper = PyModule::import(py, "rsloop.loop")?.getattr("_sendfile_static_impl")?;
        let transport = slf.bind(py);
        let write = transport.getattr("write")?;
        let drain = transport.getattr("drain")?;
        Ok(helper.call1((write, drain, file, offset, count))?.unbind())
    }

    fn drain_fast(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.core.borrow_mut().drain_fast(py)
    }

    fn wait_closed_fast(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.core.borrow_mut().wait_closed_fast(py)
    }

    fn write(&mut self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        match self.core.try_borrow_mut() {
            Ok(mut core) => core.write(py, data),
            Err(_) => {
                let bytes = if let Ok(bytes) = data.cast::<PyBytes>() {
                    bytes.as_bytes().to_vec()
                } else {
                    let bytes = coerce_bytes(py, &data)?;
                    bytes.bind(py).as_bytes().to_vec()
                };
                match try_send_bytes(self.fd, &bytes) {
                    Ok(sent) if sent == bytes.len() => Ok(()),
                    Ok(sent) => Err(PyRuntimeError::new_err(format!(
                        "reentrant write was only able to send {sent} of {} bytes",
                        bytes.len()
                    ))),
                    Err(err) => Err(PyRuntimeError::new_err(err.to_string())),
                }
            }
        }
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
        let Ok(mut transport) = core.try_borrow_mut() else {
            return defer_transport_method(py, &slf.bind(py), "close");
        };
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
        let Ok(mut transport) = core.try_borrow_mut() else {
            return defer_transport_method(py, &slf.bind(py), "close");
        };
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

    fn pause_reading(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core_ref = slf.borrow(py).core.clone();
        let Ok(mut core) = core_ref.try_borrow_mut() else {
            return defer_transport_method(py, &slf.bind(py), "pause_reading");
        };
        if core.read_paused || core.closed {
            return Ok(());
        }
        core.read_paused = true;
        core.remove_reader(py)
    }

    fn resume_reading(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let core = slf.borrow(py).core.clone();
        let Ok(mut transport) = core.try_borrow_mut() else {
            return defer_transport_method(py, &slf.bind(py), "resume_reading");
        };
        if !transport.read_paused || transport.closed || transport.closing {
            return Ok(());
        }
        if trace_stream_enabled() {
            eprintln!("stream-transport resume_reading fd={}", transport.fd);
        }
        transport.read_paused = false;
        transport.sync_interest(py)?;
        drop(transport);
        slf.call_method0(py, "_on_readable")?;
        Ok(())
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
    fn profile_state_flags(&self) -> u32 {
        let mut flags = 0_u32;
        if self.reader.is_some() {
            flags |= 1 << 0;
        }
        if self.buffered_protocol {
            flags |= 1 << 1;
        }
        if self.closing {
            flags |= 1 << 2;
        }
        if self.closed {
            flags |= 1 << 3;
        }
        if self.read_paused {
            flags |= 1 << 4;
        }
        if self.reader_registered {
            flags |= 1 << 5;
        }
        if self.writer_registered {
            flags |= 1 << 6;
        }
        if self.write_waiting_for_writable {
            flags |= 1 << 7;
        }
        if self.protocol_paused {
            flags |= 1 << 8;
        }
        if self.connection_lost_sent {
            flags |= 1 << 9;
        }
        flags
    }

    fn record_profile_event(&self, kind: &'static str, size: usize, aux: usize) {
        record_stream_profile_event_detail(
            kind,
            self.fd,
            size,
            aux,
            self.stream_token,
            self.read_buffer.len(),
            self.read_buffer_offset,
            self.pending_write_bytes,
            self.profile_state_flags(),
        );
    }

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
        if self.is_buffered_protocol(py)? {
            if let Ok(bytes) = data.cast::<PyBytes>() {
                return self.write_direct_bytes(py, bytes.as_bytes());
            }
            let bytes = coerce_bytes(py, &data)?;
            return self.write_direct_bytes(py, bytes.bind(py).as_bytes());
        }
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

    fn is_buffered_protocol(&self, _py: Python<'_>) -> PyResult<bool> {
        Ok(self.buffered_protocol)
    }

    fn write_direct_bytes(&mut self, _py: Python<'_>, bytes: &[u8]) -> PyResult<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport ssl-direct-write fd={} bytes={}",
                self.fd,
                bytes.len()
            );
        }
        match try_send_bytes(self.fd, bytes) {
            Ok(sent) if sent == bytes.len() => Ok(()),
            Ok(sent) => Err(PyRuntimeError::new_err(format!(
                "direct write sent {sent} of {} bytes",
                bytes.len()
            ))),
            Err(err) => Err(PyRuntimeError::new_err(err.to_string())),
        }
    }

    fn write_bytes_object_inner(
        &mut self,
        py: Python<'_>,
        data: &Bound<'_, PyBytes>,
    ) -> PyResult<()> {
        let bytes = data.as_bytes();
        if self.write_queue.is_empty()
            && !self.writer_registered
            && bytes.len() <= DIRECT_WRITE_LIMIT
        {
            match try_send_bytes(self.fd, bytes) {
                Ok(sent) if sent == bytes.len() => {
                    self.record_profile_event("write_direct", sent, 0);
                    return Ok(());
                }
                Ok(sent) => {
                    let tail = PyBytes::new(py, &bytes[sent..]).unbind();
                    self.queue_write_bytes(py, tail, 0)?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    self.record_profile_event("write_direct_would_block", bytes.len(), 0);
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
        if self.write_queue.is_empty()
            && !self.writer_registered
            && data.len() <= DIRECT_WRITE_LIMIT
        {
            match try_send_bytes(self.fd, data) {
                Ok(sent) if sent == data.len() => return Ok(()),
                Ok(sent) => {
                    self.queue_write_bytes(py, PyBytes::new(py, &data[sent..]).unbind(), 0)?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    self.record_profile_event("write_direct_would_block", data.len(), 0);
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

    fn queue_write_bytes(
        &mut self,
        py: Python<'_>,
        data: Py<PyBytes>,
        sent: usize,
    ) -> PyResult<()> {
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
                                self.record_profile_event(
                                    "write_queue_small_append",
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
                                let mut combined = Vec::with_capacity(existing_bytes.len() + len);
                                combined.extend_from_slice(existing_bytes);
                                combined.extend_from_slice(bytes);
                                last.data = PendingWriteData::Owned(combined);
                                self.record_profile_event(
                                    "write_queue_small_merge",
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
        self.record_profile_event("write_queue", queued, self.pending_write_bytes);
        Ok(())
    }

    pub(crate) fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport on_readable fd={} closing={} closed={} paused={} proto={}",
                self.fd,
                self.closing,
                self.closed,
                self.read_paused,
                self.protocol.bind(py).get_type().name()?
            );
        }
        if self.closing || self.closed || self.read_paused {
            return Ok(());
        }
        if self.buffered_protocol {
            if trace_stream_enabled() {
                eprintln!("stream-transport ssl-on_readable fd={}", self.fd);
            }
            loop {
                let dispatch = self.poll_ssl_readable(py)?;
                match dispatch {
                    ReadableDispatch::WouldBlock => break,
                    ReadableDispatch::Buffered {
                        buffer_updated,
                        nbytes,
                    } => {
                        let result = buffer_updated.bind(py).call1((nbytes,));
                        if let Err(err) = result {
                            return self.finish_close(py, Some(err.into_value(py).into_any()));
                        }
                        if trace_stream_enabled() {
                            eprintln!(
                                "stream-transport ssl-on_readable updated fd={} bytes={}",
                                self.fd, nbytes
                            );
                        }
                        if self.read_paused {
                            return Ok(());
                        }
                    }
                    ReadableDispatch::Eof { protocol } => {
                        protocol.bind(py).call_method0("eof_received")?;
                        return Ok(());
                    }
                    ReadableDispatch::Error { exc } => {
                        return self.finish_close(py, Some(exc));
                    }
                }
            }
            return Ok(());
        }
        if self.reader.is_none() {
            return self.on_readable_protocol(py);
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
                    self.record_profile_event("read", n, self.pending_write_bytes);
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

    fn on_readable_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        let protocol = self.protocol.clone_ref(py);
        loop {
            match recv_into(self.fd, &mut self.read_buffer) {
                Ok(0) => {
                    self.remove_reader(py)?;
                    let keep_open = protocol.bind(py).call_method0("eof_received")?;
                    if !keep_open.is_truthy()? {
                        self.finish_close(py, None)?;
                    }
                    return Ok(());
                }
                Ok(n) => {
                    if n == 0 {
                        return Ok(());
                    }
                    let payload = PyBytes::new(py, &self.read_buffer[..n]);
                    if let Err(err) = protocol.bind(py).call_method1("data_received", (payload,)) {
                        return self.finish_close(py, Some(err.into_value(py).into_any()));
                    }
                    if self.closed || self.read_paused {
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

    fn poll_ssl_readable(&mut self, py: Python<'_>) -> PyResult<ReadableDispatch> {
        if self.closing || self.closed || self.read_paused {
            return Ok(ReadableDispatch::WouldBlock);
        }
        let get_buffer = self
            .buffered_get_buffer
            .as_ref()
            .expect("buffered protocol must cache get_buffer");
        let buffer = get_buffer.bind(py).call1((self.read_buffer.len(),))?;
        let buffer = PyBuffer::<u8>::get(&buffer)?;
        let Some(cells) = buffer.as_mut_slice(py) else {
            return Ok(ReadableDispatch::Error {
                exc: PyRuntimeError::new_err(
                    "buffer_updated() requires a writable C-contiguous buffer",
                )
                .into_value(py)
                .into_any(),
            });
        };
        let slice = unsafe { cell_slice_as_bytes_mut(cells) };
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport ssl-read fd={} want={}",
                self.fd,
                slice.len()
            );
        }
        match recv_into(self.fd, slice) {
            Ok(0) => {
                self.remove_reader(py)?;
                Ok(ReadableDispatch::Eof {
                    protocol: self.protocol.clone_ref(py),
                })
            }
            Ok(n) => Ok(ReadableDispatch::Buffered {
                buffer_updated: self
                    .buffered_buffer_updated
                    .as_ref()
                    .expect("buffered protocol must cache buffer_updated")
                    .clone_ref(py),
                nbytes: n,
            }),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(ReadableDispatch::WouldBlock)
            }
            Err(err) => Ok(ReadableDispatch::Error {
                exc: PyRuntimeError::new_err(err.to_string())
                    .into_value(py)
                    .into_any(),
            }),
        }
    }

    pub(crate) fn on_writable(&mut self, py: Python<'_>) -> PyResult<()> {
        self.record_profile_event(
            "write_ready",
            self.pending_write_bytes,
            self.write_queue.len(),
        );
        self.write_waiting_for_writable = false;
        self.sync_interest(py)?;
        self.flush_write_phase(py)
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
        self.record_profile_event("write_phase_start", queued_before, self.write_queue.len());
        while !self.write_queue.is_empty() {
            match self.flush_write_once(py) {
                Ok(0) => {
                    self.write_waiting_for_writable = true;
                    self.sync_interest(py)?;
                    break;
                }
                Ok(sent) => {
                    phase_sent += sent;
                    self.record_profile_event("write_flush", sent, self.pending_write_bytes);
                    if trace_stream_enabled() {
                        eprintln!("stream-transport wrote fd={} bytes={}", self.fd, sent);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    self.write_waiting_for_writable = true;
                    self.sync_interest(py)?;
                    self.record_profile_event(
                        "write_would_block",
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
        self.record_profile_event("write_phase_end", phase_sent, self.pending_write_bytes);

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
        self.registry.bind(py).borrow().unregister_inner(self.fd);
        if self.reader_registered || self.writer_registered {
            self.disable_interest(py)?;
        }
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
            self.record_profile_event(
                if writable {
                    "write_interest_on"
                } else {
                    "write_interest_off"
                },
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
        self.record_profile_event(
            "pause_writing",
            self.pending_write_bytes,
            self.write_queue.len(),
        );
        Ok(())
    }

    fn maybe_resume_protocol(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.protocol_paused || self.pending_write_bytes > WRITE_LOW_WATER {
            return Ok(());
        }
        self.protocol.bind(py).call_method0("resume_writing")?;
        self.protocol_paused = false;
        self.record_profile_event(
            "resume_writing",
            self.pending_write_bytes,
            self.write_queue.len(),
        );
        self.queue_pending_drains(py, None)?;
        Ok(())
    }

    fn read_buffer_len(&self, buffer: *mut ffi::PyObject) -> PyResult<usize> {
        let total = bytearray_len(buffer)?;
        Ok(total.saturating_sub(self.read_buffer_offset))
    }

    fn maybe_compact_read_buffer(
        &mut self,
        py: Python<'_>,
        buffer: *mut ffi::PyObject,
    ) -> PyResult<()> {
        let offset = self.read_buffer_offset;
        if offset == 0 {
            return Ok(());
        }

        let total = bytearray_len(buffer)?;
        if offset >= total {
            if unsafe { ffi::PyByteArray_Resize(buffer, 0) } != 0 {
                return Err(PyErr::fetch(py));
            }
            self.read_buffer_offset = 0;
            return Ok(());
        }

        if offset < READ_BUFFER_COMPACT_THRESHOLD && offset * 2 < total {
            return Ok(());
        }

        let unread = total - offset;
        let src = unsafe { ffi::PyByteArray_AsString(buffer) };
        if src.is_null() {
            return Err(PyErr::fetch(py));
        }
        unsafe {
            ptr::copy(src.cast::<u8>().add(offset), src.cast::<u8>(), unread);
            if ffi::PyByteArray_Resize(buffer, unread as isize) != 0 {
                return Err(PyErr::fetch(py));
            }
        }
        self.read_buffer_offset = 0;
        Ok(())
    }

    fn consume_read_buffer_exact(
        &mut self,
        py: Python<'_>,
        buffer: *mut ffi::PyObject,
        size: usize,
    ) -> PyResult<Py<PyAny>> {
        let total = bytearray_len(buffer)?;
        let offset = self.read_buffer_offset;
        let unread = total
            .checked_sub(offset)
            .ok_or_else(|| PyRuntimeError::new_err("invalid read buffer offset"))?;
        if unread < size {
            return Err(PyRuntimeError::new_err("insufficient buffered data"));
        }
        let src = unsafe { ffi::PyByteArray_AsString(buffer) };
        if src.is_null() {
            return Err(PyErr::fetch(py));
        }
        let data = PyBytes::new(py, unsafe {
            std::slice::from_raw_parts(src.cast::<u8>().add(offset), size)
        })
        .into_any()
        .unbind();
        self.read_buffer_offset = offset + size;
        self.maybe_compact_read_buffer(py, buffer)?;
        Ok(data)
    }

    fn consume_read_buffer_all(
        &mut self,
        py: Python<'_>,
        buffer: *mut ffi::PyObject,
    ) -> PyResult<Py<PyAny>> {
        let total = bytearray_len(buffer)?;
        let offset = self.read_buffer_offset;
        let unread = total
            .checked_sub(offset)
            .ok_or_else(|| PyRuntimeError::new_err("invalid read buffer offset"))?;
        let src = unsafe { ffi::PyByteArray_AsString(buffer) };
        if src.is_null() {
            return Err(PyErr::fetch(py));
        }
        let data = PyBytes::new(py, unsafe {
            std::slice::from_raw_parts(src.cast::<u8>().add(offset), unread)
        })
        .into_any()
        .unbind();
        self.read_buffer_offset = total;
        self.maybe_compact_read_buffer(py, buffer)?;
        Ok(data)
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
        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .as_ptr();
        append_to_bytearray(buffer, &self.read_buffer[..size])?;
        if !self.try_finish_pending_read(py, &reader)? && !had_pending_read {
            wake_waiter(py, &reader)?;
        }

        let reader_transport = reader.getattr("_transport")?;
        if reader_transport.is_none() || reader.getattr("_paused")?.is_truthy()? {
            return Ok(());
        }

        let buffered = self.read_buffer_len(buffer)?;
        if buffered <= 2 * self.limit {
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
        chunk_size: usize,
    ) -> PyResult<bool> {
        let Some(pending) = self.pending_read.as_ref() else {
            return Ok(false);
        };
        let PendingReadKind::Exact(size) = &pending.kind else {
            return Ok(false);
        };
        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .as_ptr();
        let buffered = self.read_buffer_len(buffer)?;
        if buffered + chunk_size < *size {
            return Ok(false);
        }

        let pending = self.pending_read.take().expect("pending read present");
        let PendingReadKind::Exact(size) = pending.kind else {
            unreachable!("checked above");
        };
        let needed_from_chunk = size.saturating_sub(buffered);
        if trace_stream_enabled() {
            eprintln!(
                "stream-transport finish chunk fd={} needed={} chunk={}",
                self.fd, needed_from_chunk, chunk_size
            );
        }
        let payload = if buffered == 0 && needed_from_chunk == size {
            PyBytes::new(py, &self.read_buffer[..size])
                .into_any()
                .unbind()
        } else {
            join_buffered_prefix_and_chunk(
                py,
                buffer,
                self.read_buffer_offset,
                buffered,
                &self.read_buffer[..needed_from_chunk],
                size,
            )?
        };
        reader.setattr("_waiter", py.None())?;
        self.queue_completion(
            py,
            StreamCompletion::ReadResult {
                reader: reader.clone().unbind(),
                waiter: pending.waiter,
                payload,
                resume_transport: true,
            },
        )?;
        self.record_profile_event("read_wait_done", size, buffered);

        self.read_buffer_offset += buffered;
        if needed_from_chunk != chunk_size {
            append_to_bytearray(buffer, &self.read_buffer[needed_from_chunk..chunk_size])?;
        }
        self.maybe_compact_read_buffer(py, buffer)?;
        self.maybe_resume_transport(py, reader)?;
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
        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .as_ptr();
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

        if self.read_buffer_len(buffer)? >= size {
            let data = self.consume_read_buffer_exact(py, buffer, size)?;
            self.maybe_resume_transport(py, &reader)?;
            return ready_awaitable_result(py, data);
        }

        if reader.getattr("_eof")?.is_truthy()? {
            let partial = self.consume_read_buffer_all(py, buffer)?;
            return ready_awaitable_exception(py, incomplete_read_error(py, partial, Some(size))?);
        }

        if self.pending_read.is_some() {
            return Err(PyRuntimeError::new_err(
                "readexactly() called while another coroutine is already waiting for incoming data",
            ));
        }

        self.resume_reader_if_paused(py, &reader)?;

        let scheduler = self
            .loop_obj
            .bind(py)
            .getattr("_scheduler")?
            .cast_into::<Scheduler>()?
            .unbind();
        let waiter = StreamWaiter::new(py, scheduler, self.loop_obj.clone_ref(py))?;
        reader.setattr("_waiter", waiter.bind(py))?;
        self.pending_read = Some(PendingRead {
            kind: PendingReadKind::Exact(size),
            waiter: waiter.clone_ref(py),
        });
        Ok(waiter.into_any())
    }

    fn readuntil_inner(
        &mut self,
        py: Python<'_>,
        separator: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let Some(reader_obj) = self.reader.as_ref().map(|reader| reader.clone_ref(py)) else {
            return Err(PyRuntimeError::new_err(
                "stream transport has no StreamReader attached",
            ));
        };
        let reader = reader_obj.bind(py);
        if let Some(exc) = current_exception(&reader)? {
            return ready_awaitable_exception(py, exc);
        }

        let separator = match separator.cast::<PyBytes>() {
            Ok(separator) => separator.clone().unbind(),
            Err(_) => {
                let Some(reader_readuntil) = self.reader_readuntil.as_ref() else {
                    return Err(PyRuntimeError::new_err(
                        "stream transport has no StreamReader.readuntil fallback",
                    ));
                };
                return Ok(reader_readuntil.bind(py).call1((separator,))?.unbind());
            }
        };
        let separator = separator.bind(py).as_bytes();
        if separator.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Separator should be at least one-byte string",
            ));
        }

        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .as_ptr();
        if let Some(index) =
            find_subsequence_in_bytearray(buffer, separator, self.read_buffer_offset)?
        {
            let consumed = index
                .checked_sub(self.read_buffer_offset)
                .ok_or_else(|| PyRuntimeError::new_err("invalid read buffer offset"))?;
            if consumed > self.limit {
                return ready_awaitable_exception(py, limit_overrun_error(py, consumed, true)?);
            }
            let size = consumed + separator.len();
            let data = self.consume_read_buffer_exact(py, buffer, size)?;
            self.maybe_resume_transport(py, &reader)?;
            return ready_awaitable_result(py, data);
        }

        let offset = next_readuntil_offset(self.read_buffer_len(buffer)?, separator.len());
        if offset > self.limit {
            return ready_awaitable_exception(py, limit_overrun_error(py, offset, false)?);
        }

        if reader.getattr("_eof")?.is_truthy()? {
            let partial = self.consume_read_buffer_all(py, buffer)?;
            return ready_awaitable_exception(py, incomplete_read_error(py, partial, None)?);
        }

        if self.pending_read.is_some() {
            return Err(PyRuntimeError::new_err(
                "readuntil() called while another coroutine is already waiting for incoming data",
            ));
        }

        self.resume_reader_if_paused(py, &reader)?;

        let scheduler = self
            .loop_obj
            .bind(py)
            .getattr("_scheduler")?
            .cast_into::<Scheduler>()?
            .unbind();
        let waiter = StreamWaiter::new(py, scheduler, self.loop_obj.clone_ref(py))?;
        reader.setattr("_waiter", waiter.bind(py))?;
        self.pending_read = Some(PendingRead {
            kind: PendingReadKind::Until {
                separator: separator.to_vec(),
                offset,
            },
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
        self.record_profile_event(
            "drain_wait",
            self.pending_write_bytes,
            self.pending_drains.len(),
        );
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
        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .as_ptr();
        let buffered = self.read_buffer_len(buffer)?;
        enum PendingReadState {
            Exact(usize),
            Until {
                found_index: Option<usize>,
                separator_len: usize,
                next_offset: usize,
            },
        }
        let state = match &pending.kind {
            PendingReadKind::Exact(size) => PendingReadState::Exact(*size),
            PendingReadKind::Until { separator, offset } => PendingReadState::Until {
                found_index: find_subsequence_in_bytearray(
                    buffer,
                    separator,
                    self.read_buffer_offset + *offset,
                )?,
                separator_len: separator.len(),
                next_offset: next_readuntil_offset(buffered, separator.len()),
            },
        };
        match state {
            PendingReadState::Exact(size) => {
                if buffered < size {
                    return Ok(false);
                }

                let pending = self.pending_read.take().expect("pending read present");
                if trace_stream_enabled() {
                    eprintln!("stream-transport finish exact fd={} size={}", self.fd, size);
                }
                let data = self.consume_read_buffer_exact(py, buffer, size)?;
                reader.setattr("_waiter", py.None())?;
                self.queue_completion(
                    py,
                    StreamCompletion::ReadResult {
                        reader: reader.clone().unbind(),
                        waiter: pending.waiter,
                        payload: data,
                        resume_transport: true,
                    },
                )?;
                self.record_profile_event("read_wait_done", size, buffered);
                Ok(true)
            }
            PendingReadState::Until {
                found_index,
                separator_len,
                next_offset,
            } => {
                if let Some(index) = found_index {
                    let pending = self.pending_read.take().expect("pending read present");
                    let consumed = index
                        .checked_sub(self.read_buffer_offset)
                        .ok_or_else(|| PyRuntimeError::new_err("invalid read buffer offset"))?;
                    if consumed > self.limit {
                        let exc = limit_overrun_error(py, consumed, true)?;
                        reader.setattr("_waiter", py.None())?;
                        self.queue_completion(
                            py,
                            StreamCompletion::ReadException {
                                reader: reader.clone().unbind(),
                                waiter: pending.waiter,
                                exception: exc,
                            },
                        )?;
                        return Ok(true);
                    }
                    let size = consumed + separator_len;
                    let data = self.consume_read_buffer_exact(py, buffer, size)?;
                    reader.setattr("_waiter", py.None())?;
                    self.queue_completion(
                        py,
                        StreamCompletion::ReadResult {
                            reader: reader.clone().unbind(),
                            waiter: pending.waiter,
                            payload: data,
                            resume_transport: true,
                        },
                    )?;
                    self.record_profile_event("read_wait_done", size, buffered);
                    return Ok(true);
                }
                if next_offset <= self.limit {
                    if let Some(pending) = self.pending_read.as_mut() {
                        if let PendingReadKind::Until { offset, .. } = &mut pending.kind {
                            *offset = next_offset;
                        }
                    }
                    return Ok(false);
                }
                let pending = self.pending_read.take().expect("pending read present");
                let exc = limit_overrun_error(py, next_offset, false)?;
                reader.setattr("_waiter", py.None())?;
                self.queue_completion(
                    py,
                    StreamCompletion::ReadException {
                        reader: reader.clone().unbind(),
                        waiter: pending.waiter,
                        exception: exc,
                    },
                )?;
                Ok(true)
            }
        }
    }

    fn fail_pending_read_eof(
        &mut self,
        py: Python<'_>,
        _reader: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let Some(pending) = self.pending_read.take() else {
            return Ok(());
        };
        let buffer = self
            .buffer
            .as_ref()
            .expect("stream transport buffer")
            .as_ptr();
        let partial = self.consume_read_buffer_all(py, buffer)?;
        let exc = match pending.kind {
            PendingReadKind::Exact(size) => incomplete_read_error(py, partial, Some(size))?,
            PendingReadKind::Until { .. } => incomplete_read_error(py, partial, None)?,
        };
        _reader.setattr("_waiter", py.None())?;
        self.queue_completion(
            py,
            StreamCompletion::ReadException {
                reader: _reader.clone().unbind(),
                waiter: pending.waiter,
                exception: exc,
            },
        )?;
        Ok(())
    }

    fn resume_reader_if_paused(
        &mut self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if !reader.getattr("_paused")?.is_truthy()? {
            return Ok(());
        }

        reader.setattr("_paused", false)?;
        if !self.read_paused || self.closed || self.closing {
            return Ok(());
        }
        if trace_stream_enabled() {
            eprintln!("stream-transport resume_reading fd={}", self.fd);
        }
        self.read_paused = false;
        self.sync_interest(py)
    }

    fn maybe_resume_transport(
        &mut self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if !reader.getattr("_paused")?.is_truthy()? {
            return Ok(());
        }
        let buffer = reader.getattr("_buffer")?.cast_into::<PyByteArray>()?;
        if bytearray_len(buffer.as_ptr())? > self.limit {
            return Ok(());
        }
        self.resume_reader_if_paused(py, reader)
    }

    fn queue_completion(&self, py: Python<'_>, completion: StreamCompletion) -> PyResult<()> {
        self.registry.bind(py).borrow().queue_completion(completion);
        Ok(())
    }

    fn queue_pending_drains(
        &mut self,
        py: Python<'_>,
        exception: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        if self.pending_drains.is_empty() {
            return Ok(());
        }
        self.record_profile_event(
            if exception.is_some() {
                "drain_fail"
            } else {
                "drain_ready"
            },
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
            "{{\"at_us\":{},\"kind\":\"{}\",\"stream_token\":{},\"fd\":{},\"size\":{},\"aux\":{},\"read_buffer_len\":{},\"read_buffer_offset\":{},\"pending_write_bytes\":{},\"state_flags\":{}}}",
            event.at_us,
            event.kind,
            event.stream_token,
            event.fd,
            event.size,
            event.aux,
            event.read_buffer_len,
            event.read_buffer_offset,
            event.pending_write_bytes,
            event.state_flags
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
    record_stream_profile_event_detail(kind, fd, size, aux, 0, 0, 0, 0, 0);
}

fn record_stream_profile_event_detail(
    kind: &'static str,
    fd: RawFd,
    size: usize,
    aux: usize,
    stream_token: u64,
    read_buffer_len: usize,
    read_buffer_offset: usize,
    pending_write_bytes: usize,
    state_flags: u32,
) {
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
        stream_token,
        fd,
        size,
        aux,
        read_buffer_len,
        read_buffer_offset,
        pending_write_bytes,
        state_flags,
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

static READUNTIL_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"readuntil".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_transport_readuntil_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native readuntil fast path backed by the Rsloop stream transport.".as_ptr(),
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

fn create_bound_readuntil(py: Python<'_>, slf: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            &READUNTIL_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
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

unsafe extern "C" fn stream_transport_readuntil_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let transport = Bound::from_borrowed_ptr(py, slf).cast_into_unchecked::<StreamTransport>();
        let core = transport.borrow().core.clone();
        let separator = Bound::<PyAny>::from_borrowed_ptr(py, arg);
        let result = core.borrow_mut().readuntil_inner(py, &separator);
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

unsafe fn cell_slice_as_bytes_mut(cells: &[std::cell::Cell<u8>]) -> &mut [u8] {
    std::slice::from_raw_parts_mut(cells.as_ptr() as *mut u8, cells.len())
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

fn find_subsequence_in_bytearray(
    bytearray: *mut ffi::PyObject,
    needle: &[u8],
    offset: usize,
) -> PyResult<Option<usize>> {
    unsafe {
        let len = ffi::PyByteArray_Size(bytearray);
        if len < 0 {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        if needle.is_empty() {
            return Ok(Some(0));
        }
        let len = len as usize;
        let offset = offset.min(len);
        if needle.len() > len.saturating_sub(offset) {
            return Ok(None);
        }
        let src = ffi::PyByteArray_AsString(bytearray) as *const u8;
        if src.is_null() {
            return Err(PyErr::fetch(Python::assume_attached()));
        }
        let haystack = std::slice::from_raw_parts(src, len);
        Ok(memmem::find(&haystack[offset..], needle).map(|index| index + offset))
    }
}

fn join_buffered_prefix_and_chunk(
    py: Python<'_>,
    bytearray: *mut ffi::PyObject,
    offset: usize,
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
            ptr::copy_nonoverlapping(src.add(offset), dest, buffered);
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
    expected: Option<usize>,
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

fn next_readuntil_offset(buffered: usize, separator_len: usize) -> usize {
    buffered.saturating_add(1).saturating_sub(separator_len)
}

fn limit_overrun_error(
    py: Python<'_>,
    consumed: usize,
    separator_found: bool,
) -> PyResult<Py<PyAny>> {
    static LIMIT_OVERRUN_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let cls = LIMIT_OVERRUN_ERROR.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.exceptions")?
            .getattr("LimitOverrunError")?
            .unbind())
    })?;
    let message = if separator_found {
        "Separator is found, but chunk is longer than limit"
    } else {
        "Separator is not found, and chunk exceed the limit"
    };
    Ok(cls.bind(py).call1((message, consumed))?.unbind())
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
        cls.bind(py)
            .call1((message,))?
            .into_any()
            .unbind()
            .into_bound(py)
            .into_any(),
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

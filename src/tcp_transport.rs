#![cfg(unix)]

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use crossbeam_channel::{Receiver, bounded};
use parking_lot::Mutex;
use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyCFunction, PyTuple};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::completion::{
    CompletionPortInner, current_completion_port, enqueue_callback, enqueue_completion,
    enqueue_exact_read, enqueue_native_exact_read, enqueue_native_read_error,
    enqueue_native_stream_eof, enqueue_native_stream_read, enqueue_stream_eof,
    enqueue_stream_read,
};
use crate::stream_transport::StreamTransport;
use crate::stream_reader::StreamReaderBridge;

const STREAM_READ_CHUNK: usize = 64 * 1024;

enum ReadCommand {
    StartCallback {
        callback: Py<PyAny>,
        eof_callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        size: usize,
    },
    StartBridge {
        bridge: Py<StreamReaderBridge>,
        error_callback: Py<PyAny>,
        size: usize,
    },
    StartStreamTransport {
        transport: Arc<Py<StreamTransport>>,
        size: usize,
    },
    ReadExact {
        future: Py<PyAny>,
        size: usize,
    },
    Stop,
    Close,
}

enum ReadMode {
    Callback {
        callback: Py<PyAny>,
        eof_callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        buffer: BytesMut,
    },
    Bridge {
        bridge: Arc<Py<StreamReaderBridge>>,
        error_callback: Py<PyAny>,
        buffer: BytesMut,
        exact_read: Option<ExactReadState>,
    },
    StreamTransport {
        transport: Arc<Py<StreamTransport>>,
        buffer: BytesMut,
        exact_read: Option<ExactReadState>,
    },
}

enum WriteCommand {
    Queue { data: Vec<u8> },
    AddDrainWaiter { future: Py<PyAny> },
    WriteEof,
    Abort { message: String },
    Close,
}

struct QueuedWrite {
    data: Vec<u8>,
    sent: usize,
}

#[derive(Default)]
struct WriteState {
    pending_bytes: AtomicUsize,
    queued_writes: AtomicUsize,
    inflight: AtomicBool,
    half_close_requested: AtomicBool,
    write_closed: AtomicBool,
    closing: AtomicBool,
    closed: AtomicBool,
    terminal_error: Mutex<Option<String>>,
}

struct TokioTcpTransportInner {
    fd: RawFd,
    closed: AtomicBool,
    write_open: Arc<AtomicBool>,
    write_state: Arc<WriteState>,
    read_tx: mpsc::UnboundedSender<ReadCommand>,
    write_tx: mpsc::UnboundedSender<WriteCommand>,
    task: JoinHandle<()>,
    done: Receiver<()>,
}

struct ExactReadState {
    future: Option<Py<PyAny>>,
    target: usize,
    buffered: BytesMut,
}

impl TokioTcpTransportInner {
    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.write_open.store(false, Ordering::Release);

        let _ = self.read_tx.send(ReadCommand::Close);
        let _ = self.write_tx.send(WriteCommand::Close);
        if self.done.recv_timeout(Duration::from_millis(100)).is_err() {
            self.task.abort();
        }
    }
}

impl Drop for TokioTcpTransportInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct TokioTcpTransportCore {
    inner: Arc<TokioTcpTransportInner>,
}

struct SyncPyMethodDef(ffi::PyMethodDef);

unsafe impl Sync for SyncPyMethodDef {}

static TRY_WRITE_BYTES_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"try_write_bytes".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: transport_try_write_bytes_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native bytes fast path for Tokio transport writes.".as_ptr(),
});

static WRITE_BYTES_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"write_bytes".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: transport_write_bytes_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native queued bytes path for Tokio transport writes.".as_ptr(),
});

static TRANSPORT_WRITE_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"write".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: transport_instance_write_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native common-case transport write path for Tokio streams.".as_ptr(),
});

#[pymethods]
impl TokioTcpTransportCore {
    #[new]
    fn new(py: Python<'_>, fd: i32) -> PyResult<Self> {
        let port = current_completion_port(py)?;
        let inner = spawn_tcp_transport_core(fd as RawFd, port)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn start_reading(
        &self,
        callback: Py<PyAny>,
        eof_callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::StartCallback {
                callback,
                eof_callback,
                error_callback,
                size,
            })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    fn start_stream_reader_bridge_reading(
        &self,
        py: Python<'_>,
        bridge: Py<StreamReaderBridge>,
        error_callback: Py<PyAny>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::StartBridge {
                bridge: bridge.clone_ref(py),
                error_callback: error_callback.clone_ref(py),
                size,
            })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    fn start_stream_transport_reading(
        &self,
        py: Python<'_>,
        transport: Py<StreamTransport>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::StartStreamTransport {
                transport: Arc::new(transport.clone_ref(py)),
                size,
            })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    fn stop_reading(&self) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::Stop)
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    fn readexactly(&self, future: Py<PyAny>, size: usize) -> PyResult<()> {
        self.readexactly_inner(future, size)
    }

    fn bind_try_write_bytes(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_cfunction(slf.py(), slf.as_any().as_unbound(), &TRY_WRITE_BYTES_DEF.0)
    }

    fn bind_write_bytes(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_cfunction(slf.py(), slf.as_any().as_unbound(), &WRITE_BYTES_DEF.0)
    }

    fn bind_transport_write(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        transport: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let context = PyTuple::new(py, [transport.bind(py), slf.as_any()])?;
        unsafe {
            let func = ffi::PyCFunction_NewEx(
                &TRANSPORT_WRITE_DEF.0 as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
                context.as_ptr(),
                std::ptr::null_mut(),
            );
            Ok(Bound::<PyAny>::from_owned_ptr_or_err(py, func)?
                .cast_into::<PyCFunction>()?
                .into_any()
                .unbind())
        }
    }

    fn write(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        let bytes = data.extract::<Vec<u8>>()?;
        if bytes.is_empty() {
            return Ok(self.inner.write_state.pending_bytes.load(Ordering::Acquire));
        }

        let len = bytes.len();
        let pending = queue_write(&self.inner.write_state, len)?;

        if self
            .inner
            .write_tx
            .send(WriteCommand::Queue { data: bytes })
            .is_err()
        {
            rollback_queued_write(&self.inner.write_state, len);
            self.inner.write_state.closed.store(true, Ordering::Release);
            return Err(PyOSError::new_err("tcp transport core is closed"));
        }

        Ok(pending)
    }

    fn try_write(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        ensure_can_try_write(&self.inner)?;

        if let Ok(bytes) = data.cast::<PyBytes>() {
            return try_send_bytes(self.inner.fd, bytes.as_bytes()).map_err(PyOSError::new_err);
        }
        if let Ok(bytearray) = data.cast::<PyByteArray>() {
            let bytes = unsafe { bytearray.as_bytes() };
            return try_send_bytes(self.inner.fd, bytes).map_err(PyOSError::new_err);
        }
        let bytes = data.extract::<Vec<u8>>()?;
        try_send_bytes(self.inner.fd, &bytes).map_err(PyOSError::new_err)
    }

    fn try_write_bytes(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        ensure_can_try_write(&self.inner)?;
        try_send_bytes(self.inner.fd, data.as_bytes()).map_err(PyOSError::new_err)
    }

    fn write_bytes(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        queue_write_bytes(&self.inner, data.as_bytes())
    }

    fn drain_waiter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let future = new_future(py)?;
        if let Some(message) = terminal_error(&self.inner.write_state) {
            let err = PyOSError::new_err(message).into_value(py);
            future.bind(py).call_method1("set_exception", (err,))?;
            return Ok(future.into_bound(py));
        }
        if drain_waiter_ready(&self.inner.write_state) {
            future.bind(py).call_method1("set_result", (py.None(),))?;
            return Ok(future.into_bound(py));
        }

        self.inner
            .write_tx
            .send(WriteCommand::AddDrainWaiter {
                future: future.clone_ref(py),
            })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))?;
        Ok(future.into_bound(py))
    }

    fn get_write_buffer_size(&self) -> usize {
        self.inner.write_state.pending_bytes.load(Ordering::Acquire)
    }

    fn write_eof(&self) -> PyResult<()> {
        let state = &self.inner.write_state;
        if state.closed.load(Ordering::Acquire) || state.write_closed.load(Ordering::Acquire) {
            return Ok(());
        }
        if state.half_close_requested.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.write_open.store(false, Ordering::Release);
        self.inner
            .write_tx
            .send(WriteCommand::WriteEof)
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    fn close(&self) {
        self.inner.close();
    }

    fn abort(&self, message: Option<String>) -> PyResult<()> {
        let message = message.unwrap_or_else(|| "tcp transport core aborted".to_owned());
        if self.inner.write_state.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner
            .write_state
            .closing
            .store(true, Ordering::Release);
        self.inner.write_open.store(false, Ordering::Release);

        self.inner
            .write_tx
            .send(WriteCommand::Abort { message })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }
}

impl TokioTcpTransportCore {
    pub(crate) fn readexactly_inner(&self, future: Py<PyAny>, size: usize) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::ReadExact { future, size })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    pub(crate) fn start_stream_reader_bridge_reading_inner(
        &self,
        py: Python<'_>,
        bridge: Py<StreamReaderBridge>,
        error_callback: Py<PyAny>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::StartBridge {
                bridge: bridge.clone_ref(py),
                error_callback: error_callback.clone_ref(py),
                size,
            })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    pub(crate) fn start_native_stream_reading_inner(
        &self,
        py: Python<'_>,
        transport: Py<StreamTransport>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::StartStreamTransport {
                transport: Arc::new(transport.clone_ref(py)),
                size,
            })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    pub(crate) fn stop_reading_inner(&self) -> PyResult<()> {
        self.inner
            .read_tx
            .send(ReadCommand::Stop)
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    pub(crate) fn write_inner(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        let bytes = data.extract::<Vec<u8>>()?;
        if bytes.is_empty() {
            return Ok(self.inner.write_state.pending_bytes.load(Ordering::Acquire));
        }

        let len = bytes.len();
        let pending = queue_write(&self.inner.write_state, len)?;

        if self
            .inner
            .write_tx
            .send(WriteCommand::Queue { data: bytes })
            .is_err()
        {
            rollback_queued_write(&self.inner.write_state, len);
            self.inner.write_state.closed.store(true, Ordering::Release);
            return Err(PyOSError::new_err("tcp transport core is closed"));
        }

        Ok(pending)
    }

    pub(crate) fn try_write_bytes_inner(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        ensure_can_try_write(&self.inner)?;
        try_send_bytes(self.inner.fd, data.as_bytes()).map_err(PyOSError::new_err)
    }

    pub(crate) fn write_bytes_inner(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        queue_write_bytes(&self.inner, data.as_bytes())
    }

    pub(crate) fn queue_bytes_inner(&self, data: &[u8]) -> PyResult<usize> {
        queue_write_bytes(&self.inner, data)
    }

    pub(crate) fn get_write_buffer_size_inner(&self) -> usize {
        self.inner.write_state.pending_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn write_eof_inner(&self) -> PyResult<()> {
        let state = &self.inner.write_state;
        if state.closed.load(Ordering::Acquire) || state.write_closed.load(Ordering::Acquire) {
            return Ok(());
        }
        if state.half_close_requested.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.write_open.store(false, Ordering::Release);
        self.inner
            .write_tx
            .send(WriteCommand::WriteEof)
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }

    pub(crate) fn close_inner(&self) {
        self.inner.close();
    }

    pub(crate) fn abort_inner(&self, message: Option<String>) -> PyResult<()> {
        let message = message.unwrap_or_else(|| "tcp transport core aborted".to_owned());
        if self.inner.write_state.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner
            .write_state
            .closing
            .store(true, Ordering::Release);
        self.inner.write_open.store(false, Ordering::Release);

        self.inner
            .write_tx
            .send(WriteCommand::Abort { message })
            .map_err(|_| PyOSError::new_err("tcp transport core is closed"))
    }
}

fn create_bound_cfunction(
    py: Python<'_>,
    slf: &Py<PyAny>,
    def: &'static ffi::PyMethodDef,
) -> PyResult<Py<PyAny>> {
    unsafe {
        let func = ffi::PyCFunction_NewEx(
            def as *const ffi::PyMethodDef as *mut ffi::PyMethodDef,
            slf.as_ptr(),
            std::ptr::null_mut(),
        );
        Ok(Bound::<PyAny>::from_owned_ptr_or_err(py, func)?
            .cast_into::<PyCFunction>()?
            .into_any()
            .unbind())
    }
}

unsafe extern "C" fn transport_try_write_bytes_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    transport_write_bytes_impl(slf, arg, true)
}

unsafe extern "C" fn transport_write_bytes_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    transport_write_bytes_impl(slf, arg, false)
}

unsafe extern "C" fn transport_instance_write_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<()> {
        let context = Bound::from_borrowed_ptr(py, slf).cast_into::<PyTuple>()?;
        let transport = context.get_item(0)?;
        let core = context
            .get_item(1)?
            .cast_into_unchecked::<TokioTcpTransportCore>()
            .unbind();
        let core_ref = core.borrow(py);
        let data = Bound::<PyAny>::from_borrowed_ptr(py, arg);

        let Ok(payload) = data.cast::<PyBytes>() else {
            return call_python_write_fallback(&transport, &data);
        };

        if payload.as_bytes().is_empty() {
            return Ok(());
        }
        if transport.getattr("_eof")?.is_truthy()?
            || !transport.getattr("_empty_waiter")?.is_none()
            || transport.getattr("_conn_lost")?.extract::<usize>()? != 0
            || transport.getattr("_closing")?.is_truthy()?
            || transport.getattr("_protocol_paused")?.is_truthy()?
            || unsafe { ffi::PyByteArray_Size(transport.getattr("_write_buffer")?.as_ptr()) } != 0
        {
            return call_python_write_fallback(&transport, &data);
        }

        let sent = match core_ref.try_write_bytes(payload.clone()) {
            Ok(sent) => sent,
            Err(err) => {
                if err.is_instance_of::<pyo3::exceptions::PySystemExit>(py)
                    || err.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py)
                {
                    return Err(err);
                }
                let exc = err.into_value(py);
                transport.call_method1(
                    "_native_fatal_error",
                    (exc.bind(py), "Fatal write error on socket transport"),
                )?;
                return Ok(());
            }
        };

        if sent != payload.as_bytes().len() {
            let remainder = PyBytes::new(py, &payload.as_bytes()[sent..]);
            core_ref.write_bytes(remainder)?;
            transport.call_method0("_maybe_pause_protocol")?;
            if transport.getattr("_protocol_paused")?.is_truthy()? {
                transport.call_method0("_request_native_write_notify")?;
            }
        }

        Ok(())
    })();

    match result {
        Ok(()) => ffi::Py_NewRef(ffi::Py_None()),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

unsafe fn transport_write_bytes_impl(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    immediate: bool,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<usize> {
        let core = Bound::from_borrowed_ptr(py, slf)
            .cast_into_unchecked::<TokioTcpTransportCore>()
            .unbind();
        let core_ref = core.borrow(py);
        let data = Bound::<PyAny>::from_borrowed_ptr(py, arg).cast_into::<PyBytes>()?;
        if immediate {
            core_ref.try_write_bytes(data)
        } else {
            core_ref.write_bytes(data)
        }
    })();

    match result {
        Ok(value) => ffi::PyLong_FromSize_t(value),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

fn new_future(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    Ok(event_loop.call_method0("create_future")?.unbind())
}

fn spawn_tcp_transport_core(
    fd: RawFd,
    port: Arc<CompletionPortInner>,
) -> PyResult<TokioTcpTransportInner> {
    configure_socket(fd).map_err(PyOSError::new_err)?;
    let stream = dup_tcp_std_stream(fd).map_err(PyOSError::new_err)?;
    let runtime = pyo3_async_runtimes::tokio::get_runtime();
    let _guard = runtime.enter();
    let stream = TcpStream::from_std(stream).map_err(PyOSError::new_err)?;

    let (read_tx, read_rx) = mpsc::unbounded_channel();
    let (write_tx, write_rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = bounded(1);
    let write_state = Arc::new(WriteState::default());
    let write_open = Arc::new(AtomicBool::new(true));

    let task_port = port.clone();
    let write_state_task = write_state.clone();
    let write_open_task = write_open.clone();
    let task = runtime.spawn(async move {
        run_stream_task(
            stream,
            task_port,
            read_rx,
            write_rx,
            write_state_task,
            write_open_task,
        )
        .await;
        let _ = done_tx.send(());
    });

    Ok(TokioTcpTransportInner {
        fd,
        closed: AtomicBool::new(false),
        write_open,
        write_state,
        read_tx,
        write_tx,
        task,
        done: done_rx,
    })
}

fn handle_read_command(command: ReadCommand, mode: &mut Option<ReadMode>) -> bool {
    match command {
        ReadCommand::StartCallback {
            callback,
            eof_callback,
            error_callback,
            size,
        } => {
            *mode = Some(ReadMode::Callback {
                callback,
                eof_callback,
                error_callback,
                buffer: BytesMut::with_capacity(size.min(STREAM_READ_CHUNK).max(1)),
            });
            false
        }
        ReadCommand::StartBridge {
            bridge,
            error_callback,
            size,
        } => {
            *mode = Some(ReadMode::Bridge {
                bridge: Arc::new(bridge),
                error_callback,
                buffer: BytesMut::with_capacity(size.min(STREAM_READ_CHUNK).max(1)),
                exact_read: None,
            });
            false
        }
        ReadCommand::StartStreamTransport { transport, size } => {
            *mode = Some(ReadMode::StreamTransport {
                transport,
                buffer: BytesMut::with_capacity(size.min(STREAM_READ_CHUNK).max(1)),
                exact_read: None,
            });
            false
        }
        ReadCommand::ReadExact { future, size } => {
            if let Some(ReadMode::Bridge { exact_read, .. } | ReadMode::StreamTransport { exact_read, .. }) = mode.as_mut() {
                *exact_read = Some(ExactReadState {
                    future: Some(future),
                    target: size,
                    buffered: BytesMut::with_capacity(size),
                });
            }
            false
        }
        ReadCommand::Stop => {
            *mode = None;
            false
        }
        ReadCommand::Close => {
            *mode = None;
            false
        }
    }
}

fn enqueue_read_error(port: &CompletionPortInner, mode: ReadMode, err: io::Error) {
    match mode {
        ReadMode::Callback { error_callback, .. } | ReadMode::Bridge { error_callback, .. } => {
            Python::attach(|py| {
                let exc = PyOSError::new_err(err.to_string()).into_value(py);
                enqueue_callback(port, error_callback, exc.into());
            });
        }
        ReadMode::StreamTransport { transport, .. } => Python::attach(|py| {
            let exc = PyOSError::new_err(err.to_string()).into_value(py).into_any();
            enqueue_native_read_error(port, transport, exc);
        }),
    }
}

fn handle_exact_read_chunk(
    port: &CompletionPortInner,
    bridge: Option<Arc<Py<StreamReaderBridge>>>,
    transport: Option<Arc<Py<StreamTransport>>>,
    exact_read: &mut ExactReadState,
    chunk: &[u8],
) {
    let needed = exact_read.target.saturating_sub(exact_read.buffered.len());
    if chunk.len() >= needed {
        exact_read.buffered.extend_from_slice(&chunk[..needed]);
        let future = exact_read.future.take().expect("exact read future present");
        let payload = std::mem::take(&mut exact_read.buffered).freeze();
        Python::attach(|py| {
            let payload = PyBytes::new(py, &payload).into_any().unbind();
            if let Some(bridge) = bridge.as_ref() {
                enqueue_exact_read(port, bridge.clone(), future, payload, false);
            } else if let Some(transport) = transport.as_ref() {
                enqueue_native_exact_read(port, transport.clone(), future, payload, false);
            }
        });
        if chunk.len() > needed {
            let remainder = Bytes::copy_from_slice(&chunk[needed..]);
            if let Some(bridge) = bridge {
                enqueue_stream_read(port, bridge, remainder);
            } else if let Some(transport) = transport {
                enqueue_native_stream_read(port, transport, remainder);
            }
        }
        exact_read.target = 0;
        return;
    }

    exact_read.buffered.extend_from_slice(chunk);
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

fn try_read_into(stream: &TcpStream, buffer: &mut BytesMut) -> io::Result<usize> {
    let chunk = STREAM_READ_CHUNK.max(1);
    if buffer.capacity() < chunk {
        buffer.reserve(chunk - buffer.capacity());
    }
    let spare = buffer.spare_capacity_mut();
    let read_len = stream.try_read(unsafe {
        std::slice::from_raw_parts_mut(spare.as_ptr() as *mut u8, spare.len())
    })?;
    unsafe {
        buffer.set_len(read_len);
    }
    if read_len == 0 {
        return Ok(0);
    }
    Ok(read_len)
}

async fn run_stream_task(
    stream: TcpStream,
    port: Arc<CompletionPortInner>,
    mut read_rx: mpsc::UnboundedReceiver<ReadCommand>,
    mut write_rx: mpsc::UnboundedReceiver<WriteCommand>,
    state: Arc<WriteState>,
    write_open: Arc<AtomicBool>,
) {
    let stream = stream;
    let fd = stream.as_raw_fd();
    let mut mode = None::<ReadMode>;
    let mut queue = VecDeque::<QueuedWrite>::new();
    let mut current = None::<QueuedWrite>;
    let mut drain_waiters = VecDeque::<Py<PyAny>>::new();
    let mut abort_message = None::<String>;
    let mut closing = false;

    loop {
        if current.is_none() && !queue.is_empty() {
            promote_next_write(&state, &mut queue, &mut current);
        }

        if current.is_none() {
            if let Err(err) = maybe_finish_half_close(fd, &state) {
                abort_message = Some(err.to_string());
                write_open.store(false, Ordering::Release);
                mode = None;
                fail_all_waiters(&port, &state, drain_waiters, queue, abort_message.clone());
                break;
            }
            if drain_waiter_ready(&state) && !drain_waiters.is_empty() {
                resolve_drain_waiters(&port, std::mem::take(&mut drain_waiters));
            }
        }

        if closing && queue.is_empty() && current.is_none() && drain_waiters.is_empty() {
            break;
        }

        tokio::select! {
            biased;
            maybe_read_command = read_rx.recv() => {
                match maybe_read_command {
                    Some(command) => {
                        handle_read_command(command, &mut mode);
                    }
                    None => {
                        mode = None;
                    }
                }
            }
            maybe_write_command = write_rx.recv() => {
                match maybe_write_command {
                    Some(command) => {
                        if handle_write_command(
                            command,
                            &port,
                            &state,
                            &mut queue,
                            &mut drain_waiters,
                            &mut closing,
                            &mut abort_message,
                            &mut mode,
                        ) {
                            write_open.store(false, Ordering::Release);
                            fail_all_waiters(&port, &state, drain_waiters, queue, abort_message.clone());
                            break;
                        }
                    }
                    None => {
                        closing = true;
                        mode = None;
                    }
                }
            }
            result = stream.readable(), if mode.is_some() => {
                if result.is_err() {
                    if let Some(mode) = mode.take() {
                        enqueue_read_error(&port, mode, io::Error::last_os_error());
                    }
                    break;
                }
                if let Some(active) = mode.as_mut() {
                    match active {
                        ReadMode::Callback {
                            callback,
                            eof_callback,
                            buffer,
                            ..
                        } => match try_read_into(&stream, buffer) {
                            Ok(0) => {
                                let eof_callback = Python::attach(|py| eof_callback.clone_ref(py));
                                mode = None;
                                Python::attach(|py| enqueue_callback(&port, eof_callback, py.None()));
                            }
                            Ok(_n) => {
                                let chunk = buffer.split().freeze();
                                Python::attach(|py| {
                                    let payload = PyBytes::new(py, &chunk).into_any().unbind();
                                    enqueue_callback(&port, callback.clone_ref(py), payload);
                                });
                            }
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                            Err(err) => {
                                if let Some(mode) = mode.take() {
                                    enqueue_read_error(&port, mode, err);
                                }
                                break;
                            }
                        },
                        ReadMode::Bridge {
                            bridge,
                            exact_read,
                            buffer,
                            ..
                        } => match try_read_into(&stream, buffer) {
                            Ok(0) => {
                                let bridge = bridge.clone();
                                let exact_read = exact_read.take();
                                mode = None;
                                if let Some(exact_read) = exact_read {
                                    Python::attach(|py| {
                                        let partial = PyBytes::new(py, &exact_read.buffered)
                                            .into_any()
                                            .unbind();
                                        let exc = incomplete_read_error(py, partial, exact_read.target)
                                            .expect("IncompleteReadError");
                                        enqueue_exact_read(
                                            &port,
                                            bridge,
                                            exact_read.future.expect("exact read future present"),
                                            exc,
                                            true,
                                        );
                                    });
                                } else {
                                    enqueue_stream_eof(&port, bridge);
                                }
                            }
                            Ok(_n) => {
                                let chunk = buffer.split().freeze();
                                if let Some(state) = exact_read.as_mut() {
                                    handle_exact_read_chunk(
                                        &port,
                                        Some(bridge.clone()),
                                        None,
                                        state,
                                        &chunk,
                                    );
                                    if state.target == 0 {
                                        *exact_read = None;
                                    }
                                } else {
                                    enqueue_stream_read(&port, bridge.clone(), chunk);
                                }
                            }
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                            Err(err) => {
                                if let Some(mode) = mode.take() {
                                    enqueue_read_error(&port, mode, err);
                                }
                                break;
                            }
                        },
                        ReadMode::StreamTransport {
                            transport,
                            exact_read,
                            buffer,
                        } => match try_read_into(&stream, buffer) {
                            Ok(0) => {
                                let transport_obj = transport.clone();
                                let exact_read = exact_read.take();
                                mode = None;
                                Python::attach(|py| {
                                    if let Some(exact_read) = exact_read {
                                        let partial = PyBytes::new(py, &exact_read.buffered)
                                            .into_any()
                                            .unbind();
                                        let exc = incomplete_read_error(py, partial, exact_read.target)
                                            .expect("IncompleteReadError");
                                        enqueue_native_exact_read(
                                            &port,
                                            transport_obj,
                                            exact_read.future.expect("exact read future present"),
                                            exc,
                                            true,
                                        );
                                    } else {
                                        enqueue_native_stream_eof(&port, transport_obj);
                                    }
                                });
                            }
                            Ok(_n) => {
                                let transport_obj = transport.clone();
                                let chunk = buffer.split().freeze();
                                if let Some(state) = exact_read.as_mut() {
                                    handle_exact_read_chunk(
                                        &port,
                                        None,
                                        Some(transport_obj),
                                        state,
                                        &chunk,
                                    );
                                    if state.target == 0 {
                                        *exact_read = None;
                                    }
                                } else {
                                    enqueue_native_stream_read(&port, transport_obj, chunk);
                                }
                            }
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                            Err(err) => {
                                if let Some(mode) = mode.take() {
                                    enqueue_read_error(&port, mode, err);
                                }
                                break;
                            }
                        },
                    }
                }
            }
            result = stream.writable(), if current.is_some() || !queue.is_empty() => {
                if let Err(err) = result {
                    abort_message = Some(err.to_string());
                    write_open.store(false, Ordering::Release);
                    fail_all_waiters(&port, &state, drain_waiters, queue, abort_message.clone());
                    break;
                }

                if let Err(err) = flush_writes_tokio(&stream, fd, &state, &mut queue, &mut current, &mut drain_waiters, &port) {
                    abort_message = Some(err.to_string());
                    write_open.store(false, Ordering::Release);
                    fail_all_waiters(&port, &state, drain_waiters, queue, abort_message.clone());
                    break;
                }
            }
        }
    }

    state.closed.store(true, Ordering::Release);
    state.closing.store(true, Ordering::Release);
    state.pending_bytes.store(0, Ordering::Release);
    if let Some(message) = abort_message {
        *state.terminal_error.lock() = Some(message);
    }
}

fn handle_write_command(
    command: WriteCommand,
    port: &CompletionPortInner,
    state: &Arc<WriteState>,
    queue: &mut VecDeque<QueuedWrite>,
    drain_waiters: &mut VecDeque<Py<PyAny>>,
    closing: &mut bool,
    abort_message: &mut Option<String>,
    mode: &mut Option<ReadMode>,
) -> bool {
    match command {
        WriteCommand::Queue { data } => {
            if !data.is_empty() {
                queue.push_back(QueuedWrite { data, sent: 0 });
            }
            false
        }
        WriteCommand::AddDrainWaiter { future } => {
            if drain_waiter_ready(state) {
                Python::attach(|py| enqueue_completion(port, future, py.None(), false));
            } else {
                drain_waiters.push_back(future);
            }
            false
        }
        WriteCommand::WriteEof => false,
        WriteCommand::Abort { message } => {
            *closing = true;
            *mode = None;
            state.closing.store(true, Ordering::Release);
            abort_message.get_or_insert(message);
            true
        }
        WriteCommand::Close => {
            *closing = true;
            *mode = None;
            state.half_close_requested.store(true, Ordering::Release);
            false
        }
    }
}

fn ensure_can_write(state: &WriteState) -> PyResult<()> {
    if state.closed.load(Ordering::Acquire) || state.closing.load(Ordering::Acquire) {
        return Err(PyRuntimeError::new_err(
            "tcp transport core is closed for new writes",
        ));
    }
    if state.half_close_requested.load(Ordering::Acquire)
        || state.write_closed.load(Ordering::Acquire)
    {
        return Err(PyRuntimeError::new_err(
            "Cannot call write() after write_eof()",
        ));
    }
    Ok(())
}

fn ensure_can_try_write(inner: &TokioTcpTransportInner) -> PyResult<()> {
    if inner.closed.load(Ordering::Acquire) || !inner.write_open.load(Ordering::Acquire) {
        return Err(PyOSError::new_err("tcp transport core is closed"));
    }
    Ok(())
}

fn queue_write(state: &WriteState, len: usize) -> PyResult<usize> {
    ensure_can_write(state)?;
    state.queued_writes.fetch_add(1, Ordering::AcqRel);
    let pending = state.pending_bytes.fetch_add(len, Ordering::AcqRel) + len;
    if let Err(err) = ensure_can_write(state) {
        rollback_queued_write(state, len);
        return Err(err);
    }
    Ok(pending)
}

fn queue_write_bytes(inner: &TokioTcpTransportInner, bytes: &[u8]) -> PyResult<usize> {
    if bytes.is_empty() {
        return Ok(inner.write_state.pending_bytes.load(Ordering::Acquire));
    }

    let len = bytes.len();
    let pending = queue_write(&inner.write_state, len)?;

    if inner
        .write_tx
        .send(WriteCommand::Queue {
            data: bytes.to_vec(),
        })
        .is_err()
    {
        rollback_queued_write(&inner.write_state, len);
        inner.write_state.closed.store(true, Ordering::Release);
        return Err(PyOSError::new_err("tcp transport core is closed"));
    }

    Ok(pending)
}

fn call_python_write_fallback(
    transport: &Bound<'_, PyAny>,
    data: &Bound<'_, PyAny>,
) -> PyResult<()> {
    transport
        .getattr("_python_write_fallback")?
        .call1((data,))?;
    Ok(())
}


fn rollback_queued_write(state: &WriteState, len: usize) {
    state.pending_bytes.fetch_sub(len, Ordering::AcqRel);
    state.queued_writes.fetch_sub(1, Ordering::AcqRel);
}

fn terminal_error(state: &WriteState) -> Option<String> {
    state.terminal_error.lock().clone()
}

fn try_send_bytes(fd: RawFd, bytes: &[u8]) -> io::Result<usize> {
    let result = unsafe { libc::send(fd, bytes.as_ptr().cast(), bytes.len(), send_flags()) };
    if result >= 0 {
        return Ok(result as usize);
    }
    let err = io::Error::last_os_error();
    if err.kind() == io::ErrorKind::WouldBlock {
        Ok(0)
    } else {
        Err(err)
    }
}

fn drain_waiter_ready(state: &WriteState) -> bool {
    state.pending_bytes.load(Ordering::Acquire) == 0
        && !state.inflight.load(Ordering::Acquire)
        && (!state.half_close_requested.load(Ordering::Acquire)
            || state.write_closed.load(Ordering::Acquire))
        && state.terminal_error.lock().is_none()
}

fn promote_next_write(
    state: &Arc<WriteState>,
    queue: &mut VecDeque<QueuedWrite>,
    current: &mut Option<QueuedWrite>,
) {
    if current.is_some() {
        return;
    }
    if let Some(next) = queue.pop_front() {
        state.queued_writes.fetch_sub(1, Ordering::AcqRel);
        state.inflight.store(true, Ordering::Release);
        *current = Some(next);
    }
}

fn flush_writes_tokio(
    stream: &TcpStream,
    fd: RawFd,
    state: &Arc<WriteState>,
    queue: &mut VecDeque<QueuedWrite>,
    current: &mut Option<QueuedWrite>,
    drain_waiters: &mut VecDeque<Py<PyAny>>,
    port: &CompletionPortInner,
) -> io::Result<()> {
    loop {
        if current.is_none() {
            promote_next_write(state, queue, current);
        }
        let Some(write) = current.as_mut() else {
            break;
        };
        let buffer = &write.data[write.sent..];
        match stream.try_write(buffer) {
            Ok(0) => break,
            Ok(written) => {
                write.sent += written;
                state.pending_bytes.fetch_sub(written, Ordering::AcqRel);
                if write.sent == write.data.len() {
                    state.inflight.store(false, Ordering::Release);
                    *current = None;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    if current.is_none() && queue.is_empty() {
        maybe_finish_half_close(fd, state)?;
        if drain_waiter_ready(state) && !drain_waiters.is_empty() {
            let waiters = std::mem::take(drain_waiters);
            resolve_drain_waiters(port, waiters);
        }
    }
    Ok(())
}

fn maybe_finish_half_close(fd: RawFd, state: &Arc<WriteState>) -> io::Result<()> {
    let should_shutdown = state.half_close_requested.load(Ordering::Acquire)
        && !state.write_closed.load(Ordering::Acquire)
        && state.pending_bytes.load(Ordering::Acquire) == 0
        && !state.inflight.load(Ordering::Acquire);
    if !should_shutdown {
        return Ok(());
    }
    let rc = unsafe { libc::shutdown(fd, libc::SHUT_WR) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    state.write_closed.store(true, Ordering::Release);
    Ok(())
}

fn resolve_drain_waiters(port: &CompletionPortInner, waiters: VecDeque<Py<PyAny>>) {
    for future in waiters {
        Python::attach(|py| enqueue_completion(port, future, py.None(), false));
    }
}

fn fail_all_waiters(
    port: &CompletionPortInner,
    state: &Arc<WriteState>,
    waiters: VecDeque<Py<PyAny>>,
    queue: VecDeque<QueuedWrite>,
    message: Option<String>,
) {
    let error_message = message.unwrap_or_else(|| "tcp transport core closed".to_owned());
    state.pending_bytes.store(0, Ordering::Release);
    state.queued_writes.store(0, Ordering::Release);
    state.inflight.store(false, Ordering::Release);
    state.closing.store(true, Ordering::Release);
    state.closed.store(true, Ordering::Release);
    *state.terminal_error.lock() = Some(error_message.clone());
    drop(queue);
    for future in waiters {
        Python::attach(|py| {
            let err = PyOSError::new_err(error_message.clone()).into_value(py);
            enqueue_completion(port, future, err.into(), true);
        });
    }
}

fn dup_tcp_std_stream(fd: RawFd) -> io::Result<std::net::TcpStream> {
    let dup_fd = unsafe { libc::dup(fd) };
    if dup_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    set_nonblocking(dup_fd)?;
    set_nodelay(dup_fd)?;
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(dup_fd) };
    std_stream.set_nonblocking(true)?;
    Ok(std_stream)
}

fn configure_socket(fd: RawFd) -> io::Result<()> {
    set_nodelay(fd)?;
    #[cfg(target_vendor = "apple")]
    {
        let enabled: libc::c_int = 1;
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn set_nodelay(fd: RawFd) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const fn send_flags() -> libc::c_int {
    libc::MSG_NOSIGNAL
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
const fn send_flags() -> libc::c_int {
    0
}

#![cfg(unix)]

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver};
use parking_lot::Mutex;
use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCFunction};
use pyo3::types::{PyByteArray, PyBytes};
use tokio::io::unix::AsyncFd;
use tokio::io::{Interest, Ready};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::completion::{current_completion_port, enqueue_completion, CompletionPortInner};

#[derive(Clone, Copy)]
struct FdRef(RawFd);

impl AsRawFd for FdRef {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

enum Command {
    Write { data: Vec<u8> },
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
struct WriteCoreState {
    pending_bytes: usize,
    queued_writes: usize,
    inflight: bool,
    half_close_requested: bool,
    write_closed: bool,
    closing: bool,
    closed: bool,
    terminal_error: Option<String>,
}

struct TransportWriteCoreInner {
    closed: AtomicBool,
    fd: RawFd,
    state: Arc<Mutex<WriteCoreState>>,
    tx: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
    done: Receiver<()>,
}

impl TransportWriteCoreInner {
    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let _ = self.tx.send(Command::Close);
        if self.done.recv_timeout(Duration::from_millis(100)).is_err() {
            self.task.abort();
        }
    }
}

impl Drop for TransportWriteCoreInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct TransportWriteCore {
    inner: Arc<TransportWriteCoreInner>,
}

struct SyncPyMethodDef(ffi::PyMethodDef);

unsafe impl Sync for SyncPyMethodDef {}

static TRY_WRITE_BYTES_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"try_write_bytes".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: transport_try_write_bytes_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native bytes fast path for transport writes.".as_ptr(),
});

static WRITE_BYTES_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"write_bytes".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: transport_write_bytes_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native queued bytes path for transport writes.".as_ptr(),
});

#[pymethods]
impl TransportWriteCore {
    #[new]
    fn new(py: Python<'_>, fd: i32) -> PyResult<Self> {
        let port = current_completion_port(py)?;
        let state = Arc::new(Mutex::new(WriteCoreState::default()));
        let inner = spawn_transport_write_core(fd as RawFd, port, state)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn bind_try_write_bytes(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_cfunction(slf.py(), slf.as_any().as_unbound(), &TRY_WRITE_BYTES_DEF.0)
    }

    fn bind_write_bytes(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_cfunction(slf.py(), slf.as_any().as_unbound(), &WRITE_BYTES_DEF.0)
    }

    fn write(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        let bytes = data.extract::<Vec<u8>>()?;
        if bytes.is_empty() {
            return Ok(self.inner.state.lock().pending_bytes);
        }

        let len = bytes.len();
        let pending = {
            let mut state = self.inner.state.lock();
            ensure_can_write(&state)?;
            state.pending_bytes += len;
            state.queued_writes += 1;
            state.pending_bytes
        };

        if self.inner.tx.send(Command::Write { data: bytes }).is_err() {
            let mut state = self.inner.state.lock();
            state.pending_bytes = state.pending_bytes.saturating_sub(len);
            state.queued_writes = state.queued_writes.saturating_sub(1);
            state.closed = true;
            return Err(PyOSError::new_err("transport write core is closed"));
        }

        Ok(pending)
    }

    fn try_write(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        {
            let state = self.inner.state.lock();
            ensure_can_write(&state)?;
        }

        if self.inner.closed.load(Ordering::Acquire) {
            return Err(PyOSError::new_err("transport write core is closed"));
        }

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
        {
            let state = self.inner.state.lock();
            ensure_can_write(&state)?;
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(PyOSError::new_err("transport write core is closed"));
        }
        try_send_bytes(self.inner.fd, data.as_bytes()).map_err(PyOSError::new_err)
    }

    fn write_eof(&self) -> PyResult<()> {
        {
            let mut state = self.inner.state.lock();
            if state.closed || state.write_closed || state.half_close_requested {
                return Ok(());
            }
            state.half_close_requested = true;
        }

        self.inner
            .tx
            .send(Command::WriteEof)
            .map_err(|_| PyOSError::new_err("transport write core is closed"))
    }

    fn abort(&self, message: Option<String>) -> PyResult<()> {
        let message = message.unwrap_or_else(|| "transport write core aborted".to_owned());
        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Ok(());
            }
            state.closing = true;
        }

        self.inner
            .tx
            .send(Command::Abort { message })
            .map_err(|_| PyOSError::new_err("transport write core is closed"))
    }

    fn drain_waiter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let future = new_future(py)?;
        {
            let state = self.inner.state.lock();
            if let Some(message) = state.terminal_error.as_ref() {
                let err = PyOSError::new_err(message.clone()).into_value(py);
                future.bind(py).call_method1("set_exception", (err,))?;
                return Ok(future.into_bound(py));
            }
            if drain_waiter_ready(&state) {
                future.bind(py).call_method1("set_result", (py.None(),))?;
                return Ok(future.into_bound(py));
            }
        }

        self.inner
            .tx
            .send(Command::AddDrainWaiter {
                future: future.clone_ref(py),
            })
            .map_err(|_| PyOSError::new_err("transport write core is closed"))?;

        Ok(future.into_bound(py))
    }

    fn get_write_buffer_size(&self) -> usize {
        self.inner.state.lock().pending_bytes
    }

    fn write_bytes(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        let bytes = data.as_bytes();
        if bytes.is_empty() {
            return Ok(self.inner.state.lock().pending_bytes);
        }

        let len = bytes.len();
        let pending = {
            let mut state = self.inner.state.lock();
            ensure_can_write(&state)?;
            state.pending_bytes += len;
            state.queued_writes += 1;
            state.pending_bytes
        };

        if self
            .inner
            .tx
            .send(Command::Write {
                data: bytes.to_vec(),
            })
            .is_err()
        {
            let mut state = self.inner.state.lock();
            state.pending_bytes = state.pending_bytes.saturating_sub(len);
            state.queued_writes = state.queued_writes.saturating_sub(1);
            state.closed = true;
            return Err(PyOSError::new_err("transport write core is closed"));
        }

        Ok(pending)
    }

    fn queued_write_count(&self) -> usize {
        self.inner.state.lock().queued_writes
    }

    fn has_inflight_write(&self) -> bool {
        self.inner.state.lock().inflight
    }

    fn half_close_requested(&self) -> bool {
        self.inner.state.lock().half_close_requested
    }

    fn write_closed(&self) -> bool {
        self.inner.state.lock().write_closed
    }

    fn is_idle(&self) -> bool {
        let state = self.inner.state.lock();
        state.pending_bytes == 0 && !state.inflight
    }

    fn is_closing(&self) -> bool {
        self.inner.state.lock().closing
    }

    fn is_closed(&self) -> bool {
        self.inner.state.lock().closed
    }

    fn stats(&self) -> (usize, usize, bool, bool, bool, bool, bool) {
        let state = self.inner.state.lock();
        (
            state.pending_bytes,
            state.queued_writes,
            state.inflight,
            state.half_close_requested,
            state.write_closed,
            state.closing,
            state.closed,
        )
    }

    fn close(&self) {
        {
            let mut state = self.inner.state.lock();
            state.closed = true;
            state.closing = true;
        }
        self.inner.close();
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

unsafe fn transport_write_bytes_impl(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    immediate: bool,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<usize> {
        let core = Bound::from_borrowed_ptr(py, slf)
            .cast_into_unchecked::<TransportWriteCore>()
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

fn ensure_can_write(state: &WriteCoreState) -> PyResult<()> {
    if state.closed || state.closing {
        return Err(PyRuntimeError::new_err(
            "transport write core is closed for new writes",
        ));
    }
    if state.half_close_requested || state.write_closed {
        return Err(PyRuntimeError::new_err(
            "Cannot call write() after write_eof()",
        ));
    }
    Ok(())
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

fn drain_waiter_ready(state: &WriteCoreState) -> bool {
    state.pending_bytes == 0
        && !state.inflight
        && (!state.half_close_requested || state.write_closed)
        && state.terminal_error.is_none()
}

fn spawn_transport_write_core(
    fd: RawFd,
    port: Arc<CompletionPortInner>,
    state: Arc<Mutex<WriteCoreState>>,
) -> PyResult<TransportWriteCoreInner> {
    configure_socket(fd).map_err(PyOSError::new_err)?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = bounded(1);

    let task_state = state.clone();
    let task = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let async_fd = match AsyncFd::with_interest(FdRef(fd), Interest::WRITABLE) {
            Ok(async_fd) => async_fd,
            Err(err) => {
                fail_all_waiters(
                    &port,
                    &task_state,
                    VecDeque::new(),
                    VecDeque::new(),
                    Some(format!("transport write core init failed: {err}")),
                );
                let _ = done_tx.send(());
                return;
            }
        };

        let mut queue = VecDeque::<QueuedWrite>::new();
        let mut current = None::<QueuedWrite>;
        let mut drain_waiters = VecDeque::<Py<PyAny>>::new();
        let mut closing = false;
        let mut abort_message = None::<String>;

        loop {
            if closing && queue.is_empty() && current.is_none() && drain_waiters.is_empty() {
                break;
            }

            if current.is_none() && !queue.is_empty() {
                promote_next_write(&task_state, &mut queue, &mut current);
            }

            if current.is_none() {
                if let Err(err) = maybe_finish_half_close(fd, &task_state) {
                    abort_message = Some(err.to_string());
                    fail_all_waiters(
                        &port,
                        &task_state,
                        drain_waiters,
                        queue,
                        abort_message.clone(),
                    );
                    break;
                }

                if drain_waiter_ready(&task_state.lock()) && !drain_waiters.is_empty() {
                    resolve_drain_waiters(&port, drain_waiters);
                    drain_waiters = VecDeque::new();
                }

                match rx.recv().await {
                    Some(command) => {
                        if handle_command(
                            command,
                            &port,
                            &task_state,
                            &mut queue,
                            &mut drain_waiters,
                            &mut closing,
                            &mut abort_message,
                        ) {
                            fail_all_waiters(
                                &port,
                                &task_state,
                                drain_waiters,
                                queue,
                                abort_message.clone(),
                            );
                            break;
                        }
                    }
                    None => {
                        abort_message.get_or_insert_with(|| "transport write core closed".to_owned());
                        fail_all_waiters(
                            &port,
                            &task_state,
                            drain_waiters,
                            queue,
                            abort_message.clone(),
                        );
                        break;
                    }
                }
                continue;
            }

            tokio::select! {
                biased;
                maybe_command = rx.recv() => {
                    match maybe_command {
                        Some(command) => {
                            if handle_command(
                                command,
                                &port,
                                &task_state,
                                &mut queue,
                                &mut drain_waiters,
                                &mut closing,
                                &mut abort_message,
                            ) {
                                fail_all_waiters(
                                    &port,
                                    &task_state,
                                    drain_waiters,
                                    queue,
                                    abort_message.clone(),
                                );
                                break;
                            }
                        }
                        None => {
                            abort_message.get_or_insert_with(|| "transport write core closed".to_owned());
                            fail_all_waiters(
                                &port,
                                &task_state,
                                drain_waiters,
                                queue,
                                abort_message.clone(),
                            );
                            break;
                        }
                    }
                }
                result = async_fd.ready(Interest::WRITABLE) => {
                    let mut guard = match result {
                        Ok(guard) => guard,
                        Err(err) => {
                            abort_message = Some(err.to_string());
                            fail_all_waiters(
                                &port,
                                &task_state,
                                drain_waiters,
                                queue,
                                abort_message.clone(),
                            );
                            break;
                        }
                    };

                    if let Err(err) = flush_writes(
                        fd,
                        &task_state,
                        &mut queue,
                        &mut current,
                        &mut drain_waiters,
                        &port,
                    ) {
                        abort_message = Some(err.to_string());
                        fail_all_waiters(
                            &port,
                            &task_state,
                            drain_waiters,
                            queue,
                            abort_message.clone(),
                        );
                        break;
                    }

                    guard.clear_ready_matching(Ready::WRITABLE);
                }
            }
        }

        {
            let mut state = task_state.lock();
            state.closed = true;
            state.closing = true;
            if let Some(message) = abort_message {
                state.terminal_error = Some(message);
            }
        }

        let _ = done_tx.send(());
    });

    Ok(TransportWriteCoreInner {
        closed: AtomicBool::new(false),
        fd,
        state,
        tx,
        task,
        done: done_rx,
    })
}

fn handle_command(
    command: Command,
    port: &CompletionPortInner,
    state: &Arc<Mutex<WriteCoreState>>,
    queue: &mut VecDeque<QueuedWrite>,
    drain_waiters: &mut VecDeque<Py<PyAny>>,
    closing: &mut bool,
    abort_message: &mut Option<String>,
) -> bool {
    match command {
        Command::Write { data } => {
            if !data.is_empty() {
                queue.push_back(QueuedWrite { data, sent: 0 });
            }
            false
        }
        Command::AddDrainWaiter { future } => {
            if drain_waiter_ready(&state.lock()) {
                Python::attach(|py| enqueue_completion(port, future, py.None(), false));
            } else {
                drain_waiters.push_back(future);
            }
            false
        }
        Command::WriteEof => false,
        Command::Abort { message } => {
            *closing = true;
            *abort_message = Some(message);
            true
        }
        Command::Close => {
            *closing = true;
            abort_message.get_or_insert_with(|| "transport write core closed".to_owned());
            true
        }
    }
}

fn promote_next_write(
    state: &Arc<Mutex<WriteCoreState>>,
    queue: &mut VecDeque<QueuedWrite>,
    current: &mut Option<QueuedWrite>,
) {
    if current.is_some() {
        return;
    }
    if let Some(next) = queue.pop_front() {
        let mut state = state.lock();
        state.queued_writes = state.queued_writes.saturating_sub(1);
        state.inflight = true;
        *current = Some(next);
    }
}

fn flush_writes(
    fd: RawFd,
    state: &Arc<Mutex<WriteCoreState>>,
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
        let result = unsafe { libc::send(fd, buffer.as_ptr().cast(), buffer.len(), send_flags()) };

        if result > 0 {
            let written = result as usize;
            write.sent += written;
            let mut snapshot = state.lock();
            snapshot.pending_bytes = snapshot.pending_bytes.saturating_sub(written);
            if write.sent == write.data.len() {
                snapshot.inflight = false;
                *current = None;
            }
            continue;
        }

        if result == 0 {
            break;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            break;
        }

        return Err(err);
    }

    if current.is_none() && queue.is_empty() {
        maybe_finish_half_close(fd, state)?;
        if drain_waiter_ready(&state.lock()) && !drain_waiters.is_empty() {
            let waiters = std::mem::take(drain_waiters);
            resolve_drain_waiters(port, waiters);
        }
    }

    Ok(())
}

fn maybe_finish_half_close(fd: RawFd, state: &Arc<Mutex<WriteCoreState>>) -> io::Result<()> {
    let should_shutdown = {
        let state = state.lock();
        state.half_close_requested
            && !state.write_closed
            && state.pending_bytes == 0
            && !state.inflight
    };

    if !should_shutdown {
        return Ok(());
    }

    let rc = unsafe { libc::shutdown(fd, libc::SHUT_WR) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    let mut state = state.lock();
    state.write_closed = true;
    Ok(())
}

fn resolve_drain_waiters(port: &CompletionPortInner, waiters: VecDeque<Py<PyAny>>) {
    for future in waiters {
        Python::attach(|py| enqueue_completion(port, future, py.None(), false));
    }
}

fn fail_all_waiters(
    port: &CompletionPortInner,
    state: &Arc<Mutex<WriteCoreState>>,
    waiters: VecDeque<Py<PyAny>>,
    queue: VecDeque<QueuedWrite>,
    message: Option<String>,
) {
    let error_message = message.unwrap_or_else(|| "transport write core closed".to_owned());
    {
        let mut state = state.lock();
        state.pending_bytes = 0;
        state.queued_writes = 0;
        state.inflight = false;
        state.closing = true;
        state.closed = true;
        state.terminal_error = Some(error_message.clone());
    }

    drop(queue);

    for future in waiters {
        Python::attach(|py| {
            let err = PyOSError::new_err(error_message.clone()).into_value(py);
            enqueue_completion(port, future, err.into(), true);
        });
    }
}

fn configure_socket(fd: RawFd) -> io::Result<()> {
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

#[cfg(any(target_os = "linux", target_os = "android"))]
const fn send_flags() -> libc::c_int {
    libc::MSG_NOSIGNAL
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
const fn send_flags() -> libc::c_int {
    0
}

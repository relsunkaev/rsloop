#![cfg(unix)]

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver};
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::io::unix::AsyncFd;
use tokio::io::{Interest, Ready};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::completion::{
    current_completion_port, enqueue_callback, enqueue_completion, enqueue_stream_eof,
    enqueue_stream_read, CompletionPortInner,
};
use crate::stream_reader::StreamReaderBridge;

const READABLE: u8 = 0x01;
const WRITABLE: u8 = 0x02;
const STREAM_READ_CHUNK: usize = 64 * 1024;

#[derive(Clone, Copy)]
struct FdRef(RawFd);

impl AsRawFd for FdRef {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

enum Command {
    Recv {
        future: Py<PyAny>,
        size: usize,
    },
    SendAll {
        future: Py<PyAny>,
        data: Vec<u8>,
    },
    StreamWrite {
        data: Vec<u8>,
    },
    AddStreamDrainWaiter {
        future: Py<PyAny>,
    },
    RequestHalfClose,
    StartReading {
        callback: Py<PyAny>,
        eof_callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        size: usize,
    },
    StartStreamReaderBridgeReading {
        bridge: Py<StreamReaderBridge>,
        error_callback: Py<PyAny>,
        size: usize,
    },
    StopReading,
    RequestWriteNotify {
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
    },
    SendAllNotify {
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        data: Vec<u8>,
    },
    Close,
}

struct ReadOp {
    future: Py<PyAny>,
    size: usize,
}

struct WriteOp {
    completion: WriteCompletion,
    data: Vec<u8>,
    sent: usize,
}

enum WriteCompletion {
    Future(Py<PyAny>),
    Callback {
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
    },
}

enum StreamRead {
    Callback {
        callback: Py<PyAny>,
        eof_callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        size: usize,
        buffer: Vec<u8>,
    },
    Bridge {
        bridge: Arc<Py<StreamReaderBridge>>,
        error_callback: Py<PyAny>,
        size: usize,
        buffer: Vec<u8>,
    },
}

struct StreamWrite {
    data: Vec<u8>,
    sent: usize,
}

struct StreamWriteNotify {
    callback: Py<PyAny>,
    error_callback: Py<PyAny>,
}

struct DriverActor {
    fd: RawFd,
    port: Arc<CompletionPortInner>,
    queued_write_bytes: Arc<AtomicUsize>,
    queued_write_bytes_delta: usize,
    reads: VecDeque<ReadOp>,
    writes: VecDeque<WriteOp>,
    stream_read: Option<StreamRead>,
    stream_writes: VecDeque<StreamWrite>,
    stream_write_notify: Option<StreamWriteNotify>,
    stream_drain_waiters: VecDeque<Py<PyAny>>,
    stream_half_close_requested: bool,
    closing: bool,
}

impl DriverActor {
    fn new(
        fd: RawFd,
        port: Arc<CompletionPortInner>,
        queued_write_bytes: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            fd,
            port,
            queued_write_bytes,
            queued_write_bytes_delta: 0,
            reads: VecDeque::new(),
            writes: VecDeque::new(),
            stream_read: None,
            stream_writes: VecDeque::new(),
            stream_write_notify: None,
            stream_drain_waiters: VecDeque::new(),
            stream_half_close_requested: false,
            closing: false,
        }
    }

    fn is_terminated(&self) -> bool {
        self.closing
            && self.reads.is_empty()
            && self.writes.is_empty()
            && self.stream_read.is_none()
            && self.stream_writes.is_empty()
    }

    fn current_interest(&self) -> u8 {
        current_interest(
            &self.reads,
            &self.writes,
            self.stream_writes.is_empty(),
            self.stream_read.is_some(),
        )
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Recv { future, size } => self.reads.push_back(ReadOp { future, size }),
            Command::SendAll { future, data } => {
                self.writes.push_back(WriteOp {
                    completion: WriteCompletion::Future(future),
                    data,
                    sent: 0,
                });
            }
            Command::StreamWrite { data } => {
                self.stream_writes.push_back(StreamWrite { data, sent: 0 });
            }
            Command::AddStreamDrainWaiter { future } => {
                if self.stream_writes.is_empty() {
                    Python::attach(|py| enqueue_completion(&self.port, future, py.None(), false));
                } else {
                    self.stream_drain_waiters.push_back(future);
                }
            }
            Command::RequestHalfClose => {
                self.stream_half_close_requested = true;
            }
            Command::StartReading {
                callback,
                eof_callback,
                error_callback,
                size,
            } => {
                self.stream_read = Some(StreamRead::Callback {
                    callback,
                    eof_callback,
                    error_callback,
                    size,
                    buffer: vec![0_u8; size.min(STREAM_READ_CHUNK).max(1)],
                });
            }
            Command::StartStreamReaderBridgeReading {
                bridge,
                error_callback,
                size,
            } => {
                self.stream_read = Some(StreamRead::Bridge {
                    bridge: Arc::new(bridge),
                    error_callback,
                    size,
                    buffer: vec![0_u8; size.min(STREAM_READ_CHUNK).max(1)],
                });
            }
            Command::StopReading => {
                self.stream_read = None;
            }
            Command::RequestWriteNotify {
                callback,
                error_callback,
            } => {
                if self.stream_writes.is_empty() {
                    Python::attach(|py| enqueue_callback(&self.port, callback, py.None()));
                } else {
                    self.stream_write_notify = Some(StreamWriteNotify {
                        callback,
                        error_callback,
                    });
                }
            }
            Command::SendAllNotify {
                callback,
                error_callback,
                data,
            } => {
                self.writes.push_back(WriteOp {
                    completion: WriteCompletion::Callback {
                        callback,
                        error_callback,
                    },
                    data,
                    sent: 0,
                });
            }
            Command::Close => {
                self.closing = true;
            }
        }
    }

    fn handle_command_message(&mut self, command: Option<Command>) -> bool {
        match command {
            Some(command) => {
                self.handle_command(command);
                true
            }
            None => {
                self.closing = true;
                false
            }
        }
    }

    fn flush_readable(&mut self) {
        flush_stream_reads(self.fd, &self.port, &mut self.stream_read);
        flush_reads(self.fd, &self.port, &mut self.reads);
    }

    fn flush_writable(&mut self) {
        let drained = flush_stream_writes(
            self.fd,
            &self.port,
            &mut self.stream_writes,
            &mut self.stream_write_notify,
            &mut self.stream_drain_waiters,
            &mut self.stream_half_close_requested,
            &self.queued_write_bytes,
        );
        self.queued_write_bytes_delta += drained;
        flush_writes(self.fd, &self.port, &mut self.writes);
    }

    fn publish_write_progress(&mut self) {
        if self.queued_write_bytes_delta != 0 {
            self.queued_write_bytes
                .fetch_sub(self.queued_write_bytes_delta, Ordering::AcqRel);
            self.queued_write_bytes_delta = 0;
        }
    }

    fn cancel_pending(mut self, message: &str) {
        self.publish_write_progress();
        cancel_pending(&self.port, self.reads, message);
        cancel_stream_writes(
            &self.port,
            self.stream_writes,
            self.stream_write_notify,
            self.stream_drain_waiters,
            &self.queued_write_bytes,
            message,
        );
        cancel_pending_writes(&self.port, self.writes, message);
    }
}

struct SocketDriverInner {
    closed: AtomicBool,
    fd: RawFd,
    queued_write_bytes: Arc<AtomicUsize>,
    tx: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
    done: Receiver<()>,
}

impl SocketDriverInner {
    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let _ = self.tx.send(Command::Close);
        if self.done.try_recv().is_err() {
            self.task.abort();
        }
    }
}

impl Drop for SocketDriverInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct SocketDriver {
    inner: Arc<SocketDriverInner>,
}

#[pymethods]
impl SocketDriver {
    #[new]
    fn new(py: Python<'_>, fd: i32) -> PyResult<Self> {
        let fd = fd as RawFd;
        let port = current_completion_port(py)?;
        let inner = spawn_socket_driver(fd, port)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn recv<'py>(&self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyAny>> {
        let future = new_future(py)?;
        self.inner
            .tx
            .send(Command::Recv {
                future: future.clone_ref(py),
                size,
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))?;
        Ok(future.into_bound(py))
    }

    fn sendall<'py>(
        &self,
        py: Python<'py>,
        data: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = data.extract::<Vec<u8>>()?;
        let future = new_future(py)?;
        self.inner
            .tx
            .send(Command::SendAll {
                future: future.clone_ref(py),
                data: bytes,
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))?;
        Ok(future.into_bound(py))
    }

    fn start_reading(
        &self,
        callback: Py<PyAny>,
        eof_callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::StartReading {
                callback,
                eof_callback,
                error_callback,
                size,
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    fn start_stream_reader_bridge_reading(
        &self,
        py: Python<'_>,
        bridge: Py<StreamReaderBridge>,
        error_callback: Py<PyAny>,
        size: usize,
    ) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::StartStreamReaderBridgeReading {
                bridge: bridge.clone_ref(py),
                error_callback: error_callback.clone_ref(py),
                size,
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    fn stream_write(&self, data: Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = data.extract::<Vec<u8>>()?;
        self.enqueue_stream_write(bytes).map(|_| ())
    }

    fn write(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        let bytes = data.extract::<Vec<u8>>()?;
        self.enqueue_stream_write(bytes)
    }

    fn try_write(&self, data: Bound<'_, PyAny>) -> PyResult<usize> {
        let bytes = data.extract::<Vec<u8>>()?;
        try_send_bytes(self.inner.fd, &bytes).map_err(PyOSError::new_err)
    }

    fn write_bytes(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        self.enqueue_stream_write(data.as_bytes().to_vec())
    }

    fn try_write_bytes(&self, data: Bound<'_, PyBytes>) -> PyResult<usize> {
        try_send_bytes(self.inner.fd, data.as_bytes()).map_err(PyOSError::new_err)
    }

    fn get_write_buffer_size(&self) -> usize {
        self.inner.queued_write_bytes.load(Ordering::Acquire)
    }

    fn drain_waiter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let future = new_future(py)?;
        if self.inner.queued_write_bytes.load(Ordering::Acquire) == 0 {
            future.bind(py).call_method1("set_result", (py.None(),))?;
            return Ok(future.into_bound(py));
        }

        self.inner
            .tx
            .send(Command::AddStreamDrainWaiter {
                future: future.clone_ref(py),
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))?;
        Ok(future.into_bound(py))
    }

    fn write_eof(&self) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::RequestHalfClose)
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    fn stop_reading(&self) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::StopReading)
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    fn request_write_notify(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
    ) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::RequestWriteNotify {
                callback: callback.clone_ref(py),
                error_callback: error_callback.clone_ref(py),
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    fn sendall_notify(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        data: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let bytes = data.extract::<Vec<u8>>()?;
        self.inner
            .tx
            .send(Command::SendAllNotify {
                callback: callback.clone_ref(py),
                error_callback: error_callback.clone_ref(py),
                data: bytes,
            })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    fn close(&self) {
        self.inner.close();
    }

    fn write_buffer_size(&self) -> usize {
        self.inner.queued_write_bytes.load(Ordering::Acquire)
    }
}

impl SocketDriver {
    pub(crate) fn fd(&self) -> RawFd {
        self.inner.fd
    }

    pub(crate) fn write_buffer_size_native(&self) -> usize {
        self.inner.queued_write_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn queue_recv_future(&self, future: Py<PyAny>, size: usize) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::Recv { future, size })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    pub(crate) fn queue_sendall_future(&self, future: Py<PyAny>, data: Vec<u8>) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::SendAll { future, data })
            .map_err(|_| PyOSError::new_err("socket driver is closed"))
    }

    pub(crate) fn queue_stream_write_bytes(&self, bytes: Vec<u8>) -> PyResult<usize> {
        self.enqueue_stream_write(bytes)
    }

    fn enqueue_stream_write(&self, bytes: Vec<u8>) -> PyResult<usize> {
        let len = bytes.len();
        if len == 0 {
            return Ok(self.inner.queued_write_bytes.load(Ordering::Acquire));
        }
        let pending = self
            .inner
            .queued_write_bytes
            .fetch_add(len, Ordering::AcqRel)
            + len;
        self.inner
            .tx
            .send(Command::StreamWrite { data: bytes })
            .map_err(|_| {
                self.inner
                    .queued_write_bytes
                    .fetch_sub(len, Ordering::AcqRel);
                PyOSError::new_err("socket driver is closed")
            })?;
        Ok(pending)
    }
}

fn new_future(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    Ok(event_loop.call_method0("create_future")?.unbind())
}

fn spawn_socket_driver(fd: RawFd, port: Arc<CompletionPortInner>) -> PyResult<SocketDriverInner> {
    configure_socket(fd).map_err(PyOSError::new_err)?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = bounded(1);
    let queued_write_bytes = Arc::new(AtomicUsize::new(0));
    let queued_write_bytes_task = queued_write_bytes.clone();

    let task = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let async_fd =
            match AsyncFd::with_interest(FdRef(fd), Interest::READABLE.add(Interest::WRITABLE)) {
                Ok(async_fd) => async_fd,
                Err(_) => {
                    let _ = done_tx.send(());
                    return;
                }
            };

        let mut actor = DriverActor::new(fd, port, queued_write_bytes_task);

        loop {
            if actor.is_terminated() {
                break;
            }

            let interest = actor.current_interest();

            if interest == 0 {
                if !actor.handle_command_message(rx.recv().await) {
                    break;
                }
                continue;
            }

            tokio::select! {
                biased;
                maybe_command = rx.recv() => {
                    actor.handle_command_message(maybe_command);
                }
                result = async_fd.ready(to_interest(interest)) => {
                    let mut guard = match result {
                        Ok(guard) => guard,
                        Err(_) => break,
                    };

                    let observed = ready_mask(guard.ready()) & interest;
                    if observed & READABLE != 0 {
                        actor.flush_readable();
                    }
                    if observed & WRITABLE != 0 {
                        actor.flush_writable();
                    }
                    if observed != 0 {
                        guard.clear_ready_matching(to_ready(observed));
                    }
                }
            }
            actor.publish_write_progress();
        }

        actor.cancel_pending("socket driver closed");
        let _ = done_tx.send(());
    });

    Ok(SocketDriverInner {
        closed: AtomicBool::new(false),
        fd,
        queued_write_bytes,
        tx,
        task,
        done: done_rx,
    })
}

fn flush_reads(fd: RawFd, port: &CompletionPortInner, reads: &mut VecDeque<ReadOp>) {
    while let Some(read) = reads.front() {
        let mut buffer = vec![0_u8; read.size];
        let result =
            unsafe { libc::recv(fd, buffer.as_mut_ptr().cast(), buffer.len(), recv_flags()) };

        if result >= 0 {
            let read = reads.pop_front().expect("front just checked");
            buffer.truncate(result as usize);
            Python::attach(|py| {
                let payload = PyBytes::new(py, &buffer).into_any().unbind();
                enqueue_completion(port, read.future, payload, false);
            });
            continue;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            break;
        }

        let read = reads.pop_front().expect("front just checked");
        enqueue_error(port, read.future, err);
    }
}

fn flush_stream_reads(fd: RawFd, port: &CompletionPortInner, stream_read: &mut Option<StreamRead>) {
    while let Some(read) = stream_read.as_mut() {
        let (buffer, size) = match read {
            StreamRead::Callback { buffer, size, .. } | StreamRead::Bridge { buffer, size, .. } => {
                (buffer, *size)
            }
        };
        let recv_len = buffer.len().min(size.max(1));
        let result = unsafe { libc::recv(fd, buffer.as_mut_ptr().cast(), recv_len, recv_flags()) };

        if result > 0 {
            let payload = buffer[..result as usize].to_vec();
            match read {
                StreamRead::Callback { callback, .. } => Python::attach(|py| {
                    let payload = PyBytes::new(py, &payload).into_any().unbind();
                    enqueue_callback(port, callback.clone_ref(py), payload);
                }),
                StreamRead::Bridge { bridge, .. } => {
                    enqueue_stream_read(port, bridge.clone(), payload.into());
                }
            }
            continue;
        }

        if result == 0 {
            let read = stream_read
                .take()
                .expect("stream read callback should be present");
            match read {
                StreamRead::Callback { eof_callback, .. } => {
                    Python::attach(|py| enqueue_callback(port, eof_callback, py.None()));
                }
                StreamRead::Bridge { bridge, .. } => {
                    enqueue_stream_eof(port, bridge);
                }
            }
            break;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            break;
        }

        let read = stream_read
            .take()
            .expect("stream read callback should be present");
        let error_callback = match read {
            StreamRead::Callback { error_callback, .. } => error_callback,
            StreamRead::Bridge { error_callback, .. } => error_callback,
        };
        enqueue_callback_error(port, error_callback, err);
    }
}

fn flush_stream_writes(
    fd: RawFd,
    port: &CompletionPortInner,
    stream_writes: &mut VecDeque<StreamWrite>,
    stream_write_notify: &mut Option<StreamWriteNotify>,
    stream_drain_waiters: &mut VecDeque<Py<PyAny>>,
    stream_half_close_requested: &mut bool,
    queued_write_bytes: &AtomicUsize,
) -> usize {
    let mut drained = 0;
    while let Some(write) = stream_writes.front_mut() {
        let buffer = &write.data[write.sent..];
        let result = unsafe { libc::send(fd, buffer.as_ptr().cast(), buffer.len(), send_flags()) };

        if result >= 0 {
            let sent = result as usize;
            write.sent += sent;
            drained += sent;
            if write.sent == write.data.len() {
                stream_writes.pop_front().expect("front just checked");
                if stream_writes.is_empty() {
                    if let Some(notify) = stream_write_notify.take() {
                        Python::attach(|py| enqueue_callback(port, notify.callback, py.None()));
                    }
                    while let Some(waiter) = stream_drain_waiters.pop_front() {
                        Python::attach(|py| enqueue_completion(port, waiter, py.None(), false));
                    }
                    if *stream_half_close_requested {
                        let _ = unsafe { libc::shutdown(fd, libc::SHUT_WR) };
                        *stream_half_close_requested = false;
                    }
                }
            }
            continue;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return drained;
        }

        cancel_stream_writes(
            port,
            std::mem::take(stream_writes),
            stream_write_notify.take(),
            std::mem::take(stream_drain_waiters),
            queued_write_bytes,
            &err.to_string(),
        );
        return drained;
    }
    drained
}

fn flush_writes(fd: RawFd, port: &CompletionPortInner, writes: &mut VecDeque<WriteOp>) {
    while let Some(write) = writes.front_mut() {
        let buffer = &write.data[write.sent..];
        let result = unsafe { libc::send(fd, buffer.as_ptr().cast(), buffer.len(), send_flags()) };

        if result >= 0 {
            write.sent += result as usize;
            if write.sent == write.data.len() {
                let write = writes.pop_front().expect("front just checked");
                match write.completion {
                    WriteCompletion::Future(future) => {
                        Python::attach(|py| enqueue_completion(port, future, py.None(), false))
                    }
                    WriteCompletion::Callback { callback, .. } => {
                        Python::attach(|py| enqueue_callback(port, callback, py.None()))
                    }
                }
            }
            continue;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            break;
        }

        let write = writes.pop_front().expect("front just checked");
        enqueue_write_error(port, write.completion, err);
    }
}

fn cancel_pending(port: &CompletionPortInner, reads: VecDeque<ReadOp>, message: &str) {
    let message = message.to_owned();
    for read in reads {
        Python::attach(|py| {
            let err = PyOSError::new_err(message.clone()).into_value(py);
            enqueue_completion(port, read.future, err.into(), true);
        });
    }
}

fn cancel_pending_writes(port: &CompletionPortInner, writes: VecDeque<WriteOp>, message: &str) {
    let message = message.to_owned();
    for write in writes {
        enqueue_write_error_message(port, write.completion, message.clone());
    }
}

fn cancel_stream_writes(
    port: &CompletionPortInner,
    stream_writes: VecDeque<StreamWrite>,
    stream_write_notify: Option<StreamWriteNotify>,
    stream_drain_waiters: VecDeque<Py<PyAny>>,
    queued_write_bytes: &AtomicUsize,
    message: &str,
) {
    let remaining = stream_writes
        .iter()
        .map(|write| write.data.len().saturating_sub(write.sent))
        .sum::<usize>();
    if remaining != 0 {
        queued_write_bytes.fetch_sub(remaining, Ordering::AcqRel);
    }
    if let Some(notify) = stream_write_notify {
        Python::attach(|py| {
            let err = PyOSError::new_err(message.to_owned()).into_value(py);
            enqueue_callback(port, notify.error_callback, err.into());
        });
    }
    for waiter in stream_drain_waiters {
        Python::attach(|py| {
            let err = PyOSError::new_err(message.to_owned()).into_value(py);
            enqueue_completion(port, waiter, err.into(), true);
        });
    }
}

fn enqueue_error(port: &CompletionPortInner, future: Py<PyAny>, err: io::Error) {
    Python::attach(|py| {
        let payload = PyOSError::new_err(err.to_string()).into_value(py);
        enqueue_completion(port, future, payload.into(), true);
    });
}

fn enqueue_callback_error(port: &CompletionPortInner, callback: Py<PyAny>, err: io::Error) {
    Python::attach(|py| {
        let payload = PyOSError::new_err(err.to_string()).into_value(py);
        enqueue_callback(port, callback, payload.into());
    });
}

fn enqueue_write_error(port: &CompletionPortInner, completion: WriteCompletion, err: io::Error) {
    match completion {
        WriteCompletion::Future(future) => enqueue_error(port, future, err),
        WriteCompletion::Callback { error_callback, .. } => {
            enqueue_callback_error(port, error_callback, err)
        }
    }
}

fn enqueue_write_error_message(
    port: &CompletionPortInner,
    completion: WriteCompletion,
    message: String,
) {
    match completion {
        WriteCompletion::Future(future) => {
            Python::attach(|py| {
                let err = PyOSError::new_err(message.clone()).into_value(py);
                enqueue_completion(port, future, err.into(), true);
            });
        }
        WriteCompletion::Callback { error_callback, .. } => {
            Python::attach(|py| {
                let err = PyOSError::new_err(message.clone()).into_value(py);
                enqueue_callback(port, error_callback, err.into());
            });
        }
    }
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

fn current_interest(
    reads: &VecDeque<ReadOp>,
    writes: &VecDeque<WriteOp>,
    stream_writes_empty: bool,
    stream_reading: bool,
) -> u8 {
    (if reads.is_empty() && !stream_reading {
        0
    } else {
        READABLE
    }) | (if writes.is_empty() && stream_writes_empty {
        0
    } else {
        WRITABLE
    })
}

fn ready_mask(ready: tokio::io::Ready) -> u8 {
    let mut mask = 0;
    if ready.is_readable() {
        mask |= READABLE;
    }
    if ready.is_writable() {
        mask |= WRITABLE;
    }
    mask
}

fn to_interest(mask: u8) -> Interest {
    match mask {
        READABLE => Interest::READABLE,
        WRITABLE => Interest::WRITABLE,
        mask if mask == READABLE | WRITABLE => Interest::READABLE.add(Interest::WRITABLE),
        _ => Interest::READABLE,
    }
}

fn to_ready(mask: u8) -> Ready {
    match mask {
        READABLE => Ready::READABLE,
        WRITABLE => Ready::WRITABLE,
        mask if mask == READABLE | WRITABLE => Ready::READABLE | Ready::WRITABLE,
        _ => Ready::EMPTY,
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

const fn recv_flags() -> libc::c_int {
    0
}

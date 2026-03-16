#![cfg(unix)]

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

use crate::stream_transport::StreamTransport;
use crate::stream_reader::StreamReaderBridge;

pub(crate) enum Completion {
    Future {
        future: Py<PyAny>,
        payload: Py<PyAny>,
        is_err: bool,
    },
    Callback {
        callback: Py<PyAny>,
        payload: Py<PyAny>,
    },
    StreamRead {
        bridge: Arc<Py<StreamReaderBridge>>,
        data: Bytes,
    },
    StreamEof {
        bridge: Arc<Py<StreamReaderBridge>>,
    },
    ExactRead {
        bridge: Arc<Py<StreamReaderBridge>>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        is_err: bool,
    },
    NativeStreamRead {
        transport: Arc<Py<StreamTransport>>,
        data: Bytes,
    },
    NativeStreamEof {
        transport: Arc<Py<StreamTransport>>,
    },
    NativeExactRead {
        transport: Arc<Py<StreamTransport>>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        is_err: bool,
    },
    NativeReadError {
        transport: Arc<Py<StreamTransport>>,
        payload: Py<PyAny>,
    },
}

pub(crate) struct CompletionPortInner {
    rx: Receiver<Completion>,
    tx: Sender<Completion>,
    read_fd: RawFd,
    write_fd: RawFd,
    closed: AtomicBool,
    notified: AtomicBool,
    direct_waiting: AtomicBool,
}

impl CompletionPortInner {
    fn new() -> io::Result<Self> {
        let (read_fd, write_fd) = create_pipe()?;
        let (tx, rx) = unbounded();

        Ok(Self {
            rx,
            tx,
            read_fd,
            write_fd,
            closed: AtomicBool::new(false),
            notified: AtomicBool::new(false),
            direct_waiting: AtomicBool::new(false),
        })
    }

    fn enqueue(&self, completion: Completion) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }

        if self.tx.send(completion).is_err() {
            return;
        }

        if self.direct_waiting.load(Ordering::Acquire) {
            return;
        }

        if !self.notified.swap(true, Ordering::AcqRel) {
            let byte = [1_u8];
            unsafe {
                let result = libc::write(self.write_fd, byte.as_ptr().cast(), byte.len());
                if result < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() != io::ErrorKind::WouldBlock {
                        let _ = err;
                    }
                }
            }
        }
    }

    fn drain_fd(&self) {
        let mut buffer = [MaybeUninit::<u8>::uninit(); 256];

        loop {
            let result =
                unsafe { libc::read(self.read_fd, buffer.as_mut_ptr().cast(), buffer.len()) };

            if result > 0 {
                continue;
            }

            if result == 0 {
                break;
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                break;
            }
            break;
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.notified.store(false, Ordering::Release);

        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }

        while self.rx.try_recv().is_ok() {}
    }
}

impl Drop for CompletionPortInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct CompletionPort {
    inner: Arc<CompletionPortInner>,
}

#[pymethods]
impl CompletionPort {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(CompletionPortInner::new().map_err(PyOSError::new_err)?),
        })
    }

    fn fileno(&self) -> i32 {
        self.inner.read_fd as i32
    }

    fn drain(&self) -> Vec<(u8, Py<PyAny>, Py<PyAny>, bool)> {
        drain_ready_completions(&self.inner)
            .into_iter()
            .map(|completion| match completion {
                Completion::Future {
                    future,
                    payload,
                    is_err,
                } => (0_u8, future, payload, is_err),
                Completion::Callback { callback, payload } => {
                    (1_u8, callback, payload, false)
                }
                Completion::StreamRead { bridge, data } => Python::attach(|py| {
                    let target = bridge
                        .as_ref()
                        .bind(py)
                        .getattr("feed_data")
                        .expect("stream bridge feed_data");
                    let payload = pyo3::types::PyBytes::new(py, &data).into_any().unbind();
                    (1_u8, target.unbind(), payload, false)
                }),
                Completion::StreamEof { bridge } => Python::attach(|py| {
                    let target = bridge
                        .as_ref()
                        .bind(py)
                        .getattr("feed_eof")
                        .expect("stream bridge feed_eof");
                    (1_u8, target.unbind(), py.None(), false)
                }),
                Completion::ExactRead {
                    bridge,
                    future,
                    payload,
                    is_err,
                } => Python::attach(|py| {
                    let target = bridge
                        .as_ref()
                        .bind(py)
                        .getattr("complete_exact_read")
                        .expect("stream bridge complete_exact_read");
                    let payload = pyo3::types::PyTuple::new(
                        py,
                        [
                            future.bind(py).clone().into_any(),
                            payload.bind(py).clone(),
                        ],
                    )
                    .expect("exact read tuple")
                    .into_any()
                    .unbind();
                    (2_u8, target.unbind(), payload, is_err)
                }),
                Completion::NativeStreamRead { transport, data } => Python::attach(|py| {
                    let target = transport
                        .as_ref()
                        .bind(py)
                        .getattr("_drain_feed_data")
                        .expect("stream transport _drain_feed_data");
                    let payload = pyo3::types::PyBytes::new(py, &data).into_any().unbind();
                    (1_u8, target.unbind(), payload, false)
                }),
                Completion::NativeStreamEof { transport } => Python::attach(|py| {
                    let target = transport
                        .as_ref()
                        .bind(py)
                        .getattr("_drain_feed_eof")
                        .expect("stream transport _drain_feed_eof");
                    (1_u8, target.unbind(), py.None(), false)
                }),
                Completion::NativeExactRead {
                    transport,
                    future,
                    payload,
                    is_err,
                } => Python::attach(|py| {
                    let target = transport
                        .as_ref()
                        .bind(py)
                        .getattr("_drain_complete_exact_read")
                        .expect("stream transport _drain_complete_exact_read");
                    let payload = pyo3::types::PyTuple::new(
                        py,
                        [
                            future.bind(py).clone().into_any(),
                            payload.bind(py).clone(),
                        ],
                    )
                    .expect("exact read tuple")
                    .into_any()
                    .unbind();
                    (2_u8, target.unbind(), payload, is_err)
                }),
                Completion::NativeReadError { transport, payload } => Python::attach(|py| {
                    let target = transport
                        .as_ref()
                        .bind(py)
                        .getattr("on_read_error")
                        .expect("stream transport on_read_error");
                    (1_u8, target.unbind(), payload, false)
                }),
            })
            .collect()
    }

    fn close(&self) {
        self.inner.close();
    }
}

impl CompletionPort {
    pub(crate) fn inner(&self) -> Arc<CompletionPortInner> {
        self.inner.clone()
    }

    pub(crate) fn fileno_inner(&self) -> RawFd {
        self.inner.read_fd
    }

    pub(crate) fn has_pending_inner(&self) -> bool {
        !self.inner.rx.is_empty()
    }

    pub(crate) fn wait_and_drain_inner(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Vec<Completion>> {
        let mut drained = Vec::new();
        self.wait_and_drain_into_inner(py, timeout, &mut drained)?;
        Ok(drained)
    }

    pub(crate) fn drain_inner(&self) -> Vec<Completion> {
        let mut drained = Vec::new();
        self.drain_into_inner(&mut drained);
        drained
    }

    pub(crate) fn wait_and_drain_into_inner(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
        drained: &mut Vec<Completion>,
    ) -> PyResult<()> {
        drained.clear();
        if matches!(timeout, Some(value) if value <= 0.0) || self.has_pending_inner() {
            self.drain_into_inner(drained);
            return Ok(());
        }
        py.detach(|| wait_and_drain_channel_into(&self.inner, timeout, drained))
            .map_err(PyOSError::new_err)
    }

    pub(crate) fn drain_into_inner(&self, drained: &mut Vec<Completion>) {
        drained.clear();
        drain_ready_completions_into(&self.inner, drained);
    }
}

fn wait_and_drain_channel(
    inner: &CompletionPortInner,
    timeout: Option<f64>,
) -> io::Result<Vec<Completion>> {
    let mut drained = Vec::new();
    wait_and_drain_channel_into(inner, timeout, &mut drained)?;
    Ok(drained)
}

fn wait_and_drain_channel_into(
    inner: &CompletionPortInner,
    timeout: Option<f64>,
    drained: &mut Vec<Completion>,
) -> io::Result<()> {
    let first = match timeout {
        None => {
            inner.direct_waiting.store(true, Ordering::Release);
            let result = inner
                .rx
                .recv()
                .map(Some)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "completion port closed"));
            inner.direct_waiting.store(false, Ordering::Release);
            result?
        }
        Some(timeout) if timeout <= 0.0 => match inner.rx.try_recv() {
            Ok(completion) => Some(completion),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "completion port closed",
                ));
            }
        },
        Some(timeout) => {
            inner.direct_waiting.store(true, Ordering::Release);
            let result = match inner
                .rx
                .recv_timeout(std::time::Duration::from_secs_f64(timeout))
            {
                Ok(completion) => Ok(Some(completion)),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => Ok(None),
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "completion port closed",
                )),
            };
            inner.direct_waiting.store(false, Ordering::Release);
            result?
        }
    };

    let Some(first) = first else {
        drained.clear();
        return Ok(());
    };

    drained.clear();
    drained.push(first);
    drain_ready_completions_into(inner, drained);

    Ok(())
}

fn drain_ready_completions(inner: &CompletionPortInner) -> Vec<Completion> {
    let mut drained = Vec::new();
    drain_ready_completions_into(inner, &mut drained);
    drained
}

fn drain_ready_completions_into(
    inner: &CompletionPortInner,
    drained: &mut Vec<Completion>,
) {
    loop {
        inner.drain_fd();
        while let Ok(completion) = inner.rx.try_recv() {
            drained.push(completion);
        }

        inner.notified.store(false, Ordering::Release);
        match inner.rx.try_recv() {
            Ok(completion) => {
                drained.push(completion);
                inner.notified.store(true, Ordering::Release);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
}

pub(crate) fn current_completion_port(py: Python<'_>) -> PyResult<Arc<CompletionPortInner>> {
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let port = event_loop.getattr("_kioto_completion_port")?;
    let port = port.extract::<PyRef<'_, CompletionPort>>()?;
    Ok(port.inner())
}

pub(crate) fn enqueue_completion(
    port: &CompletionPortInner,
    future: Py<PyAny>,
    payload: Py<PyAny>,
    is_err: bool,
) {
    port.enqueue(Completion::Future {
        future,
        payload,
        is_err,
    });
}

pub(crate) fn enqueue_callback(
    port: &CompletionPortInner,
    callback: Py<PyAny>,
    payload: Py<PyAny>,
) {
    port.enqueue(Completion::Callback { callback, payload });
}

pub(crate) fn enqueue_stream_read(
    port: &CompletionPortInner,
    bridge: Arc<Py<StreamReaderBridge>>,
    data: Bytes,
) {
    port.enqueue(Completion::StreamRead { bridge, data });
}

pub(crate) fn enqueue_stream_eof(
    port: &CompletionPortInner,
    bridge: Arc<Py<StreamReaderBridge>>,
) {
    port.enqueue(Completion::StreamEof { bridge });
}

pub(crate) fn enqueue_exact_read(
    port: &CompletionPortInner,
    bridge: Arc<Py<StreamReaderBridge>>,
    future: Py<PyAny>,
    payload: Py<PyAny>,
    is_err: bool,
) {
    port.enqueue(Completion::ExactRead {
        bridge,
        future,
        payload,
        is_err,
    });
}

pub(crate) fn enqueue_native_stream_read(
    port: &CompletionPortInner,
    transport: Arc<Py<StreamTransport>>,
    data: Bytes,
) {
    port.enqueue(Completion::NativeStreamRead { transport, data });
}

pub(crate) fn enqueue_native_stream_eof(
    port: &CompletionPortInner,
    transport: Arc<Py<StreamTransport>>,
) {
    port.enqueue(Completion::NativeStreamEof { transport });
}

pub(crate) fn enqueue_native_exact_read(
    port: &CompletionPortInner,
    transport: Arc<Py<StreamTransport>>,
    future: Py<PyAny>,
    payload: Py<PyAny>,
    is_err: bool,
) {
    port.enqueue(Completion::NativeExactRead {
        transport,
        future,
        payload,
        is_err,
    });
}

pub(crate) fn enqueue_native_read_error(
    port: &CompletionPortInner,
    transport: Arc<Py<StreamTransport>>,
    payload: Py<PyAny>,
) {
    port.enqueue(Completion::NativeReadError { transport, payload });
}

fn create_pipe() -> io::Result<(RawFd, RawFd)> {
    let mut fds = [0_i32; 2];
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    set_nonblocking(fds[0])?;
    set_nonblocking(fds[1])?;
    set_cloexec(fds[0])?;
    set_cloexec(fds[1])?;

    Ok((fds[0], fds[1]))
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

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

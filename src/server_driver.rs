#![cfg(unix)]

use std::collections::VecDeque;
use std::ffi::CStr;
use std::io;
use std::mem::{size_of, zeroed};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::{Receiver, bounded};
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::completion::{
    CompletionPortInner, current_completion_port, enqueue_callback, enqueue_completion,
};

#[derive(Clone, Copy)]
struct FdRef(RawFd);

impl AsRawFd for FdRef {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

enum Command {
    Accept {
        future: Py<PyAny>,
    },
    StartServing {
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        backlog: usize,
    },
    StopServing,
    Close,
}

enum AcceptDelivery {
    Future {
        max_accepts: usize,
    },
    Callback {
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        backlog: usize,
    },
}

struct ServingState {
    callback: Py<PyAny>,
    error_callback: Py<PyAny>,
    backlog: usize,
}

impl ServingState {
    fn new(callback: Py<PyAny>, error_callback: Py<PyAny>, backlog: usize) -> Self {
        Self {
            callback,
            error_callback,
            backlog: backlog.max(1),
        }
    }
}

#[derive(Default)]
struct ServerActorState {
    pending_accepts: VecDeque<Py<PyAny>>,
    serving: Option<ServingState>,
    closing: bool,
}

impl ServerActorState {
    fn is_idle(&self) -> bool {
        self.pending_accepts.is_empty() && self.serving.is_none()
    }

    fn should_exit(&self) -> bool {
        self.closing && self.is_idle()
    }

    fn apply_command(&mut self, command: Command) {
        match command {
            Command::Accept { future } => self.pending_accepts.push_back(future),
            Command::StartServing {
                callback,
                error_callback,
                backlog,
            } => {
                self.serving = Some(ServingState::new(callback, error_callback, backlog));
            }
            Command::StopServing => self.serving = None,
            Command::Close => self.closing = true,
        }
    }
}

struct ServerDriverInner {
    closed: AtomicBool,
    tx: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
    done: Receiver<()>,
}

impl ServerDriverInner {
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

impl Drop for ServerDriverInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct ServerDriver {
    inner: Arc<ServerDriverInner>,
}

#[pymethods]
impl ServerDriver {
    #[new]
    fn new(py: Python<'_>, fd: i32) -> PyResult<Self> {
        let port = current_completion_port(py)?;
        let inner = spawn_server_driver(fd as RawFd, port)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn accept<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let future = new_future(py)?;
        self.inner
            .tx
            .send(Command::Accept {
                future: future.clone_ref(py),
            })
            .map_err(|_| PyOSError::new_err("server driver is closed"))?;
        Ok(future.into_bound(py))
    }

    fn start_serving(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        error_callback: Py<PyAny>,
        backlog: usize,
    ) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::StartServing {
                callback: callback.clone_ref(py),
                error_callback: error_callback.clone_ref(py),
                backlog: backlog.max(1),
            })
            .map_err(|_| PyOSError::new_err("server driver is closed"))
    }

    fn stop_serving(&self) -> PyResult<()> {
        self.inner
            .tx
            .send(Command::StopServing)
            .map_err(|_| PyOSError::new_err("server driver is closed"))
    }

    fn close(&self) {
        self.inner.close();
    }
}

fn new_future(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    Ok(event_loop.call_method0("create_future")?.unbind())
}

fn spawn_server_driver(fd: RawFd, port: Arc<CompletionPortInner>) -> PyResult<ServerDriverInner> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = bounded(1);

    let task = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let async_fd = match AsyncFd::new(FdRef(fd)) {
            Ok(async_fd) => async_fd,
            Err(_) => {
                let _ = done_tx.send(());
                return;
            }
        };

        let mut state = ServerActorState::default();

        loop {
            if state.should_exit() {
                break;
            }

            if state.is_idle() && !state.closing {
                match rx.recv().await {
                    Some(command) => state.apply_command(command),
                    None => state.closing = true,
                }
                continue;
            }

            tokio::select! {
                biased;
                maybe_command = rx.recv() => {
                    match maybe_command {
                        Some(command) => state.apply_command(command),
                        None => state.closing = true,
                    }
                }
                result = async_fd.readable(), if !state.closing && !state.is_idle() => {
                    let mut guard = match result {
                        Ok(guard) => guard,
                        Err(_) => break,
                    };

                    flush_accepts(fd, &port, &mut state);
                    guard.clear_ready();
                }
            }
        }

        cancel_pending_accepts(&port, state.pending_accepts, "server driver closed");
        let _ = done_tx.send(());
    });

    Ok(ServerDriverInner {
        closed: AtomicBool::new(false),
        tx,
        task,
        done: done_rx,
    })
}

fn flush_accepts(fd: RawFd, port: &CompletionPortInner, state: &mut ServerActorState) {
    loop {
        let delivery =
            Python::attach(
                |py| match (state.pending_accepts.len(), state.serving.as_ref()) {
                    (pending, _) if pending > 0 => Some(AcceptDelivery::Future {
                        max_accepts: pending,
                    }),
                    (0, Some(serving)) => Some(AcceptDelivery::Callback {
                        callback: serving.callback.clone_ref(py),
                        error_callback: serving.error_callback.clone_ref(py),
                        backlog: serving.backlog,
                    }),
                    (0, None) => None,
                    _ => unreachable!("pending accepts should always take precedence"),
                },
            );
        let Some(delivery) = delivery else {
            return;
        };

        let max_accepts = match delivery {
            AcceptDelivery::Future { max_accepts } => max_accepts,
            AcceptDelivery::Callback { backlog, .. } => backlog,
        };

        let mut delivered = 0usize;
        while delivered < max_accepts {
            match accept_one(fd) {
                Ok((conn_fd, address)) => {
                    let payload = Python::attach(|py| accepted_payload(py, conn_fd, address));
                    match (&delivery, payload) {
                        (AcceptDelivery::Future { .. }, Ok(payload)) => {
                            let future = state
                                .pending_accepts
                                .pop_front()
                                .expect("future delivery should still be queued");
                            enqueue_completion(port, future, payload, false);
                        }
                        (AcceptDelivery::Callback { callback, .. }, Ok(payload)) => {
                            Python::attach(|py| {
                                enqueue_callback(port, callback.clone_ref(py), payload)
                            })
                        }
                        (AcceptDelivery::Future { .. }, Err(err)) => {
                            let future = state
                                .pending_accepts
                                .pop_front()
                                .expect("future delivery should still be queued");
                            Python::attach(|py| {
                                enqueue_completion(
                                    port,
                                    future,
                                    PyOSError::new_err(err.to_string()).into_value(py).into(),
                                    true,
                                )
                            });
                        }
                        (AcceptDelivery::Callback { error_callback, .. }, Err(err)) => {
                            Python::attach(|py| {
                                enqueue_callback(
                                    port,
                                    error_callback.clone_ref(py),
                                    PyOSError::new_err(err.to_string()).into_value(py).into(),
                                )
                            })
                        }
                    }
                    delivered += 1;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return,
                Err(err) => {
                    match &delivery {
                        AcceptDelivery::Future { .. } => {
                            if let Some(future) = state.pending_accepts.pop_front() {
                                Python::attach(|py| {
                                    enqueue_completion(
                                        port,
                                        future,
                                        PyOSError::new_err(err.to_string()).into_value(py).into(),
                                        true,
                                    )
                                });
                            }
                        }
                        AcceptDelivery::Callback { error_callback, .. } => Python::attach(|py| {
                            enqueue_callback(
                                port,
                                error_callback.clone_ref(py),
                                PyOSError::new_err(err.to_string()).into_value(py).into(),
                            )
                        }),
                    }
                    return;
                }
            }
        }

        if matches!(delivery, AcceptDelivery::Future { .. }) {
            return;
        }
    }
}

fn cancel_pending_accepts(
    port: &CompletionPortInner,
    pending_accepts: VecDeque<Py<PyAny>>,
    message: &str,
) {
    let message = message.to_owned();
    for future in pending_accepts {
        Python::attach(|py| {
            enqueue_completion(
                port,
                future,
                PyOSError::new_err(message.clone()).into_value(py).into(),
                true,
            )
        });
    }
}

#[derive(Clone)]
enum AcceptedAddress {
    Inet(SocketAddrV4),
    Inet6(SocketAddrV6),
    Unix(Option<String>),
    Unknown(i32),
}

fn accept_one(fd: RawFd) -> io::Result<(RawFd, AcceptedAddress)> {
    let mut storage = unsafe { zeroed::<libc::sockaddr_storage>() };
    let mut len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let conn_fd = unsafe {
        libc::accept(
            fd,
            (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
            &mut len,
        )
    };
    if conn_fd < 0 {
        return Err(io::Error::last_os_error());
    }

    set_nonblocking(conn_fd)?;
    set_cloexec(conn_fd)?;
    configure_socket(conn_fd)?;

    Ok((conn_fd, parse_address(&storage)))
}

fn accepted_payload(py: Python<'_>, fd: RawFd, address: AcceptedAddress) -> PyResult<Py<PyAny>> {
    let fd_obj = fd.into_pyobject(py)?.unbind().into_any();
    let addr_obj = match address {
        AcceptedAddress::Inet(addr) => (addr.ip().to_string(), addr.port())
            .into_pyobject(py)?
            .unbind()
            .into_any(),
        AcceptedAddress::Inet6(addr) => (
            addr.ip().to_string(),
            addr.port(),
            addr.flowinfo(),
            addr.scope_id(),
        )
            .into_pyobject(py)?
            .unbind()
            .into_any(),
        AcceptedAddress::Unix(Some(path)) => PyString::new(py, &path).into_any().unbind(),
        AcceptedAddress::Unix(None) => py.None(),
        AcceptedAddress::Unknown(family) => family.into_pyobject(py)?.unbind().into_any(),
    };

    Ok(PyTuple::new(py, [fd_obj, addr_obj])?.into_any().unbind())
}

fn parse_address(storage: &libc::sockaddr_storage) -> AcceptedAddress {
    match storage.ss_family as i32 {
        libc::AF_INET => {
            let addr =
                unsafe { &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            let ip = Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr));
            let port = u16::from_be(addr.sin_port);
            AcceptedAddress::Inet(SocketAddrV4::new(ip, port))
        }
        libc::AF_INET6 => {
            let addr = unsafe {
                &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>()
            };
            let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);
            let port = u16::from_be(addr.sin6_port);
            AcceptedAddress::Inet6(SocketAddrV6::new(
                ip,
                port,
                addr.sin6_flowinfo,
                addr.sin6_scope_id,
            ))
        }
        libc::AF_UNIX => {
            let addr =
                unsafe { &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_un>() };
            if addr.sun_path[0] == 0 {
                AcceptedAddress::Unix(None)
            } else {
                let path = unsafe { CStr::from_ptr(addr.sun_path.as_ptr()) };
                AcceptedAddress::Unix(Some(path.to_string_lossy().into_owned()))
            }
        }
        family => AcceptedAddress::Unknown(family),
    }
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

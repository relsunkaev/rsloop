#![cfg(unix)]

use std::cell::Cell;
use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBlockingIOError, PyInterruptedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};

use crate::poller::TokioPoller;
use crate::socket_registry::SocketStateRegistry;

struct PendingRecv {
    future: Py<PyAny>,
    size: usize,
}

struct PendingRecvInto {
    future: Py<PyAny>,
    buffer: Py<PyAny>,
}

struct PendingAccept {
    future: Py<PyAny>,
}

enum PendingRead {
    Recv(PendingRecv),
    RecvInto(PendingRecvInto),
    Accept(PendingAccept),
}

impl PendingRead {
    fn future(&self) -> &Py<PyAny> {
        match self {
            Self::Recv(op) => &op.future,
            Self::RecvInto(op) => &op.future,
            Self::Accept(op) => &op.future,
        }
    }
}

struct PendingSend {
    future: Py<PyAny>,
    data: Py<PyAny>,
    len: usize,
    sent: usize,
}

struct PendingConnect {
    future: Py<PyAny>,
}

#[pyclass(module = "kioto._kioto")]
pub struct SocketState {
    sock: Py<PyAny>,
    fd: RawFd,
    poller: Py<TokioPoller>,
    registry: Py<SocketStateRegistry>,
    read_buffer: Vec<u8>,
    reads: VecDeque<PendingRead>,
    writes: VecDeque<PendingSend>,
    connects: VecDeque<PendingConnect>,
    closed: bool,
}

#[pymethods]
impl SocketState {
    #[new]
    fn new(py: Python<'_>, sock: Py<PyAny>, loop_obj: Py<PyAny>) -> PyResult<Self> {
        let fd = sock.bind(py).call_method0("fileno")?.extract::<i32>()? as RawFd;
        let poller = loop_obj
            .bind(py)
            .getattr("_poller")?
            .extract::<Py<TokioPoller>>()?;
        let registry = loop_obj
            .bind(py)
            .getattr("_socket_registry")?
            .extract::<Py<SocketStateRegistry>>()?;
        Ok(Self {
            sock,
            fd,
            poller,
            registry,
            read_buffer: vec![0; 64 * 1024],
            reads: VecDeque::new(),
            writes: VecDeque::new(),
            connects: VecDeque::new(),
            closed: false,
        })
    }

    #[getter]
    fn sock(&self, py: Python<'_>) -> Py<PyAny> {
        self.sock.clone_ref(py)
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        self.close_inner(py)
    }

    fn enqueue_recv(&mut self, py: Python<'_>, future: Py<PyAny>, size: usize) -> PyResult<()> {
        self.reads
            .push_back(PendingRead::Recv(PendingRecv { future, size }));
        self.sync_interest(py)
    }

    fn enqueue_recv_into(
        &mut self,
        py: Python<'_>,
        future: Py<PyAny>,
        buffer: Py<PyAny>,
    ) -> PyResult<()> {
        // Validate once up front so the async path doesn't fail after registration.
        let _ = bytes_like_len(buffer.bind(py))?;
        self.reads
            .push_back(PendingRead::RecvInto(PendingRecvInto { future, buffer }));
        self.sync_interest(py)
    }

    fn enqueue_accept(&mut self, py: Python<'_>, future: Py<PyAny>) -> PyResult<()> {
        self.reads
            .push_back(PendingRead::Accept(PendingAccept { future }));
        self.sync_interest(py)
    }

    fn enqueue_sendall(
        &mut self,
        py: Python<'_>,
        future: Py<PyAny>,
        data: Bound<'_, PyAny>,
        sent: usize,
    ) -> PyResult<()> {
        let data_len = bytes_like_len(&data)?;
        let sent = sent.min(data_len);
        if sent == data_len {
            if !future_done(py, &future)? {
                future.bind(py).call_method1("set_result", (py.None(),))?;
            }
            return Ok(());
        }
        self.writes.push_back(PendingSend {
            future,
            data: data.unbind(),
            len: data_len,
            sent,
        });
        self.sync_interest(py)
    }

    fn enqueue_connect(
        &mut self,
        py: Python<'_>,
        future: Py<PyAny>,
        _address: Py<PyAny>,
    ) -> PyResult<()> {
        self.connects.push_back(PendingConnect { future });
        self.sync_interest(py)
    }
}

impl SocketState {
    pub(crate) fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        while let Some(op) = self.reads.front() {
            if future_done(py, op.future())? {
                self.reads.pop_front();
                continue;
            }

            match op {
                PendingRead::Recv(op) => {
                    let size = op.size.max(1);
                    if self.read_buffer.len() < size {
                        self.read_buffer.resize(size, 0);
                    }
                    match recv_into(self.fd, &mut self.read_buffer[..size]) {
                        Ok(0) => {
                            let payload = PyBytes::new(py, b"").unbind().into_any();
                            let PendingRead::Recv(op) =
                                self.reads.pop_front().expect("front above")
                            else {
                                unreachable!("recv variant changed while queued");
                            };
                            if !future_done(py, &op.future)? {
                                op.future
                                    .bind(py)
                                    .call_method1("set_result", (payload.bind(py),))?;
                            }
                        }
                        Ok(n) => {
                            let payload =
                                PyBytes::new(py, &self.read_buffer[..n]).unbind().into_any();
                            let PendingRead::Recv(op) =
                                self.reads.pop_front().expect("front above")
                            else {
                                unreachable!("recv variant changed while queued");
                            };
                            if !future_done(py, &op.future)? {
                                op.future
                                    .bind(py)
                                    .call_method1("set_result", (payload.bind(py),))?;
                            }
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) => {
                            let PendingRead::Recv(op) =
                                self.reads.pop_front().expect("front above")
                            else {
                                unreachable!("recv variant changed while queued");
                            };
                            if !future_done(py, &op.future)? {
                                op.future.bind(py).call_method1(
                                    "set_exception",
                                    (PyRuntimeError::new_err(err.to_string()).into_value(py),),
                                )?;
                            }
                        }
                    }
                }
                PendingRead::RecvInto(op) => match recv_into_object(py, self.fd, &op.buffer)? {
                    Ok(0) => {
                        let PendingRead::RecvInto(op) =
                            self.reads.pop_front().expect("front above")
                        else {
                            unreachable!("recv_into variant changed while queued");
                        };
                        if !future_done(py, &op.future)? {
                            op.future.bind(py).call_method1("set_result", (0,))?;
                        }
                    }
                    Ok(n) => {
                        let PendingRead::RecvInto(op) =
                            self.reads.pop_front().expect("front above")
                        else {
                            unreachable!("recv_into variant changed while queued");
                        };
                        if !future_done(py, &op.future)? {
                            op.future.bind(py).call_method1("set_result", (n,))?;
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) => {
                        let PendingRead::RecvInto(op) =
                            self.reads.pop_front().expect("front above")
                        else {
                            unreachable!("recv_into variant changed while queued");
                        };
                        if !future_done(py, &op.future)? {
                            op.future.bind(py).call_method1(
                                "set_exception",
                                (PyRuntimeError::new_err(err.to_string()).into_value(py),),
                            )?;
                        }
                    }
                },
                PendingRead::Accept(_) => match accept_socket(py, &self.sock) {
                    Ok(payload) => {
                        let PendingRead::Accept(op) = self.reads.pop_front().expect("front above")
                        else {
                            unreachable!("accept variant changed while queued");
                        };
                        if !future_done(py, &op.future)? {
                            op.future
                                .bind(py)
                                .call_method1("set_result", (payload.bind(py),))?;
                        }
                    }
                    Err(err)
                        if err.is_instance_of::<PyBlockingIOError>(py)
                            || err.is_instance_of::<PyInterruptedError>(py) =>
                    {
                        break;
                    }
                    Err(err) => {
                        let PendingRead::Accept(op) = self.reads.pop_front().expect("front above")
                        else {
                            unreachable!("accept variant changed while queued");
                        };
                        if !future_done(py, &op.future)? {
                            op.future
                                .bind(py)
                                .call_method1("set_exception", (err.into_value(py),))?;
                        }
                    }
                },
            }
        }
        self.sync_interest(py)
    }

    pub(crate) fn on_writable(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }

        while let Some(op) = self.connects.front() {
            if future_done(py, &op.future)? {
                self.connects.pop_front();
                continue;
            }
            match socket_error(self.fd) {
                Ok(0) => {
                    let op = self.connects.pop_front().expect("front above");
                    if !future_done(py, &op.future)? {
                        op.future
                            .bind(py)
                            .call_method1("set_result", (py.None(),))?;
                    }
                }
                Ok(err) => {
                    let op = self.connects.pop_front().expect("front above");
                    if !future_done(py, &op.future)? {
                        op.future.bind(py).call_method1(
                            "set_exception",
                            (pyo3::exceptions::PyOSError::new_err(err),),
                        )?;
                    }
                }
                Err(err) => {
                    let op = self.connects.pop_front().expect("front above");
                    if !future_done(py, &op.future)? {
                        op.future.bind(py).call_method1(
                            "set_exception",
                            (PyRuntimeError::new_err(err.to_string()).into_value(py),),
                        )?;
                    }
                }
            }
        }

        while let Some(op) = self.writes.front_mut() {
            if future_done(py, &op.future)? {
                self.writes.pop_front();
                continue;
            }
            match send_from_object(py, self.fd, &op.data, op.sent, op.len)? {
                Ok(0) => break,
                Ok(n) => {
                    op.sent += n;
                    if op.sent == op.len {
                        let op = self.writes.pop_front().expect("front above");
                        if !future_done(py, &op.future)? {
                            op.future
                                .bind(py)
                                .call_method1("set_result", (py.None(),))?;
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    let op = self.writes.pop_front().expect("front above");
                    if !future_done(py, &op.future)? {
                        op.future.bind(py).call_method1(
                            "set_exception",
                            (PyRuntimeError::new_err(err.to_string()).into_value(py),),
                        )?;
                    }
                }
            }
        }

        self.sync_interest(py)
    }

    fn close_inner(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.registry
            .bind(py)
            .borrow_mut()
            .unregister_inner(self.fd);
        self.poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fd, false, false)?;
        for op in self.reads.drain(..) {
            if !future_done(py, op.future())? {
                op.future().bind(py).call_method0("cancel")?;
            }
        }
        for op in self.writes.drain(..) {
            if !future_done(py, &op.future)? {
                op.future.bind(py).call_method0("cancel")?;
            }
        }
        for op in self.connects.drain(..) {
            if !future_done(py, &op.future)? {
                op.future.bind(py).call_method0("cancel")?;
            }
        }
        Ok(())
    }

    fn sync_interest(&self, py: Python<'_>) -> PyResult<()> {
        let readable = !self.closed && !self.reads.is_empty();
        let writable = !self.closed && (!self.writes.is_empty() || !self.connects.is_empty());
        self.poller
            .bind(py)
            .borrow()
            .set_interest_inner(self.fd, readable, writable)?;
        Ok(())
    }
}

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    future.bind(py).call_method0("done")?.is_truthy()
}

fn bytes_like_len(obj: &Bound<'_, PyAny>) -> PyResult<usize> {
    Ok(PyBuffer::<u8>::get(obj)?.len_bytes())
}

fn recv_into_object(
    py: Python<'_>,
    fd: RawFd,
    buffer_obj: &Py<PyAny>,
) -> PyResult<io::Result<usize>> {
    let buffer = PyBuffer::<u8>::get(&buffer_obj.bind(py))?;
    let Some(cells) = buffer.as_mut_slice(py) else {
        return Err(PyRuntimeError::new_err(
            "recv_into() requires a writable C-contiguous buffer",
        ));
    };
    let slice = unsafe { cell_slice_as_bytes_mut(cells) };
    Ok(recv_into(fd, slice))
}

fn send_from_object(
    py: Python<'_>,
    fd: RawFd,
    data: &Py<PyAny>,
    sent: usize,
    len: usize,
) -> PyResult<io::Result<usize>> {
    let buffer = PyBuffer::<u8>::get(&data.bind(py))?;
    let Some(cells) = buffer.as_slice(py) else {
        return Err(PyRuntimeError::new_err(
            "sock_sendall() requires a C-contiguous bytes-like object",
        ));
    };
    let bytes = unsafe { cell_slice_as_bytes(cells) };
    let end = len.min(bytes.len());
    if sent >= end {
        return Ok(Ok(0));
    }
    Ok(send_from(fd, &bytes[sent..end]))
}

fn accept_socket(py: Python<'_>, sock: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let accepted = sock.bind(py).call_method0("accept")?;
    let conn = accepted.get_item(0)?;
    conn.call_method1("setblocking", (false,))?;
    Ok(accepted.unbind())
}

fn recv_into(fd: RawFd, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        let n = unsafe { libc::recv(fd, buffer.as_mut_ptr().cast(), buffer.len(), 0) };
        if n >= 0 {
            return Ok(n as usize);
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(err);
    }
}

fn send_from(fd: RawFd, buffer: &[u8]) -> io::Result<usize> {
    loop {
        let n = unsafe { libc::send(fd, buffer.as_ptr().cast(), buffer.len(), 0) };
        if n >= 0 {
            return Ok(n as usize);
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(err);
    }
}

fn socket_error(fd: RawFd) -> io::Result<i32> {
    let mut err: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&mut err as *mut libc::c_int).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(err)
    }
}

unsafe fn cell_slice_as_bytes(cells: &[pyo3::buffer::ReadOnlyCell<u8>]) -> &[u8] {
    std::slice::from_raw_parts(cells.as_ptr().cast::<u8>(), cells.len())
}

unsafe fn cell_slice_as_bytes_mut(cells: &[Cell<u8>]) -> &mut [u8] {
    std::slice::from_raw_parts_mut(cells.as_ptr().cast::<u8>() as *mut u8, cells.len())
}

#![cfg(unix)]

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

struct PendingRecv {
    future: Py<PyAny>,
    size: usize,
}

struct PendingSend {
    future: Py<PyAny>,
    data: Vec<u8>,
    sent: usize,
}

struct PendingConnect {
    future: Py<PyAny>,
}

#[pyclass(module = "kioto._kioto")]
pub struct SocketState {
    sock: Py<PyAny>,
    fd: RawFd,
    loop_obj: Py<PyAny>,
    read_buffer: Vec<u8>,
    reads: VecDeque<PendingRecv>,
    writes: VecDeque<PendingSend>,
    connects: VecDeque<PendingConnect>,
    closed: bool,
}

#[pymethods]
impl SocketState {
    #[new]
    fn new(py: Python<'_>, sock: Py<PyAny>, loop_obj: Py<PyAny>) -> PyResult<Self> {
        let fd = sock.bind(py).call_method0("fileno")?.extract::<i32>()? as RawFd;
        Ok(Self {
            sock,
            fd,
            loop_obj,
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
        self.reads.push_back(PendingRecv { future, size });
        self.sync_interest(py)
    }

    fn enqueue_sendall(
        &mut self,
        py: Python<'_>,
        future: Py<PyAny>,
        data: Bound<'_, PyAny>,
        sent: usize,
    ) -> PyResult<()> {
        let data = data.extract::<Vec<u8>>()?;
        let sent = sent.min(data.len());
        if sent == data.len() {
            if !future_done(py, &future)? {
                future.bind(py).call_method1("set_result", (py.None(),))?;
            }
            return Ok(());
        }
        self.writes.push_back(PendingSend {
            future,
            data,
            sent,
        });
        self.sync_interest(py)
    }

    fn enqueue_connect(&mut self, py: Python<'_>, future: Py<PyAny>, _address: Py<PyAny>) -> PyResult<()> {
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
            if future_done(py, &op.future)? {
                self.reads.pop_front();
                continue;
            }

            if self.read_buffer.len() < op.size.max(1) {
                self.read_buffer.resize(op.size.max(1), 0);
            }
            let result = recv_into(self.fd, &mut self.read_buffer[..op.size.max(1)]);
            match result {
                Ok(0) => {
                    let payload = PyBytes::new(py, b"").unbind().into_any();
                    let op = self.reads.pop_front().expect("front above");
                    if !future_done(py, &op.future)? {
                        op.future.bind(py).call_method1("set_result", (payload.bind(py),))?;
                    }
                }
                Ok(n) => {
                    let payload = PyBytes::new(py, &self.read_buffer[..n]).unbind().into_any();
                    let op = self.reads.pop_front().expect("front above");
                    if !future_done(py, &op.future)? {
                        op.future.bind(py).call_method1("set_result", (payload.bind(py),))?;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => {
                    let op = self.reads.pop_front().expect("front above");
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
                        op.future.bind(py).call_method1("set_result", (py.None(),))?;
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
            match send_from(self.fd, &op.data[op.sent..]) {
                Ok(0) => break,
                Ok(n) => {
                    op.sent += n;
                    if op.sent == op.data.len() {
                        let op = self.writes.pop_front().expect("front above");
                        if !future_done(py, &op.future)? {
                            op.future.bind(py).call_method1("set_result", (py.None(),))?;
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
        self.loop_obj
            .bind(py)
            .getattr("_socket_registry")?
            .call_method1("unregister", (self.fd,))?;
        self.loop_obj
            .bind(py)
            .getattr("_poller")?
            .call_method1("set_interest", (self.fd, false, false))?;
        for op in self.reads.drain(..) {
            if !future_done(py, &op.future)? {
                op.future.bind(py).call_method0("cancel")?;
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
        self.loop_obj
            .bind(py)
            .getattr("_poller")?
            .call_method1("set_interest", (self.fd, readable, writable))?;
        Ok(())
    }
}

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    future.bind(py).call_method0("done")?.is_truthy()
}

fn recv_into(fd: RawFd, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        let n = unsafe {
            libc::recv(
                fd,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                0,
            )
        };
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
        let n = unsafe {
            libc::send(
                fd,
                buffer.as_ptr().cast(),
                buffer.len(),
                0,
            )
        };
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

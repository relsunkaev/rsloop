#![cfg(unix)]

use std::io;
use std::os::fd::RawFd;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

const READ_MAX_SIZE: usize = 256 * 1024;
const READ_OPPORTUNISTIC_THRESHOLD: usize = 64 * 1024;

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct ReadPipeTransport {
    loop_obj: Py<PyAny>,
    pipe: Option<Py<PyAny>>,
    protocol: Option<Py<PyAny>>,
    extra: Py<PyDict>,
    fileno: RawFd,
    closing: bool,
    paused: bool,
    read_buf: Vec<u8>,
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
        ensure_pipe_like_fd(fileno)?;
        set_nonblocking(fileno)?;

        let extra_dict = if let Some(extra) = extra {
            extra.bind(py).downcast::<PyDict>()?.copy()?
        } else {
            PyDict::new(py)
        };
        extra_dict.set_item("pipe", pipe.clone_ref(py))?;

        Ok(Self {
            loop_obj,
            pipe: Some(pipe),
            protocol: Some(protocol),
            extra: extra_dict.unbind(),
            fileno,
            closing: false,
            paused: false,
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
        slf.call_method0("_add_reader")?;
        if let Some(waiter) = waiter {
            py.import("asyncio.futures")?.call_method1(
                "_set_result_unless_cancelled",
                (waiter, py.None()),
            )?;
        }
        Ok(())
    }

    fn _add_reader(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let (should_add, fileno, loop_obj) = {
            let this = slf.borrow();
            (
                !this.paused && !this.closing && this.pipe.is_some(),
                this.fileno,
                this.loop_obj.clone_ref(py),
            )
        };
        if !should_add {
            return Ok(());
        }
        let cb = slf.getattr("_read_ready")?;
        loop_obj.bind(py).call_method1("_add_reader", (fileno, cb))?;
        Ok(())
    }

    fn is_reading(&self) -> bool {
        !self.paused && !self.closing
    }

    fn pause_reading(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let (should_remove, fileno, loop_obj) = {
            let mut this = slf.borrow_mut();
            if this.closing || this.paused {
                return Ok(());
            }
            this.paused = true;
            (true, this.fileno, this.loop_obj.clone_ref(py))
        };
        if should_remove {
            let _ = loop_obj.bind(py).call_method1("_remove_reader", (fileno,));
        }
        Ok(())
    }

    fn resume_reading(slf: Bound<'_, Self>) -> PyResult<()> {
        {
            let mut this = slf.borrow_mut();
            if this.closing || !this.paused {
                return Ok(());
            }
            this.paused = false;
        }
        slf.call_method0("_add_reader")?;
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

    fn get_extra_info(&self, py: Python<'_>, name: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
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
        close_transport(slf, None)
    }

    fn _read_ready(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let protocol = {
            let this = slf.borrow();
            this.protocol
                .as_ref()
                .map(|value| value.clone_ref(py))
                .ok_or_else(|| PyRuntimeError::new_err("pipe transport has no protocol"))?
        };

        let mut first_chunk: Option<Py<PyBytes>> = None;
        let mut second_chunk: Option<Py<PyBytes>> = None;
        let mut eof = false;
        let mut read_error: Option<io::Error> = None;

        {
            let mut this = slf.borrow_mut();
            if this.closing || this.paused || this.pipe.is_none() {
                return Ok(());
            }
            match read_once(this.fileno, &mut this.read_buf) {
                Ok(Some(read)) => {
                    first_chunk = Some(PyBytes::new(py, &this.read_buf[..read]).unbind());
                    if read >= READ_OPPORTUNISTIC_THRESHOLD {
                        match read_once(this.fileno, &mut this.read_buf) {
                            Ok(Some(extra)) => {
                                second_chunk =
                                    Some(PyBytes::new(py, &this.read_buf[..extra]).unbind());
                            }
                            Ok(None) => {
                                eof = true;
                            }
                            Err(err) => {
                                if err.kind() != io::ErrorKind::WouldBlock
                                    && err.kind() != io::ErrorKind::Interrupted
                                {
                                    read_error = Some(err);
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    eof = true;
                }
                Err(err) => {
                    if err.kind() != io::ErrorKind::WouldBlock
                        && err.kind() != io::ErrorKind::Interrupted
                    {
                        read_error = Some(err);
                    }
                }
            }
        }

        if let Some(err) = read_error {
            let exc = PyRuntimeError::new_err(err.to_string());
            return close_transport(slf, Some(exc));
        }

        if let Some(first) = first_chunk {
            protocol.bind(py).call_method1("data_received", (first,))?;
            if let Some(second) = second_chunk {
                protocol.bind(py).call_method1("data_received", (second,))?;
            }
            if eof {
                return close_transport(slf, None);
            }
            return Ok(());
        }

        if eof {
            return close_transport(slf, None);
        }

        Ok(())
    }
}

fn close_transport(slf: Bound<'_, ReadPipeTransport>, exc: Option<PyErr>) -> PyResult<()> {
    let py = slf.py();
    let (fileno, loop_obj, protocol, pipe) = {
        let mut this = slf.borrow_mut();
        if this.closing {
            return Ok(());
        }
        this.closing = true;
        let fileno = this.fileno;
        let loop_obj = this.loop_obj.clone_ref(py);
        let protocol = this.protocol.take();
        let pipe = this.pipe.take();
        (fileno, loop_obj, protocol, pipe)
    };

    let _ = loop_obj.bind(py).call_method1("_remove_reader", (fileno,));

    if let Some(protocol) = protocol {
        if exc.is_none() {
            let _ = protocol.bind(py).call_method0("eof_received");
        }
        let arg = if let Some(exc) = exc {
            exc.value(py).clone().into_any().unbind()
        } else {
            py.None()
        };
        let _ = protocol.bind(py).call_method1("connection_lost", (arg,));
    }
    if let Some(pipe) = pipe {
        let _ = pipe.bind(py).call_method0("close");
    }
    Ok(())
}

fn ensure_pipe_like_fd(fd: RawFd) -> PyResult<()> {
    let mut stat_buf = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat_buf.as_mut_ptr()) };
    if rc != 0 {
        return Err(PyRuntimeError::new_err(io::Error::last_os_error().to_string()));
    }
    let mode = unsafe { stat_buf.assume_init().st_mode };
    let kind = mode & libc::S_IFMT;
    let ok = kind == libc::S_IFIFO || kind == libc::S_IFSOCK || kind == libc::S_IFCHR;
    if ok {
        Ok(())
    } else {
        Err(PyValueError::new_err(
            "Pipe transport is for pipes/sockets only.",
        ))
    }
}

fn set_nonblocking(fd: RawFd) -> PyResult<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(PyRuntimeError::new_err(io::Error::last_os_error().to_string()));
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc < 0 {
        return Err(PyRuntimeError::new_err(io::Error::last_os_error().to_string()));
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

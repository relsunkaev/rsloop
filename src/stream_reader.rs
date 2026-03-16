#![cfg(unix)]

use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::ffi;
use pyo3::exceptions::{PyAttributeError, PyNotImplementedError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyByteArray, PyBytes};
use parking_lot::Mutex;

use crate::tcp_transport::TokioTcpTransportCore;

#[pyclass(module = "kioto._kioto", freelist = 256)]
pub struct StreamReaderBridge {
    reader: Py<PyAny>,
    transport: Py<PyAny>,
    loop_obj: Py<PyAny>,
    buffer: Py<PyByteArray>,
    limit: usize,
    has_pending_read: AtomicBool,
    pending_read: Mutex<Option<PendingRead>>,
    tcp_core: PyOnceLock<Py<TokioTcpTransportCore>>,
}

struct PendingRead {
    size: usize,
    future: Py<PyAny>,
}

#[pymethods]
impl StreamReaderBridge {
    #[new]
    fn new(py: Python<'_>, reader: Py<PyAny>, transport: Py<PyAny>) -> PyResult<Self> {
        let reader_ref = reader.bind(py);
        let loop_obj = reader_ref.getattr("_loop")?.unbind();
        let buffer = reader_ref
            .getattr("_buffer")?
            .cast_into::<PyByteArray>()?
            .unbind();
        let limit = reader_ref.getattr("_limit")?.extract()?;
        Ok(Self {
            reader,
            transport,
            loop_obj,
            buffer,
            limit,
            has_pending_read: AtomicBool::new(false),
            pending_read: Mutex::new(None),
            tcp_core: PyOnceLock::new(),
        })
    }

    fn bind_readexactly(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_readexactly(slf.py(), slf.as_any().as_unbound())
    }

    fn feed_data(&self, py: Python<'_>, data: Bound<'_, PyBytes>) -> PyResult<()> {
        self.feed_data_inner(py, data.as_bytes())
    }

    fn feed_eof(&self, py: Python<'_>, _payload: Py<PyAny>) -> PyResult<()> {
        self.feed_eof_inner(py)
    }

    fn complete_exact_read(
        &self,
        py: Python<'_>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        is_err: bool,
    ) -> PyResult<()> {
        self.complete_exact_read_inner(py, future, payload, is_err)
    }

    fn attach_tcp_core(
        &self,
        py: Python<'_>,
        core: Py<TokioTcpTransportCore>,
    ) -> PyResult<()> {
        let _ = self
            .tcp_core
            .get_or_try_init(py, || -> PyResult<Py<TokioTcpTransportCore>> { Ok(core) })?;
        Ok(())
    }
}

impl StreamReaderBridge {
    pub(crate) fn feed_data_inner(
        &self,
        py: Python<'_>,
        data: &[u8],
    ) -> PyResult<()> {
        let reader = self.reader.bind(py);
        if data.is_empty() {
            return Ok(());
        }

        let buffer = self.buffer.bind(py);
        append_to_bytearray(buffer.as_ptr(), data)?;
        if !self.try_finish_pending_read(py, &reader)? {
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

        match self.transport.bind(py).call_method0("pause_reading") {
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

    pub(crate) fn feed_eof_inner(&self, py: Python<'_>) -> PyResult<()> {
        let reader = self.reader.bind(py);
        reader.setattr("_eof", true)?;
        self.fail_pending_read_eof(py, &reader)?;
        wake_waiter(py, &reader)
    }

    fn readexactly_inner(
        &self,
        py: Python<'_>,
        size: usize,
    ) -> PyResult<Py<PyAny>> {
        let reader = self.reader.bind(py);
        if let Some(exc) = current_exception(py, &reader)? {
            return ready_future_exception(py, &reader, exc);
        }
        if size == 0 {
            let payload = PyBytes::new(py, b"").into_any().unbind();
            return ready_future_result(py, &reader, payload);
        }

        if try_consume_exact(&reader, self.buffer.bind(py), size)? {
            let data = consume_exact(py, self.buffer.bind(py).as_ptr(), size)?;
            maybe_resume_transport(&reader)?;
            return ready_future_result(py, &reader, data);
        }

        if reader.getattr("_eof")?.is_truthy()? {
            let partial = consume_all(py, self.buffer.bind(py).as_ptr())?;
            return ready_future_exception(
                py,
                &reader,
                incomplete_read_error(py, partial, size)?,
            );
        }

        if reader.getattr("_paused")?.is_truthy()? {
            reader.setattr("_paused", false)?;
            self.transport.bind(py).call_method0("resume_reading")?;
        }

        let future = self.loop_obj.bind(py).call_method0("create_future")?.unbind();
        reader.setattr("_waiter", future.bind(py))?;
        let mut pending_read = self.pending_read.lock();
        if pending_read.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "readexactly() called while another coroutine is already waiting for incoming data",
            ));
        }
        *pending_read = Some(PendingRead {
            size,
            future: future.clone_ref(py),
        });
        self.has_pending_read.store(true, Ordering::Release);
        drop(pending_read);
        if bytearray_len(self.buffer.bind(py).as_ptr())? == 0 {
            match self.try_start_native_exact_read(py, &future, size) {
                Ok(true) => return Ok(future),
                Ok(false) => {}
                Err(err) => {
                    self.pending_read.lock().take();
                    self.has_pending_read.store(false, Ordering::Release);
                    reader.setattr("_waiter", py.None())?;
                    return Err(err);
                }
            }
        }
        Ok(future)
    }

    fn try_finish_pending_read(
        &self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if !self.has_pending_read.load(Ordering::Acquire) {
            return Ok(false);
        }
        let pending = {
            let buffered = bytearray_len(self.buffer.bind(py).as_ptr())?;
            let mut pending = self.pending_read.lock();
            match pending.as_ref() {
                Some(pending_read) if buffered >= pending_read.size => {
                    let pending_read = pending.take().expect("pending read present");
                    self.has_pending_read.store(false, Ordering::Release);
                    pending_read
                }
                Some(_) => return Ok(false),
                None => return Ok(false),
            }
        };

        let data = consume_exact(py, self.buffer.bind(py).as_ptr(), pending.size)?;
        clear_waiter(reader, py)?;
        if !future_done(py, &pending.future)? {
            pending.future.bind(py).call_method1("set_result", (data.bind(py),))?;
        }
        maybe_resume_transport(reader)?;
        Ok(true)
    }

    fn fail_pending_read_eof(
        &self,
        py: Python<'_>,
        reader: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if !self.has_pending_read.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(pending) = self.pending_read.lock().take() else {
            return Ok(());
        };
        self.has_pending_read.store(false, Ordering::Release);
        clear_waiter(reader, py)?;
        if future_done(py, &pending.future)? {
            return Ok(());
        }
        let partial = consume_all(py, self.buffer.bind(py).as_ptr())?;
        let exc = incomplete_read_error(py, partial, pending.size)?;
        pending.future.bind(py).call_method1("set_exception", (exc.bind(py),))?;
        Ok(())
    }

    pub(crate) fn complete_exact_read_inner(
        &self,
        py: Python<'_>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        is_err: bool,
    ) -> PyResult<()> {
        let reader = self.reader.bind(py);
        clear_waiter(&reader, py)?;

        if self.has_pending_read.load(Ordering::Acquire) {
            let mut pending = self.pending_read.lock();
            if pending
                .as_ref()
                .map(|pending_read| pending_read.future.is(&future))
                .unwrap_or(false)
            {
                pending.take();
                self.has_pending_read.store(false, Ordering::Release);
            }
        }

        if future_done(py, &future)? {
            return Ok(());
        }

        if is_err {
            future
                .bind(py)
                .call_method1("set_exception", (payload.bind(py),))?;
            return Ok(());
        }

        future
            .bind(py)
            .call_method1("set_result", (payload.bind(py),))?;
        maybe_resume_transport(&reader)?;
        Ok(())
    }

    fn try_start_native_exact_read(
        &self,
        py: Python<'_>,
        future: &Py<PyAny>,
        size: usize,
    ) -> PyResult<bool> {
        if let Some(core) = self.tcp_core.get(py) {
            core.borrow(py)
                .readexactly_inner(future.clone_ref(py), size)?;
            return Ok(true);
        }
        match self
            .transport
            .bind(py)
            .call_method1("_kioto_native_readexactly", (future.bind(py), size))
        {
            Ok(result) => result.extract(),
            Err(err) if err.is_instance_of::<PyAttributeError>(py) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

struct SyncPyMethodDef(ffi::PyMethodDef);

unsafe impl Sync for SyncPyMethodDef {}

static READEXACTLY_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"readexactly".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: stream_reader_readexactly_c,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"Native exact-read fast path backed by the Kioto stream bridge.".as_ptr(),
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

unsafe extern "C" fn stream_reader_readexactly_c(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let bridge = Bound::from_borrowed_ptr(py, slf)
            .cast_into_unchecked::<StreamReaderBridge>()
            .unbind();
        let bridge_ref = bridge.borrow(py);
        let size = Bound::<PyAny>::from_borrowed_ptr(py, arg).extract::<isize>()?;
        if size < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "readexactly size can not be less than zero",
            ));
        }
        bridge_ref.readexactly_inner(py, size as usize)
    })();

    match result {
        Ok(obj) => obj.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
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

fn try_consume_exact(
    _reader: &Bound<'_, PyAny>,
    buffer: &Bound<'_, PyByteArray>,
    size: usize,
) -> PyResult<bool> {
    let len = unsafe { ffi::PyByteArray_Size(buffer.as_ptr()) };
    Ok(len >= size as isize)
}

fn consume_exact(py: Python<'_>, bytearray: *mut ffi::PyObject, size: usize) -> PyResult<Py<PyAny>> {
    unsafe {
        let len = ffi::PyByteArray_Size(bytearray);
        if len < size as isize {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "insufficient buffered data",
            ));
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
        let data = PyBytes::new(py, std::slice::from_raw_parts(src.cast_const(), len as usize))
            .into_any()
            .unbind();
        if ffi::PyByteArray_Resize(bytearray, 0) != 0 {
            return Err(PyErr::fetch(py));
        }
        Ok(data)
    }
}

fn ready_future_result(
    py: Python<'_>,
    reader: &Bound<'_, PyAny>,
    payload: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let future = reader.getattr("_loop")?.call_method0("create_future")?;
    future.call_method1("set_result", (payload.bind(py),))?;
    Ok(future.unbind())
}

fn ready_future_exception(
    py: Python<'_>,
    reader: &Bound<'_, PyAny>,
    exc: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let future = reader.getattr("_loop")?.call_method0("create_future")?;
    future.call_method1("set_exception", (exc.bind(py),))?;
    Ok(future.unbind())
}

fn incomplete_read_error(py: Python<'_>, partial: Py<PyAny>, expected: usize) -> PyResult<Py<PyAny>> {
    static INCOMPLETE_READ_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let cls = INCOMPLETE_READ_ERROR.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.exceptions")?
            .getattr("IncompleteReadError")?
            .unbind())
    })?;
    Ok(cls
        .bind(py)
        .call1((partial.bind(py), expected))?
        .unbind())
}

fn current_exception(_py: Python<'_>, reader: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    let exc = reader.getattr("_exception")?;
    if exc.is_none() {
        Ok(None)
    } else {
        Ok(Some(exc.unbind()))
    }
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

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    if future.bind(py).call_method0("cancelled")?.is_truthy()? {
        return Ok(true);
    }
    future.bind(py).call_method0("done")?.is_truthy()
}

#![cfg(unix)]

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCFunction, PyTuple};

use crate::handles::{make_handle, make_handle_fastcall};
use crate::scheduler::Scheduler;

struct SyncPyMethodDef(ffi::PyMethodDef);

unsafe impl Sync for SyncPyMethodDef {}

static CALL_SOON_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"call_soon".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFastWithKeywords: loop_api_call_soon_fast,
    },
    ml_flags: ffi::METH_FASTCALL | ffi::METH_KEYWORDS,
    ml_doc: c"Native FASTCALL call_soon implementation.".as_ptr(),
});

static CALL_SOON_THREADSAFE_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"call_soon_threadsafe".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFastWithKeywords: loop_api_call_soon_threadsafe_fast,
    },
    ml_flags: ffi::METH_FASTCALL | ffi::METH_KEYWORDS,
    ml_doc: c"Native FASTCALL call_soon_threadsafe implementation.".as_ptr(),
});

static CALL_AT_DEF: SyncPyMethodDef = SyncPyMethodDef(ffi::PyMethodDef {
    ml_name: c"call_at".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFastWithKeywords: loop_api_call_at_fast,
    },
    ml_flags: ffi::METH_FASTCALL | ffi::METH_KEYWORDS,
    ml_doc: c"Native FASTCALL call_at implementation.".as_ptr(),
});

#[pyclass(module = "kioto._kioto")]
pub struct LoopApi {
    loop_obj: Py<PyAny>,
    scheduler: Py<Scheduler>,
    debug: AtomicBool,
    closed: AtomicBool,
    wake_fd: AtomicI32,
    wake_pending: AtomicBool,
}

#[pymethods]
impl LoopApi {
    #[new]
    #[pyo3(signature = (loop_obj, scheduler, debug=false))]
    fn new(loop_obj: Py<PyAny>, scheduler: Py<Scheduler>, debug: bool) -> Self {
        Self {
            loop_obj,
            scheduler,
            debug: AtomicBool::new(debug),
            closed: AtomicBool::new(false),
            wake_fd: AtomicI32::new(-1),
            wake_pending: AtomicBool::new(false),
        }
    }

    fn _call_soon(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        context: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let handle = make_handle(
            py,
            callback,
            args,
            self.loop_obj.clone_ref(py),
            Some(context),
            None,
        )?;
        self.scheduler
            .bind(py)
            .borrow()
            .push_ready_inner(handle.clone_ref(py));
        Ok(handle)
    }

    fn bind_call_soon(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_fastcall(slf.py(), slf.as_any().as_unbound(), &CALL_SOON_DEF.0)
    }

    fn bind_call_soon_threadsafe(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_fastcall(
            slf.py(),
            slf.as_any().as_unbound(),
            &CALL_SOON_THREADSAFE_DEF.0,
        )
    }

    fn bind_call_at(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        create_bound_fastcall(slf.py(), slf.as_any().as_unbound(), &CALL_AT_DEF.0)
    }

    fn set_debug(&self, enabled: bool) {
        self.debug.store(enabled, Ordering::Release);
    }

    fn set_closed(&self, closed: bool) {
        self.closed.store(closed, Ordering::Release);
    }

    fn set_wakeup_fd(&self, fd: i32) {
        self.wake_fd.store(fd, Ordering::Release);
    }

    fn clear_wakeup(&self) {
        self.wake_pending.store(false, Ordering::Release);
    }
}

impl LoopApi {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("Event loop is closed"));
        }
        Ok(())
    }

    fn wake_loop(&self) -> PyResult<()> {
        let fd = self.wake_fd.load(Ordering::Acquire);
        if fd < 0 {
            let py = unsafe { Python::assume_attached() };
            self.loop_obj.bind(py).call_method0("_write_to_self")?;
            return Ok(());
        }
        if self
            .wake_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        write_wakeup_fd(fd)
    }
}

fn create_bound_fastcall(
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

unsafe extern "C" fn loop_api_call_soon_fast(
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargsf: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    run_fastcall(slf, args, nargsf, kwnames, FastcallMode::CallSoon)
}

unsafe extern "C" fn loop_api_call_soon_threadsafe_fast(
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargsf: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    run_fastcall(slf, args, nargsf, kwnames, FastcallMode::CallSoonThreadsafe)
}

unsafe extern "C" fn loop_api_call_at_fast(
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargsf: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    run_fastcall(slf, args, nargsf, kwnames, FastcallMode::CallAt)
}

#[derive(Clone, Copy)]
enum FastcallMode {
    CallSoon,
    CallSoonThreadsafe,
    CallAt,
}

unsafe fn run_fastcall(
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargsf: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
    mode: FastcallMode,
) -> *mut ffi::PyObject {
    let py = Python::assume_attached();
    let result = (|| -> PyResult<Py<PyAny>> {
        let api = Bound::from_borrowed_ptr(py, slf)
            .cast_into_unchecked::<LoopApi>()
            .unbind();
        let api_ref = api.borrow(py);
        api_ref.ensure_open()?;

        let nargs = ffi::PyVectorcall_NARGS(nargsf as usize) as usize;
        let kwcount = keyword_count(kwnames)?;

        let (when, callback_index, extra_arg_offset, callback_label) = match mode {
            FastcallMode::CallSoon | FastcallMode::CallSoonThreadsafe => {
                if nargs < 1 {
                    return Err(PyRuntimeError::new_err(
                        "call_soon requires at least 1 positional argument",
                    ));
                }
                (
                    None,
                    0usize,
                    1usize,
                    match mode {
                        FastcallMode::CallSoon => "call_soon",
                        FastcallMode::CallSoonThreadsafe => "call_soon_threadsafe",
                        FastcallMode::CallAt => unreachable!(),
                    },
                )
            }
            FastcallMode::CallAt => {
                if nargs < 2 {
                    return Err(PyRuntimeError::new_err(
                        "call_at requires at least 2 positional arguments",
                    ));
                }
                let when = Bound::from_borrowed_ptr(py, *args.add(0)).extract::<f64>()?;
                (Some(when), 1usize, 2usize, "call_at")
            }
        };

        let context = parse_context_keyword(py, args, nargs, kwnames, kwcount)?;
        let callback_ptr = *args.add(callback_index);
        let callback = Bound::from_borrowed_ptr(py, callback_ptr);

        if api_ref.debug.load(Ordering::Acquire) {
            if matches!(mode, FastcallMode::CallSoon | FastcallMode::CallAt) {
                api_ref.loop_obj.bind(py).call_method0("_check_thread")?;
            }
            api_ref
                .loop_obj
                .bind(py)
                .call_method1("_check_callback", (callback, callback_label))?;
        }

        let extra_arg_count = nargs.saturating_sub(extra_arg_offset);
        let handle = make_handle_fastcall(
            py,
            callback_ptr,
            args.add(extra_arg_offset),
            extra_arg_count,
            api_ref.loop_obj.clone_ref(py),
            context,
            when,
            api_ref.debug.load(Ordering::Acquire),
        )?;

        let scheduler = api_ref.scheduler.bind(py).borrow();
        if let Some(when) = when {
            scheduler.push_timer_inner(handle.clone_ref(py), when);
        } else {
            if matches!(mode, FastcallMode::CallSoonThreadsafe) {
                scheduler.push_ready_threadsafe_inner(handle.clone_ref(py));
                api_ref.wake_loop()?;
            } else {
                scheduler.push_ready_inner(handle.clone_ref(py));
            }
        }
        Ok(handle)
    })();

    match result {
        Ok(handle) => handle.into_ptr(),
        Err(err) => {
            err.restore(py);
            std::ptr::null_mut()
        }
    }
}

fn write_wakeup_fd(fd: RawFd) -> PyResult<()> {
    let byte = [0_u8; 1];
    loop {
        let wrote = unsafe { libc::write(fd, byte.as_ptr().cast(), 1) };
        if wrote == 1 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        match err.kind() {
            std::io::ErrorKind::Interrupted => continue,
            std::io::ErrorKind::WouldBlock => return Ok(()),
            _ => return Err(PyRuntimeError::new_err(err.to_string())),
        }
    }
}

fn keyword_count(kwnames: *mut ffi::PyObject) -> PyResult<usize> {
    if kwnames.is_null() {
        return Ok(0);
    }
    Ok(unsafe { ffi::PyTuple_GET_SIZE(kwnames) as usize })
}

unsafe fn parse_context_keyword(
    py: Python<'_>,
    args: *const *mut ffi::PyObject,
    nargs: usize,
    kwnames: *mut ffi::PyObject,
    kwcount: usize,
) -> PyResult<Option<Py<PyAny>>> {
    if kwcount == 0 {
        return Ok(None);
    }
    if kwcount != 1 {
        return Err(PyRuntimeError::new_err("unexpected keyword arguments"));
    }

    let keyword = Bound::from_borrowed_ptr(py, ffi::PyTuple_GET_ITEM(kwnames, 0));
    if keyword.extract::<&str>()? != "context" {
        return Err(PyRuntimeError::new_err("unexpected keyword arguments"));
    }

    let value = Bound::from_borrowed_ptr(py, *args.add(nargs)).unbind();
    if value.bind(py).is_none() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

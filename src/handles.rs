#![cfg(unix)]

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use pyo3::exceptions::{PyAttributeError, PyKeyboardInterrupt, PyRuntimeError, PySystemExit};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyDict, PyTuple, PyType};
use pyo3::PyTypeInfo;

enum HandleArgs {
    Two([Py<PyAny>; 2]),
    Tuple(Py<PyTuple>),
}

struct HandleState {
    callback: Option<Py<PyAny>>,
    args: Option<HandleArgs>,
    context: Py<PyAny>,
    loop_obj: Py<PyAny>,
    when: Option<f64>,
    scheduled: bool,
    source_traceback: Option<Py<PyAny>>,
}

#[pyclass(module = "kioto._kioto", weakref, freelist = 1024)]
pub struct Handle {
    cancelled: AtomicBool,
    state: Mutex<HandleState>,
}

#[pyclass(module = "kioto._kioto", weakref, freelist = 1024)]
pub struct ZeroArgHandle {
    cancelled: AtomicBool,
    scheduled: AtomicBool,
    callback: Py<PyAny>,
    context: Py<PyAny>,
    loop_obj: Py<PyAny>,
    when: Option<f64>,
    source_traceback: Option<Py<PyAny>>,
}

#[pyclass(module = "kioto._kioto", weakref, freelist = 1024)]
pub struct OneArgHandle {
    cancelled: AtomicBool,
    scheduled: AtomicBool,
    callback: Py<PyAny>,
    arg: Py<PyAny>,
    context: Py<PyAny>,
    loop_obj: Py<PyAny>,
    when: Option<f64>,
    source_traceback: Option<Py<PyAny>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FutureHandleOp {
    SetResult = 0,
    SetException = 1,
    SetResultUnlessCancelled = 2,
}

impl FutureHandleOp {
    fn from_i32(value: i32) -> PyResult<Self> {
        match value {
            0 => Ok(Self::SetResult),
            1 => Ok(Self::SetException),
            2 => Ok(Self::SetResultUnlessCancelled),
            _ => Err(PyRuntimeError::new_err("invalid future handle operation")),
        }
    }

    fn method_name(self) -> &'static str {
        match self {
            Self::SetResult | Self::SetResultUnlessCancelled => "set_result",
            Self::SetException => "set_exception",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::SetResult => "Future.set_result",
            Self::SetException => "Future.set_exception",
            Self::SetResultUnlessCancelled => "_set_result_unless_cancelled",
        }
    }
}

struct FutureHandleState {
    future: Option<Py<PyAny>>,
    payload: Option<Py<PyAny>>,
    loop_obj: Py<PyAny>,
    when: Option<f64>,
    scheduled: bool,
    source_traceback: Option<Py<PyAny>>,
    op: FutureHandleOp,
}

#[pyclass(module = "kioto._kioto", weakref, freelist = 1024)]
pub struct FutureHandle {
    cancelled: AtomicBool,
    state: Mutex<FutureHandleState>,
}

#[pymethods]
impl Handle {
    pub(crate) fn cancelled_inner(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn mark_unscheduled_inner(&self) {
        self.state.lock().scheduled = false;
    }

    pub(crate) fn run_inner(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_bound(slf.bind(py), py)
    }

    pub(crate) fn run_bound(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        if slf.borrow().cancelled_inner() {
            return Ok(());
        }

        let (context, callback, args, loop_obj, source_traceback) = {
            let borrowed = slf.borrow();
            let state = borrowed.state.lock();
            let Some(callback) = state.callback.as_ref() else {
                return Ok(());
            };
            let Some(args) = state.args.as_ref() else {
                return Ok(());
            };
            (
                state.context.clone_ref(py),
                callback.clone_ref(py),
                args.clone_refs(py),
                state.loop_obj.clone_ref(py),
                state.source_traceback.as_ref().map(|tb| tb.clone_ref(py)),
            )
        };

        if let Err(err) = run_handle(py, &context, &callback, &args) {
            if err.is_instance_of::<PySystemExit>(py)
                || err.is_instance_of::<PyKeyboardInterrupt>(py)
            {
                return Err(err);
            }

            let ctx = PyDict::new(py);
            ctx.set_item("message", format_exception_message(py, &callback))?;
            ctx.set_item("exception", err.into_value(py))?;
            ctx.set_item("handle", slf.clone().unbind().into_any())?;
            if let Some(source_traceback) = source_traceback {
                ctx.set_item("source_traceback", source_traceback)?;
            }
            loop_obj
                .bind(py)
                .call_method1("call_exception_handler", (ctx,))?;
        }

        Ok(())
    }

    #[new]
    #[pyo3(signature = (callback, args, loop_obj, context=None, when=None))]
    fn new(
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
    ) -> PyResult<Self> {
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        let source_traceback = capture_source_traceback(py, &loop_obj, None)?;

        Ok(Self {
            cancelled: AtomicBool::new(false),
            state: Mutex::new(HandleState {
                callback: Some(callback),
                args: Some(HandleArgs::from_tuple(args)?),
                context,
                loop_obj,
                when,
                scheduled: when.is_some(),
                source_traceback,
            }),
        })
    }

    #[getter]
    #[pyo3(name = "_cancelled")]
    fn cancelled_attr(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[getter]
    #[pyo3(name = "_scheduled")]
    fn scheduled_attr(&self) -> bool {
        self.state.lock().scheduled
    }

    #[setter]
    #[pyo3(name = "_scheduled")]
    fn set_scheduled_attr(&self, scheduled: bool) {
        self.state.lock().scheduled = scheduled;
    }

    #[getter]
    #[pyo3(name = "_callback")]
    fn callback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.state
            .lock()
            .callback
            .as_ref()
            .map(|callback| callback.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[getter]
    #[pyo3(name = "_source_traceback")]
    fn source_traceback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.state
            .lock()
            .source_traceback
            .as_ref()
            .map(|traceback| traceback.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let debug = {
            let state = self.state.lock();
            state
                .loop_obj
                .bind(py)
                .call_method0("get_debug")?
                .is_truthy()?
        };

        let mut state = self.state.lock();
        if debug && state.source_traceback.is_none() {
            state.source_traceback = capture_source_traceback(py, &state.loop_obj, None)?;
        }
        state.callback = None;
        state.args = None;
        Ok(())
    }

    fn cancelled(&self) -> bool {
        self.cancelled_attr()
    }

    fn get_context(&self, py: Python<'_>) -> Py<PyAny> {
        self.state.lock().context.clone_ref(py)
    }

    fn when(&self) -> PyResult<f64> {
        self.state
            .lock()
            .when
            .ok_or_else(|| PyAttributeError::new_err("Handle has no scheduled time"))
    }

    fn _run(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_inner(slf, py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let state = self.state.lock();
        let mut info = vec!["Handle".to_string()];
        if self.cancelled.load(Ordering::Acquire) {
            info.push("cancelled".to_string());
        }
        if let Some(when) = state.when {
            info.push(format!("when={when}"));
        }
        if let Some(callback) = state.callback.as_ref() {
            info.push(format_callback(py, callback)?);
        }
        Ok(format!("<{}>", info.join(" ")))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

#[pymethods]
impl ZeroArgHandle {
    #[new]
    #[pyo3(signature = (callback, loop_obj, context=None, when=None))]
    fn new(
        py: Python<'_>,
        callback: Py<PyAny>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
    ) -> PyResult<Self> {
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        let source_traceback = capture_source_traceback(py, &loop_obj, None)?;
        Ok(Self {
            cancelled: AtomicBool::new(false),
            scheduled: AtomicBool::new(when.is_some()),
            callback,
            context,
            loop_obj,
            when,
            source_traceback,
        })
    }

    #[getter]
    #[pyo3(name = "_cancelled")]
    fn cancelled_attr(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[getter]
    #[pyo3(name = "_scheduled")]
    fn scheduled_attr(&self) -> bool {
        self.scheduled.load(Ordering::Acquire)
    }

    #[setter]
    #[pyo3(name = "_scheduled")]
    fn set_scheduled_attr(&self, scheduled: bool) {
        self.scheduled.store(scheduled, Ordering::Release);
    }

    #[getter]
    #[pyo3(name = "_callback")]
    fn callback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.callback.clone_ref(py)
    }

    #[getter]
    #[pyo3(name = "_source_traceback")]
    fn source_traceback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.source_traceback
            .as_ref()
            .map(|traceback| traceback.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn cancelled(&self) -> bool {
        self.cancelled_attr()
    }

    fn get_context(&self, py: Python<'_>) -> Py<PyAny> {
        self.context.clone_ref(py)
    }

    fn when(&self) -> PyResult<f64> {
        self.when
            .ok_or_else(|| PyAttributeError::new_err("Handle has no scheduled time"))
    }

    fn _run(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_inner(slf, py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let mut info = vec!["ZeroArgHandle".to_string()];
        if self.cancelled.load(Ordering::Acquire) {
            info.push("cancelled".to_string());
        }
        if let Some(when) = self.when {
            info.push(format!("when={when}"));
        }
        info.push(format_callback(py, &self.callback)?);
        Ok(format!("<{}>", info.join(" ")))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

#[pymethods]
impl OneArgHandle {
    #[new]
    #[pyo3(signature = (callback, arg, loop_obj, context=None, when=None))]
    fn new(
        py: Python<'_>,
        callback: Py<PyAny>,
        arg: Py<PyAny>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
    ) -> PyResult<Self> {
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        let source_traceback = capture_source_traceback(py, &loop_obj, None)?;
        Ok(Self {
            cancelled: AtomicBool::new(false),
            scheduled: AtomicBool::new(when.is_some()),
            callback,
            arg,
            context,
            loop_obj,
            when,
            source_traceback,
        })
    }

    #[getter]
    #[pyo3(name = "_cancelled")]
    fn cancelled_attr(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[getter]
    #[pyo3(name = "_scheduled")]
    fn scheduled_attr(&self) -> bool {
        self.scheduled.load(Ordering::Acquire)
    }

    #[setter]
    #[pyo3(name = "_scheduled")]
    fn set_scheduled_attr(&self, scheduled: bool) {
        self.scheduled.store(scheduled, Ordering::Release);
    }

    #[getter]
    #[pyo3(name = "_callback")]
    fn callback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.callback.clone_ref(py)
    }

    #[getter]
    #[pyo3(name = "_source_traceback")]
    fn source_traceback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.source_traceback
            .as_ref()
            .map(|traceback| traceback.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn cancelled(&self) -> bool {
        self.cancelled_attr()
    }

    fn get_context(&self, py: Python<'_>) -> Py<PyAny> {
        self.context.clone_ref(py)
    }

    fn when(&self) -> PyResult<f64> {
        self.when
            .ok_or_else(|| PyAttributeError::new_err("Handle has no scheduled time"))
    }

    fn _run(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_inner(slf, py)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let mut info = vec!["OneArgHandle".to_string()];
        if self.cancelled.load(Ordering::Acquire) {
            info.push("cancelled".to_string());
        }
        if let Some(when) = self.when {
            info.push(format!("when={when}"));
        }
        info.push(format_callback(py, &self.callback)?);
        Ok(format!("<{}>", info.join(" ")))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

#[pymethods]
impl FutureHandle {
    pub(crate) fn cancelled_inner(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn mark_unscheduled_inner(&self) {
        self.state.lock().scheduled = false;
    }

    pub(crate) fn run_inner(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_bound(slf.bind(py), py)
    }

    pub(crate) fn run_bound(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        if slf.borrow().cancelled_inner() {
            return Ok(());
        }

        let (future, payload, op, loop_obj, source_traceback) = {
            let borrowed = slf.borrow();
            let state = borrowed.state.lock();
            let Some(future) = state.future.as_ref() else {
                return Ok(());
            };
            let Some(payload) = state.payload.as_ref() else {
                return Ok(());
            };
            (
                future.clone_ref(py),
                payload.clone_ref(py),
                state.op,
                state.loop_obj.clone_ref(py),
                state.source_traceback.as_ref().map(|tb| tb.clone_ref(py)),
            )
        };

        if op == FutureHandleOp::SetResultUnlessCancelled
            && future.bind(py).call_method0("cancelled")?.is_truthy()?
        {
            return Ok(());
        }

        if let Err(err) = future
            .bind(py)
            .call_method1(op.method_name(), (payload.bind(py),))
            .map(|_| ())
        {
            if err.is_instance_of::<PySystemExit>(py)
                || err.is_instance_of::<PyKeyboardInterrupt>(py)
            {
                return Err(err);
            }

            let ctx = PyDict::new(py);
            ctx.set_item("message", format!("Exception in callback {}", op.label()))?;
            ctx.set_item("exception", err.into_value(py))?;
            ctx.set_item("handle", slf.clone().unbind().into_any())?;
            if let Some(source_traceback) = source_traceback {
                ctx.set_item("source_traceback", source_traceback)?;
            }
            loop_obj
                .bind(py)
                .call_method1("call_exception_handler", (ctx,))?;
        }

        Ok(())
    }

    #[new]
    #[pyo3(signature = (future, payload, loop_obj, op, when=None))]
    fn new(
        py: Python<'_>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        loop_obj: Py<PyAny>,
        op: i32,
        when: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            cancelled: AtomicBool::new(false),
            state: Mutex::new(FutureHandleState {
                future: Some(future),
                payload: Some(payload),
                loop_obj: loop_obj.clone_ref(py),
                when,
                scheduled: when.is_some(),
                source_traceback: capture_source_traceback(py, &loop_obj, None)?,
                op: FutureHandleOp::from_i32(op)?,
            }),
        })
    }

    #[getter]
    #[pyo3(name = "_cancelled")]
    fn cancelled_attr(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[getter]
    #[pyo3(name = "_scheduled")]
    fn scheduled_attr(&self) -> bool {
        self.state.lock().scheduled
    }

    #[setter]
    #[pyo3(name = "_scheduled")]
    fn set_scheduled_attr(&self, scheduled: bool) {
        self.state.lock().scheduled = scheduled;
    }

    #[getter]
    #[pyo3(name = "_callback")]
    fn callback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    #[pyo3(name = "_source_traceback")]
    fn source_traceback_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.state
            .lock()
            .source_traceback
            .as_ref()
            .map(|traceback| traceback.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }

        let mut state = self.state.lock();
        state.future = None;
        state.payload = None;
    }

    fn cancelled(&self) -> bool {
        self.cancelled_attr()
    }

    fn get_context(&self, py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    fn when(&self) -> PyResult<f64> {
        self.state
            .lock()
            .when
            .ok_or_else(|| PyAttributeError::new_err("Handle has no scheduled time"))
    }

    fn _run(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_inner(slf, py)
    }

    fn __repr__(&self) -> String {
        let state = self.state.lock();
        let mut info = vec!["FutureHandle".to_string()];
        if self.cancelled.load(Ordering::Acquire) {
            info.push("cancelled".to_string());
        }
        if let Some(when) = state.when {
            info.push(format!("when={when}"));
        }
        info.push(state.op.label().to_string());
        format!("<{}>", info.join(" "))
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

pub(crate) fn is_native_cancelled(py: Python<'_>, handle: &Py<PyAny>) -> Option<bool> {
    match native_handle_kind(py, handle)? {
        NativeHandleKind::ZeroArg => Some(
            unsafe { handle.bind(py).cast_unchecked::<ZeroArgHandle>() }
                .borrow()
                .cancelled
                .load(Ordering::Acquire),
        ),
        NativeHandleKind::OneArg => Some(
            unsafe { handle.bind(py).cast_unchecked::<OneArgHandle>() }
                .borrow()
                .cancelled
                .load(Ordering::Acquire),
        ),
        NativeHandleKind::Handle => Some(
            unsafe { handle.bind(py).cast_unchecked::<Handle>() }
                .borrow()
                .cancelled_inner(),
        ),
        NativeHandleKind::Future => Some(
            unsafe { handle.bind(py).cast_unchecked::<FutureHandle>() }
                .borrow()
                .cancelled_inner(),
        ),
    }
}

impl Handle {
    pub(crate) fn create(
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        Self::create_with_debug(py, callback, args, loop_obj, context, when, false)
    }

    pub(crate) fn create_with_debug(
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
        debug: bool,
    ) -> PyResult<Py<PyAny>> {
        Self::create_with_debug_from_args(
            py,
            callback,
            HandleArgs::from_tuple(args)?,
            loop_obj,
            context,
            when,
            debug,
        )
    }

    fn create_with_debug_from_args(
        py: Python<'_>,
        callback: Py<PyAny>,
        args: HandleArgs,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
        debug: bool,
    ) -> PyResult<Py<PyAny>> {
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        let source_traceback = capture_source_traceback(py, &loop_obj, Some(debug))?;
        Ok(Py::new(
            py,
            Handle {
                cancelled: AtomicBool::new(false),
                state: Mutex::new(HandleState {
                    callback: Some(callback),
                    args: Some(args),
                    context,
                    loop_obj,
                    when,
                    scheduled: when.is_some(),
                    source_traceback,
                }),
            },
        )?
        .into_any())
    }
}

impl ZeroArgHandle {
    pub(crate) fn create(
        py: Python<'_>,
        callback: Py<PyAny>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        Self::create_with_debug(py, callback, loop_obj, context, when, false)
    }

    pub(crate) fn create_with_debug(
        py: Python<'_>,
        callback: Py<PyAny>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
        debug: bool,
    ) -> PyResult<Py<PyAny>> {
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        let source_traceback = capture_source_traceback(py, &loop_obj, Some(debug))?;
        Ok(Py::new(
            py,
            ZeroArgHandle {
                cancelled: AtomicBool::new(false),
                scheduled: AtomicBool::new(when.is_some()),
                callback,
                context,
                loop_obj,
                when,
                source_traceback,
            },
        )?
        .into_any())
    }

    pub(crate) fn run_inner(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_bound(slf.bind(py), py)
    }

    pub(crate) fn run_bound(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let borrowed = slf.borrow();
        if borrowed.cancelled.load(Ordering::Acquire) {
            return Ok(());
        }

        let context = borrowed.context.clone_ref(py);
        let callback = borrowed.callback.clone_ref(py);
        let loop_obj = borrowed.loop_obj.clone_ref(py);
        let source_traceback = borrowed
            .source_traceback
            .as_ref()
            .map(|tb| tb.clone_ref(py));
        drop(borrowed);

        if let Err(err) = run_zero_arg_handle(py, &context, &callback) {
            if err.is_instance_of::<PySystemExit>(py)
                || err.is_instance_of::<PyKeyboardInterrupt>(py)
            {
                return Err(err);
            }

            let ctx = PyDict::new(py);
            ctx.set_item("message", format_exception_message(py, &callback))?;
            ctx.set_item("exception", err.into_value(py))?;
            ctx.set_item("handle", slf.clone().unbind().into_any())?;
            if let Some(source_traceback) = source_traceback {
                ctx.set_item("source_traceback", source_traceback)?;
            }
            loop_obj
                .bind(py)
                .call_method1("call_exception_handler", (ctx,))?;
        }

        Ok(())
    }
}

impl HandleArgs {
    fn from_tuple(args: Bound<'_, PyTuple>) -> PyResult<Self> {
        Ok(Self::Tuple(args.unbind()))
    }

    unsafe fn from_fastcall(
        py: Python<'_>,
        args: *const *mut ffi::PyObject,
        arg_count: usize,
    ) -> PyResult<Self> {
        Ok(Self::Tuple(tuple_from_fastcall(py, args, arg_count)?))
    }

    fn clone_refs(&self, py: Python<'_>) -> Self {
        match self {
            Self::Two([arg0, arg1]) => Self::Two([arg0.clone_ref(py), arg1.clone_ref(py)]),
            Self::Tuple(args) => Self::Tuple(args.clone_ref(py)),
        }
    }
}

impl OneArgHandle {
    pub(crate) fn create(
        py: Python<'_>,
        callback: Py<PyAny>,
        arg: Py<PyAny>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        Self::create_with_debug(py, callback, arg, loop_obj, context, when, false)
    }

    pub(crate) fn create_with_debug(
        py: Python<'_>,
        callback: Py<PyAny>,
        arg: Py<PyAny>,
        loop_obj: Py<PyAny>,
        context: Option<Py<PyAny>>,
        when: Option<f64>,
        debug: bool,
    ) -> PyResult<Py<PyAny>> {
        let context = match context {
            Some(context) => context,
            None => copy_context(py)?,
        };
        let source_traceback = capture_source_traceback(py, &loop_obj, Some(debug))?;
        Ok(Py::new(
            py,
            OneArgHandle {
                cancelled: AtomicBool::new(false),
                scheduled: AtomicBool::new(when.is_some()),
                callback,
                arg,
                context,
                loop_obj,
                when,
                source_traceback,
            },
        )?
        .into_any())
    }

    pub(crate) fn run_inner(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        Self::run_bound(slf.bind(py), py)
    }

    pub(crate) fn run_bound(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let borrowed = slf.borrow();
        if borrowed.cancelled.load(Ordering::Acquire) {
            return Ok(());
        }

        let context = borrowed.context.clone_ref(py);
        let callback = borrowed.callback.clone_ref(py);
        let arg = borrowed.arg.clone_ref(py);
        let loop_obj = borrowed.loop_obj.clone_ref(py);
        let source_traceback = borrowed
            .source_traceback
            .as_ref()
            .map(|tb| tb.clone_ref(py));
        drop(borrowed);

        if let Err(err) = run_one_arg_handle(py, &context, &callback, &arg) {
            if err.is_instance_of::<PySystemExit>(py)
                || err.is_instance_of::<PyKeyboardInterrupt>(py)
            {
                return Err(err);
            }

            let ctx = PyDict::new(py);
            ctx.set_item("message", format_exception_message(py, &callback))?;
            ctx.set_item("exception", err.into_value(py))?;
            ctx.set_item("handle", slf.clone().unbind().into_any())?;
            if let Some(source_traceback) = source_traceback {
                ctx.set_item("source_traceback", source_traceback)?;
            }
            loop_obj
                .bind(py)
                .call_method1("call_exception_handler", (ctx,))?;
        }

        Ok(())
    }
}

impl FutureHandle {
    pub(crate) fn create(
        py: Python<'_>,
        future: Py<PyAny>,
        payload: Py<PyAny>,
        loop_obj: Py<PyAny>,
        op: FutureHandleOp,
        when: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        Ok(Py::new(
            py,
            Self::new(py, future, payload, loop_obj, op as i32, when)?,
        )?
        .into_any())
    }
}

pub(crate) fn mark_native_unscheduled(py: Python<'_>, handle: &Py<PyAny>) -> bool {
    let Some(kind) = native_handle_kind(py, handle) else {
        return false;
    };
    match kind {
        NativeHandleKind::ZeroArg => unsafe { handle.bind(py).cast_unchecked::<ZeroArgHandle>() }
            .borrow()
            .scheduled
            .store(false, Ordering::Release),
        NativeHandleKind::OneArg => unsafe { handle.bind(py).cast_unchecked::<OneArgHandle>() }
            .borrow()
            .scheduled
            .store(false, Ordering::Release),
        NativeHandleKind::Handle => unsafe { handle.bind(py).cast_unchecked::<Handle>() }
            .borrow()
            .mark_unscheduled_inner(),
        NativeHandleKind::Future => unsafe { handle.bind(py).cast_unchecked::<FutureHandle>() }
            .borrow()
            .mark_unscheduled_inner(),
    }
    true
}

pub(crate) fn run_native_handle(py: Python<'_>, handle: &Py<PyAny>) -> Option<PyResult<()>> {
    match native_handle_kind(py, handle)? {
        NativeHandleKind::ZeroArg => Some(ZeroArgHandle::run_bound(
            unsafe { handle.cast_bound_unchecked::<ZeroArgHandle>(py) },
            py,
        )),
        NativeHandleKind::OneArg => Some(OneArgHandle::run_bound(
            unsafe { handle.cast_bound_unchecked::<OneArgHandle>(py) },
            py,
        )),
        NativeHandleKind::Handle => Some(Handle::run_bound(
            unsafe { handle.cast_bound_unchecked::<Handle>(py) },
            py,
        )),
        NativeHandleKind::Future => Some(FutureHandle::run_bound(
            unsafe { handle.cast_bound_unchecked::<FutureHandle>(py) },
            py,
        )),
    }
}

#[derive(Clone, Copy)]
enum NativeHandleKind {
    ZeroArg,
    OneArg,
    Handle,
    Future,
}

fn native_handle_kind(py: Python<'_>, handle: &Py<PyAny>) -> Option<NativeHandleKind> {
    let ty = unsafe { ffi::Py_TYPE(handle.as_ptr()) } as usize;
    if ty == native_type_ptr::<ZeroArgHandle>(py) {
        return Some(NativeHandleKind::ZeroArg);
    }
    if ty == native_type_ptr::<OneArgHandle>(py) {
        return Some(NativeHandleKind::OneArg);
    }
    if ty == native_type_ptr::<Handle>(py) {
        return Some(NativeHandleKind::Handle);
    }
    if ty == native_type_ptr::<FutureHandle>(py) {
        return Some(NativeHandleKind::Future);
    }
    None
}

fn native_type_ptr<T: PyTypeInfo + 'static>(py: Python<'_>) -> usize {
    static ZERO_ARG_HANDLE_TYPE: PyOnceLock<usize> = PyOnceLock::new();
    static ONE_ARG_HANDLE_TYPE: PyOnceLock<usize> = PyOnceLock::new();
    static HANDLE_TYPE: PyOnceLock<usize> = PyOnceLock::new();
    static FUTURE_HANDLE_TYPE: PyOnceLock<usize> = PyOnceLock::new();

    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<ZeroArgHandle>() {
        return *ZERO_ARG_HANDLE_TYPE.get_or_init(py, || PyType::new::<ZeroArgHandle>(py).as_ptr() as usize);
    }
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<OneArgHandle>() {
        return *ONE_ARG_HANDLE_TYPE.get_or_init(py, || PyType::new::<OneArgHandle>(py).as_ptr() as usize);
    }
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Handle>() {
        return *HANDLE_TYPE.get_or_init(py, || PyType::new::<Handle>(py).as_ptr() as usize);
    }
    *FUTURE_HANDLE_TYPE.get_or_init(py, || PyType::new::<FutureHandle>(py).as_ptr() as usize)
}

#[pyfunction]
#[pyo3(signature = (callback, args, loop_obj, context=None, when=None))]
pub(crate) fn make_handle(
    py: Python<'_>,
    callback: Py<PyAny>,
    args: Bound<'_, PyTuple>,
    loop_obj: Py<PyAny>,
    context: Option<Py<PyAny>>,
    when: Option<f64>,
) -> PyResult<Py<PyAny>> {
    if let Some(handle) =
        try_make_future_handle(py, &callback, &args, loop_obj.clone_ref(py), when)?
    {
        return Ok(handle);
    }

    if args.is_empty() {
        return ZeroArgHandle::create(py, callback, loop_obj, context, when);
    }
    if args.len() == 1 {
        return OneArgHandle::create(
            py,
            callback,
            args.get_item(0)?.unbind(),
            loop_obj,
            context,
            when,
        );
    }
    Handle::create(py, callback, args, loop_obj, context, when)
}

pub(crate) unsafe fn make_handle_fastcall(
    py: Python<'_>,
    callback: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    arg_count: usize,
    loop_obj: Py<PyAny>,
    context: Option<Py<PyAny>>,
    when: Option<f64>,
    debug: bool,
) -> PyResult<Py<PyAny>> {
    let callback = Bound::from_borrowed_ptr(py, callback).unbind();

    if let Some(handle) = try_make_future_handle_fastcall(
        py,
        &callback,
        args,
        arg_count,
        loop_obj.clone_ref(py),
        when,
    )? {
        return Ok(handle);
    }

    if arg_count == 0 {
        return ZeroArgHandle::create_with_debug(py, callback, loop_obj, context, when, debug);
    }
    if arg_count == 1 {
        let arg = Bound::from_borrowed_ptr(py, *args.add(0)).unbind();
        return OneArgHandle::create_with_debug(
            py,
            callback,
            arg,
            loop_obj,
            context,
            when,
            debug,
        );
    }
    let args = HandleArgs::from_fastcall(py, args, arg_count)?;
    Handle::create_with_debug_from_args(py, callback, args, loop_obj, context, when, debug)
}

fn try_make_future_handle(
    py: Python<'_>,
    callback: &Py<PyAny>,
    args: &Bound<'_, PyTuple>,
    loop_obj: Py<PyAny>,
    when: Option<f64>,
) -> PyResult<Option<Py<PyAny>>> {
    match args.len() {
        1 => {
            let bound_callback = callback.bind(py);
            let Ok(target) = bound_callback.getattr("__self__") else {
                return Ok(None);
            };
            if !target.is_instance(future_type(py)?)? {
                return Ok(None);
            }
            let Ok(name) = bound_callback.getattr("__name__") else {
                return Ok(None);
            };
            let op = match name.extract::<&str>()? {
                "set_result" => FutureHandleOp::SetResult,
                "set_exception" => FutureHandleOp::SetException,
                _ => return Ok(None),
            };
            let payload = args.get_item(0)?.unbind();
            return FutureHandle::create(py, target.unbind(), payload, loop_obj, op, when)
                .map(Some);
        }
        2 => {
            if !callback.bind(py).is(set_result_unless_cancelled(py)?) {
                return Ok(None);
            }
            let future = args.get_item(0)?;
            if !future.is_instance(future_type(py)?)? {
                return Ok(None);
            }
            let payload = args.get_item(1)?.unbind();
            return FutureHandle::create(
                py,
                future.unbind(),
                payload,
                loop_obj,
                FutureHandleOp::SetResultUnlessCancelled,
                when,
            )
            .map(Some);
        }
        _ => {}
    }

    Ok(None)
}

unsafe fn try_make_future_handle_fastcall(
    py: Python<'_>,
    callback: &Py<PyAny>,
    args: *const *mut ffi::PyObject,
    arg_count: usize,
    loop_obj: Py<PyAny>,
    when: Option<f64>,
) -> PyResult<Option<Py<PyAny>>> {
    match arg_count {
        1 => {
            let bound_callback = callback.bind(py);
            let Ok(target) = bound_callback.getattr("__self__") else {
                return Ok(None);
            };
            if !target.is_instance(future_type(py)?)? {
                return Ok(None);
            }
            let Ok(name) = bound_callback.getattr("__name__") else {
                return Ok(None);
            };
            let op = match name.extract::<&str>()? {
                "set_result" => FutureHandleOp::SetResult,
                "set_exception" => FutureHandleOp::SetException,
                _ => return Ok(None),
            };
            let payload = Bound::from_borrowed_ptr(py, *args.add(0)).unbind();
            return FutureHandle::create(py, target.unbind(), payload, loop_obj, op, when)
                .map(Some);
        }
        2 => {
            if !callback.bind(py).is(set_result_unless_cancelled(py)?) {
                return Ok(None);
            }
            let future = Bound::from_borrowed_ptr(py, *args.add(0));
            if !future.is_instance(future_type(py)?)? {
                return Ok(None);
            }
            let payload = Bound::from_borrowed_ptr(py, *args.add(1)).unbind();
            return FutureHandle::create(
                py,
                future.unbind(),
                payload,
                loop_obj,
                FutureHandleOp::SetResultUnlessCancelled,
                when,
            )
            .map(Some);
        }
        _ => {}
    }

    Ok(None)
}
fn tuple_from_fastcall(
    py: Python<'_>,
    args: *const *mut ffi::PyObject,
    arg_count: usize,
) -> PyResult<Py<PyTuple>> {
    unsafe {
        let tuple_ptr = ffi::PyTuple_New(arg_count as ffi::Py_ssize_t);
        let tuple = Bound::<PyAny>::from_owned_ptr_or_err(py, tuple_ptr)?;
        for index in 0..arg_count {
            let item = *args.add(index);
            ffi::Py_INCREF(item);
            ffi::PyTuple_SET_ITEM(tuple.as_ptr(), index as ffi::Py_ssize_t, item);
        }
        Ok(tuple.cast_into_unchecked::<PyTuple>().unbind())
    }
}

fn future_type(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static FUTURE_TYPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let future_type = FUTURE_TYPE.get_or_try_init(py, || -> PyResult<_> {
        Ok(py.import("asyncio")?.getattr("Future")?.unbind())
    })?;
    Ok(future_type.bind(py))
}

fn set_result_unless_cancelled(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static SET_RESULT_UNLESS_CANCELLED: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let func = SET_RESULT_UNLESS_CANCELLED.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.futures")?
            .getattr("_set_result_unless_cancelled")?
            .unbind())
    })?;
    Ok(func.bind(py))
}

fn copy_context(py: Python<'_>) -> PyResult<Py<PyAny>> {
    unsafe {
        let ptr = ffi::PyContext_CopyCurrent();
        Bound::from_owned_ptr_or_err(py, ptr).map(Bound::unbind)
    }
}

fn capture_source_traceback(
    py: Python<'_>,
    loop_obj: &Py<PyAny>,
    debug: Option<bool>,
) -> PyResult<Option<Py<PyAny>>> {
    let debug = match debug {
        Some(debug) => debug,
        None => loop_obj.bind(py).call_method0("get_debug")?.is_truthy()?,
    };
    if !debug {
        return Ok(None);
    }

    let sys = py.import("sys")?;
    let format_helpers = py.import("asyncio.format_helpers")?;
    let frame = sys.getattr("_getframe")?.call1((1,))?;
    Ok(Some(
        format_helpers
            .getattr("extract_stack")?
            .call1((frame,))?
            .unbind(),
    ))
}

fn format_callback(py: Python<'_>, callback: &Py<PyAny>) -> PyResult<String> {
    callback
        .bind(py)
        .repr()?
        .extract::<String>()
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

fn format_exception_message(py: Python<'_>, callback: &Py<PyAny>) -> String {
    match format_callback(py, callback) {
        Ok(callback) => format!("Exception in callback {callback}"),
        Err(_) => "Exception in callback".to_string(),
    }
}

fn run_handle(
    py: Python<'_>,
    context: &Py<PyAny>,
    callback: &Py<PyAny>,
    args: &HandleArgs,
) -> PyResult<()> {
    let callback = callback.bind(py);
    with_context(py, context, || {
        unsafe {
            match args {
                HandleArgs::Two([arg0, arg1]) => {
                    let arg0 = arg0.bind(py);
                    let arg1 = arg1.bind(py);
                    let mut argv = [arg0.as_ptr(), arg1.as_ptr()];
                    Ok(ffi::PyObject_Vectorcall(
                        callback.as_ptr(),
                        argv.as_mut_ptr(),
                        argv.len(),
                        std::ptr::null_mut(),
                    ))
                }
                HandleArgs::Tuple(args) => match args.bind(py).len() {
                    0 => Ok(ffi::PyObject_CallNoArgs(callback.as_ptr())),
                    1 => {
                        let args = args.bind(py);
                        let arg0 = args.get_item(0)?;
                        Ok(ffi::PyObject_CallOneArg(callback.as_ptr(), arg0.as_ptr()))
                    }
                    2 => {
                        let args = args.bind(py);
                        let arg0 = args.get_item(0)?;
                        let arg1 = args.get_item(1)?;
                        let mut argv = [arg0.as_ptr(), arg1.as_ptr()];
                        Ok(ffi::PyObject_Vectorcall(
                            callback.as_ptr(),
                            argv.as_mut_ptr(),
                            argv.len(),
                            std::ptr::null_mut(),
                        ))
                    }
                    _ => {
                        let args = args.bind(py);
                        let mut argv: Vec<*mut ffi::PyObject> = Vec::with_capacity(args.len());
                        for item in args.iter() {
                            argv.push(item.as_ptr());
                        }
                        Ok(ffi::PyObject_Vectorcall(
                            callback.as_ptr(),
                            argv.as_mut_ptr(),
                            argv.len(),
                            std::ptr::null_mut(),
                        ))
                    }
                },
            }
        }
    })
}

fn run_zero_arg_handle(py: Python<'_>, context: &Py<PyAny>, callback: &Py<PyAny>) -> PyResult<()> {
    let callback = callback.bind(py);
    with_context(py, context, || unsafe { Ok(ffi::PyObject_CallNoArgs(callback.as_ptr())) })
}

pub(crate) fn run_one_arg_handle(
    py: Python<'_>,
    context: &Py<PyAny>,
    callback: &Py<PyAny>,
    arg: &Py<PyAny>,
) -> PyResult<()> {
    let callback = callback.bind(py);
    let arg = arg.bind(py);
    with_context(py, context, || unsafe {
        Ok(ffi::PyObject_CallOneArg(callback.as_ptr(), arg.as_ptr()))
    })
}

pub(crate) fn run_one_arg_direct(
    py: Python<'_>,
    callback: &Py<PyAny>,
    arg: &Py<PyAny>,
) -> PyResult<()> {
    let callback = callback.bind(py);
    let arg = arg.bind(py);

    unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            ffi::PyObject_CallOneArg(callback.as_ptr(), arg.as_ptr()),
        )?;
    }

    Ok(())
}

fn with_context<F>(py: Python<'_>, context: &Py<PyAny>, f: F) -> PyResult<()>
where
    F: FnOnce() -> PyResult<*mut ffi::PyObject>,
{
    unsafe {
        let ctx = context.as_ptr();
        if ffi::PyContext_Enter(ctx) != 0 {
            return Err(PyErr::fetch(py));
        }

        let call_result = f().and_then(|obj| Bound::from_owned_ptr_or_err(py, obj).map(|_| ()));
        let exit_result = if ffi::PyContext_Exit(ctx) != 0 {
            Err(PyErr::fetch(py))
        } else {
            Ok(())
        };

        match (call_result, exit_result) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

use std::future::Future;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_async_runtimes::TaskLocals;

use crate::completion::{current_completion_port, enqueue_completion};

#[pyfunction]
pub fn backend_name() -> &'static str {
    "tokio"
}

#[pyfunction]
pub fn sleep<'py>(py: Python<'py>, delay_secs: f64) -> PyResult<Bound<'py, PyAny>> {
    spawn_into_python(py, async move {
        tokio::time::sleep(Duration::from_secs_f64(delay_secs)).await;
        Ok(Python::attach(|py| py.None()))
    })
}

#[pyfunction]
pub fn run_in_tokio<'py>(
    py: Python<'py>,
    awaitable: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
    let awaitable = awaitable.unbind();

    spawn_into_python(py, async move {
        pyo3_async_runtimes::tokio::scope(locals, async move {
            let future = Python::attach(move |py| {
                let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
                pyo3_async_runtimes::into_future_with_locals(&locals, awaitable.into_bound(py))
            })?;

            future.await
        })
        .await
    })
}

#[pyfunction]
pub fn wrap_future<'py>(
    py: Python<'py>,
    awaitable: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;
    let awaitable = awaitable.unbind();

    spawn_into_python(py, async move {
        pyo3_async_runtimes::tokio::scope(locals, async move {
            let future = Python::attach(move |py| {
                let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
                pyo3_async_runtimes::into_future_with_locals(&locals, awaitable.into_bound(py))
            })?;

            future.await
        })
        .await
    })
}

fn spawn_into_python<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> pyo3::IntoPyObject<'a> + Send + 'static,
{
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let py_future = event_loop.call_method0("create_future")?.unbind();
    let port = current_completion_port(py)?;
    let future_ref = py_future.clone_ref(py);

    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let result = fut.await;

        Python::attach(move |py| {
            let payload = match result {
                Ok(value) => value.into_py_any(py).map(|value| (value, false)),
                Err(err) => err.into_py_any(py).map(|value| (value, true)),
            };

            if let Ok((payload, is_err)) = payload {
                enqueue_completion(&port, future_ref, payload, is_err);
            }
        });
    });

    Ok(py_future.into_bound(py))
}

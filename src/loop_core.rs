#![cfg(unix)]

use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;

#[pyclass(module = "rsloop._rsloop")]
pub struct RsloopLoopCore {
    loop_obj: Py<PyAny>,
    scheduler: Py<PyAny>,
    poller: Py<PyAny>,
    completion_port: Py<PyAny>,
    fd_registry: Py<PyAny>,
    running: AtomicBool,
    stopping: AtomicBool,
    closed: AtomicBool,
    debug: AtomicBool,
}

#[pymethods]
impl RsloopLoopCore {
    #[new]
    #[pyo3(signature = (
        loop_obj,
        scheduler,
        poller,
        completion_port,
        fd_registry,
        *,
        debug=false,
        stopping=false,
        closed=false
    ))]
    fn new(
        loop_obj: Py<PyAny>,
        scheduler: Py<PyAny>,
        poller: Py<PyAny>,
        completion_port: Py<PyAny>,
        fd_registry: Py<PyAny>,
        debug: bool,
        stopping: bool,
        closed: bool,
    ) -> Self {
        Self {
            loop_obj,
            scheduler,
            poller,
            completion_port,
            fd_registry,
            running: AtomicBool::new(false),
            stopping: AtomicBool::new(stopping),
            closed: AtomicBool::new(closed),
            debug: AtomicBool::new(debug),
        }
    }

    fn run_once(
        &self,
        py: Python<'_>,
        clock_resolution: f64,
        slow_callback_duration: f64,
    ) -> PyResult<()> {
        self.ensure_open()?;

        self.scheduler.bind(py).call_method1(
            "run_once",
            (
                self.loop_obj.bind(py),
                self.poller.bind(py),
                self.completion_port.bind(py),
                self.stopping.load(Ordering::Acquire),
                clock_resolution,
                self.debug.load(Ordering::Acquire),
                slow_callback_duration,
            ),
        )?;

        self.sync_stopping_from_loop(py)?;
        Ok(())
    }

    fn run_forever(
        &self,
        py: Python<'_>,
        clock_resolution: f64,
        slow_callback_duration: f64,
    ) -> PyResult<()> {
        self.ensure_open()?;
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(PyRuntimeError::new_err("Event loop is already running"));
        }

        self.stopping.store(false, Ordering::Release);
        if self.loop_obj.bind(py).hasattr("_stopping")? {
            self.loop_obj.bind(py).setattr("_stopping", false)?;
        }

        let setup_result = if self.loop_obj.bind(py).hasattr("_run_forever_setup")? {
            self.loop_obj.bind(py).call_method0("_run_forever_setup")?;
            Ok(())
        } else {
            Ok(())
        };

        let result = setup_result.and_then(|_| {
            loop {
                self.run_once(py, clock_resolution, slow_callback_duration)?;
                if self.stopping.load(Ordering::Acquire) {
                    break;
                }
            }
            Ok(())
        });

        let cleanup_result = if self.loop_obj.bind(py).hasattr("_run_forever_cleanup")? {
            self.loop_obj
                .bind(py)
                .call_method0("_run_forever_cleanup")?;
            Ok(())
        } else {
            Ok(())
        };

        self.running.store(false, Ordering::Release);

        result?;
        cleanup_result
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        self.stopping.store(true, Ordering::Release);
        if self.loop_obj.bind(py).hasattr("_stopping")? {
            self.loop_obj.bind(py).setattr("_stopping", true)?;
        }
        if self.loop_obj.bind(py).hasattr("_write_to_self")? {
            self.loop_obj.bind(py).call_method0("_write_to_self")?;
        }
        Ok(())
    }

    fn add_reader(
        &self,
        py: Python<'_>,
        fd: i32,
        handle: Py<PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let previous = self
            .fd_registry
            .bind(py)
            .call_method1("add_reader", (fd, handle.bind(py)))?;
        self.sync_fd_interest(py, fd)?;
        Self::optional_object(previous)
    }

    fn remove_reader(&self, py: Python<'_>, fd: i32) -> PyResult<Option<Py<PyAny>>> {
        let previous = self
            .fd_registry
            .bind(py)
            .call_method1("remove_reader", (fd,))?;
        self.sync_fd_interest(py, fd)?;
        Self::optional_object(previous)
    }

    fn add_writer(
        &self,
        py: Python<'_>,
        fd: i32,
        handle: Py<PyAny>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let previous = self
            .fd_registry
            .bind(py)
            .call_method1("add_writer", (fd, handle.bind(py)))?;
        self.sync_fd_interest(py, fd)?;
        Self::optional_object(previous)
    }

    fn remove_writer(&self, py: Python<'_>, fd: i32) -> PyResult<Option<Py<PyAny>>> {
        let previous = self
            .fd_registry
            .bind(py)
            .call_method1("remove_writer", (fd,))?;
        self.sync_fd_interest(py, fd)?;
        Self::optional_object(previous)
    }

    fn fd_interest(&self, py: Python<'_>, fd: i32) -> PyResult<(bool, bool)> {
        self.fd_registry
            .bind(py)
            .call_method1("interest", (fd,))?
            .extract()
    }

    fn clear_fd_callbacks(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let cleared = self.fd_registry.bind(py).call_method0("clear")?.extract()?;
        Ok(cleared)
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn get_debug(&self) -> bool {
        self.debug.load(Ordering::Acquire)
    }

    fn set_debug(&self, enabled: bool) {
        self.debug.store(enabled, Ordering::Release);
    }

    fn set_closed(&self, closed: bool) {
        self.closed.store(closed, Ordering::Release);
    }

    fn set_stopping(&self, stopping: bool) {
        self.stopping.store(stopping, Ordering::Release);
    }

    fn scheduler(&self, py: Python<'_>) -> Py<PyAny> {
        self.scheduler.clone_ref(py)
    }

    fn poller(&self, py: Python<'_>) -> Py<PyAny> {
        self.poller.clone_ref(py)
    }

    fn completion_port(&self, py: Python<'_>) -> Py<PyAny> {
        self.completion_port.clone_ref(py)
    }

    fn fd_registry(&self, py: Python<'_>) -> Py<PyAny> {
        self.fd_registry.clone_ref(py)
    }

    fn loop_object(&self, py: Python<'_>) -> Py<PyAny> {
        self.loop_obj.clone_ref(py)
    }
}

impl RsloopLoopCore {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("Event loop is closed"));
        }
        Ok(())
    }

    fn sync_stopping_from_loop(&self, py: Python<'_>) -> PyResult<()> {
        if self.loop_obj.bind(py).hasattr("_stopping")? {
            let stopping = self.loop_obj.bind(py).getattr("_stopping")?.is_truthy()?;
            self.stopping.store(stopping, Ordering::Release);
        }
        Ok(())
    }

    fn sync_fd_interest(&self, py: Python<'_>, fd: i32) -> PyResult<()> {
        let (readable, writable): (bool, bool) = self
            .fd_registry
            .bind(py)
            .call_method1("interest", (fd,))?
            .extract()?;
        self.poller
            .bind(py)
            .call_method1("set_interest", (fd, readable, writable))?;
        Ok(())
    }

    fn optional_object(value: Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        if value.is_none() {
            Ok(None)
        } else {
            Ok(Some(value.unbind()))
        }
    }
}

pub(crate) fn add_loop_core_class(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RsloopLoopCore>()
}

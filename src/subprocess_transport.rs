#![cfg(unix)]

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;

enum PendingCall {
    NoArgs(Py<PyAny>),
    OneArg(Py<PyAny>, Py<PyAny>),
    TwoArgs(Py<PyAny>, Py<PyAny>, Py<PyAny>),
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct SubprocessTransport {
    loop_obj: Py<PyAny>,
    loop_thread_id: Option<u64>,
    protocol: Py<PyAny>,
    proc: Py<PyAny>,
    extra: Py<PyAny>,
    pid: i32,
    returncode: Option<i32>,
    exit_waiters: Vec<Py<PyAny>>,
    pending_calls: Option<Vec<PendingCall>>,
    pipes: HashMap<i32, Py<PyAny>>,
    finished: bool,
    pipes_connected: bool,
    closed: bool,
}

#[pymethods]
impl SubprocessTransport {
    #[new]
    #[pyo3(signature = (loop_obj, protocol, proc, extra=None))]
    fn new(
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        protocol: Py<PyAny>,
        proc: Py<PyAny>,
        extra: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let extra = match extra {
            Some(extra) => extra,
            None => PyDict::new(py).into_any().unbind(),
        };
        let pid = proc.bind(py).getattr("pid")?.extract::<i32>()?;
        let loop_thread_id = loop_obj
            .bind(py)
            .getattr("_thread_id")
            .ok()
            .and_then(|value| value.extract::<u64>().ok());
        extra.bind(py).set_item("subprocess", proc.clone_ref(py))?;

        Ok(Self {
            loop_obj,
            loop_thread_id,
            protocol,
            proc,
            extra,
            pid,
            returncode: None,
            exit_waiters: Vec::new(),
            pending_calls: Some(Vec::new()),
            pipes: HashMap::new(),
            finished: false,
            pipes_connected: false,
            closed: false,
        })
    }

    fn get_pid(&self) -> i32 {
        self.pid
    }

    fn get_returncode(&self) -> Option<i32> {
        self.returncode
    }

    fn get_pipe_transport(&self, py: Python<'_>, fd: i32) -> PyResult<Py<PyAny>> {
        if let Some(proto) = self.pipes.get(&fd) {
            if let Ok(pipe) = proto.bind(py).getattr("pipe") {
                return Ok(pipe.unbind());
            }
        }
        Ok(py.None())
    }

    fn get_protocol(&self, py: Python<'_>) -> Py<PyAny> {
        self.protocol.clone_ref(py)
    }

    fn _set_pipe_proto(&mut self, fd: i32, proto: Py<PyAny>) {
        self.pipes.insert(fd, proto);
    }

    fn _connection_made(&mut self, py: Python<'_>) -> PyResult<()> {
        self.assert_loop_thread(py)?;
        if let Some(pending) = self.pending_calls.take() {
            for call in pending {
                Self::schedule_call(py, &self.loop_obj, call)?;
            }
        }
        self.pipes_connected = true;
        self.try_finish(py)
    }

    fn _process_exited(&mut self, py: Python<'_>, returncode: i32) -> PyResult<()> {
        self.assert_loop_thread(py)?;
        if self.returncode.is_some() {
            return Ok(());
        }
        self.returncode = Some(returncode);
        if self.proc.bind(py).getattr("returncode")?.is_none() {
            self.proc.bind(py).setattr("returncode", returncode)?;
        }
        let callback = self.protocol.bind(py).getattr("process_exited")?.unbind();
        self.call_or_queue(py, PendingCall::NoArgs(callback))?;
        self.try_finish(py)
    }

    fn _pipe_connection_lost(
        &mut self,
        py: Python<'_>,
        fd: i32,
        exc: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.assert_loop_thread(py)?;
        let callback = self
            .protocol
            .bind(py)
            .getattr("pipe_connection_lost")?
            .unbind();
        let fd_obj = fd.into_py_any(py)?;
        let exc_obj = exc.unwrap_or_else(|| py.None());
        self.call_or_queue(py, PendingCall::TwoArgs(callback, fd_obj, exc_obj))?;
        self.try_finish(py)
    }

    fn _pipe_data_received(&mut self, py: Python<'_>, fd: i32, data: Py<PyAny>) -> PyResult<()> {
        self.assert_loop_thread(py)?;
        let callback = self
            .protocol
            .bind(py)
            .getattr("pipe_data_received")?
            .unbind();
        let fd_obj = fd.into_py_any(py)?;
        self.call_or_queue(py, PendingCall::TwoArgs(callback, fd_obj, data))?;
        Ok(())
    }

    fn _wait(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.assert_loop_thread(py)?;
        let loop_obj = self.loop_obj.bind(py);
        let waiter = loop_obj.call_method0("create_future")?.unbind();
        if let Some(returncode) = self.returncode {
            waiter.call_method1(py, "set_result", (returncode,))?;
            return Ok(waiter);
        }
        self.exit_waiters.push(waiter.clone_ref(py));
        Ok(waiter)
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        self.assert_loop_thread(py)?;
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        let loop_closed = self
            .loop_obj
            .bind(py)
            .call_method0("is_closed")?
            .extract::<bool>()?;
        if !loop_closed {
            for (fd, proto) in self.pipes.iter() {
                if let Ok(pipe) = proto.bind(py).getattr("pipe") {
                    let _ = pipe.call_method0("close");
                }
            }
        }

        if self.returncode.is_none() {
            let poll = self.proc.bind(py).call_method0("poll")?;
            if poll.is_none() {
                let _ = self.proc.bind(py).call_method0("kill");
            }
        }
        Ok(())
    }

    fn send_signal(&self, py: Python<'_>, sig: i32) -> PyResult<()> {
        let os = py.import("os")?;
        let _ = os.call_method1("kill", (self.pid, sig));
        Ok(())
    }

    fn terminate(&self, py: Python<'_>) -> PyResult<()> {
        let _ = self.proc.bind(py).call_method0("terminate");
        Ok(())
    }

    fn kill(&self, py: Python<'_>) -> PyResult<()> {
        let _ = self.proc.bind(py).call_method0("kill");
        Ok(())
    }

    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(
        &self,
        py: Python<'_>,
        name: &str,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if name == "subprocess" {
            return Ok(self.proc.clone_ref(py));
        }
        let extra = self.extra.bind(py).cast::<PyDict>()?;
        if let Some(value) = extra.get_item(name)? {
            Ok(value.unbind())
        } else {
            Ok(default.unwrap_or_else(|| py.None()))
        }
    }
}

impl SubprocessTransport {
    fn assert_loop_thread(&self, py: Python<'_>) -> PyResult<()> {
        let Some(expected_thread_id) = self.loop_thread_id else {
            return Ok(());
        };
        let threading = py.import("threading")?;
        let current_thread_id = threading.call_method0("get_ident")?.extract::<u64>()?;
        if current_thread_id != expected_thread_id {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "subprocess transport callback on non-loop thread: expected={}, got={}",
                expected_thread_id, current_thread_id
            )));
        }
        Ok(())
    }

    fn call_or_queue(&mut self, py: Python<'_>, call: PendingCall) -> PyResult<()> {
        if let Some(pending) = &mut self.pending_calls {
            pending.push(call);
            return Ok(());
        }
        Self::schedule_call(py, &self.loop_obj, call)
    }

    fn try_finish(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.returncode.is_none() {
            return Ok(());
        }

        if !self.pipes_connected {
            self.wakeup_waiters(py)?;
            return Ok(());
        }

        let mut all_disconnected = true;
        for proto in self.pipes.values() {
            let disconnected = proto
                .bind(py)
                .getattr("disconnected")?
                .extract::<bool>()
                .unwrap_or(false);
            if !disconnected {
                all_disconnected = false;
                break;
            }
        }

        if all_disconnected && !self.finished {
            self.finished = true;
            let callback = self.protocol.bind(py).getattr("connection_lost")?.unbind();
            self.call_or_queue(py, PendingCall::OneArg(callback, py.None()))?;
            self.wakeup_waiters(py)?;
        }

        Ok(())
    }

    fn wakeup_waiters(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(returncode) = self.returncode {
            for waiter in self.exit_waiters.drain(..) {
                if !waiter
                    .bind(py)
                    .call_method0("cancelled")?
                    .extract::<bool>()?
                {
                    let _ = waiter.call_method1(py, "set_result", (returncode,));
                }
            }
        }
        Ok(())
    }

    fn schedule_call(py: Python<'_>, loop_obj: &Py<PyAny>, call: PendingCall) -> PyResult<()> {
        match call {
            PendingCall::NoArgs(callback) => {
                loop_obj
                    .bind(py)
                    .call_method1("call_soon", (callback.bind(py),))?;
            }
            PendingCall::OneArg(callback, arg0) => {
                loop_obj
                    .bind(py)
                    .call_method1("call_soon", (callback.bind(py), arg0.bind(py)))?;
            }
            PendingCall::TwoArgs(callback, arg0, arg1) => {
                loop_obj.bind(py).call_method1(
                    "call_soon",
                    (callback.bind(py), arg0.bind(py), arg1.bind(py)),
                )?;
            }
        }
        Ok(())
    }
}

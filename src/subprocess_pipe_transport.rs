#![cfg(unix)]

use pyo3::prelude::*;

#[derive(FromPyObject, Debug)]
#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct ReadSubprocessPipeProto {
    transport: Py<PyAny>,
    fd: i32,
    disconnected: bool,
    pipe: Option<Py<PyAny>>,
}

#[pymethods]
impl ReadSubprocessPipeProto {
    #[new]
    fn new(transport: Py<PyAny>, fd: i32) -> Self {
        Self {
            transport,
            fd,
            disconnected: false,
            pipe: None,
        }
    }

    fn connection_made(&mut self, transport: Py<PyAny>) -> PyResult<()> {
        self.pipe = Some(transport);
        Ok(())
    }

    fn data_received(&self, py: Python<'_>, data: Py<PyAny>) -> PyResult<()> {
        self.transport
            .bind(py)
            .call_method1("_pipe_data_received", (self.fd, data))?;
        Ok(())
    }

    fn connection_lost(&mut self, py: Python<'_>, exc: Option<Py<PyAny>>) -> PyResult<()> {
        if self.disconnected {
            return Ok(());
        }
        self.disconnected = true;
        self.transport.bind(py).call_method1(
            "_pipe_connection_lost",
            (self.fd, exc.unwrap_or_else(|| py.None())),
        )?;
        Ok(())
    }

    #[getter]
    fn disconnected(&self) -> bool {
        self.disconnected
    }

    #[setter]
    fn set_disconnected(&mut self, value: bool) {
        self.disconnected = value;
    }

    #[getter]
    fn pipe(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .pipe
            .as_ref()
            .map(|p| p.clone_ref(py))
            .unwrap_or_else(|| py.None()))
    }
}

#[derive(FromPyObject, Debug)]
#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct WriteSubprocessPipeProto {
    transport: Py<PyAny>,
    fd: i32,
    disconnected: bool,
    pipe: Option<Py<PyAny>>,
}

#[pymethods]
impl WriteSubprocessPipeProto {
    #[new]
    fn new(transport: Py<PyAny>, fd: i32) -> Self {
        Self {
            transport,
            fd,
            disconnected: false,
            pipe: None,
        }
    }

    fn connection_made(&mut self, transport: Py<PyAny>) -> PyResult<()> {
        self.pipe = Some(transport);
        Ok(())
    }

    fn pause_writing(&self, py: Python<'_>) -> PyResult<()> {
        let protocol = self.transport.bind(py).call_method0("get_protocol")?;
        let _ = protocol.call_method0("pause_writing");
        Ok(())
    }

    fn resume_writing(&self, py: Python<'_>) -> PyResult<()> {
        let protocol = self.transport.bind(py).call_method0("get_protocol")?;
        let _ = protocol.call_method0("resume_writing");
        Ok(())
    }

    fn connection_lost(&mut self, py: Python<'_>, exc: Option<Py<PyAny>>) -> PyResult<()> {
        if self.disconnected {
            return Ok(());
        }
        self.disconnected = true;
        self.transport.bind(py).call_method1(
            "_pipe_connection_lost",
            (self.fd, exc.unwrap_or_else(|| py.None())),
        )?;
        Ok(())
    }

    #[getter]
    fn disconnected(&self) -> bool {
        self.disconnected
    }

    #[setter]
    fn set_disconnected(&mut self, value: bool) {
        self.disconnected = value;
    }

    #[getter]
    fn pipe(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .pipe
            .as_ref()
            .map(|p| p.clone_ref(py))
            .unwrap_or_else(|| py.None()))
    }
}

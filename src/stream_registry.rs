#![cfg(unix)]

use std::collections::HashMap;
use std::env;
use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::stream_transport::StreamTransport;

#[pyclass(module = "kioto._kioto")]
pub struct StreamTransportRegistry {
    transports: HashMap<RawFd, Py<StreamTransport>>,
}

#[pymethods]
impl StreamTransportRegistry {
    #[new]
    fn new() -> Self {
        Self {
            transports: HashMap::new(),
        }
    }

    fn register(&mut self, fd: i32, transport: Py<StreamTransport>) {
        self.transports.insert(fd as RawFd, transport);
    }

    fn unregister(&mut self, fd: i32) {
        self.transports.remove(&(fd as RawFd));
    }

    fn clear(&mut self) {
        self.transports.clear();
    }
}

impl StreamTransportRegistry {
    pub(crate) fn dispatch_inner(
        &self,
        py: Python<'_>,
        events: &[(i32, u8)],
        remaining: &mut Vec<(i32, u8)>,
    ) -> PyResult<()> {
        remaining.clear();
        for (fd, mask) in events {
            if let Some(transport) = self.transports.get(&(*fd as RawFd)) {
                if env::var_os("KIOTO_TRACE_STREAM").is_some() {
                    eprintln!("stream-registry dispatch mask={mask}");
                }
                let bound = transport.bind(py);
                if mask & 0x01 != 0 {
                    let mut stream = bound.borrow_mut();
                    stream.on_readable(py)?;
                }
                if mask & 0x02 != 0 {
                    let mut stream = bound.borrow_mut();
                    stream.on_writable(py)?;
                }
                continue;
            }
            remaining.push((*fd, *mask));
        }
        Ok(())
    }
}

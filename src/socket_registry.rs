#![cfg(unix)]

use std::collections::HashMap;
use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::socket_state::SocketState;

#[pyclass(module = "kioto._kioto")]
pub struct SocketStateRegistry {
    states: HashMap<RawFd, Py<SocketState>>,
}

#[pymethods]
impl SocketStateRegistry {
    #[new]
    fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    fn register(&mut self, fd: i32, state: Py<SocketState>) {
        self.states.insert(fd as RawFd, state);
    }

    fn unregister(&mut self, fd: i32) {
        self.states.remove(&(fd as RawFd));
    }

    fn clear(&mut self) {
        self.states.clear();
    }
}

impl SocketStateRegistry {
    pub(crate) fn dispatch_inner(
        &self,
        py: Python<'_>,
        events: &[(i32, u8)],
        remaining: &mut Vec<(i32, u8)>,
    ) -> PyResult<()> {
        remaining.clear();
        for (fd, mask) in events {
            if let Some(state) = self.states.get(&(*fd as RawFd)) {
                let bound = state.bind(py);
                if mask & 0x01 != 0 {
                    let mut state = bound.borrow_mut();
                    state.on_readable(py)?;
                }
                if mask & 0x02 != 0 {
                    let mut state = bound.borrow_mut();
                    state.on_writable(py)?;
                }
                continue;
            }
            remaining.push((*fd, *mask));
        }
        Ok(())
    }
}

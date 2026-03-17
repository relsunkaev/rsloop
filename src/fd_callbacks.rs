#![cfg(unix)]

use std::collections::HashMap;
use std::os::fd::RawFd;

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::handles::is_native_cancelled;

const EVENT_READ: u8 = 0x01;
const EVENT_WRITE: u8 = 0x02;

#[derive(Default)]
struct FdCallbackState {
    reader: Option<Py<PyAny>>,
    writer: Option<Py<PyAny>>,
}

#[derive(Default)]
struct FdCallbackEntry {
    state: Mutex<FdCallbackState>,
}

#[pyclass(module = "kioto._kioto")]
pub struct FdCallbackRegistry {
    states: RwLock<HashMap<RawFd, Arc<FdCallbackEntry>>>,
}

#[pymethods]
impl FdCallbackRegistry {
    #[new]
    fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    fn add_reader(&self, fd: i32, handle: Py<PyAny>) -> Option<Py<PyAny>> {
        self.add(fd as RawFd, handle, EventKind::Reader)
    }

    fn add_writer(&self, fd: i32, handle: Py<PyAny>) -> Option<Py<PyAny>> {
        self.add(fd as RawFd, handle, EventKind::Writer)
    }

    fn remove_reader(&self, fd: i32) -> Option<Py<PyAny>> {
        self.remove(fd as RawFd, EventKind::Reader)
    }

    fn remove_writer(&self, fd: i32) -> Option<Py<PyAny>> {
        self.remove(fd as RawFd, EventKind::Writer)
    }

    fn interest(&self, fd: i32) -> (bool, bool) {
        match self.states.read().get(&(fd as RawFd)) {
            Some(entry) => {
                let state = entry.state.lock();
                (state.reader.is_some(), state.writer.is_some())
            }
            None => (false, false),
        }
    }

    fn clear(&self) -> Vec<Py<PyAny>> {
        let mut states = self.states.write();
        states
            .drain()
            .flat_map(|(_, entry)| {
                let mut state = entry.state.lock();
                [state.reader.take(), state.writer.take()]
            })
            .flatten()
            .collect()
    }

    fn dispatch(&self, py: Python<'_>, events: Vec<(i32, u8)>) -> PyResult<Vec<Py<PyAny>>> {
        let mut ready = Vec::with_capacity(events.len() * 2);
        let entries: Vec<_> = {
            let states = self.states.read();
            events
                .into_iter()
                .filter_map(|(fd, mask)| {
                    states
                        .get(&(fd as RawFd))
                        .cloned()
                        .map(|entry| (fd as RawFd, mask, entry))
                })
                .collect()
        };
        let mut empty_fds = Vec::new();

        for (fd, mask, entry) in entries {
            let mut state = entry.state.lock();

            if mask & EVENT_READ != 0 {
                if let Some(handle) = state.reader.as_ref() {
                    if is_cancelled(py, handle)? {
                        state.reader = None;
                    } else {
                        ready.push(handle.clone_ref(py));
                    }
                }
            }

            if mask & EVENT_WRITE != 0 {
                if let Some(handle) = state.writer.as_ref() {
                    if is_cancelled(py, handle)? {
                        state.writer = None;
                    } else {
                        ready.push(handle.clone_ref(py));
                    }
                }
            }

            if state.reader.is_none() && state.writer.is_none() {
                empty_fds.push(fd);
            }
        }

        if !empty_fds.is_empty() {
            let mut states = self.states.write();
            for fd in empty_fds {
                let should_remove = states
                    .get(&fd)
                    .map(|entry| {
                        let state = entry.state.lock();
                        state.reader.is_none() && state.writer.is_none()
                    })
                    .unwrap_or(false);
                if should_remove {
                    states.remove(&fd);
                }
            }
        }

        Ok(ready)
    }
}

impl FdCallbackRegistry {
    fn add(&self, fd: RawFd, handle: Py<PyAny>, kind: EventKind) -> Option<Py<PyAny>> {
        let existing = {
            let states = self.states.read();
            states.get(&fd).cloned()
        };
        let entry = if let Some(entry) = existing {
            entry
        } else {
            let mut states = self.states.write();
            states
                .entry(fd)
                .or_insert_with(|| Arc::new(FdCallbackEntry::default()))
                .clone()
        };
        let mut state = entry.state.lock();
        match kind {
            EventKind::Reader => state.reader.replace(handle),
            EventKind::Writer => state.writer.replace(handle),
        }
    }

    fn remove(&self, fd: RawFd, kind: EventKind) -> Option<Py<PyAny>> {
        let entry = self.states.read().get(&fd).cloned()?;
        let mut state = entry.state.lock();
        let removed = match kind {
            EventKind::Reader => state.reader.take(),
            EventKind::Writer => state.writer.take(),
        };
        let should_remove = state.reader.is_none() && state.writer.is_none();
        drop(state);
        if should_remove {
            let mut states = self.states.write();
            let should_remove = states
                .get(&fd)
                .map(|entry| {
                    let state = entry.state.lock();
                    state.reader.is_none() && state.writer.is_none()
                })
                .unwrap_or(false);
            if should_remove {
                states.remove(&fd);
            }
        }
        removed
    }
}

#[derive(Clone, Copy)]
enum EventKind {
    Reader,
    Writer,
}

fn is_cancelled(py: Python<'_>, handle: &Py<PyAny>) -> PyResult<bool> {
    if let Some(cancelled) = is_native_cancelled(py, handle) {
        return Ok(cancelled);
    }

    handle
        .bind(py)
        .getattr("_cancelled")?
        .is_truthy()
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

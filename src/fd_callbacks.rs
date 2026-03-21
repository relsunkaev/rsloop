#![cfg(unix)]

use std::cell::RefCell;
use std::os::fd::RawFd;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::handles::is_native_cancelled;
use crate::scheduler::Scheduler;

const EVENT_READ: u8 = 0x01;
const EVENT_WRITE: u8 = 0x02;
const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[derive(Default)]
struct FdCallbackState {
    reader: Option<Py<PyAny>>,
    writer: Option<Py<PyAny>>,
}

#[derive(Default)]
#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct FdCallbackRegistry {
    states: RefCell<Vec<Option<Box<[FdCallbackState]>>>>,
}

#[pymethods]
impl FdCallbackRegistry {
    #[new]
    fn new() -> Self {
        Self {
            states: RefCell::new(Vec::new()),
        }
    }

    fn add_reader(&self, fd: i32, handle: Py<PyAny>) -> (Option<Py<PyAny>>, bool, bool) {
        self.add(fd as RawFd, handle, EventKind::Reader)
    }

    fn add_writer(&self, fd: i32, handle: Py<PyAny>) -> (Option<Py<PyAny>>, bool, bool) {
        self.add(fd as RawFd, handle, EventKind::Writer)
    }

    fn remove_reader(&self, fd: i32) -> (Option<Py<PyAny>>, bool, bool) {
        self.remove(fd as RawFd, EventKind::Reader)
    }

    fn remove_writer(&self, fd: i32) -> (Option<Py<PyAny>>, bool, bool) {
        self.remove(fd as RawFd, EventKind::Writer)
    }

    fn interest(&self, fd: i32) -> (bool, bool) {
        let index = fd as usize;
        let states = self.states.borrow();
        let Some(Some(segment)) = states.get(index >> SEGMENT_BITS) else {
            return (false, false);
        };
        let state = &segment[index & SEGMENT_MASK];
        (state.reader.is_some(), state.writer.is_some())
    }

    fn clear(&self) -> Vec<Py<PyAny>> {
        let mut states = self.states.borrow_mut();
        let mut cleared = Vec::new();
        for segment in states.iter_mut().flatten() {
            for state in segment.iter_mut() {
                cleared.extend([state.reader.take(), state.writer.take()].into_iter().flatten());
            }
        }
        states.clear();
        cleared
    }

    fn dispatch(&self, py: Python<'_>, events: Vec<(i32, u8)>) -> PyResult<Vec<Py<PyAny>>> {
        let mut ready = Vec::with_capacity(events.len() * 2);
        self.dispatch_with(py, &events, |handle| ready.push(handle))?;
        Ok(ready)
    }
}

impl FdCallbackRegistry {
    pub(crate) fn dispatch_handles(
        &self,
        py: Python<'_>,
        events: &[(i32, u8)],
        ready: &mut Vec<Py<PyAny>>,
    ) -> PyResult<()> {
        ready.clear();
        ready.reserve(events.len() * 2);
        self.dispatch_with(py, events, |handle| ready.push(handle))?;
        Ok(())
    }

    pub(crate) fn dispatch_ready_into(
        &self,
        py: Python<'_>,
        events: &[(i32, u8)],
        scheduler: &Scheduler,
    ) -> PyResult<usize> {
        self.dispatch_with(py, events, |handle| scheduler.push_ready_inner(handle))
    }

    fn add(&self, fd: RawFd, handle: Py<PyAny>, kind: EventKind) -> (Option<Py<PyAny>>, bool, bool) {
        let index = fd as usize;
        let mut states = self.states.borrow_mut();
        let segment = Self::segment_mut(&mut states, index);
        let state = &mut segment[index & SEGMENT_MASK];
        let previous = match kind {
            EventKind::Reader => state.reader.replace(handle),
            EventKind::Writer => state.writer.replace(handle),
        };
        (previous, state.reader.is_some(), state.writer.is_some())
    }

    fn remove(&self, fd: RawFd, kind: EventKind) -> (Option<Py<PyAny>>, bool, bool) {
        let index = fd as usize;
        let mut states = self.states.borrow_mut();
        let Some(Some(segment)) = states.get_mut(index >> SEGMENT_BITS) else {
            return (None, false, false);
        };
        let state = &mut segment[index & SEGMENT_MASK];
        let removed = match kind {
            EventKind::Reader => state.reader.take(),
            EventKind::Writer => state.writer.take(),
        };
        (removed, state.reader.is_some(), state.writer.is_some())
    }

    fn dispatch_with<F>(
        &self,
        py: Python<'_>,
        events: &[(i32, u8)],
        mut on_ready: F,
    ) -> PyResult<usize>
    where
        F: FnMut(Py<PyAny>),
    {
        let mut dispatched = 0usize;
        let mut states = self.states.borrow_mut();
        for (fd, mask) in events.iter().copied() {
            let index = fd as usize;
            let Some(Some(segment)) = states.get_mut(index >> SEGMENT_BITS) else {
                continue;
            };
            let state = &mut segment[index & SEGMENT_MASK];

            if mask & EVENT_READ != 0 {
                if let Some(handle) = state.reader.as_ref() {
                    if is_cancelled(py, handle)? {
                        state.reader = None;
                    } else {
                        on_ready(handle.clone_ref(py));
                        dispatched += 1;
                    }
                }
            }

            if mask & EVENT_WRITE != 0 {
                if let Some(handle) = state.writer.as_ref() {
                    if is_cancelled(py, handle)? {
                        state.writer = None;
                    } else {
                        on_ready(handle.clone_ref(py));
                        dispatched += 1;
                    }
                }
            }
        }

        Ok(dispatched)
    }

    fn segment_mut(
        states: &mut Vec<Option<Box<[FdCallbackState]>>>,
        index: usize,
    ) -> &mut [FdCallbackState] {
        let segment_index = index >> SEGMENT_BITS;
        if segment_index >= states.len() {
            states.resize_with(segment_index + 1, || None);
        }
        states[segment_index]
            .get_or_insert_with(|| {
                let mut segment = Vec::with_capacity(SEGMENT_SIZE);
                segment.resize_with(SEGMENT_SIZE, FdCallbackState::default);
                segment.into_boxed_slice()
            })
            .as_mut()
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

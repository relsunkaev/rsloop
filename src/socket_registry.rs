#![cfg(unix)]

use std::cell::RefCell;
use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::socket_state::{SocketCoreRef, SocketState};

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct SocketStateRegistry {
    segments: RefCell<Vec<Option<Box<[Option<SocketCoreRef>]>>>>,
}

#[pymethods]
impl SocketStateRegistry {
    #[new]
    fn new() -> Self {
        Self {
            segments: RefCell::new(Vec::new()),
        }
    }

    fn register(&self, fd: i32, state: Py<SocketState>) {
        let core = Python::attach(|py| state.borrow(py).core.clone());
        self.register_inner(fd as RawFd, core);
    }

    fn unregister(&self, fd: i32) {
        self.unregister_inner(fd as RawFd);
    }

    fn clear(&self) {
        self.segments.borrow_mut().clear();
    }
}

impl SocketStateRegistry {
    pub(crate) fn dispatch_one(&self, py: Python<'_>, fd: i32, mask: u8) -> PyResult<bool> {
        let index = fd as usize;
        let segments = self.segments.borrow();
        let Some(segment) = segments.get(index >> SEGMENT_BITS).and_then(|segment| segment.as_ref()) else {
            return Ok(false);
        };
        let Some(state) = segment[index & SEGMENT_MASK].as_ref() else {
            return Ok(false);
        };
        if mask & 0x01 != 0 {
            let mut state = state.borrow_mut();
            state.on_readable(py)?;
        }
        if mask & 0x02 != 0 {
            let mut state = state.borrow_mut();
            state.on_writable(py)?;
        }
        Ok(true)
    }

    pub(crate) fn register_inner(&self, fd: RawFd, state: SocketCoreRef) {
        let index = fd as usize;
        let mut segments = self.segments.borrow_mut();
        let segment = Self::segment_mut(&mut segments, index);
        segment[index & SEGMENT_MASK] = Some(state);
    }

    pub(crate) fn unregister_inner(&self, fd: RawFd) {
        let index = fd as usize;
        let segment_index = index >> SEGMENT_BITS;
        let mut segments = self.segments.borrow_mut();
        let Some(Some(segment)) = segments.get_mut(segment_index) else {
            return;
        };
        segment[index & SEGMENT_MASK] = None;
    }

    pub(crate) fn dispatch_inner(
        &self,
        py: Python<'_>,
        events: &[(i32, u8)],
        remaining: &mut Vec<(i32, u8)>,
    ) -> PyResult<()> {
        remaining.clear();
        for (fd, mask) in events {
            if !self.dispatch_one(py, *fd, *mask)? {
                remaining.push((*fd, *mask));
            }
        }
        Ok(())
    }

    fn segment_mut(
        segments: &mut Vec<Option<Box<[Option<SocketCoreRef>]>>>,
        index: usize,
    ) -> &mut [Option<SocketCoreRef>] {
        let segment_index = index >> SEGMENT_BITS;
        if segment_index >= segments.len() {
            segments.resize_with(segment_index + 1, || None);
        }
        segments[segment_index]
            .get_or_insert_with(|| {
                let mut segment = Vec::with_capacity(SEGMENT_SIZE);
                segment.resize_with(SEGMENT_SIZE, || None);
                segment.into_boxed_slice()
            })
            .as_mut()
    }
}

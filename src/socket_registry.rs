#![cfg(unix)]

use std::cell::RefCell;
use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::socket_state::SocketState;

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "kioto._kioto", unsendable)]
pub struct SocketStateRegistry {
    segments: RefCell<Vec<Option<Box<[Option<Py<SocketState>>]>>>>,
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
        self.register_inner(fd as RawFd, state);
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
        let Some(state) = self.get(py, fd as RawFd) else {
            return Ok(false);
        };
        let bound = state.bind(py);
        if mask & 0x01 != 0 {
            let mut state = bound.borrow_mut();
            state.on_readable(py)?;
        }
        if mask & 0x02 != 0 {
            let mut state = bound.borrow_mut();
            state.on_writable(py)?;
        }
        Ok(true)
    }

    pub(crate) fn register_inner(&self, fd: RawFd, state: Py<SocketState>) {
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

    fn get(&self, py: Python<'_>, fd: RawFd) -> Option<Py<SocketState>> {
        let index = fd as usize;
        let segments = self.segments.borrow();
        let segment = segments.get(index >> SEGMENT_BITS)?.as_ref()?;
        segment[index & SEGMENT_MASK].as_ref().map(|state| state.clone_ref(py))
    }

    fn segment_mut(
        segments: &mut Vec<Option<Box<[Option<Py<SocketState>>]>>>,
        index: usize,
    ) -> &mut [Option<Py<SocketState>>] {
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

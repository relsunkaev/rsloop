#![cfg(unix)]

use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::socket_state::SocketState;

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "kioto._kioto")]
pub struct SocketStateRegistry {
    segments: Vec<Option<Box<[Option<Py<SocketState>>]>>>,
}

#[pymethods]
impl SocketStateRegistry {
    #[new]
    fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    fn register(&mut self, fd: i32, state: Py<SocketState>) {
        self.register_inner(fd as RawFd, state);
    }

    fn unregister(&mut self, fd: i32) {
        self.unregister_inner(fd as RawFd);
    }

    fn clear(&mut self) {
        self.segments.clear();
    }
}

impl SocketStateRegistry {
    pub(crate) fn register_inner(&mut self, fd: RawFd, state: Py<SocketState>) {
        let index = fd as usize;
        let segment = self.segment_mut(index);
        segment[index & SEGMENT_MASK] = Some(state);
    }

    pub(crate) fn unregister_inner(&mut self, fd: RawFd) {
        let index = fd as usize;
        let segment_index = index >> SEGMENT_BITS;
        let Some(Some(segment)) = self.segments.get_mut(segment_index) else {
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
            if let Some(state) = self.get(*fd as RawFd) {
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

    fn get(&self, fd: RawFd) -> Option<&Py<SocketState>> {
        let index = fd as usize;
        let segment = self.segments.get(index >> SEGMENT_BITS)?.as_ref()?;
        segment[index & SEGMENT_MASK].as_ref()
    }

    fn segment_mut(&mut self, index: usize) -> &mut [Option<Py<SocketState>>] {
        let segment_index = index >> SEGMENT_BITS;
        if segment_index >= self.segments.len() {
            self.segments.resize_with(segment_index + 1, || None);
        }
        self.segments[segment_index]
            .get_or_insert_with(|| {
                let mut segment = Vec::with_capacity(SEGMENT_SIZE);
                segment.resize_with(SEGMENT_SIZE, || None);
                segment.into_boxed_slice()
            })
            .as_mut()
    }
}

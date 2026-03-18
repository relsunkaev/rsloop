#![cfg(unix)]

use std::cell::RefCell;
use std::env;
use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::stream_transport::StreamTransport;

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "kioto._kioto", unsendable)]
pub struct StreamTransportRegistry {
    segments: RefCell<Vec<Option<Box<[Option<Py<StreamTransport>>]>>>>,
}

#[pymethods]
impl StreamTransportRegistry {
    #[new]
    fn new() -> Self {
        Self {
            segments: RefCell::new(Vec::new()),
        }
    }

    fn register(&self, fd: i32, transport: Py<StreamTransport>) {
        self.register_inner(fd as RawFd, transport);
    }

    fn unregister(&self, fd: i32) {
        self.unregister_inner(fd as RawFd);
    }

    fn clear(&self) {
        self.segments.borrow_mut().clear();
    }
}

impl StreamTransportRegistry {
    pub(crate) fn dispatch_one(&self, py: Python<'_>, fd: i32, mask: u8) -> PyResult<bool> {
        let Some(transport) = self.get(py, fd as RawFd) else {
            return Ok(false);
        };
        if env::var_os("KIOTO_TRACE_STREAM").is_some() {
            eprintln!("stream-registry dispatch mask={mask}");
        }
        let bound = transport.bind(py);
        let mut stream = bound.borrow_mut();
        stream.on_events(py, mask)?;
        Ok(true)
    }

    pub(crate) fn register_inner(&self, fd: RawFd, transport: Py<StreamTransport>) {
        let index = fd as usize;
        let mut segments = self.segments.borrow_mut();
        let segment = Self::segment_mut(&mut segments, index);
        segment[index & SEGMENT_MASK] = Some(transport);
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

    fn get(&self, py: Python<'_>, fd: RawFd) -> Option<Py<StreamTransport>> {
        let index = fd as usize;
        let segments = self.segments.borrow();
        let segment = segments.get(index >> SEGMENT_BITS)?.as_ref()?;
        segment[index & SEGMENT_MASK].as_ref().map(|transport| transport.clone_ref(py))
    }

    fn segment_mut(
        segments: &mut Vec<Option<Box<[Option<Py<StreamTransport>>]>>>,
        index: usize,
    ) -> &mut [Option<Py<StreamTransport>>] {
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

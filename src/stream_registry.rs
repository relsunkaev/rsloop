#![cfg(unix)]

use std::env;
use std::os::fd::RawFd;

use pyo3::prelude::*;

use crate::stream_transport::StreamTransport;

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "kioto._kioto")]
pub struct StreamTransportRegistry {
    segments: Vec<Option<Box<[Option<Py<StreamTransport>>]>>>,
}

#[pymethods]
impl StreamTransportRegistry {
    #[new]
    fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    fn register(&mut self, fd: i32, transport: Py<StreamTransport>) {
        self.register_inner(fd as RawFd, transport);
    }

    fn unregister(&mut self, fd: i32) {
        self.unregister_inner(fd as RawFd);
    }

    fn clear(&mut self) {
        self.segments.clear();
    }
}

impl StreamTransportRegistry {
    pub(crate) fn register_inner(&mut self, fd: RawFd, transport: Py<StreamTransport>) {
        let index = fd as usize;
        let segment = self.segment_mut(index);
        segment[index & SEGMENT_MASK] = Some(transport);
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
            if let Some(transport) = self.get(*fd as RawFd) {
                if env::var_os("KIOTO_TRACE_STREAM").is_some() {
                    eprintln!("stream-registry dispatch mask={mask}");
                }
                let bound = transport.bind(py);
                let mut stream = bound.borrow_mut();
                stream.on_events(py, *mask)?;
                continue;
            }
            remaining.push((*fd, *mask));
        }
        Ok(())
    }

    fn get(&self, fd: RawFd) -> Option<&Py<StreamTransport>> {
        let index = fd as usize;
        let segment = self.segments.get(index >> SEGMENT_BITS)?.as_ref()?;
        segment[index & SEGMENT_MASK].as_ref()
    }

    fn segment_mut(&mut self, index: usize) -> &mut [Option<Py<StreamTransport>>] {
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

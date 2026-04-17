#![cfg(unix)]

use std::cell::RefCell;
use std::os::fd::RawFd;
use std::time::Instant;

use pyo3::prelude::*;
use pyo3::PyClass;

use crate::pipe_transport::{PipeCompletion, ReadPipeTransport, WritePipeTransport};

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[derive(Default, Clone, Copy)]
pub(crate) struct PipeCompletionDrainStats {
    pub(crate) total: u32,
    pub(crate) data_received: u32,
    pub(crate) connection_lost: u32,
    pub(crate) data_received_us: u64,
    pub(crate) connection_lost_us: u64,
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct PipeTransportRegistry {
    read_segments: RefCell<Vec<Option<Box<[Option<Py<ReadPipeTransport>>]>>>>,
    write_segments: RefCell<Vec<Option<Box<[Option<Py<WritePipeTransport>>]>>>>,
    completions: RefCell<Vec<PipeCompletion>>,
}

#[pymethods]
impl PipeTransportRegistry {
    #[new]
    fn new() -> Self {
        Self {
            read_segments: RefCell::new(Vec::new()),
            write_segments: RefCell::new(Vec::new()),
            completions: RefCell::new(Vec::new()),
        }
    }

    fn clear(&self) {
        self.read_segments.borrow_mut().clear();
        self.write_segments.borrow_mut().clear();
        self.completions.borrow_mut().clear();
    }
}

impl PipeTransportRegistry {
    pub(crate) fn register_read_inner(&self, fd: RawFd, transport: Py<ReadPipeTransport>) {
        let index = fd as usize;
        let mut segments = self.read_segments.borrow_mut();
        let segment = Self::segment_mut(&mut segments, index);
        segment[index & SEGMENT_MASK] = Some(transport);
    }

    pub(crate) fn unregister_read_inner(&self, fd: RawFd) {
        let index = fd as usize;
        let segment_index = index >> SEGMENT_BITS;
        let mut segments = self.read_segments.borrow_mut();
        let Some(Some(segment)) = segments.get_mut(segment_index) else {
            return;
        };
        segment[index & SEGMENT_MASK] = None;
    }

    pub(crate) fn register_write_inner(&self, fd: RawFd, transport: Py<WritePipeTransport>) {
        let index = fd as usize;
        let mut segments = self.write_segments.borrow_mut();
        let segment = Self::segment_mut(&mut segments, index);
        segment[index & SEGMENT_MASK] = Some(transport);
    }

    pub(crate) fn unregister_write_inner(&self, fd: RawFd) {
        let index = fd as usize;
        let segment_index = index >> SEGMENT_BITS;
        let mut segments = self.write_segments.borrow_mut();
        let Some(Some(segment)) = segments.get_mut(segment_index) else {
            return;
        };
        segment[index & SEGMENT_MASK] = None;
    }

    pub(crate) fn dispatch_one(&self, py: Python<'_>, fd: i32, mask: u8) -> PyResult<bool> {
        let index = fd as usize;
        let read_transport = {
            let segments = self.read_segments.borrow();
            if let Some(segment) = segments
                .get(index >> SEGMENT_BITS)
                .and_then(|segment| segment.as_ref())
            {
                segment[index & SEGMENT_MASK]
                    .as_ref()
                    .map(|transport| transport.clone_ref(py))
            } else {
                None
            }
        };
        if let Some(transport) = read_transport {
            if mask & 0x01 != 0 {
                transport.bind(py).borrow_mut().on_readable(py)?;
                return Ok(true);
            }
            return Ok(false);
        }

        let write_transport = {
            let segments = self.write_segments.borrow();
            let Some(segment) = segments
                .get(index >> SEGMENT_BITS)
                .and_then(|segment| segment.as_ref())
            else {
                return Ok(false);
            };
            let Some(transport) = segment[index & SEGMENT_MASK].as_ref() else {
                return Ok(false);
            };
            transport.clone_ref(py)
        };
        write_transport.bind(py).borrow_mut().on_events(py, mask)
    }

    pub(crate) fn queue_completion(&self, completion: PipeCompletion) {
        self.completions.borrow_mut().push(completion);
    }

    pub(crate) fn has_completions(&self) -> bool {
        !self.completions.borrow().is_empty()
    }

    pub(crate) fn drain_completion_queue(
        &self,
        py: Python<'_>,
        profile_timing: bool,
    ) -> PyResult<PipeCompletionDrainStats> {
        let completions = {
            let mut queued = self.completions.borrow_mut();
            if queued.is_empty() {
                return Ok(PipeCompletionDrainStats::default());
            }
            std::mem::take(&mut *queued)
        };
        let mut stats = PipeCompletionDrainStats::default();
        for completion in completions {
            match completion {
                PipeCompletion::DataReceived { transport, data } => {
                    let started = profile_timing.then(Instant::now);
                    transport.bind(py).borrow_mut().deliver_data(py, data)?;
                    stats.data_received += 1;
                    if let Some(started) = started {
                        stats.data_received_us += started.elapsed().as_micros() as u64;
                    }
                }
                PipeCompletion::ConnectionLost {
                    transport,
                    exception,
                    deliver_eof,
                } => {
                    let started = profile_timing.then(Instant::now);
                    transport
                        .bind(py)
                        .borrow_mut()
                        .finish_close(py, exception, deliver_eof)?;
                    stats.connection_lost += 1;
                    if let Some(started) = started {
                        stats.connection_lost_us += started.elapsed().as_micros() as u64;
                    }
                }
            }
            stats.total += 1;
        }
        Ok(stats)
    }

    fn segment_mut<T>(
        segments: &mut Vec<Option<Box<[Option<Py<T>>]>>>,
        index: usize,
    ) -> &mut [Option<Py<T>>]
    where
        T: PyClass,
    {
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

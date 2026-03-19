#![cfg(unix)]

use std::cell::RefCell;
use std::env;
use std::os::fd::RawFd;
use std::sync::OnceLock;

use pyo3::prelude::*;

use crate::stream_transport::{StreamCompletion, StreamCoreRef, StreamTransport, StreamWaiter};

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "kioto._kioto", unsendable)]
pub struct StreamTransportRegistry {
    segments: RefCell<Vec<Option<Box<[Option<StreamCoreRef>]>>>>,
    write_queue: RefCell<Vec<(RawFd, u64)>>,
    completions: RefCell<Vec<StreamCompletion>>,
}

#[pymethods]
impl StreamTransportRegistry {
    #[new]
    fn new() -> Self {
        Self {
            segments: RefCell::new(Vec::new()),
            write_queue: RefCell::new(Vec::new()),
            completions: RefCell::new(Vec::new()),
        }
    }

    fn register(&self, fd: i32, transport: Py<StreamTransport>) {
        let core = Python::attach(|py| transport.borrow(py).core.clone());
        self.register_inner(fd as RawFd, core);
    }

    fn unregister(&self, fd: i32) {
        self.unregister_inner(fd as RawFd);
    }

    fn clear(&self) {
        self.segments.borrow_mut().clear();
        self.write_queue.borrow_mut().clear();
        self.completions.borrow_mut().clear();
    }
}

impl StreamTransportRegistry {
    pub(crate) fn dispatch_one(&self, py: Python<'_>, fd: i32, mask: u8) -> PyResult<bool> {
        let index = fd as usize;
        let segments = self.segments.borrow();
        let Some(segment) = segments.get(index >> SEGMENT_BITS).and_then(|segment| segment.as_ref()) else {
            return Ok(false);
        };
        let Some(transport) = segment[index & SEGMENT_MASK].as_ref() else {
            return Ok(false);
        };
        if trace_stream_enabled() {
            eprintln!("stream-registry dispatch mask={mask}");
        }
        transport.borrow_mut().on_events(py, mask)?;
        Ok(true)
    }

    pub(crate) fn queue_write_phase(&self, fd: RawFd, token: u64) {
        self.write_queue.borrow_mut().push((fd, token));
    }

    pub(crate) fn queue_completion(&self, completion: StreamCompletion) {
        self.completions.borrow_mut().push(completion);
    }

    pub(crate) fn has_completions(&self) -> bool {
        !self.completions.borrow().is_empty()
    }

    pub(crate) fn drain_completion_queue(&self, py: Python<'_>) -> PyResult<usize> {
        let completions = {
            let mut queued = self.completions.borrow_mut();
            if queued.is_empty() {
                return Ok(0);
            }
            std::mem::take(&mut *queued)
        };
        let mut drained = 0usize;
        for completion in completions {
            match completion {
                StreamCompletion::ReadResult {
                    reader,
                    waiter,
                    payload,
                    resume_transport,
                } => {
                    let reader = reader.bind(py);
                    clear_waiter(reader, py)?;
                    StreamWaiter::finish_result(&waiter, py, payload)?;
                    if resume_transport {
                        maybe_resume_transport(reader)?;
                    }
                }
                StreamCompletion::ReadException {
                    reader,
                    waiter,
                    exception,
                } => {
                    let reader = reader.bind(py);
                    clear_waiter(reader, py)?;
                    StreamWaiter::finish_exception(&waiter, py, exception)?;
                }
                StreamCompletion::FutureResult { future, value } => {
                    if !future_done(py, &future)? {
                        future.bind(py).call_method1("set_result", (value,))?;
                    }
                }
                StreamCompletion::FutureException { future, exception } => {
                    if !future_done(py, &future)? {
                        future.bind(py).call_method1("set_exception", (exception,))?;
                    }
                }
            }
            drained += 1;
        }
        Ok(drained)
    }

    pub(crate) fn flush_write_queue(&self, py: Python<'_>) -> PyResult<usize> {
        let mut flushed = 0usize;
        loop {
            let drained = {
                let mut queued = self.write_queue.borrow_mut();
                if queued.is_empty() {
                    return Ok(flushed);
                }
                std::mem::take(&mut *queued)
            };
            for (fd, token) in drained {
                let index = fd as usize;
                let segments = self.segments.borrow();
                let Some(segment) = segments.get(index >> SEGMENT_BITS).and_then(|segment| segment.as_ref()) else {
                    continue;
                };
                let Some(transport) = segment[index & SEGMENT_MASK].as_ref() else {
                    continue;
                };
                let mut transport = transport.borrow_mut();
                if transport.stream_token != token || transport.closed {
                    continue;
                }
                transport.write_queued = false;
                transport.flush_write_phase(py)?;
                flushed += 1;
            }
        }
    }

    pub(crate) fn register_inner(&self, fd: RawFd, transport: StreamCoreRef) {
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

    fn segment_mut(
        segments: &mut Vec<Option<Box<[Option<StreamCoreRef>]>>>,
        index: usize,
    ) -> &mut [Option<StreamCoreRef>] {
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

fn trace_stream_enabled() -> bool {
    static TRACE_STREAM: OnceLock<bool> = OnceLock::new();
    *TRACE_STREAM.get_or_init(|| env::var_os("KIOTO_TRACE_STREAM").is_some())
}

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    future.bind(py).call_method0("done")?.is_truthy()
}

fn clear_waiter(reader: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<()> {
    reader.setattr("_waiter", py.None())
}

fn maybe_resume_transport(reader: &Bound<'_, PyAny>) -> PyResult<()> {
    if reader.getattr("_paused")?.is_truthy()? {
        reader.call_method0("_maybe_resume_transport")?;
    }
    Ok(())
}

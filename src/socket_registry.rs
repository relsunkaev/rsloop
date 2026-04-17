#![cfg(unix)]

use std::cell::RefCell;
use std::os::fd::RawFd;
use std::time::Instant;

use pyo3::prelude::*;

use crate::socket_state::{SocketCompletion, SocketCoreRef, SocketState};

const SEGMENT_BITS: usize = 8;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS;
const SEGMENT_MASK: usize = SEGMENT_SIZE - 1;

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct SocketStateRegistry {
    segments: RefCell<Vec<Option<Box<[Option<SocketCoreRef>]>>>>,
    completions: RefCell<Vec<SocketCompletion>>,
}

#[derive(Default, Clone, Copy)]
pub(crate) struct SocketCompletionDrainStats {
    pub(crate) total: u32,
    pub(crate) future_result: u32,
    pub(crate) future_exception: u32,
    pub(crate) future_result_us: u64,
    pub(crate) future_exception_us: u64,
}

#[pymethods]
impl SocketStateRegistry {
    #[new]
    fn new() -> Self {
        Self {
            segments: RefCell::new(Vec::new()),
            completions: RefCell::new(Vec::new()),
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
        self.completions.borrow_mut().clear();
    }
}

impl SocketStateRegistry {
    pub(crate) fn queue_completion(&self, completion: SocketCompletion) {
        self.completions.borrow_mut().push(completion);
    }

    pub(crate) fn has_completions(&self) -> bool {
        !self.completions.borrow().is_empty()
    }

    pub(crate) fn drain_completion_queue(
        &self,
        py: Python<'_>,
        profile_timing: bool,
    ) -> PyResult<SocketCompletionDrainStats> {
        let completions = {
            let mut queued = self.completions.borrow_mut();
            if queued.is_empty() {
                return Ok(SocketCompletionDrainStats::default());
            }
            std::mem::take(&mut *queued)
        };
        let mut stats = SocketCompletionDrainStats::default();
        for completion in completions {
            match completion {
                SocketCompletion::FutureResult { future, value } => {
                    let started = profile_timing.then(Instant::now);
                    if !future_done(py, &future)? {
                        future.bind(py).call_method1("set_result", (value,))?;
                    }
                    stats.future_result += 1;
                    if let Some(started) = started {
                        stats.future_result_us += started.elapsed().as_micros() as u64;
                    }
                }
                SocketCompletion::FutureException { future, exception } => {
                    let started = profile_timing.then(Instant::now);
                    if !future_done(py, &future)? {
                        future
                            .bind(py)
                            .call_method1("set_exception", (exception,))?;
                    }
                    stats.future_exception += 1;
                    if let Some(started) = started {
                        stats.future_exception_us += started.elapsed().as_micros() as u64;
                    }
                }
            }
            stats.total += 1;
        }
        Ok(stats)
    }

    pub(crate) fn dispatch_one(&self, py: Python<'_>, fd: i32, mask: u8) -> PyResult<bool> {
        let index = fd as usize;
        let segments = self.segments.borrow();
        let Some(segment) = segments
            .get(index >> SEGMENT_BITS)
            .and_then(|segment| segment.as_ref())
        else {
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

fn future_done(py: Python<'_>, future: &Py<PyAny>) -> PyResult<bool> {
    future.bind(py).call_method0("done")?.is_truthy()
}

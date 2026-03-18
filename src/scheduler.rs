#![cfg(unix)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::env;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use crate::completion::{Completion, CompletionPort};
use crate::handles::{is_native_cancelled, mark_native_unscheduled, run_native_handle};
use crate::poller::TokioPoller;
use crate::socket_registry::SocketStateRegistry;
use crate::stream_registry::StreamTransportRegistry;

#[derive(Debug)]
struct TimerEntry {
    when: f64,
    seq: u64,
    handle: Py<PyAny>,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when && self.seq == other.seq
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .when
            .partial_cmp(&self.when)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct Scheduler {
    ready_local: Mutex<Vec<Py<PyAny>>>,
    ready_local_len: AtomicUsize,
    ready_remote: SegQueue<Py<PyAny>>,
    ready_remote_len: AtomicUsize,
    timers: Mutex<BinaryHeap<TimerEntry>>,
    timer_len: AtomicUsize,
    seq: AtomicU64,
    stopping: AtomicUsize,
}

struct LoopClock {
    started_at: Instant,
    base_time: f64,
}

#[derive(Default)]
struct PhaseStats {
    iterations: u64,
    direct_waits: u64,
    poll_waits: u64,
    completion_items: u64,
    ready_handles: u64,
    select: Duration,
    fd_dispatch: Duration,
    completions: Duration,
    timers: Duration,
    ready: Duration,
}

#[pymethods]
impl Scheduler {
    #[new]
    fn new() -> Self {
        Self {
            ready_local: Mutex::new(Vec::new()),
            ready_local_len: AtomicUsize::new(0),
            ready_remote: SegQueue::new(),
            ready_remote_len: AtomicUsize::new(0),
            timers: Mutex::new(BinaryHeap::new()),
            timer_len: AtomicUsize::new(0),
            seq: AtomicU64::new(1),
            stopping: AtomicUsize::new(0),
        }
    }

    fn push_ready(&self, handle: Py<PyAny>) {
        self.push_ready_inner(handle);
    }

    fn push_ready_threadsafe(&self, handle: Py<PyAny>) {
        self.push_ready_threadsafe_inner(handle);
    }

    fn push_timer(&self, handle: Py<PyAny>, when: f64) {
        self.push_timer_inner(handle, when);
    }

    fn has_ready(&self) -> bool {
        self.ready_remote_len.load(AtomicOrdering::Acquire) != 0
            || self.ready_local_len.load(AtomicOrdering::Acquire) != 0
    }

    fn pop_ready(&self) -> Vec<Py<PyAny>> {
        if self.ready_remote_len.load(AtomicOrdering::Acquire) == 0
            && self.ready_local_len.load(AtomicOrdering::Acquire) == 0
        {
            return Vec::new();
        }
        let mut batch = Vec::new();
        {
            let mut ready_local = self.ready_local.lock();
            if !ready_local.is_empty() {
                batch = std::mem::take(&mut *ready_local);
                self.ready_local_len
                    .fetch_sub(batch.len(), AtomicOrdering::AcqRel);
            }
        }
        let mut drained_remote = 0usize;
        while let Some(handle) = self.ready_remote.pop() {
            drained_remote += 1;
            batch.push(handle);
        }
        if drained_remote != 0 {
            self.ready_remote_len
                .fetch_sub(drained_remote, AtomicOrdering::AcqRel);
        }
        batch
    }

    fn pop_due(&self, py: Python<'_>, now: f64) -> PyResult<Vec<Py<PyAny>>> {
        let mut due = Vec::new();
        let mut timers = self.timers.lock();

        while let Some(entry) = timers.peek() {
            if entry.when > now {
                break;
            }
            let entry = timers.pop().expect("peeked above");
            self.timer_len.fetch_sub(1, AtomicOrdering::AcqRel);
            mark_unscheduled(py, &entry.handle)?;
            if !is_cancelled(py, &entry.handle)? {
                due.push(entry.handle);
            }
        }

        Ok(due)
    }

    fn next_timer(&self, py: Python<'_>) -> PyResult<Option<f64>> {
        self.next_timer_inner(py)
    }

    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        self.ready_local.lock().clear();
        self.ready_local_len.store(0, AtomicOrdering::Release);
        while self.ready_remote.pop().is_some() {}
        self.ready_remote_len.store(0, AtomicOrdering::Release);
        let mut timers = self.timers.lock();
        while let Some(entry) = timers.pop() {
            mark_unscheduled(py, &entry.handle)?;
        }
        self.timer_len.store(0, AtomicOrdering::Release);
        Ok(())
    }

    fn set_stopping(&self, stopping: bool) {
        self.stopping
            .store(usize::from(stopping), AtomicOrdering::Release);
    }

    #[pyo3(signature = (loop_obj, poller, completion_port, stream_registry, socket_registry, stopping, clock_resolution, debug, slow_callback_duration))]
    fn run_once(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        poller: PyRef<'_, TokioPoller>,
        completion_port: PyRef<'_, CompletionPort>,
        stream_registry: Option<Py<StreamTransportRegistry>>,
        socket_registry: Option<Py<SocketStateRegistry>>,
        stopping: bool,
        clock_resolution: f64,
        debug: bool,
        slow_callback_duration: f64,
    ) -> PyResult<()> {
        let self_pipe_fd = current_self_pipe_fd(py, &loop_obj)?;
        let mut completions = Vec::new();
        let mut fd_events = Vec::new();
        let mut generic_fd_events = Vec::new();
        let mut stream_fd_events = Vec::new();
        let mut ready_batch = Vec::new();
        self.run_once_inner(
            py,
            &loop_obj,
            &poller,
            &completion_port,
            self_pipe_fd,
            stream_registry.as_ref(),
            socket_registry.as_ref(),
            stopping,
            clock_resolution,
            debug,
            slow_callback_duration,
            None,
            &mut completions,
            &mut fd_events,
            &mut ready_batch,
            &mut generic_fd_events,
            &mut stream_fd_events,
            None,
        )
    }

    #[pyo3(signature = (loop_obj, poller, completion_port, stream_registry, socket_registry, clock_resolution, debug, slow_callback_duration))]
    fn run_forever_native(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        poller: PyRef<'_, TokioPoller>,
        completion_port: PyRef<'_, CompletionPort>,
        stream_registry: Option<Py<StreamTransportRegistry>>,
        socket_registry: Option<Py<SocketStateRegistry>>,
        clock_resolution: f64,
        debug: bool,
        slow_callback_duration: f64,
    ) -> PyResult<()> {
        let self_pipe_fd = current_self_pipe_fd(py, &loop_obj)?;
        let clock = LoopClock {
            started_at: Instant::now(),
            base_time: loop_time(py, &loop_obj)?,
        };
        let mut completions = Vec::new();
        let mut fd_events = Vec::new();
        let mut generic_fd_events = Vec::new();
        let mut stream_fd_events = Vec::new();
        let mut ready_batch = Vec::new();
        let profiling = env::var_os("KIOTO_PROFILE_SCHED").is_some();
        let mut stats = PhaseStats::default();
        self.stopping.store(0, AtomicOrdering::Release);
        loop {
            self.run_once_inner(
                py,
                &loop_obj,
                &poller,
                &completion_port,
                self_pipe_fd,
                stream_registry.as_ref(),
                socket_registry.as_ref(),
                false,
                clock_resolution,
                debug,
                slow_callback_duration,
                Some(&clock),
                &mut completions,
                &mut fd_events,
                &mut ready_batch,
                &mut generic_fd_events,
                &mut stream_fd_events,
                profiling.then_some(&mut stats),
            )?;
            if self.stopping.load(AtomicOrdering::Acquire) != 0 {
                break;
            }
        }

        if profiling {
            eprintln!(
                "kioto scheduler phases iterations={} direct_waits={} poll_waits={} completion_items={} ready_handles={} select={:.6}s fd_dispatch={:.6}s completions={:.6}s timers={:.6}s ready={:.6}s",
                stats.iterations,
                stats.direct_waits,
                stats.poll_waits,
                stats.completion_items,
                stats.ready_handles,
                stats.select.as_secs_f64(),
                stats.fd_dispatch.as_secs_f64(),
                stats.completions.as_secs_f64(),
                stats.timers.as_secs_f64(),
                stats.ready.as_secs_f64(),
            );
        }

        Ok(())
    }
}

impl Scheduler {
    fn run_once_inner(
        &self,
        py: Python<'_>,
        loop_obj: &Py<PyAny>,
        poller: &TokioPoller,
        completion_port: &CompletionPort,
        self_pipe_fd: Option<i32>,
        stream_registry: Option<&Py<StreamTransportRegistry>>,
        socket_registry: Option<&Py<SocketStateRegistry>>,
        stopping: bool,
        clock_resolution: f64,
        debug: bool,
        slow_callback_duration: f64,
        clock: Option<&LoopClock>,
        completions: &mut Vec<Completion>,
        fd_events: &mut Vec<(i32, u8)>,
        ready_batch: &mut Vec<Py<PyAny>>,
        generic_fd_events: &mut Vec<(i32, u8)>,
        stream_fd_events: &mut Vec<(i32, u8)>,
        stats: Option<&mut PhaseStats>,
    ) -> PyResult<()> {
        let mut stats = stats;
        let has_ready = self.has_ready();
        let next_timer = if has_ready || stopping {
            None
        } else {
            self.next_timer_inner(py)?
        };
        let timeout = if has_ready || stopping {
            Some(0.0)
        } else {
            match next_timer {
                Some(when) => {
                    let now = loop_time_cached(py, loop_obj, clock)?;
                    Some((when - now).clamp(0.0, maximum_select_timeout(py)?))
                }
                None => None,
            }
        };

        let completion_fd = completion_port.fileno_inner();
        fd_events.clear();
        completions.clear();
        let select_started = stats.as_ref().map(|_| Instant::now());
        if self_pipe_fd.is_none() && poller.is_only_registered_fd(completion_fd as i32) {
            if let Some(stats) = stats.as_deref_mut() {
                stats.direct_waits += 1;
            }
            if completion_port.has_pending_inner() {
                completion_port.drain_into_inner(completions);
            } else {
                completion_port.wait_and_drain_into_inner(py, timeout, completions)?;
            }
        } else {
            if let Some(stats) = stats.as_deref_mut() {
                stats.poll_waits += 1;
            }
            poller.select_inner_into(py, timeout, fd_events)?;
            let event_count = fd_events.len();
            for index in 0..event_count {
                let (fd, _mask) = fd_events[index];
                if fd == completion_fd {
                    completion_port.drain_into_inner(completions);
                    fd_events[index] = (-1, 0);
                }
            }
            fd_events.retain(|(fd, mask)| *fd >= 0 && *mask != 0);
        }
        if let (Some(stats), Some(started)) = (stats.as_deref_mut(), select_started) {
            stats.iterations += 1;
            stats.select += started.elapsed();
        }

        if !fd_events.is_empty() {
            let started = stats.as_ref().map(|_| Instant::now());
            if let Some(registry) = stream_registry {
                registry
                    .bind(py)
                    .borrow()
                    .dispatch_inner(py, fd_events, stream_fd_events)?;
                if let Some(socket_registry) = socket_registry {
                    socket_registry.bind(py).borrow().dispatch_inner(
                        py,
                        stream_fd_events,
                        generic_fd_events,
                    )?;
                } else {
                    generic_fd_events.clear();
                    generic_fd_events.extend_from_slice(stream_fd_events);
                }
                if !generic_fd_events.is_empty() {
                    loop_obj
                        .bind(py)
                        .call_method1("_process_fd_events", (&*generic_fd_events,))?;
                }
            } else if let Some(socket_registry) = socket_registry {
                socket_registry
                    .bind(py)
                    .borrow()
                    .dispatch_inner(py, fd_events, generic_fd_events)?;
                if !generic_fd_events.is_empty() {
                    loop_obj
                        .bind(py)
                        .call_method1("_process_fd_events", (&*generic_fd_events,))?;
                }
            } else {
                loop_obj
                    .bind(py)
                    .call_method1("_process_fd_events", (&*fd_events,))?;
            }
            if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                stats.fd_dispatch += started.elapsed();
            }
        }

        if !completions.is_empty() {
            if let Some(stats) = stats.as_deref_mut() {
                stats.completion_items += completions.len() as u64;
            }
            let started = stats.as_ref().map(|_| Instant::now());
            drain_completions(py, completions)?;
            if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                stats.completions += started.elapsed();
            }
        }

        if next_timer.is_some() {
            let started = stats.as_ref().map(|_| Instant::now());
            let end_time = loop_time_cached(py, loop_obj, clock)? + clock_resolution;
            self.promote_due_inner(py, end_time)?;
            if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                stats.timers += started.elapsed();
            }
        }
        let ready_started = stats.as_ref().map(|_| Instant::now());
        if let Some(stats) = stats.as_deref_mut() {
                stats.ready_handles +=
                ready_batch.len() as u64
                    + self.ready_remote_len.load(AtomicOrdering::Acquire) as u64
                    + self.ready_local_len.load(AtomicOrdering::Acquire) as u64;
        }
        self.run_ready_inner(py, &loop_obj, debug, slow_callback_duration, ready_batch)?;
        if let (Some(stats), Some(started)) = (stats.as_deref_mut(), ready_started) {
            stats.ready += started.elapsed();
        }

        Ok(())
    }
    pub(crate) fn push_ready_inner(&self, handle: Py<PyAny>) {
        self.ready_local.lock().push(handle);
        self.ready_local_len.fetch_add(1, AtomicOrdering::AcqRel);
    }

    pub(crate) fn push_ready_threadsafe_inner(&self, handle: Py<PyAny>) {
        self.ready_remote.push(handle);
        self.ready_remote_len.fetch_add(1, AtomicOrdering::AcqRel);
    }

    pub(crate) fn push_timer_inner(&self, handle: Py<PyAny>, when: f64) {
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        self.timers.lock().push(TimerEntry { when, seq, handle });
        self.timer_len.fetch_add(1, AtomicOrdering::AcqRel);
    }

    fn next_timer_inner(&self, py: Python<'_>) -> PyResult<Option<f64>> {
        if self.timer_len.load(AtomicOrdering::Acquire) == 0 {
            return Ok(None);
        }
        let mut timers = self.timers.lock();

        while let Some(entry) = timers.peek() {
            let when = entry.when;
            if is_cancelled(py, &entry.handle)? {
                let entry = timers.pop().expect("peeked above");
                self.timer_len.fetch_sub(1, AtomicOrdering::AcqRel);
                mark_unscheduled(py, &entry.handle)?;
                continue;
            }
            return Ok(Some(when));
        }

        Ok(None)
    }

    fn promote_due_inner(&self, py: Python<'_>, now: f64) -> PyResult<()> {
        let mut due = Vec::new();
        {
            let mut timers = self.timers.lock();
            while let Some(entry) = timers.peek() {
                if entry.when > now {
                    break;
                }
                let entry = timers.pop().expect("peeked above");
                self.timer_len.fetch_sub(1, AtomicOrdering::AcqRel);
                mark_unscheduled(py, &entry.handle)?;
                if !is_cancelled(py, &entry.handle)? {
                    due.push(entry.handle);
                }
            }
        }

        if !due.is_empty() {
            self.ready_local_len
                .fetch_add(due.len(), AtomicOrdering::AcqRel);
            self.ready_local.lock().extend(due);
        }

        Ok(())
    }

    fn run_ready_inner(
        &self,
        py: Python<'_>,
        loop_obj: &Py<PyAny>,
        debug: bool,
        slow_callback_duration: f64,
        ready_batch: &mut Vec<Py<PyAny>>,
    ) -> PyResult<()> {
        if self.ready_remote_len.load(AtomicOrdering::Acquire) == 0
            && self.ready_local_len.load(AtomicOrdering::Acquire) == 0
        {
            return Ok(());
        }
        ready_batch.clear();
        {
            let mut ready_local = self.ready_local.lock();
            if !ready_local.is_empty() {
                let drained = std::mem::take(&mut *ready_local);
                self.ready_local_len
                    .fetch_sub(drained.len(), AtomicOrdering::AcqRel);
                ready_batch.extend(drained);
            }
        }
        let mut drained_remote = 0usize;
        while let Some(handle) = self.ready_remote.pop() {
            drained_remote += 1;
            ready_batch.push(handle);
        }
        if drained_remote != 0 {
            self.ready_remote_len
                .fetch_sub(drained_remote, AtomicOrdering::AcqRel);
        }

        for handle in ready_batch.drain(..) {
            run_ready_handle(py, loop_obj, &handle, debug, slow_callback_duration)?;
        }

        Ok(())
    }
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

fn mark_unscheduled(py: Python<'_>, handle: &Py<PyAny>) -> PyResult<()> {
    if mark_native_unscheduled(py, handle) {
        return Ok(());
    }
    handle
        .bind(py)
        .setattr("_scheduled", false)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

fn drain_completions(py: Python<'_>, completions: &[Completion]) -> PyResult<()> {
    for completion in completions {
        match completion {
            Completion::Future {
                future,
                payload,
                is_err,
            } => {
                if future.bind(py).call_method0("cancelled")?.is_truthy()? {
                    continue;
                }
                if *is_err {
                    future
                        .bind(py)
                        .call_method1("set_exception", (payload.bind(py),))?;
                } else {
                    future
                        .bind(py)
                        .call_method1("set_result", (payload.bind(py),))?;
                }
            }
        }
    }

    Ok(())
}

fn loop_time(py: Python<'_>, loop_obj: &Py<PyAny>) -> PyResult<f64> {
    loop_obj.bind(py).call_method0("time")?.extract()
}

fn loop_time_cached(
    py: Python<'_>,
    loop_obj: &Py<PyAny>,
    clock: Option<&LoopClock>,
) -> PyResult<f64> {
    if let Some(clock) = clock {
        return Ok(clock.base_time + clock.started_at.elapsed().as_secs_f64());
    }
    loop_time(py, loop_obj)
}

fn current_self_pipe_fd(py: Python<'_>, loop_obj: &Py<PyAny>) -> PyResult<Option<i32>> {
    let ssock = loop_obj.bind(py).getattr("_ssock")?;
    if ssock.is_none() {
        return Ok(None);
    }
    Ok(Some(ssock.call_method0("fileno")?.extract()?))
}

fn maximum_select_timeout(py: Python<'_>) -> PyResult<f64> {
    static MAXIMUM_SELECT_TIMEOUT: PyOnceLock<f64> = PyOnceLock::new();
    Ok(
        *MAXIMUM_SELECT_TIMEOUT.get_or_try_init(py, || -> PyResult<f64> {
            py.import("asyncio.base_events")?
                .getattr("MAXIMUM_SELECT_TIMEOUT")?
                .extract()
        })?,
    )
}

fn run_ready_handle(
    py: Python<'_>,
    loop_obj: &Py<PyAny>,
    handle: &Py<PyAny>,
    debug: bool,
    slow_callback_duration: f64,
) -> PyResult<()> {
    let native = run_native_handle(py, handle);
    if native.is_none() && is_cancelled(py, handle)? {
        return Ok(());
    }
    if debug {
        loop_obj.bind(py).setattr("_current_handle", handle)?;
        let started = loop_time(py, loop_obj)?;
        let result = match native {
            Some(result) => result,
            None => handle.bind(py).call_method0("_run").map(|_| ()),
        };
        let finished = loop_time(py, loop_obj)?;
        loop_obj.bind(py).setattr("_current_handle", py.None())?;
        result?;
        if finished - started >= slow_callback_duration {
            asyncio_logger(py)?.call_method1(
                py,
                "warning",
                (
                    "Executing %s took %.3f seconds",
                    handle.bind(py),
                    finished - started,
                ),
            )?;
        }
    } else {
        match native {
            Some(result) => result?,
            None => {
                handle.bind(py).call_method0("_run")?;
            }
        }
    }

    Ok(())
}

fn asyncio_logger(py: Python<'_>) -> PyResult<Py<PyAny>> {
    static LOGGER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let logger = LOGGER.get_or_try_init(py, || -> PyResult<_> {
        Ok(py
            .import("asyncio.selector_events")?
            .getattr("logger")?
            .unbind())
    })?;
    Ok(logger.clone_ref(py))
}

#![cfg(unix)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::env;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use crate::completion::{Completion, CompletionPort};
use crate::fd_callbacks::FdCallbackRegistry;
use crate::handles::{
    NativeHandleKind, is_native_cancelled, mark_native_unscheduled, native_handle_kind,
    run_native_handle, take_one_arg_profile_json,
};
use crate::loop_api::LoopApi;
use crate::poller::TokioPoller;
use crate::socket_registry::SocketStateRegistry;
use crate::stream_registry::StreamTransportRegistry;
use crate::stream_transport::take_profile_stream_json;

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

#[pyclass(module = "rsloop._rsloop")]
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
    fd_events: u64,
    stream_fd_hits: u64,
    socket_fd_hits: u64,
    generic_fd_hits: u64,
    ready_local_items: u64,
    ready_remote_items: u64,
    ready_zero_arg: u64,
    ready_one_arg: u64,
    ready_handle_native: u64,
    ready_future_native: u64,
    ready_python: u64,
    select: Duration,
    fd_dispatch: Duration,
    completions: Duration,
    timers: Duration,
    ready: Duration,
    writes: Duration,
    tick_stats: TickStats,
}

#[derive(Default)]
struct TickStats {
    fd_events: Vec<u32>,
    stream_fd_hits: Vec<u32>,
    socket_fd_hits: Vec<u32>,
    generic_fd_hits: Vec<u32>,
    ready_handles: Vec<u32>,
    ready_local_items: Vec<u32>,
    ready_remote_items: Vec<u32>,
    ready_zero_arg: Vec<u32>,
    ready_one_arg: Vec<u32>,
    ready_handle_native: Vec<u32>,
    ready_future_native: Vec<u32>,
    ready_python: Vec<u32>,
    completion_items: Vec<u32>,
    writes_flushed: Vec<u32>,
}

#[derive(Default)]
struct TickSample {
    fd_events: u32,
    stream_fd_hits: u32,
    socket_fd_hits: u32,
    generic_fd_hits: u32,
    ready_handles: u32,
    ready_local_items: u32,
    ready_remote_items: u32,
    ready_zero_arg: u32,
    ready_one_arg: u32,
    ready_handle_native: u32,
    ready_future_native: u32,
    ready_python: u32,
    completion_items: u32,
    writes_flushed: u32,
}

impl PhaseStats {
    fn record_tick(&mut self, tick: &TickSample) {
        self.ready_handles += tick.ready_handles as u64;
        self.fd_events += tick.fd_events as u64;
        self.stream_fd_hits += tick.stream_fd_hits as u64;
        self.socket_fd_hits += tick.socket_fd_hits as u64;
        self.generic_fd_hits += tick.generic_fd_hits as u64;
        self.ready_local_items += tick.ready_local_items as u64;
        self.ready_remote_items += tick.ready_remote_items as u64;
        self.ready_zero_arg += tick.ready_zero_arg as u64;
        self.ready_one_arg += tick.ready_one_arg as u64;
        self.ready_handle_native += tick.ready_handle_native as u64;
        self.ready_future_native += tick.ready_future_native as u64;
        self.ready_python += tick.ready_python as u64;
        self.completion_items += tick.completion_items as u64;
        self.tick_stats.fd_events.push(tick.fd_events);
        self.tick_stats.stream_fd_hits.push(tick.stream_fd_hits);
        self.tick_stats.socket_fd_hits.push(tick.socket_fd_hits);
        self.tick_stats.generic_fd_hits.push(tick.generic_fd_hits);
        self.tick_stats.ready_handles.push(tick.ready_handles);
        self.tick_stats.ready_local_items.push(tick.ready_local_items);
        self.tick_stats.ready_remote_items.push(tick.ready_remote_items);
        self.tick_stats.ready_zero_arg.push(tick.ready_zero_arg);
        self.tick_stats.ready_one_arg.push(tick.ready_one_arg);
        self.tick_stats
            .ready_handle_native
            .push(tick.ready_handle_native);
        self.tick_stats
            .ready_future_native
            .push(tick.ready_future_native);
        self.tick_stats.ready_python.push(tick.ready_python);
        self.tick_stats.completion_items.push(tick.completion_items);
        self.tick_stats.writes_flushed.push(tick.writes_flushed);
    }
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
                self.ready_local_len.store(0, AtomicOrdering::Release);
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

    #[pyo3(signature = (loop_obj, native_api, poller, completion_port, fd_registry, stream_registry, socket_registry, stopping, clock_resolution, debug, slow_callback_duration))]
    fn run_once(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        native_api: Option<Py<LoopApi>>,
        poller: PyRef<'_, TokioPoller>,
        completion_port: PyRef<'_, CompletionPort>,
        fd_registry: Option<Py<FdCallbackRegistry>>,
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
        let mut _stream_fd_events = Vec::new();
        let mut ready_batch = Vec::new();
        self.run_once_inner(
            py,
            &loop_obj,
            &poller,
            &completion_port,
            self_pipe_fd,
            native_api.as_ref(),
            fd_registry.as_ref(),
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
            &mut _stream_fd_events,
            None,
        )
    }

    #[pyo3(signature = (loop_obj, native_api, poller, completion_port, fd_registry, stream_registry, socket_registry, clock_resolution, debug, slow_callback_duration))]
    fn run_forever_native(
        &self,
        py: Python<'_>,
        loop_obj: Py<PyAny>,
        native_api: Option<Py<LoopApi>>,
        poller: PyRef<'_, TokioPoller>,
        completion_port: PyRef<'_, CompletionPort>,
        fd_registry: Option<Py<FdCallbackRegistry>>,
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
        let mut _stream_fd_events = Vec::new();
        let mut ready_batch = Vec::new();
        let profiling_json = env::var_os("RSLOOP_PROFILE_SCHED_JSON").is_some();
        let profiling = profiling_json || env::var_os("RSLOOP_PROFILE_SCHED").is_some();
        let mut stats = PhaseStats::default();
        self.stopping.store(0, AtomicOrdering::Release);
        loop {
            self.run_once_inner(
                py,
                &loop_obj,
                &poller,
                &completion_port,
                self_pipe_fd,
                native_api.as_ref(),
                fd_registry.as_ref(),
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
                &mut _stream_fd_events,
                profiling.then_some(&mut stats),
            )?;
            let stopping = native_api
                .as_ref()
                .map(|native_api| native_api.bind(py).borrow().is_stopping())
                .unwrap_or(false);
            if stopping || self.stopping.load(AtomicOrdering::Acquire) != 0 {
                break;
            }
        }

        if profiling {
            eprintln!(
                "rsloop scheduler phases iterations={} direct_waits={} poll_waits={} completion_items={} ready_handles={} fd_events={} stream_fd_hits={} socket_fd_hits={} generic_fd_hits={} ready_local_items={} ready_remote_items={} select={:.6}s fd_dispatch={:.6}s completions={:.6}s timers={:.6}s ready={:.6}s writes={:.6}s",
                stats.iterations,
                stats.direct_waits,
                stats.poll_waits,
                stats.completion_items,
                stats.ready_handles,
                stats.fd_events,
                stats.stream_fd_hits,
                stats.socket_fd_hits,
                stats.generic_fd_hits,
                stats.ready_local_items,
                stats.ready_remote_items,
                stats.select.as_secs_f64(),
                stats.fd_dispatch.as_secs_f64(),
                stats.completions.as_secs_f64(),
                stats.timers.as_secs_f64(),
                stats.ready.as_secs_f64(),
                stats.writes.as_secs_f64(),
            );
        }
        if profiling_json {
            eprintln!("RSLOOP_PROFILE_SCHED_JSON {}", scheduler_profile_json(&stats));
            if let Some(trace_json) = take_profile_stream_json() {
                eprintln!("RSLOOP_PROFILE_STREAM_JSON {trace_json}");
            }
            if let Some(one_arg_json) = take_one_arg_profile_json() {
                eprintln!("RSLOOP_PROFILE_ONEARG_JSON {one_arg_json}");
            }
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
        native_api: Option<&Py<LoopApi>>,
        fd_registry: Option<&Py<FdCallbackRegistry>>,
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
        _stream_fd_events: &mut Vec<(i32, u8)>,
        stats: Option<&mut PhaseStats>,
    ) -> PyResult<()> {
        let mut stats = stats;
        let mut tick = TickSample::default();
        let mut generic_ready_handles = Vec::new();
        if let Some(native_api) = native_api {
            let native_api = native_api.bind(py);
            let mut fallback = native_api.borrow().drain_ready_fallback();
            if !fallback.is_empty() {
                self.ready_local.lock().extend(fallback.drain(..));
            }
            let mut timer_fallback = native_api.borrow().drain_timer_fallback();
            if !timer_fallback.is_empty() {
                for (handle, when) in timer_fallback.drain(..) {
                    self.push_timer_inner(handle, when);
                }
            }
        }
        let stopping = stopping
            || native_api
                .as_ref()
                .map(|native_api| native_api.bind(py).borrow().is_stopping())
                .unwrap_or(false);
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
                let stream_registry = registry.bind(py);
                let stream_registry = stream_registry.borrow();
                let socket_registry = socket_registry.map(|registry| registry.bind(py));
                let socket_registry = socket_registry.as_ref().map(|registry| registry.borrow());
                generic_fd_events.clear();
                for (fd, mask) in fd_events.iter().copied() {
                    tick.fd_events += 1;
                    if self_pipe_fd.is_some_and(|self_pipe_fd| fd == self_pipe_fd) {
                        loop_obj.bind(py).call_method0("_read_from_self")?;
                        continue;
                    }
                    if stream_registry.dispatch_one(py, fd, mask)? {
                        tick.stream_fd_hits += 1;
                        continue;
                    }
                    if let Some(socket_registry) = socket_registry.as_ref() {
                        if socket_registry.dispatch_one(py, fd, mask)? {
                            tick.socket_fd_hits += 1;
                            continue;
                        }
                    }
                    tick.generic_fd_hits += 1;
                    generic_fd_events.push((fd, mask));
                }
                if !generic_fd_events.is_empty() {
                    collect_generic_fd_events(
                        py,
                        loop_obj,
                        fd_registry,
                        &*generic_fd_events,
                        &mut generic_ready_handles,
                    )?;
                }
            } else if let Some(socket_registry) = socket_registry {
                let socket_registry = socket_registry.bind(py);
                let socket_registry = socket_registry.borrow();
                generic_fd_events.clear();
                for (fd, mask) in fd_events.iter().copied() {
                    tick.fd_events += 1;
                    if self_pipe_fd.is_some_and(|self_pipe_fd| fd == self_pipe_fd) {
                        loop_obj.bind(py).call_method0("_read_from_self")?;
                        continue;
                    }
                    if !socket_registry.dispatch_one(py, fd, mask)? {
                        tick.generic_fd_hits += 1;
                        generic_fd_events.push((fd, mask));
                    } else {
                        tick.socket_fd_hits += 1;
                    }
                }
                if !generic_fd_events.is_empty() {
                    collect_generic_fd_events(
                        py,
                        loop_obj,
                        fd_registry,
                        &*generic_fd_events,
                        &mut generic_ready_handles,
                    )?;
                }
            } else {
                let mut python_fd_events = Vec::new();
                for (fd, mask) in fd_events.iter().copied() {
                    tick.fd_events += 1;
                    if self_pipe_fd.is_some_and(|self_pipe_fd| fd == self_pipe_fd) {
                        loop_obj.bind(py).call_method0("_read_from_self")?;
                        continue;
                    }
                    tick.generic_fd_hits += 1;
                    python_fd_events.push((fd, mask));
                }
                if !python_fd_events.is_empty() {
                    collect_generic_fd_events(
                        py,
                        loop_obj,
                        fd_registry,
                        &*python_fd_events,
                        &mut generic_ready_handles,
                    )?;
                }
            }
            if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                stats.fd_dispatch += started.elapsed();
            }
        }

        if let Some(registry) = stream_registry {
            let started = stats.as_ref().map(|_| Instant::now());
            let drained = registry.bind(py).borrow().drain_completion_queue(py)? as u32;
            tick.completion_items += drained;
            if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                stats.completions += started.elapsed();
            }
        }

        if !completions.is_empty() {
            tick.completion_items += completions.len() as u32;
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
        for handle in generic_ready_handles.drain(..) {
            run_ready_handle(py, loop_obj, &handle, debug, slow_callback_duration)?;
        }
        let ready_started = stats.as_ref().map(|_| Instant::now());
        self.run_ready_inner(
            py,
            &loop_obj,
            debug,
            slow_callback_duration,
            ready_batch,
            &mut tick,
        )?;
        if let Some(stats) = stats.as_deref_mut() {
            stats.ready += ready_started.map(|started| started.elapsed()).unwrap_or_default();
        }

        if let Some(registry) = stream_registry {
            let started = stats.as_ref().map(|_| Instant::now());
            tick.writes_flushed = registry.bind(py).borrow().flush_write_queue(py)? as u32;
            if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                stats.writes += started.elapsed();
            }
            if registry.bind(py).borrow().has_completions() {
                let started = stats.as_ref().map(|_| Instant::now());
                let drained = registry.bind(py).borrow().drain_completion_queue(py)? as u32;
                tick.completion_items += drained;
                if let (Some(stats), Some(started)) = (stats.as_deref_mut(), started) {
                    stats.completions += started.elapsed();
                }
                let ready_started = stats.as_ref().map(|_| Instant::now());
                self.run_ready_inner(
                    py,
                    &loop_obj,
                    debug,
                    slow_callback_duration,
                    ready_batch,
                    &mut tick,
                )?;
                if let Some(stats) = stats.as_deref_mut() {
                    stats.ready += ready_started.map(|started| started.elapsed()).unwrap_or_default();
                }
            }
        }
        if let Some(stats) = stats.as_deref_mut() {
            stats.record_tick(&tick);
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
            let due_len = due.len();
            self.ready_local.lock().extend(due);
            self.ready_local_len
                .fetch_add(due_len, AtomicOrdering::AcqRel);
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
        tick: &mut TickSample,
    ) -> PyResult<()> {
        if self.ready_remote_len.load(AtomicOrdering::Acquire) == 0
            && self.ready_local_len.load(AtomicOrdering::Acquire) == 0
        {
            return Ok(());
        }
        ready_batch.clear();
        let mut ready_local_items = 0usize;
        {
            let mut ready_local = self.ready_local.lock();
            if !ready_local.is_empty() {
                ready_local_items = ready_local.len();
                std::mem::swap(&mut *ready_local, ready_batch);
                self.ready_local_len.store(0, AtomicOrdering::Release);
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
        tick.ready_local_items += ready_local_items as u32;
        tick.ready_remote_items += drained_remote as u32;
        tick.ready_handles += (ready_local_items + drained_remote) as u32;

        for handle in ready_batch.drain(..) {
            match native_handle_kind(py, &handle) {
                Some(NativeHandleKind::ZeroArg) => tick.ready_zero_arg += 1,
                Some(NativeHandleKind::OneArg) => tick.ready_one_arg += 1,
                Some(NativeHandleKind::Handle) => tick.ready_handle_native += 1,
                Some(NativeHandleKind::Future) => tick.ready_future_native += 1,
                None => tick.ready_python += 1,
            }
            run_ready_handle(py, loop_obj, &handle, debug, slow_callback_duration)?;
        }

        Ok(())
    }
}

fn scheduler_profile_json(stats: &PhaseStats) -> String {
    let mut out = String::new();
    out.push('{');
    let _ = write!(
        out,
        "\"iterations\":{},\"direct_waits\":{},\"poll_waits\":{},\"completion_items\":{},\"ready_handles\":{},\"fd_events\":{},\"stream_fd_hits\":{},\"socket_fd_hits\":{},\"generic_fd_hits\":{},\"ready_local_items\":{},\"ready_remote_items\":{},\"ready_zero_arg\":{},\"ready_one_arg\":{},\"ready_handle_native\":{},\"ready_future_native\":{},\"ready_python\":{},\"phase_seconds\":{{\"select\":{:.6},\"fd_dispatch\":{:.6},\"completions\":{:.6},\"timers\":{:.6},\"ready\":{:.6},\"writes\":{:.6}}},\"ticks\":{{",
        stats.iterations,
        stats.direct_waits,
        stats.poll_waits,
        stats.completion_items,
        stats.ready_handles,
        stats.fd_events,
        stats.stream_fd_hits,
        stats.socket_fd_hits,
        stats.generic_fd_hits,
        stats.ready_local_items,
        stats.ready_remote_items,
        stats.ready_zero_arg,
        stats.ready_one_arg,
        stats.ready_handle_native,
        stats.ready_future_native,
        stats.ready_python,
        stats.select.as_secs_f64(),
        stats.fd_dispatch.as_secs_f64(),
        stats.completions.as_secs_f64(),
        stats.timers.as_secs_f64(),
        stats.ready.as_secs_f64(),
        stats.writes.as_secs_f64(),
    );
    append_distribution_json(&mut out, "fd_events", &stats.tick_stats.fd_events);
    out.push(',');
    append_distribution_json(&mut out, "stream_fd_hits", &stats.tick_stats.stream_fd_hits);
    out.push(',');
    append_distribution_json(&mut out, "socket_fd_hits", &stats.tick_stats.socket_fd_hits);
    out.push(',');
    append_distribution_json(&mut out, "generic_fd_hits", &stats.tick_stats.generic_fd_hits);
    out.push(',');
    append_distribution_json(&mut out, "ready_handles", &stats.tick_stats.ready_handles);
    out.push(',');
    append_distribution_json(&mut out, "ready_local_items", &stats.tick_stats.ready_local_items);
    out.push(',');
    append_distribution_json(&mut out, "ready_remote_items", &stats.tick_stats.ready_remote_items);
    out.push(',');
    append_distribution_json(&mut out, "ready_zero_arg", &stats.tick_stats.ready_zero_arg);
    out.push(',');
    append_distribution_json(&mut out, "ready_one_arg", &stats.tick_stats.ready_one_arg);
    out.push(',');
    append_distribution_json(
        &mut out,
        "ready_handle_native",
        &stats.tick_stats.ready_handle_native,
    );
    out.push(',');
    append_distribution_json(
        &mut out,
        "ready_future_native",
        &stats.tick_stats.ready_future_native,
    );
    out.push(',');
    append_distribution_json(&mut out, "ready_python", &stats.tick_stats.ready_python);
    out.push(',');
    append_distribution_json(&mut out, "completion_items", &stats.tick_stats.completion_items);
    out.push(',');
    append_distribution_json(&mut out, "writes_flushed", &stats.tick_stats.writes_flushed);
    out.push_str("}}");
    out
}

fn append_distribution_json(out: &mut String, name: &str, values: &[u32]) {
    let _ = write!(out, "\"{name}\":");
    if values.is_empty() {
        out.push_str("{\"count\":0,\"sum\":0,\"mean\":0.0,\"p50\":0,\"p95\":0,\"max\":0}");
        return;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let count = sorted.len();
    let sum: u64 = values.iter().map(|value| *value as u64).sum();
    let mean = sum as f64 / count as f64;
    let p50 = percentile(&sorted, 0.50);
    let p95 = percentile(&sorted, 0.95);
    let max = *sorted.last().unwrap_or(&0);
    let _ = write!(
        out,
        "{{\"count\":{},\"sum\":{},\"mean\":{:.3},\"p50\":{},\"p95\":{},\"max\":{}}}",
        count, sum, mean, p50, p95, max
    );
}

fn percentile(sorted: &[u32], pct: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn collect_generic_fd_events(
    py: Python<'_>,
    loop_obj: &Py<PyAny>,
    fd_registry: Option<&Py<FdCallbackRegistry>>,
    events: &[(i32, u8)],
    ready: &mut Vec<Py<PyAny>>,
) -> PyResult<()> {
    if let Some(fd_registry) = fd_registry {
        fd_registry
            .bind(py)
            .borrow()
            .dispatch_handles(py, events, ready)?;
        return Ok(());
    }
    loop_obj
        .bind(py)
        .call_method1("_process_fd_events", (events,))?;
    Ok(())
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

fn drain_self_pipe_fd(fd: i32) -> PyResult<()> {
    let mut buf = [0_u8; 4096];
    loop {
        let rc = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if rc > 0 {
            continue;
        }
        if rc == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        match err.kind() {
            std::io::ErrorKind::Interrupted => continue,
            std::io::ErrorKind::WouldBlock => return Ok(()),
            _ => return Err(PyRuntimeError::new_err(err.to_string())),
        }
    }
}

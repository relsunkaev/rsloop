#![cfg(unix)]

use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use parking_lot::Mutex;
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

const READABLE: u8 = 0x01;
const WRITABLE: u8 = 0x02;
const ALL_INTERESTS: u8 = READABLE | WRITABLE;

#[derive(Clone, Copy, Eq, PartialEq)]
struct FileIdentity {
    dev: libc::dev_t,
    ino: libc::ino_t,
}

struct Registration {
    token: Token,
    interest: u8,
    identity: FileIdentity,
}

struct PollerState {
    closed: bool,
    poll: Poll,
    events: Events,
    registrations: HashMap<RawFd, Registration>,
}

impl PollerState {
    fn new() -> io::Result<Self> {
        Ok(Self {
            closed: false,
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            registrations: HashMap::new(),
        })
    }

    fn set_interest(&mut self, fd: RawFd, mask: u8) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "poller is closed",
            ));
        }

        if mask == 0 {
            self.remove(fd)?;
            return Ok(());
        }

        let identity = file_identity(fd)?;
        if let Some(existing) = self.registrations.get_mut(&fd) {
            if existing.identity != identity {
                self.remove(fd)?;
            } else if existing.interest != mask {
                let mut source = SourceFd(&fd);
                self.poll
                    .registry()
                    .reregister(&mut source, existing.token, to_interest(mask))?;
                existing.interest = mask;
                return Ok(());
            } else {
                return Ok(());
            }
        }

        let token = token_for_fd(fd);
        let mut source = SourceFd(&fd);
        self.poll
            .registry()
            .register(&mut source, token, to_interest(mask))?;
        self.registrations.insert(
            fd,
            Registration {
                token,
                interest: mask,
                identity,
            },
        );
        Ok(())
    }

    fn remove(&mut self, fd: RawFd) -> io::Result<()> {
        let Some(_registration) = self.registrations.remove(&fd) else {
            return Ok(());
        };
        let mut source = SourceFd(&fd);
        match self.poll.registry().deregister(&mut source) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn select_into(&mut self, timeout: Option<f64>, ready: &mut Vec<(i32, u8)>) -> io::Result<()> {
        if self.closed {
            ready.clear();
            return Ok(());
        }
        let timeout = timeout.and_then(duration_from_secs);
        self.poll.poll(&mut self.events, timeout)?;
        ready.clear();
        ready.reserve(self.events.iter().count());
        for event in self.events.iter() {
            let fd = fd_for_token(event.token());
            let mut mask = 0;
            if event.is_readable() {
                mask |= READABLE;
            }
            if event.is_writable() {
                mask |= WRITABLE;
            }
            if mask != 0 {
                let fd = fd as i32;
                if let Some((_, existing_mask)) =
                    ready.iter_mut().find(|(existing_fd, _)| *existing_fd == fd)
                {
                    *existing_mask |= mask;
                } else {
                    ready.push((fd, mask));
                }
            }
        }
        Ok(())
    }
}

#[pyclass(module = "kioto._kioto")]
pub struct TokioPoller {
    state: Mutex<PollerState>,
}

#[pymethods]
impl TokioPoller {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self {
            state: Mutex::new(PollerState::new().map_err(PyOSError::new_err)?),
        })
    }

    fn set_interest(&self, fd: i32, readable: bool, writable: bool) -> PyResult<()> {
        self.state
            .lock()
            .set_interest(fd as RawFd, interest_mask(readable, writable))
            .map_err(PyOSError::new_err)
    }

    fn unregister(&self, fd: i32) {
        let _ = self.state.lock().remove(fd as RawFd);
    }

    #[pyo3(signature = (timeout=None))]
    fn select(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Vec<(i32, u8)>> {
        let mut ready = Vec::new();
        py.detach(|| self.state.lock().select_into(timeout, &mut ready))
            .map_err(PyOSError::new_err)?;
        Ok(ready)
    }

    fn close(&self) {
        let mut state = self.state.lock();
        if state.closed {
            return;
        }
        state.closed = true;
        let fds = state.registrations.keys().copied().collect::<Vec<_>>();
        for fd in fds {
            let _ = state.remove(fd);
        }
    }
}

impl TokioPoller {
    pub(crate) fn set_interest_inner(
        &self,
        fd: RawFd,
        readable: bool,
        writable: bool,
    ) -> PyResult<()> {
        self.state
            .lock()
            .set_interest(fd, interest_mask(readable, writable))
            .map_err(PyOSError::new_err)
    }

    pub(crate) fn select_inner_into(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
        ready: &mut Vec<(i32, u8)>,
    ) -> PyResult<()> {
        py.detach(|| self.state.lock().select_into(timeout, ready))
            .map_err(PyOSError::new_err)
    }

    pub(crate) fn is_only_registered_fd(&self, fd: i32) -> bool {
        let state = self.state.lock();
        state.registrations.len() == 1 && state.registrations.contains_key(&(fd as RawFd))
    }
}

fn interest_mask(readable: bool, writable: bool) -> u8 {
    (if readable { READABLE } else { 0 }) | (if writable { WRITABLE } else { 0 })
}

fn token_for_fd(fd: RawFd) -> Token {
    Token(fd as usize)
}

fn fd_for_token(token: Token) -> RawFd {
    token.0 as RawFd
}

fn to_interest(mask: u8) -> Interest {
    match mask {
        READABLE => Interest::READABLE,
        WRITABLE => Interest::WRITABLE,
        ALL_INTERESTS => Interest::READABLE.add(Interest::WRITABLE),
        _ => unreachable!("unsupported readiness mask"),
    }
}

fn duration_from_secs(timeout: f64) -> Option<Duration> {
    if timeout <= 0.0 {
        Some(Duration::ZERO)
    } else {
        Some(Duration::from_secs_f64(timeout))
    }
}

fn file_identity(fd: RawFd) -> io::Result<FileIdentity> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok(FileIdentity {
        dev: stat.st_dev,
        ino: stat.st_ino,
    })
}

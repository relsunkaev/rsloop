#![cfg(unix)]

use std::ffi::CString;
use std::io;
use std::os::fd::RawFd;

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct NativePipeEndpoint {
    fd: RawFd,
    closed: bool,
}

#[pymethods]
impl NativePipeEndpoint {
    #[new]
    fn new(fd: RawFd) -> Self {
        Self { fd, closed: false }
    }

    fn fileno(&self) -> PyResult<RawFd> {
        if self.closed {
            Err(PyOSError::new_err("I/O operation on closed file"))
        } else {
            Ok(self.fd)
        }
    }

    fn close(&mut self) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        close_fd(self.fd)
    }

    fn __del__(&mut self) {
        if !self.closed {
            let _ = close_fd(self.fd);
            self.closed = true;
        }
    }
}

#[pyclass(module = "rsloop._rsloop", unsendable)]
pub struct NativeSubprocessProcess {
    #[pyo3(get)]
    pid: i32,
    #[pyo3(get, set)]
    returncode: Option<i32>,
}

#[pymethods]
impl NativeSubprocessProcess {
    #[new]
    fn new(pid: i32) -> Self {
        Self {
            pid,
            returncode: None,
        }
    }

    fn poll(&self) -> Option<i32> {
        self.returncode
    }

    fn send_signal(&self, sig: i32) -> PyResult<()> {
        if self.returncode.is_some() {
            return Ok(());
        }
        send_signal(self.pid, sig)
    }

    fn terminate(&self) -> PyResult<()> {
        send_signal(self.pid, libc::SIGTERM)
    }

    fn kill(&self) -> PyResult<()> {
        send_signal(self.pid, libc::SIGKILL)
    }
}

#[derive(Clone, Copy)]
enum StdioMode {
    Inherit,
    Pipe,
    DevNull,
    Stdout,
}

struct SpawnResult {
    pid: i32,
    stdin_parent: Option<RawFd>,
    stdout_parent: Option<RawFd>,
    stderr_parent: Option<RawFd>,
}

#[pyfunction]
#[pyo3(signature = (
    args,
    shell,
    stdin_mode,
    stdout_mode,
    stderr_mode,
    env=None,
))]
pub fn spawn_subprocess(
    py: Python<'_>,
    args: Bound<'_, PyAny>,
    shell: bool,
    stdin_mode: &str,
    stdout_mode: &str,
    stderr_mode: &str,
    env: Option<Bound<'_, PyAny>>,
) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
    let stdin_mode = parse_stdio_mode(stdin_mode, false)?;
    let stdout_mode = parse_stdio_mode(stdout_mode, false)?;
    let stderr_mode = parse_stdio_mode(stderr_mode, true)?;

    if matches!(stdout_mode, StdioMode::Stdout) {
        return Err(PyValueError::new_err(
            "stdout mode must not use stderr redirection",
        ));
    }

    let argv = build_argv(py, args, shell)?;
    let envp = build_envp(py, env)?;
    let spawn = spawn_posix(argv, envp, stdin_mode, stdout_mode, stderr_mode)?;

    let proc = Py::new(py, NativeSubprocessProcess::new(spawn.pid))?
        .into_bound(py)
        .into_any()
        .unbind();
    let stdin_pipe = wrap_endpoint(py, spawn.stdin_parent)?;
    let stdout_pipe = wrap_endpoint(py, spawn.stdout_parent)?;
    let stderr_pipe = wrap_endpoint(py, spawn.stderr_parent)?;

    Ok((proc, stdin_pipe, stdout_pipe, stderr_pipe))
}

fn wrap_endpoint(py: Python<'_>, fd: Option<RawFd>) -> PyResult<Py<PyAny>> {
    match fd {
        Some(fd) => Ok(Py::new(py, NativePipeEndpoint::new(fd))?
            .into_bound(py)
            .into_any()
            .unbind()),
        None => Ok(py.None()),
    }
}

fn parse_stdio_mode(mode: &str, allow_stdout: bool) -> PyResult<StdioMode> {
    match mode {
        "inherit" => Ok(StdioMode::Inherit),
        "pipe" => Ok(StdioMode::Pipe),
        "devnull" => Ok(StdioMode::DevNull),
        "stdout" if allow_stdout => Ok(StdioMode::Stdout),
        _ => Err(PyValueError::new_err(format!(
            "unsupported stdio mode: {mode}"
        ))),
    }
}

fn build_argv(
    py: Python<'_>,
    args: Bound<'_, PyAny>,
    shell: bool,
) -> PyResult<Vec<CString>> {
    if shell {
        let command = to_fs_bytes(py, &args)?;
        return Ok(vec![
            CString::new("/bin/sh").unwrap(),
            CString::new("-c").unwrap(),
            CString::new(command)
                .map_err(|_| PyValueError::new_err("shell command contains NUL byte"))?,
        ]);
    }

    let seq = args
        .try_iter()
        .map_err(|_| PyValueError::new_err("subprocess args must be iterable"))?;
    let mut argv = Vec::new();
    for item in seq {
        let item = item?;
        let bytes = to_fs_bytes(py, &item)?;
        argv.push(
            CString::new(bytes)
                .map_err(|_| PyValueError::new_err("subprocess arg contains NUL byte"))?,
        );
    }
    if argv.is_empty() {
        return Err(PyValueError::new_err("subprocess args are empty"));
    }
    Ok(argv)
}

fn to_fs_bytes(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let os = py.import("os")?;
    os.call_method1("fsencode", (value,))?.extract::<Vec<u8>>()
}

fn spawn_posix(
    argv: Vec<CString>,
    envp: Vec<CString>,
    stdin_mode: StdioMode,
    stdout_mode: StdioMode,
    stderr_mode: StdioMode,
) -> PyResult<SpawnResult> {
    let mut file_actions = std::mem::MaybeUninit::<libc::posix_spawn_file_actions_t>::uninit();
    let rc = unsafe { libc::posix_spawn_file_actions_init(file_actions.as_mut_ptr()) };
    if rc != 0 {
        return Err(PyOSError::new_err(io::Error::from_raw_os_error(rc).to_string()));
    }
    let mut file_actions = PosixSpawnFileActions(unsafe { file_actions.assume_init() });

    let mut attr = std::mem::MaybeUninit::<libc::posix_spawnattr_t>::uninit();
    let rc = unsafe { libc::posix_spawnattr_init(attr.as_mut_ptr()) };
    if rc != 0 {
        return Err(PyOSError::new_err(io::Error::from_raw_os_error(rc).to_string()));
    }
    let mut attr = PosixSpawnAttr(unsafe { attr.assume_init() });
    let mut parent_cleanup = Vec::new();

    let stdin_parent = setup_stdin(&mut file_actions, stdin_mode, &mut parent_cleanup)?;
    let stdout_state = setup_stdout(&mut file_actions, stdout_mode, &mut parent_cleanup)?;
    let stderr_parent = setup_stderr(&mut file_actions, stderr_mode, &mut parent_cleanup)?;

    let mut argv_ptrs = build_ptrs(&argv);
    let mut envp_ptrs = build_ptrs(&envp);

    let mut pid: libc::pid_t = 0;
    let rc = unsafe {
        libc::posix_spawnp(
            &mut pid as *mut _,
            argv[0].as_ptr(),
            &mut file_actions.0 as *mut _,
            &mut attr.0 as *mut _,
            argv_ptrs.as_mut_ptr(),
            envp_ptrs.as_mut_ptr(),
        )
    };

    for fd in parent_cleanup.drain(..) {
        let _ = close_fd(fd);
    }

    if rc != 0 {
        if let Some(fd) = stdin_parent {
            let _ = close_fd(fd);
        }
        if let Some(fd) = stdout_state.parent_fd {
            let _ = close_fd(fd);
        }
        if let Some(fd) = stderr_parent {
            let _ = close_fd(fd);
        }
        return Err(PyOSError::new_err(io::Error::from_raw_os_error(rc).to_string()));
    }

    Ok(SpawnResult {
        pid,
        stdin_parent,
        stdout_parent: stdout_state.parent_fd,
        stderr_parent,
    })
}

struct StdoutState {
    parent_fd: Option<RawFd>,
}

fn setup_stdin(
    file_actions: &mut PosixSpawnFileActions,
    mode: StdioMode,
    parent_cleanup: &mut Vec<RawFd>,
) -> PyResult<Option<RawFd>> {
    match mode {
        StdioMode::Inherit => Ok(None),
        StdioMode::DevNull => {
            let fd = open_devnull(libc::O_RDONLY)?;
            file_actions.add_dup2(fd, libc::STDIN_FILENO)?;
            file_actions.add_close(fd)?;
            parent_cleanup.push(fd);
            Ok(None)
        }
        StdioMode::Pipe => {
            let (read_fd, write_fd) = make_pipe()?;
            set_cloexec(write_fd)?;
            file_actions.add_dup2(read_fd, libc::STDIN_FILENO)?;
            file_actions.add_close(read_fd)?;
            parent_cleanup.push(read_fd);
            Ok(Some(write_fd))
        }
        StdioMode::Stdout => Err(PyValueError::new_err(
            "stdin cannot use stdout redirection",
        )),
    }
}

fn setup_stdout(
    file_actions: &mut PosixSpawnFileActions,
    mode: StdioMode,
    parent_cleanup: &mut Vec<RawFd>,
) -> PyResult<StdoutState> {
    match mode {
        StdioMode::Inherit => Ok(StdoutState {
            parent_fd: None,
        }),
        StdioMode::DevNull => {
            let fd = open_devnull(libc::O_WRONLY)?;
            file_actions.add_dup2(fd, libc::STDOUT_FILENO)?;
            file_actions.add_close(fd)?;
            parent_cleanup.push(fd);
            Ok(StdoutState {
                parent_fd: None,
            })
        }
        StdioMode::Pipe => {
            let (read_fd, write_fd) = make_pipe()?;
            set_cloexec(read_fd)?;
            file_actions.add_dup2(write_fd, libc::STDOUT_FILENO)?;
            file_actions.add_close(write_fd)?;
            parent_cleanup.push(write_fd);
            Ok(StdoutState {
                parent_fd: Some(read_fd),
            })
        }
        StdioMode::Stdout => Err(PyValueError::new_err(
            "stdout cannot use stdout redirection",
        )),
    }
}

fn setup_stderr(
    file_actions: &mut PosixSpawnFileActions,
    mode: StdioMode,
    parent_cleanup: &mut Vec<RawFd>,
) -> PyResult<Option<RawFd>> {
    match mode {
        StdioMode::Inherit => Ok(None),
        StdioMode::DevNull => {
            let fd = open_devnull(libc::O_WRONLY)?;
            file_actions.add_dup2(fd, libc::STDERR_FILENO)?;
            file_actions.add_close(fd)?;
            parent_cleanup.push(fd);
            Ok(None)
        }
        StdioMode::Pipe => {
            let (read_fd, write_fd) = make_pipe()?;
            set_cloexec(read_fd)?;
            file_actions.add_dup2(write_fd, libc::STDERR_FILENO)?;
            file_actions.add_close(write_fd)?;
            parent_cleanup.push(write_fd);
            Ok(Some(read_fd))
        }
        StdioMode::Stdout => {
            file_actions.add_dup2(libc::STDOUT_FILENO, libc::STDERR_FILENO)?;
            Ok(None)
        }
    }
}

fn build_envp(py: Python<'_>, env: Option<Bound<'_, PyAny>>) -> PyResult<Vec<CString>> {
    let mut envp = Vec::new();
    match env {
        Some(env) => {
            let env_dict = env.cast::<PyDict>()?;
            for (key, value) in env_dict.iter() {
                let key_bytes = to_fs_bytes(py, &key)?;
                let value_bytes = to_fs_bytes(py, &value)?;
                let mut pair = key_bytes;
                pair.push(b'=');
                pair.extend(value_bytes);
                envp.push(
                    CString::new(pair)
                        .map_err(|_| PyValueError::new_err("environment contains NUL byte"))?,
                );
            }
        }
        None => {
            for (key, value) in std::env::vars_os() {
                let mut pair = key.into_encoded_bytes();
                pair.push(b'=');
                pair.extend(value.into_encoded_bytes());
                envp.push(
                    CString::new(pair)
                        .map_err(|_| PyValueError::new_err("environment contains NUL byte"))?,
                );
            }
        }
    }
    Ok(envp)
}

fn build_ptrs(values: &[CString]) -> Vec<*mut libc::c_char> {
    let mut ptrs = values.iter().map(|value| value.as_ptr()).collect::<Vec<_>>();
    ptrs.push(std::ptr::null());
    ptrs
        .into_iter()
        .map(|ptr| ptr.cast_mut())
        .collect::<Vec<*mut libc::c_char>>()
}

fn make_pipe() -> PyResult<(RawFd, RawFd)> {
    let mut fds = [0; 2];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        Err(PyOSError::new_err(io::Error::last_os_error().to_string()))
    } else {
        Ok((fds[0], fds[1]))
    }
}

fn open_devnull(flags: libc::c_int) -> PyResult<RawFd> {
    let path = CString::new("/dev/null").unwrap();
    let fd = unsafe { libc::open(path.as_ptr(), flags) };
    if fd < 0 {
        Err(PyOSError::new_err(io::Error::last_os_error().to_string()))
    } else {
        Ok(fd)
    }
}

fn set_cloexec(fd: RawFd) -> PyResult<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(PyOSError::new_err(io::Error::last_os_error().to_string()));
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    if rc < 0 {
        return Err(PyOSError::new_err(io::Error::last_os_error().to_string()));
    }
    Ok(())
}

fn close_fd(fd: RawFd) -> PyResult<()> {
    let rc = unsafe { libc::close(fd) };
    if rc != 0 {
        Err(PyOSError::new_err(io::Error::last_os_error().to_string()))
    } else {
        Ok(())
    }
}

fn send_signal(pid: i32, sig: i32) -> PyResult<()> {
    let rc = unsafe { libc::kill(pid, sig) };
    if rc != 0 {
        Err(PyOSError::new_err(io::Error::last_os_error().to_string()))
    } else {
        Ok(())
    }
}

struct PosixSpawnFileActions(libc::posix_spawn_file_actions_t);

impl PosixSpawnFileActions {
    fn add_dup2(&mut self, from: RawFd, to: RawFd) -> PyResult<()> {
        let rc = unsafe { libc::posix_spawn_file_actions_adddup2(&mut self.0 as *mut _, from, to) };
        if rc != 0 {
            Err(PyOSError::new_err(io::Error::from_raw_os_error(rc).to_string()))
        } else {
            Ok(())
        }
    }

    fn add_close(&mut self, fd: RawFd) -> PyResult<()> {
        let rc = unsafe { libc::posix_spawn_file_actions_addclose(&mut self.0 as *mut _, fd) };
        if rc != 0 {
            Err(PyOSError::new_err(io::Error::from_raw_os_error(rc).to_string()))
        } else {
            Ok(())
        }
    }
}

impl Drop for PosixSpawnFileActions {
    fn drop(&mut self) {
        unsafe {
            libc::posix_spawn_file_actions_destroy(&mut self.0 as *mut _);
        }
    }
}

struct PosixSpawnAttr(libc::posix_spawnattr_t);

impl Drop for PosixSpawnAttr {
    fn drop(&mut self) {
        unsafe {
            libc::posix_spawnattr_destroy(&mut self.0 as *mut _);
        }
    }
}

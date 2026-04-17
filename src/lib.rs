#[cfg(not(unix))]
compile_error!("rsloop currently supports unix platforms only");

mod bridge;
#[cfg(unix)]
mod completion;
#[cfg(unix)]
mod fd_callbacks;
#[cfg(unix)]
mod handles;
#[cfg(unix)]
mod loop_api;
#[cfg(unix)]
mod native_subprocess;
#[cfg(unix)]
mod pipe_registry;
#[cfg(unix)]
mod pipe_transport;
#[cfg(unix)]
mod poller;
#[cfg(unix)]
mod scheduler;
#[cfg(unix)]
mod socket_registry;
#[cfg(unix)]
mod socket_state;
#[cfg(unix)]
mod stream_registry;
#[cfg(unix)]
mod stream_transport;
#[cfg(unix)]
mod subprocess_pipe_transport;
#[cfg(unix)]
mod subprocess_transport;
#[cfg(unix)]
mod tls;

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

#[cfg(unix)]
use completion::CompletionPort;
#[cfg(unix)]
use fd_callbacks::FdCallbackRegistry;
#[cfg(unix)]
use handles::{make_handle, FutureHandle, Handle, OneArgHandle, ZeroArgHandle};
#[cfg(unix)]
use loop_api::LoopApi;
#[cfg(unix)]
use native_subprocess::{spawn_subprocess, NativePipeEndpoint, NativeSubprocessProcess};
#[cfg(unix)]
use pipe_registry::PipeTransportRegistry;
#[cfg(unix)]
use pipe_transport::{ReadPipeTransport, WritePipeTransport};
#[cfg(unix)]
use poller::TokioPoller;
#[cfg(unix)]
use scheduler::Scheduler;
#[cfg(unix)]
use socket_registry::SocketStateRegistry;
#[cfg(unix)]
use socket_state::SocketState;
#[cfg(unix)]
use stream_registry::StreamTransportRegistry;
#[cfg(unix)]
use stream_transport::StreamTransport;
#[cfg(unix)]
use subprocess_pipe_transport::{ReadSubprocessPipeProto, WriteSubprocessPipeProto};
#[cfg(unix)]
use subprocess_transport::SubprocessTransport;
#[cfg(unix)]
use tls::RsloopTLSContext;

#[pymodule]
fn _rsloop(m: &Bound<'_, PyModule>) -> PyResult<()> {
    Python::initialize();
    m.add_function(wrap_pyfunction!(bridge::backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::sleep, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::run_in_tokio, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::wrap_future, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::register_waitpid_exit_callback, m)?)?;
    #[cfg(unix)]
    m.add_class::<CompletionPort>()?;
    #[cfg(unix)]
    m.add_class::<FdCallbackRegistry>()?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(make_handle, m)?)?;
    #[cfg(unix)]
    m.add_class::<Handle>()?;
    #[cfg(unix)]
    m.add_class::<FutureHandle>()?;
    #[cfg(unix)]
    m.add_class::<ZeroArgHandle>()?;
    #[cfg(unix)]
    m.add_class::<OneArgHandle>()?;
    #[cfg(unix)]
    m.add_class::<LoopApi>()?;
    #[cfg(unix)]
    m.add_class::<NativePipeEndpoint>()?;
    #[cfg(unix)]
    m.add_class::<NativeSubprocessProcess>()?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(spawn_subprocess, m)?)?;
    #[cfg(unix)]
    m.add_class::<StreamTransport>()?;
    #[cfg(unix)]
    m.add_class::<PipeTransportRegistry>()?;
    #[cfg(unix)]
    m.add_class::<ReadPipeTransport>()?;
    #[cfg(unix)]
    m.add_class::<WritePipeTransport>()?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(
        stream_transport::feed_stream_reader_data,
        m
    )?)?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(
        stream_transport::feed_stream_reader_eof,
        m
    )?)?;
    #[cfg(unix)]
    m.add_class::<TokioPoller>()?;
    #[cfg(unix)]
    m.add_class::<Scheduler>()?;
    #[cfg(unix)]
    m.add_class::<SubprocessTransport>()?;
    #[cfg(unix)]
    m.add_class::<ReadSubprocessPipeProto>()?;
    #[cfg(unix)]
    m.add_class::<WriteSubprocessPipeProto>()?;
    #[cfg(unix)]
    m.add_class::<SocketStateRegistry>()?;
    #[cfg(unix)]
    m.add_class::<SocketState>()?;
    #[cfg(unix)]
    m.add_class::<StreamTransportRegistry>()?;
    #[cfg(unix)]
    m.add_class::<RsloopTLSContext>()?;
    Ok(())
}

#[cfg(not(unix))]
compile_error!("kioto currently supports unix platforms only");

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

#[pymodule]
fn _kioto(m: &Bound<'_, PyModule>) -> PyResult<()> {
    Python::initialize();
    m.add_function(wrap_pyfunction!(bridge::backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::sleep, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::run_in_tokio, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::wrap_future, m)?)?;
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
    m.add_class::<StreamTransport>()?;
    #[cfg(unix)]
    m.add_class::<TokioPoller>()?;
    #[cfg(unix)]
    m.add_class::<Scheduler>()?;
    #[cfg(unix)]
    m.add_class::<SocketStateRegistry>()?;
    #[cfg(unix)]
    m.add_class::<SocketState>()?;
    #[cfg(unix)]
    m.add_class::<StreamTransportRegistry>()?;
    Ok(())
}

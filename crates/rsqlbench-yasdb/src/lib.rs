mod loader;
pub(crate) mod native;
mod sut;
pub(crate) mod wrapper;

pub(crate) use loader::*;
pub use sut::*;
use wrapper::{DbcHandle, EnvHandle};

pub(crate) struct Connection {
    conn_handle: DbcHandle,
    _env_handle: EnvHandle, // must be dropped after `conn_handle`
}

unsafe impl Send for Connection {}

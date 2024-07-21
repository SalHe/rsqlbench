mod loader;
pub(crate) mod native;
mod sut;
mod terminal;
pub(crate) mod wrapper;

pub(crate) use loader::*;
pub use sut::*;
pub(crate) use terminal::*;
use wrapper::{DbcHandle, EnvHandle};

pub(crate) struct Connection {
    conn_handle: DbcHandle,
    _env_handle: EnvHandle, // must be dropped after `conn_handle`
}

unsafe impl Send for Connection {}

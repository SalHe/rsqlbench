mod benchmark;
mod connection;
mod loader;
pub mod tpcc;

pub use benchmark::*;
pub use connection::*;
pub use loader::*;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BenchConfig {
    pub loader: Loader,
    pub connection: Connection,
    pub benchmark: Benchmark,
}

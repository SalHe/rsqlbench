mod benchmark;
mod connection;
mod loader;
pub mod tpcc;

pub use benchmark::*;
pub use connection::*;
pub use loader::*;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BenchConfig {
    pub loader: Loader,
    pub connection: Connection,
    pub benchmark: Benchmark,
}

impl BenchConfig {
    pub fn new(config: Option<&str>) -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name(config.unwrap_or("rsqlbench")))
            .add_source(Environment::with_prefix("RSB"))
            .build()?;
        s.try_deserialize()
    }
}

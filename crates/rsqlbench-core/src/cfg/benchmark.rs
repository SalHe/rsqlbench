use serde::Deserialize;

use super::tpcc::TpccBenchmark;

#[derive(Debug, Deserialize)]
pub struct Benchmark {
    /// TPC-C specified configuration.
    pub tpcc: TpccBenchmark,
}

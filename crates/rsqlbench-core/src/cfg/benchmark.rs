use serde::Deserialize;

use super::tpcc::TpccBenchmark;

#[derive(Debug, Deserialize)]
pub struct Benchmark {
    pub tpcc: TpccBenchmark,
}

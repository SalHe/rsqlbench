# RSqlBenchmark

[![build](https://github.com/SalHe/rsqlbench/actions/workflows/ci.yaml/badge.svg)](https://github.com/SalHe/rsqlbench/actions/workflows/ci.yaml)

A TPC-C like benchmark tool.

## Features

- MySQL TPC-C based on [SQLx](https://github.com/launchbadge/sqlx)
- [YashanDB](https://yashandb.com/) TPC-C base on [C-Driver official](https://doc.yashandb.com/yashandb/23.2/zh/%E5%BC%80%E5%8F%91%E6%89%8B%E5%86%8C/C%E8%AF%AD%E8%A8%80%E7%B3%BB%E9%A9%B1%E5%8A%A8/00C%E8%AF%AD%E8%A8%80%E7%B3%BB%E9%A9%B1%E5%8A%A8.html): No Rust native async support, call APIs within [tokio::task::spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html). Better performance should be with async support. When benchmark with too many terminals, configure [max_blocking_threads](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.max_blocking_threads)(unimplemented now)
- [Prometheus](https://github.com/prometheus/prometheus) Metrics

## Building for [YashanDB](https://yashandb.com/)

```shell
# Download yasdb C driver.
./scripts/download-yascli.sh
```

```shell
# Build rsqlbench
cargo build --feature yasdb --release
```

## TODOs

- [ ] Safety of yasdb: spawn_blocking using ref.

## References

- [TPC-C Specification](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf)
- SQLs from:
    - [HammerDB](https://github.com/TPC-Council/HammerDB)
    - [pgsql-io/benchmarksql](https://github.com/pgsql-io/benchmarksql)

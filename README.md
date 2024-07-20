# RSqlBenchmark

[![build](https://github.com/SalHe/rsqlbench/actions/workflows/ci.yaml/badge.svg)](https://github.com/SalHe/rsqlbench/actions/workflows/ci.yaml)

A TPC-C like benchmark tool.

## Building for [YashanDB](https://yashandb.com/)

```shell
# Download yasdb C driver.
mkdir crates/rsqlbench-yasdb/yascli
(cd crates/rsqlbench-yasdb/yascli ; wget https://linked.yashandb.com/resource/yashandb-client-23.2.1.100-linux-x86_64.tar.gz -O yascli.tar.gz ; tar xzf yascli.tar.gz)
```

```shell
# Build rsqlbench
cargo build --feature yasdb --release
```

## References

- [TPC-C Specification](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf)
- Cited some SQLs from:
    - [HammerDB](https://github.com/TPC-Council/HammerDB)
    - [pgsql-io/benchmarksql](https://github.com/pgsql-io/benchmarksql)

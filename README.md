# RSqlBenchmark

[![build](https://github.com/SalHe/rsqlbench/actions/workflows/ci.yaml/badge.svg)](https://github.com/SalHe/rsqlbench/actions/workflows/ci.yaml)

A TPC-C like benchmark tool.

## Building for [YashanDB](https://yashandb.com/)

```shell
# Download yasdb C driver.
./scripts/download-yascli.sh
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

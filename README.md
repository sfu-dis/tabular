# Tabular

## Setup
### Hugepages
```
echo <number-of-1GB-hugepages> | sudo tee /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages
```
### Dependencies

#### Build tools
- gcc
- cmake
- python
- cpplint
#### Libraries
- glog
- gtest
- tbb
- jemalloc

## Build
```shell
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release # RelWithDebInfo, Debug
make -j`nproc`
```

## Index Benchmarks
### Variants
Hand-crafted:
- `btree_olc`
- `hashtable_olc`

Tabular:
- `btree_inline_tabular`
- `hashtable_inline_tabular`

Naive-OCC:
- `btree_inline_tabular_materialized`
- `hashtable_inline_tabular_materialized`

Naive-MVCC:
- `btree_tabular`
- `hashtable_mvcc_tabular`

Pessimistic Locking:
- `btree_lc_stdrw`
- `btree_lc_tbbrw`
- `hashtable_lc_stdrw`
- `hashtable_lc_tbbrw`

### Workload Types
- `C`: Lookup-only
- `G`: Lookup-mostly
- `A`: Balanced
- `H`: Update-mostly
- `F`: Update-only

### Command-line interface
```shell
# Volatile
$BUILD_DIR/benchmarks/ycsb/ycsb_<variant>_bench_256 --workload <workload> --threads <number of threads> --records <number of keys> --duration <seconds>
# Persistent
$BUILD_DIR/benchmarks/ycsb/ycsb_<variant>_bench_256 --workload <workload> --threads <number of threads> --records <number of keys> --duration <seconds> --persistent --log-dir=<logging directory>
# Persistent (No I/O)
$BUILD_DIR/benchmarks/ycsb/ycsb_<variant>_log_nop_bench_256 --workload <workload> --threads <number of threads> --records <number of keys> --duration <seconds> --persistent --log-dir=<logging directory>
```

## TPC-C Benchmarks
```shell
$BUILD_DIR/benchmarks/tpcc/tpcc_masstree_bench --duration <seconds> --threads <number of threads> --scale-factor <scale factor>
$BUILD_DIR/benchmarks/tpcc/tpcc_tabular_btree_bench --duration <seconds> --threads <number of threads> --scale-factor <scale factor>
```

## Notes
To minimize NUMA effect, run benchmarks with `numactl --membind 0` or `numactl --interleave 0,1,..,<N>` when threads are within one socket or across `N` sockets.

Every benchmark binary accepts `--help` to print complete available options.

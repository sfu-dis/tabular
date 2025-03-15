## Tabular: Efficiently Building Efficient Indexes

Tabular is a parallel programming library that models data structures as relational tables to provide concurrency and persistence transparently. 

More details are described in our VLDB 2025 paper below (preprint [here](https://www.cs.sfu.ca/~tzwang/tabular.pdf)). If you use our work, please cite:
```
Tabular: Efficiently Building Efficient Indexes.
Ziyi Yan, Mohamed Farouk Drira, Tianxun Hu and Tianzheng Wang.
VLDB 2025
```

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
- `btree_olc_bench_256`
- `hashtable_olc_bench_256`
- `art_olc_bench`

Tabular:
- `btree_inline_tabular_bench_256`
- `hashtable_inline_tabular_bench_256`
- `art_tabular_bench`

Naive-OCC:
- `btree_inline_tabular_materialized_bench_256`
- `hashtable_inline_tabular_materialized_bench_256`
- `art_tabular_materialized_bench`

Naive-MVCC:
- `btree_tabular_bench_256`
- `hashtable_mvcc_tabular_bench_256`

Pessimistic Locking:
- `btree_lc_stdrw_bench_256`
- `btree_lc_tbbrw_bench_256`
- `hashtable_lc_stdrw_bench_256`
- `hashtable_lc_tbbrw_bench_256`
- `art_lc_stdrw_bench`
- `art_lc_tbbrw_bench`

Other indexes:
- `bptree_bench_1K_16_16`
- `masstree_bench`

### Workload Types
- `C`: Lookup-only
- `G`: Lookup-mostly
- `A`: Balanced
- `H`: Update-mostly
- `F`: Update-only

### Command-line interface
```shell
# Volatile
$BUILD_DIR/benchmarks/ycsb/ycsb_<variant> --workload <workload> --threads <number of threads> --records <number of keys> --duration <seconds>
# Persistent (only works for Tabular indexes)
$BUILD_DIR/benchmarks/ycsb/ycsb_<variant> --workload <workload> --threads <number of threads> --records <number of keys> --duration <seconds> --persistent --log-dir=<logging directory>
```

## TPC-C Benchmarks
```shell
$BUILD_DIR/benchmarks/tpcc/tpcc_masstree_bench --duration <seconds> --threads <number of threads> --scale-factor <scale factor>
$BUILD_DIR/benchmarks/tpcc/tpcc_tabular_btree_bench --duration <seconds> --threads <number of threads> --scale-factor <scale factor>
```

## Notes
To minimize NUMA effect, run benchmarks with `numactl --membind 0` or `numactl --interleave 0,1,..,<N>` when threads are within one socket or across `N` sockets.

Every benchmark binary accepts `--help` to print complete available options.

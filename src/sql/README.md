# Noname SQL Layer

In Noname SQL layer, we use DuckDB as a SQL parser to generate query plans.
Query executors are created based these query plans.

## Current files which contains DuckDB code:
- benchmarks/tpcc/procedures/new\_order.cc
- benchmarks/tpcc/procedures/payment.cc
- benchmarks/tpcc/tpcc\_load.cc
- src/catalog/catalog.cc
- src/catalog/catalog.h
- src/noname\_defs.h
- src/sql/duckdb\_internal.h
- src/sql/duckdb\_query.cc
- src/sql/duckdb\_query.h
- src/sql/execution/pull/aggregate\_pull\_executor.cc
- src/sql/execution/pull/aggregate\_pull\_executor.h
- src/sql/execution/pull/delete\_pull\_executor.cc
- src/sql/execution/pull/delete\_pull\_executor.h
- src/sql/execution/pull/filter\_pull\_executor.cc
- src/sql/execution/pull/filter\_pull\_executor.h
- src/sql/execution/pull/join\_pull\_executor.cc
- src/sql/execution/pull/join\_pull\_executor.h
- src/sql/execution/pull/limit\_pull\_executor.cc
- src/sql/execution/pull/limit\_pull\_executor.h
- src/sql/execution/pull/order\_pull\_executor.cc
- src/sql/execution/pull/order\_pull\_executor.h
- src/sql/execution/pull/projection\_pull\_executor.cc
- src/sql/execution/pull/projection\_pull\_executor.h
- src/sql/execution/pull/pull\_executor.cc
- src/sql/execution/pull/pull\_executor.h
- src/sql/execution/pull/scan\_pull\_executor.cc
- src/sql/execution/pull/scan\_pull\_executor.h
- src/sql/execution/pull/update\_pull\_executor.cc
- src/sql/execution/pull/update\_pull\_executor.h
- src/sql/query.cc
- src/sql/query.h
- src/table/tuple.cc
- src/table/tuple.h
- tests/sql/query\_test.cc

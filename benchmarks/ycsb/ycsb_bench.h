/*
 * Copyright (C) 2023 Data-Intensive Systems Lab, Simon Fraser University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <unordered_map>
#include <string>

#include "../perf.h"
#include "foedus/uniform_random.hpp"
#include "foedus/zipfian_random.hpp"
#include "table/ia_table.h"
#include "transaction/transaction.h"
#include "util.h"
#include "ycsb_config.h"
#include "ycsb_loader.h"
#include "ycsb_workload.h"
#include "ycsb_worker.h"

#ifdef BTREE_OPTIQL
#include "index/indexes/BTreeOLC/BTreeOMCSLeaf.h"
#endif
#ifdef BTREE_OLC
#include "index/indexes/BTreeOLC/BTreeOLCNB.h"
#endif
#ifdef BTREE_LC_STDRW
#include "index/indexes/BTreeOLC/BTreeLC.h"
#endif
#ifdef BTREE_LC_TBBRW
#include "index/indexes/BTreeOLC/BTreeLC.h"
#endif
#ifdef BTREE_TABULAR
#include "tabular/btree.h"
#endif
#ifdef BTREE_INLINE_TABULAR
#include "tabular/inline_btree.h"
#endif
#ifdef HASHTABLE_OPTIMISTIC_LC
#include "index/hash_table.h"
#endif
#ifdef HASHTABLE_LC_STDRW
#include "index/hash_table_lc.h"
#endif
#ifdef HASHTABLE_LC_TBBRW
#include "index/hash_table_lc.h"
#endif
#ifdef HASHTABLE_INLINE_TABULAR
#include "tabular/hash_table.h"
#endif
#ifdef HASHTABLE_MVCC_TABULAR
#include "tabular/mvcc_hash_table.h"
#endif

namespace noname {
namespace benchmark {
namespace ycsb {

/**
 * @brief Workload Type in ERMIA
 */
enum class WorkloadType { Insert, Read, Update, Scan, ReadModifyWrite };

template <typename IndexType, typename TableType>
struct YCSBBench : public PerformanceTest {
  YCSBBench(YCSBConfig config, TableType *table, IndexType *index);

  /// @brief Init YCSB loaders and loaders start to load data to the table and the index
  void Load();

  /// @brief Override from PerformanceTest and will trigger a worker thread to run benchmark
  /// @param thread_id Worker thread id
  void WorkerRun(uint32_t thread_id, uint64_t number_of_operations) override;

  bool RunNextWorkload(YCSBWorkerLocalData *worker);

  uint64_t GenerateRandomKey(YCSBWorkerLocalData *worker);

  struct ScanRange {
    uint64_t start;
    uint64_t length;
  };

  inline static constexpr uint64_t MAX_SCAN_RANGE_LENGTH = 100;

  ScanRange GenerateRandomScanRange(YCSBWorkerLocalData *worker);

  // TODO(Yue): Wrapped a struct like ResultCode for transactional operations

  /// @brief Transactional Insert
  /// @return Whether the transactional operation is successful or not
  bool TxnInsert(YCSBWorkerLocalData *worker);

  /// @brief Transactional Read
  /// @return Whether the transactional operation is successful or not
  bool TxnRead(YCSBWorkerLocalData *worker);

  // bool TxnReadAMACMultiGet();
  // bool TxnReadSimpleCoroMultiGet();

  /// @brief Transactional Update
  /// @return Whether the transactional operation is successful or not
  bool TxnUpdate(YCSBWorkerLocalData *worker);

  /// @brief Transactional Scan
  /// @return Whether the transactional operation is successful or not
  bool TxnScan(YCSBWorkerLocalData *worker);

  // bool TxnScanWithIterator();

  /// @brief Transactional Read Modify Write
  /// @return Whether the transactional operation is successful or not
  bool TxnRMW(YCSBWorkerLocalData *worker);

  /// @brief Table to be benchmarked
  TableType* table_;

  /// @brief Index to be benchmarked
  IndexType* index_;

  /// @brief YCSB config for benchmark
  /// The config contains the number of worker threads, the time duration of
  /// benchmark in seconds, the number of loaders and workload options.
  YCSBConfig config_;

  std::atomic<uint64_t> next_insert_key_;

  WorkloadPercentage workload_percentage_;
};

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

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

#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>

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
#include "table/ia_table.h"
#include "thread/thread.h"
#include "transaction/transaction.h"

namespace noname {
namespace benchmark {
namespace ycsb {

/**
 * @brief YCSB Load that maintains a array of loader thread
 */
template <class IndexType, class TableType>
struct YCSBLoad {
  /// Constructor
  YCSBLoad(TableType* table, IndexType* index, thread::ThreadPool *thread_pool,
           int32_t num_of_loaders, int32_t num_of_records)
      : table_(table),
        index_(index),
        thread_pool_(thread_pool),
        num_of_loaders_(num_of_loaders),
        num_of_records_(num_of_records),
        num_of_finished_threads_(0) {}

 public:
  /// @brief Load worker function which inserts keys in range [key_start, end)
  /// @param thread_id Loader theard id
  /// @param key_start First key to load
  /// @param key_end End of the key range
  void Load(uint32_t thread_id, uint64_t key_start, uint64_t key_end);

  /// @brief Make loader in loaders array run
  void Run();

  /// @brief Verify the loaded data
  void Verify();

  /// @brief Table init in ycsb_bench.cc
  TableType* table_;

  /// @brief Index init in ycsb_bench.cc
  IndexType* index_;

  /// @brief Array of all loader threads
  thread::ThreadPool* thread_pool_;

  /// @brief Number of loaders
  int32_t num_of_loaders_;

  /// @brief Number of record to be inserted per loader
  int32_t num_of_records_;

  /// @brief Number of loader threads that finish
  int32_t num_of_finished_threads_;

  /// @brief Mutex
  std::mutex finish_mx_;

  /// @brief Conditional variable
  std::condition_variable finish_cv_;
};

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

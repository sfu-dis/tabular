/*
 * Copyright (C) 2024 Data-Intensive Systems Lab, Simon Fraser University.
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

#include <thread>
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "benchmarks/tpcc/tpcc_ddl.h"
#include "benchmarks/tpcc/tpcc_util.h"
#include "sql/query.h"
#include "thread/thread.h"

namespace noname {
namespace benchmark {
namespace tpcc {

struct TPCCLoad {
  // Constructor
  TPCCLoad() {}
  TPCCLoad(thread::ThreadPool *thread_pool, uint32_t nthreads)
    : thread_pool(thread_pool), nthreads(nthreads) {}

  // Destructor
  ~TPCCLoad() {}

  void CreateTables();
  void CreateIndexes();
  void Run();
  void LoadAll(uint w_id);
  void LoadWarehouse(uint w_id);
  void LoadItems();
  void LoadStocks(uint w_id);
  void LoadDistricts(uint w_id);
  void LoadCustomers(uint w_id);
  void LoadHistory(uint w_id);
  void LoadOorders(uint w_id);
  void LoadNewOrders(uint w_id);
  void LoadOrderLines(uint w_id);

  // List of all loader threads
  std::vector<thread::Thread *> loaders;
  thread::ThreadPool *thread_pool;

  // Number of worker threads
  uint32_t nthreads;
};

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


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

#include "benchmarks/perf.h"
#include "benchmarks/tpcc/procedures/new_order.h"
#include "benchmarks/tpcc/procedures/payment.h"
#include "benchmarks/tpcc/procedures/stock_level.h"
#include "benchmarks/tpcc/procedures/order_status.h"
#include "benchmarks/tpcc/procedures/delivery.h"
#include "benchmarks/tpcc/tpcc_load.h"
#include "benchmarks/tpcc/tpcc_util.h"
#include "engine/noname/noname.h"
#include "sql/query.h"

namespace noname {
namespace benchmark {
namespace tpcc {

struct TPCCBench : public PerformanceTest {
  TPCCBench(uint32_t threads, uint32_t seconds, uint64_t number_of_operations,
            benchmark::ProfilingOption profiling_option);

  std::pair<bool, int> TxnRun(uint32_t thread_id);

  // Load the table to prepare for running benchmark tests
  void Load();

  // Benchmark worker function
  void WorkerRun(uint32_t thread_id, uint64_t number_of_operations) override;

  // Constructor
  TPCCBench();

  // Destructor
  ~TPCCBench();

  // Noname engine
  Engine db;

  util::fast_random r{85541689};
  
  //cumulative distributions of transactions
  uint tx_distribution[5] = {45, 88, 92, 96, 100};
};

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


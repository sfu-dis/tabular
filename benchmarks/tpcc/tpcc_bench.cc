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
#include <malloc.h>
#include <boost/program_options/value_semantic.hpp>

#include "benchmarks/tpcc/tpcc_bench.h"
#include "config/config.h"


#ifdef OMCS_OFFSET
#include "index/latches/OMCSOffset.h"
#endif


namespace noname {
namespace benchmark {
namespace tpcc {

TPCCBench::TPCCBench(uint32_t threads, uint32_t seconds,
                     uint64_t number_of_operations,
                     benchmark::ProfilingOption profiling_option)
    : PerformanceTest(threads, seconds, number_of_operations,
                      noname::benchmark::print_interval_ms, profiling_option) {
  if (g_new_order_fast_id_gen) {
    void *const px =
        memalign(CACHELINE_SIZE,
                 sizeof(util::aligned_padded_elem<std::atomic<uint64_t>>) *
                     NumWarehouses() * NumDistrictsPerWarehouse());
    g_district_ids =
        reinterpret_cast<util::aligned_padded_elem<std::atomic<uint64_t>> *>(
            px);
    for (size_t i = 0; i < NumWarehouses() * NumDistrictsPerWarehouse(); i++)
      new (&g_district_ids[i]) std::atomic<uint64_t>(3001);
  }
}

TPCCBench::~TPCCBench() {
}

void TPCCBench::Load() {
  TPCCLoad tpcc_load{&thread_pool, nthreads};
  tpcc_load.Run();
}

void TPCCBench::WorkerRun(uint32_t thread_id, uint64_t number_of_operations) {

#ifdef OMCS_OFFSET
  offset::reset_tls_qnodes();
#endif

  ++thread_start_barrier;

  while (!bench_start_barrier) {}

  while (!shutdown) {
    auto [committed, index] = TxnRun(thread_id);
    if (committed) {
      ++ncommits[thread_id];
    } else {
      ++naborts[thread_id];
      ++part_naborts[index][thread_id];
    }
  }
}

std::pair<bool, int> TPCCBench::TxnRun(uint32_t thread_id) {
  	
  thread_local sql::SQLQuery noname_query(nullptr);
  thread_local NewOrder new_order(db, noname_query, (thread_id % NumWarehouses()) + 1);
  thread_local Payment payment(db, noname_query, (thread_id % NumWarehouses()) + 1);
  thread_local StockLevel stock_level(db, noname_query, (thread_id % NumWarehouses()) + 1);
  thread_local OrderStatus order_status(db, noname_query, (thread_id % NumWarehouses()) + 1);
  thread_local Delivery delivery(db, noname_query, (thread_id % NumWarehouses()) + 1);

  auto *t = db.NewTransaction();
  noname_query.transaction = t;

  bool success;
  uint tx_type = RandomNumber(r, 1, 100);
  int index = -1; 
  if (tx_type <= tx_distribution[0]) {
    success = new_order.run();
    index = 0;
  } else if (tx_type <= tx_distribution[1]) {
    success = payment.run();
    index = 1;
  } else if (tx_type <= tx_distribution[2]) {
    success = delivery.run();
    index = 2;
  } else if (tx_type <= tx_distribution[3]) {
    success = stock_level.run();
    index = 3;
  } else {
    success = order_status.run();
    index = 4;
  }

  if (success) {
    db.TransactionCommit();
  } else {
    db.TransactionRollback();
  }
  
  return {success, index};
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname

int main(int argc, char **argv) {
  system("mkdir -p /dev/shm/$(whoami)/noname-log && rm -rf /dev/shm/$(whoami)/noname-log/*");

  ::google::InitGoogleLogging(argv[0]);
  namespace po = boost::program_options;
  auto desc = noname::config::default_options();
  desc.add_options()
    ("help,h", "print help message")
    ("threads", po::value<uint64_t>()->required(), "Number of threads")
    ("scale-factor", po::value<uint64_t>(), "Number of warehouses")
    ("duration", po::value<uint64_t>(), "Duration in seconds (time-based mode)")
    ("operations", po::value<uint64_t>(),
      "Number of operations (nops-based mode)")
    ("print-interval-ms", po::value<uint64_t>()
                            ->default_value(1000), "Print throughput in microsecond intervals")
    ("profiling", po::value<noname::benchmark::ProfilingOption>()
                    ->default_value(noname::benchmark::ProfilingOption::None, "none"),
        "Profiling mode: none, perf-stat, perf-record, vtune. Default: none")
  ;  // NOLINT(whitespace/semicolon)

  using noname::config::FLAG_STORE;
  try {
    noname::config::parse_args(desc, argc, argv);
  } catch (const std::exception &e) {
    if (!FLAG_STORE.count("help")) {
      std::cerr << "Error: " << e.what() << std::endl;
    }
    std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
    std::cout << desc << std::endl;
    std::exit(EXIT_FAILURE);
  }
  uint64_t duration = 0;
  uint64_t operations = 0;
  if (!FLAG_STORE.count("duration") && !FLAG_STORE.count("operations") ||
      FLAG_STORE.count("duration") && FLAG_STORE.count("operations")) {
    std::cerr << "Error: Either \"--duration\" or \"--operations\" "
                 "should be present"
              << std::endl;
    std::exit(EXIT_FAILURE);
  } else if (FLAG_STORE.count("duration")) {
    duration = FLAG_STORE["duration"].as<uint64_t>();
    std::cout << "Time-based mode" << std::endl;
  } else if (FLAG_STORE.count("operations")) {
    operations = FLAG_STORE["operations"].as<uint64_t>();
    std::cout << "NOps-based mode" << std::endl;
  }

  auto threads = FLAG_STORE["threads"].as<uint64_t>();
  noname::benchmark::tpcc::scale_factor = FLAG_STORE["scale-factor"].as<uint64_t>();
  noname::benchmark::print_interval_ms = FLAG_STORE["print-interval-ms"].as<uint64_t>();
  auto profiling = FLAG_STORE["profiling"].as<noname::benchmark::ProfilingOption>();

  noname::benchmark::tpcc::TPCCBench test(threads, duration, operations, profiling);
  test.Load();
  test.Run();

  return 0;
}

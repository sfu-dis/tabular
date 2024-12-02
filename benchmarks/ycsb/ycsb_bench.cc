/*
 *
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

#include <vector>
#include <filesystem>

#include <boost/program_options/value_semantic.hpp>

#include "util.h"
#include "ycsb_bench.h"
#include "ycsb_worker.h"
#include "config/config.h"

#ifdef OMCS_OFFSET
#include "index/latches/OMCSOffset.h"
#endif

namespace noname {
namespace benchmark {
namespace ycsb {

WorkloadPercentage::WorkloadPercentage() {}

WorkloadPercentage::WorkloadPercentage(char description,
                                       int16_t insert_percent,
                                       int16_t read_percent,
                                       int16_t update_percent,
                                       int16_t scan_percent,
                                       int16_t rmw_percent)
    : description_(description) {
  insert_percent_ = insert_percent;
  read_percent_ = read_percent + insert_percent_;
  update_percent_ = update_percent + read_percent_;
  scan_percent_ = scan_percent + update_percent_;
  rmw_percent_ = rmw_percent + scan_percent_;
}

template <typename IndexType, typename TableType>
YCSBBench<IndexType, TableType>::YCSBBench(YCSBConfig config, TableType *table,
                                           IndexType *index)
    : PerformanceTest(config.num_of_threads_, config.num_of_seconds_,
                      config.num_of_operations_, config.print_interval_ms_,
                      config.profiling_option_),
      table_(table), index_(index), config_(config),
      next_insert_key_(config.option_.initial_table_size) {
  workload_percentage_ =
      noname::benchmark::ycsb::YCSBConfig::preset_workloads.at(
          config_.workload_);
  #if defined(ARTOLC) || defined(ART_LC)
  records_ =
      new ARTWrapper<ART_OLC::Tree, uint64_t,
                        uint64_t>::Record[config_.option_.initial_table_size];
  #endif
  #ifdef ART_TABULAR
  records_ =
      new ARTWrapper<noname::tabular::art::Tree, uint64_t,
                        uint64_t>::Record[config_.option_.initial_table_size];
  #endif
}

template <typename IndexType, typename TableType>
void YCSBBench<IndexType, TableType>::Load() {
  // The number of record to the inserted per loader should be (table size) / (#loaders)
  auto ycsb_load = YCSBLoad<IndexType, TableType>(
      #if defined(ARTOLC) || defined(ART_LC) || defined(ART_TABULAR)
      reinterpret_cast<TableType *>(records_),
      #else
      table_,
      #endif
      index_, &thread_pool, config_.num_of_loaders_,
      config_.option_.initial_table_size);
  auto start = util::GetCurrentUSec();
  ycsb_load.Run();
  auto end = util::GetCurrentUSec();
  std::cout << "Load time: " << end - start << " us" << std::endl;
  if (config_.verify_) {
    ycsb_load.Verify();
  }
}

template <typename IndexType, typename TableType>
void YCSBBench<IndexType, TableType>::WorkerRun(uint32_t thread_id, uint64_t number_of_operations) {
  // 1. Mark self as ready using the thread start barrier
  // 2. Wait (busy-spin) for other threads to become ready using the benchmark start barrier
  // 3. Do real work until [shutdown] is set, by repeating the [read_update_pct] transaction.
  // 4. For each committed/aborted transaction, increment the
  //    [ncommits]/[naborts] stats using [thread_id]

#ifdef OMCS_OFFSET
  offset::reset_tls_qnodes();
#endif

  auto worker = YCSBWorkerLocalData();

  // Magic number for seeding from ERMIA:
  // https://github.com/sfu-dis/ermia/blob/c8784ef76becf141b703feb3a0321f683d135411/benchmarks/ycsb.cc#L59
  uint64_t seed = 1237 + thread_id;
  worker.fast_random_.set_seed(seed);
  if (config_.option_.zipfian) {
    worker.zipfian_random_.init(config_.option_.initial_table_size,
                         config_.option_.zipfian_theta, seed);
  } else {
    worker.uniform_random_.set_current_seed(seed);
  }

  if (number_of_operations) {
    ++thread_start_barrier;
    // 2. Wait for others to become ready
    while (!bench_start_barrier) {}
    // 3. Do real work until `number_of_operations` finished and notify main thread
    std::cout << "[worker] nops-based run: " << number_of_operations << std::endl;
    for (uint64_t i = 0; i < number_of_operations; i++) {
      auto committed = RunNextWorkload(&worker);

      if (committed) {
        ncommits[thread_id] += config_.option_.num_of_records_per_transaction_;
      } else {
        CHECK(false) << "index-only benchmark never aborts";
        ++naborts[thread_id];
      }
    }
    {
      std::lock_guard lk(finish_mx);
      num_of_finished_threads += 1;
    }
    finish_cv.notify_one();
  } else {
    ++thread_start_barrier;
    // 2. Wait for others to become ready
    while (!bench_start_barrier) {}
    // 3. Do real work until shutdown
    while (!shutdown) {
      auto committed = RunNextWorkload(&worker);

      if (committed) {
        ncommits[thread_id] += config_.option_.num_of_records_per_transaction_;
      } else {
        CHECK(false) << "index-only benchmark never aborts";
        ++naborts[thread_id];
      }
    }
  }
  num_system_aborts[thread_id] += worker.num_aborts;
  num_system_read_aborts[thread_id] += worker.num_read_aborts;
  num_system_update_aborts[thread_id] += worker.num_update_aborts;
  num_system_insert_aborts[thread_id] += worker.num_insert_aborts;
  num_system_scan_aborts[thread_id] += worker.num_scan_aborts;
}

// TODO(Yue): Add random workload for other txn_operations
template <typename IndexType, typename TableType>
bool YCSBBench<IndexType, TableType>::RunNextWorkload(YCSBWorkerLocalData *worker) {
  double d = worker->fast_random_.next_uniform() * 100;
  if (d < workload_percentage_.insert_percent_) {
    return TxnInsert(worker);
  } else if (d < workload_percentage_.read_percent_) {
    return TxnRead(worker);
  } else if (d < workload_percentage_.update_percent_) {
    return TxnUpdate(worker);
  } else if (d < workload_percentage_.scan_percent_) {
    return TxnScan(worker);
  } else {
    return TxnRMW(worker);
  }
}

template <typename IndexType, typename TableType>
uint64_t YCSBBench<IndexType, TableType>::GenerateRandomKey(YCSBWorkerLocalData *worker) {
  uint64_t r = 0;
  if (config_.option_.zipfian) {
    r = worker->zipfian_random_.next() + 1;
  } else {
    r = worker->uniform_random_.uniform_within(
        1, next_insert_key_.load(std::memory_order::acquire));
  }
  return r;
}

template <typename IndexType, typename TableType>
YCSBBench<IndexType, TableType>::ScanRange
YCSBBench<IndexType, TableType>::GenerateRandomScanRange(YCSBWorkerLocalData *worker) {
  uint64_t start = 0;
  uint64_t length = 0;
  if (config_.option_.zipfian) {
    start = worker->zipfian_random_.next();
  } else {
    start = worker->uniform_random_.uniform_within(
        0, next_insert_key_.load(std::memory_order::acquire) - 1);
  }
  length = worker->uniform_random_.uniform_within(1, MAX_SCAN_RANGE_LENGTH);

  return YCSBBench::ScanRange{start, length};
}

template <typename IndexType, typename TableType>
bool YCSBBench<IndexType, TableType>::TxnInsert(YCSBWorkerLocalData *worker) {
  auto num_of_records = config_.option_.num_of_records_per_transaction_;
  bool ok;
  for (int n = 0; n < num_of_records; ++n) {
    auto insert_key = next_insert_key_.fetch_add(1, std::memory_order::acq_rel);
    #if defined(BTREE_INLINE_TABULAR)
    auto result = IndexType::Result::TRANSACTION_FAILED;
    while (result == IndexType::Result::TRANSACTION_FAILED) {
      #if defined(MATERIALIZED_INSERT)
      result = index_->insert_internal(insert_key, insert_key);
      #else
      result = index_->insert_internal_callback(insert_key, insert_key);
      #endif
      worker->num_aborts += 1;
      worker->num_insert_aborts += 1;
    }
    worker->num_aborts -= 1;
    worker->num_insert_aborts -= 1;
    ok = result == IndexType::Result::SUCCEED;
    #elif defined(BTREE_TABULAR)
    auto result = IndexType::Result::TRANSACTION_FAILED;
    while (result == IndexType::Result::TRANSACTION_FAILED) {
      result = index_->insert_internal(insert_key, insert_key);
      worker->num_aborts += 1;
      worker->num_insert_aborts += 1;
    }
    worker->num_aborts -= 1;
    worker->num_insert_aborts -= 1;
    ok = result == IndexType::Result::SUCCEED;
    #elif defined(HASHTABLE_INLINE_TABULAR)
    std::optional<bool> ret;
    auto hash = index::Hash(insert_key);
    do {
      ret = index_->insert_internal_callback(hash, insert_key, insert_key);
      worker->num_aborts += 1;
      worker->num_insert_aborts += 1;
    } while (!ret);
    worker->num_aborts -= 1;
    worker->num_insert_aborts -= 1;
    ok = ret.value();
    #elif defined(HASHTABLE_MVCC_TABULAR)
    std::optional<bool> ret;
    auto hash = index::Hash(insert_key);
    do {
      ret = index_->insert_internal(hash, insert_key, insert_key);
      worker->num_aborts += 1;
      worker->num_insert_aborts += 1;
    } while (!ret);
    worker->num_aborts -= 1;
    worker->num_insert_aborts -= 1;
    ok = ret.value();
    #else
    ok = index_->insert(insert_key, insert_key);
    #endif
    CHECK(ok);
  }
  return true;
}

template <typename IndexType, typename TableType>
bool YCSBBench<IndexType, TableType>::TxnRead(YCSBWorkerLocalData *worker) {
  volatile uint64_t result;
  bool ok;
  auto num_of_records = config_.option_.num_of_records_per_transaction_;
  auto num_of_repeats = config_.option_.num_of_repeats_per_record_;
  for (int n = 0; n < num_of_records; ++n) {
    auto random_key = GenerateRandomKey(worker);
    for (int i = 0; i < num_of_repeats; ++i) {
      uint64_t oid = table::kInvalidOID;
      #if defined(BTREE_INLINE_TABULAR)
      std::optional<bool> ret;
      do {
        ret = index_->lookup_internal(random_key, oid);
        worker->num_aborts += 1;
        worker->num_read_aborts += 1;
      } while (!ret);
      worker->num_aborts -= 1;
      worker->num_read_aborts -= 1;
      ok = ret.value();
      #elif defined(HASHTABLE_INLINE_TABULAR) || defined(HASHTABLE_MVCC_TABULAR)
      std::optional<bool> ret;
      auto hash = index::Hash(random_key);
      do {
        ret = index_->lookup_internal(hash, random_key, oid);
        worker->num_aborts += 1;
        worker->num_read_aborts += 1;
      } while (!ret);
      worker->num_aborts -= 1;
      worker->num_read_aborts -= 1;
      ok = ret.value();
      #elif defined(ARTOLC) || defined(ART_LC) || defined(ART_TABULAR)
      auto bkey = __builtin_bswap64(random_key);
      ok = index_->lookup(bkey, oid);
      #else  // Inherently no abort for BTREE_TABULAR variant
      ok = index_->lookup(random_key, oid);
      #endif
      CHECK(ok || random_key > config_.option_.initial_table_size)
          << "lookup for key " << random_key << " failed";
      result = oid;  // Prevent index lookup from being optimized out.
    }
  }
  return true;
}

template <typename IndexType, typename TableType>
bool YCSBBench<IndexType, TableType>::TxnUpdate(YCSBWorkerLocalData *worker) {
  auto num_of_records = config_.option_.num_of_records_per_transaction_;
  auto num_of_repeats = config_.option_.num_of_repeats_per_record_;
  for (int n = 0; n < num_of_records; ++n) {
    uint64_t random_key = GenerateRandomKey(worker);
    for (int i = 0; i < num_of_repeats; ++i) {
      bool ok;
      #if defined(BTREE_INLINE_TABULAR)
      std::optional<bool> ret;
      do {
        #if defined(MATERIALIZED_UPDATE)
        ret = index_->update_internal(random_key, random_key);
        #else
        ret = index_->update_internal_callback(random_key, random_key);
        #endif
        worker->num_aborts += 1;
        worker->num_update_aborts += 1;
      } while (!ret);
      worker->num_aborts -= 1;
      worker->num_update_aborts -= 1;
      ok = ret.value();
      #elif defined(BTREE_TABULAR)
      auto result = IndexType::Result::TRANSACTION_FAILED;
      while (result == IndexType::Result::TRANSACTION_FAILED) {
        result = index_->update_internal(random_key, random_key);
        worker->num_aborts += 1;
        worker->num_update_aborts += 1;
      }
      worker->num_aborts -= 1;
      worker->num_update_aborts -= 1;
      ok = result == IndexType::Result::SUCCEED;
      #elif defined(HASHTABLE_INLINE_TABULAR)  || defined(HASHTABLE_MVCC_TABULAR)
      std::optional<bool> ret;
      auto hash = index::Hash(random_key);
      do {
        ret = index_->update_internal(hash, random_key, random_key);
        worker->num_aborts += 1;
        worker->num_update_aborts += 1;
      } while (!ret);
      worker->num_aborts -= 1;
      worker->num_update_aborts -= 1;
      ok =  ret.value();
      #elif defined(ARTOLC) || defined(ART_LC) || defined(ART_TABULAR)
      auto bkey = __builtin_bswap64(random_key);
      ok = index_->update(bkey, reinterpret_cast<uint64_t>(&records_[random_key]));
      #else
      ok = index_->update(random_key, random_key);
      #endif
      CHECK(ok) << "Failed to update key: " << random_key;
    }
  }
  return true;
}

template <typename IndexType, typename TableType>
bool YCSBBench<IndexType, TableType>::TxnScan(YCSBWorkerLocalData *worker) {
  table::OID scan_oids[MAX_SCAN_RANGE_LENGTH];
  volatile uint64_t count;
  for (int i = 0; i < config_.option_.num_of_repeats_per_record_; ++i) {
    ScanRange range = GenerateRandomScanRange(worker);
    #if defined(BTREE_INLINE_TABULAR)
    std::optional<uint64_t> ret;
    do {
      ret = index_->scan_internal(range.start, range.length, scan_oids);
      worker->num_aborts += 1;
      worker->num_scan_aborts += 1;
    } while (!ret);
    worker->num_aborts -= 1;
    worker->num_scan_aborts -= 1;
    count = ret.value();
    #else
    count = index_->scan(range.start, range.length, scan_oids);
    #endif
    for (size_t i = 0; i < count; i++) {
      auto matched = scan_oids[i] == (range.start + i);
      CHECK(matched || (range.start + i) >= config_.option_.initial_table_size)
          << "Scan failed: scan_oids[i] != range.start + i " << "("
          << scan_oids[i] << " vs " << range.start + i << ")";
    }
  }
  return true;
}

template <typename IndexType, typename TableType>
bool YCSBBench<IndexType, TableType>::TxnRMW(YCSBWorkerLocalData *worker) {
  std::cout << "TXN RMW HAVE NOT BEEN IMPLEMENTED" << std::endl;
  return false;
}

#ifdef BTREE_OPTIQL
template class YCSBBench<btreeolc::BTreeOMCSLeaf<uint64_t, uint64_t>,
                         table::IndirectionArrayTable>;
#endif
#ifdef BTREE_OLC
template class YCSBBench<btreeolc::BTreeOLC<uint64_t, uint64_t>,
                         table::IndirectionArrayTable>;
#endif
#ifdef BTREE_LC_STDRW
template class YCSBBench<btreeolc::BTreeLC<uint64_t, uint64_t>,
                         table::IndirectionArrayTable>;
#endif
#ifdef BTREE_LC_TBBRW
template class YCSBBench<btreeolc::BTreeLC<uint64_t, uint64_t>,
                         table::IndirectionArrayTable>;
#endif
#ifdef BTREE_TABULAR
template class YCSBBench<tabular::BTree<uint64_t, uint64_t>,
                         table::IndirectionArrayTable>;
#endif
#ifdef BTREE_INLINE_TABULAR
template class YCSBBench<tabular::InlineBTree<uint64_t, uint64_t>,
                         table::IndirectionArrayTable>;
#endif

// Custom validator for YCSBWorkloadType option
void validate(boost::any &v, const std::vector<std::string> &xs,  // NOLINT(runtime/references)
              YCSBWorkload *, long) {  // NOLINT(runtime/int)
  po::check_first_occurrence(v);
  std::string s(po::get_single_string(xs));
  if (s.size() != 1) {
    throw po::invalid_option_value(s);
  }
  char ch = s.front();
  if (ch < 'A' || ch > 'H') {
    throw po::invalid_option_value(s);
  }
  v = boost::any(YCSBWorkload { ch - 'A'});
}

// Custom validator for YCSBDistribution option
void validate(boost::any &v, const std::vector<std::string> &xs,  // NOLINT(runtime/references)
              YCSBDistribution *, long) {  // NOLINT(runtime/int)
  po::check_first_occurrence(v);
  std::string s(po::get_single_string(xs));
  if (s == "UNIFORM") {
    v = boost::any(YCSBDistribution::UNIFORM);
  } else if (s == "ZIPFIAN") {
    v = boost::any(YCSBDistribution::ZIPFIAN);
  } else {
    throw po::invalid_option_value(s);
  }
}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

int main(int argc, char** argv) {
  ::google::InitGoogleLogging(argv[0]);

  using namespace noname::benchmark::ycsb;  // NOLINT(build/namespaces)

  namespace po = boost::program_options;
  auto desc = noname::config::default_options();
  desc.add_options()
    ("help,h", "print help message")
    ("workload", po::value<YCSBWorkload>()->required(),
        "YCSB workload:\n"
        "A: 50% Lookup, 50% Update\n"
        "B: 95% Lookup, 5% Update\n"
        "C: 100% Lookup\n"
        "D: 95% Insert, 5% Lookup\n"
        "E: 95% Insert, 5% Scan\n"
        "F: 100% Update\n"
        "G: 80% Lookup, 20% Update\n"
        "H: 20% Lookup, 80% Update\n"
        )
    ("threads", po::value<uint64_t>()->required(), "Number of threads")
    ("loaders", po::value<uint64_t>()->default_value(0),
      "Number of loaders. Default to the same as number of threads")
    ("duration", po::value<uint64_t>(), "Duration in seconds (time-based mode)")
    ("operations", po::value<uint64_t>(),
      "Number of operations (nops-based mode)")
    ("records", po::value<uint64_t>()->required(), "Number of records in the user table")
    ("records-per-txn", po::value<uint64_t>()->default_value(10),
        "Number of records accessed per transaction")
    ("distribution", po::value<YCSBDistribution>()
                    ->default_value(YCSBDistribution::UNIFORM, "UNIFORM"),
      "Distribution of primary keys: UNIFORM or ZIPFIAN")
    ("zipfian-theta", po::value<double>()->default_value(0.99), "Zipfian's theta")
    ("repeats-per-record", po::value<uint64_t>()->default_value(1),
        "Number of repeats of access to each record")
    ("load-only", "Only load the data instead of running workload")
    ("persistent", "Run with persistence guarantee")
    ("verify", "Whether to verify loaded data")
    ("profiling", po::value<noname::benchmark::ProfilingOption>()
                    ->default_value(noname::benchmark::ProfilingOption::None, "none"),
        "Profiling mode: none, perf-stat, perf-record, vtune. Default: none")
    ("print-interval-ms", po::value<uint64_t>()
                            ->default_value(1000), "Print throughput in microsecond intervals")
    ("log-dir", po::value<std::string>()->default_value("") , "Path to logging directory")
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

  auto workload = FLAG_STORE["workload"].as<YCSBWorkload>();
  auto threads = FLAG_STORE["threads"].as<uint64_t>();
  auto loaders = FLAG_STORE["loaders"].as<uint64_t>();
  if (loaders == 0) {
    loaders = threads;
  }
  auto records = FLAG_STORE["records"].as<uint64_t>();
  auto num_records_per_txn = FLAG_STORE["records-per-txn"].as<uint64_t>();
  auto distribution = FLAG_STORE["distribution"].as<YCSBDistribution>();
  auto zipfian_theta = FLAG_STORE["zipfian-theta"].as<double>();
  auto repeats = FLAG_STORE["repeats-per-record"].as<uint64_t>();
  auto profiling = FLAG_STORE["profiling"].as<noname::benchmark::ProfilingOption>();
  auto load_only = !!FLAG_STORE.count("load-only");
  auto is_persistent = !!FLAG_STORE.count("persistent");
  auto verify = !!FLAG_STORE.count("verify");
  auto print_interval_ms = FLAG_STORE["print-interval-ms"].as<uint64_t>();
  auto log_dir = FLAG_STORE["log-dir"].as<std::string>();
  if (is_persistent) {
    if (log_dir.size() == 0) {
      std::cerr
          << "Need to specify logging directory if --persistent is present."
          << std::endl;
      std::exit(EXIT_FAILURE);
    }
    auto path = std::filesystem::path(log_dir);
    if (!std::filesystem::exists(path)) {
      std::cerr << path << " does not exist." << std::endl;
      std::exit(EXIT_FAILURE);
    }
    if (!std::filesystem::is_directory(path)) {
      std::cerr << path << " is not a directory." << std::endl;
      std::exit(EXIT_FAILURE);
    }
  }

  noname::table::config_t config;

  WorkloadOption option(repeats, num_records_per_txn, records, distribution, zipfian_theta);
  YCSBConfig ycsb_config(threads, duration, loaders, operations, workload,
                         records, profiling, verify, print_interval_ms, option);


  constexpr int USERNAME_MAX_SIZE = 32;
  char user[USERNAME_MAX_SIZE];
  getlogin_r(user, USERNAME_MAX_SIZE);

#ifdef BTREE_OPTIQL
  using IndexType = btreeolc::BTreeOMCSLeaf<uint64_t, uint64_t>;
  offset::init_qnodes();
  auto index = IndexType();
#endif
#ifdef BTREE_OLC
  using IndexType = btreeolc::BTreeOLC<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#ifdef BTREE_LC_STDRW
  using IndexType = btreeolc::BTreeLC<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#ifdef BTREE_LC_TBBRW
  using IndexType = btreeolc::BTreeLC<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#ifdef BTREE_TABULAR
  using IndexType = noname::tabular::BTree<uint64_t, uint64_t>;
  auto index = IndexType(config);
#endif
#ifdef BTREE_INLINE_TABULAR
  using IndexType = noname::tabular::InlineBTree<uint64_t, uint64_t>;

  auto index = IndexType(config, is_persistent, log_dir, threads);
#endif
#ifdef HASHTABLE_OPTIMISTIC_LC
  using IndexType = noname::index::HashTable<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#ifdef HASHTABLE_LC_STDRW
  using IndexType = noname::index::HashTableLC<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#ifdef HASHTABLE_LC_TBBRW
  using IndexType = noname::index::HashTableLC<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#ifdef HASHTABLE_INLINE_TABULAR
  using IndexType = noname::tabular::HashTable<uint64_t, uint64_t>;
  auto index = IndexType(config, is_persistent, log_dir, threads);
#endif
#ifdef HASHTABLE_MVCC_TABULAR
  using IndexType = noname::tabular::MVCCHashTable<uint64_t, uint64_t>;
  auto index = IndexType(config);
#endif
#ifdef MASSTREE
  using IndexType = noname::benchmark::ycsb::MasstreeWrapper<uint64_t, uint64_t>;
  auto index = IndexType();
#endif
#if defined(ARTOLC) || defined(ART_LC)
  using IndexType = noname::benchmark::ycsb::ARTWrapper<ART_OLC::Tree, uint64_t, uint64_t>;
  auto tree = new ART_OLC::Tree(IndexType::loadKey, IndexType::removeNode);
  auto index = IndexType(tree);
#endif
#ifdef ART_TABULAR
  using IndexType =
      noname::benchmark::ycsb::ARTWrapper<noname::tabular::art::Tree,
                                             uint64_t, uint64_t>;
  auto tree =
      new noname::tabular::art::Tree(IndexType::loadKey, IndexType::removeNode,
                                     config, is_persistent, log_dir, threads);
  auto index = IndexType(tree);
#endif
#ifdef BPTREE
  #ifndef BPTREE_INTERNAL_BYTES
  #define BPTREE_INTERNAL_BYTES 1024
  #endif
  #ifndef BPTREE_LOG_SIZE
  #define BPTREE_LOG_SIZE 32
  #endif
  #ifndef BPTREE_BLOCK_SIZE
  #define BPTREE_BLOCK_SIZE 32
  #endif
  using IndexType = noname::benchmark::ycsb::BPTreeWrapper<
      uint64_t, uint64_t, BPTREE_INTERNAL_BYTES, BPTREE_LOG_SIZE, BPTREE_BLOCK_SIZE>;
  auto index = IndexType();
#endif

  auto ycsb_bench =
      noname::benchmark::ycsb::YCSBBench<IndexType, noname::table::IndirectionArrayTable>(
          ycsb_config, nullptr, &index);
  std::cout << "Loading..." << std::endl;
  ycsb_bench.Load();
  std::cout << "Loading Done" << std::endl;
  if (load_only) {
    std::exit(EXIT_SUCCESS);
  }
  ycsb_bench.Run();
  return 0;
}

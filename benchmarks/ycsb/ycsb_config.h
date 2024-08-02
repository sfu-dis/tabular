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
#include <string>
#include <unordered_map>

#include "ycsb_workload.h"

namespace noname {
namespace benchmark {
namespace ycsb {

enum class YCSBWorkload {
  A, B, C, D, E, F, G, H
};

enum class YCSBDistribution {
  UNIFORM, ZIPFIAN
};

struct WorkloadOption {
  WorkloadOption(uint64_t num_of_repeats_per_record,
                 uint64_t num_of_records_per_transaction,
                 uint64_t initial_table_size, YCSBDistribution distribution,
                 double_t zipfian_theta)
      : num_of_repeats_per_record_(num_of_repeats_per_record),
        num_of_records_per_transaction_(num_of_records_per_transaction),
        initial_table_size(initial_table_size), zipfian(distribution == YCSBDistribution::ZIPFIAN),
        zipfian_theta(zipfian_theta) {}

  int32_t num_of_repeats_per_record_;
  int32_t num_of_records_per_transaction_;
  int32_t initial_table_size;
  bool zipfian;
  double_t zipfian_theta;
};

struct YCSBConfig {
  YCSBConfig(uint64_t num_of_threads, uint64_t num_of_seconds,
             uint64_t num_of_loaders, uint64_t num_of_operations,
             YCSBWorkload workload, int32_t key_size,
             noname::benchmark::ProfilingOption profiling_option, bool verify,
             uint64_t print_interval_ms, const WorkloadOption &option)
      : num_of_threads_(num_of_threads), num_of_seconds_(num_of_seconds),
        num_of_loaders_(num_of_loaders), num_of_operations_(num_of_operations),
        workload_(workload), key_size_(key_size),
        profiling_option_(profiling_option), verify_(verify),
        print_interval_ms_(print_interval_ms), option_(option) {}

  uint64_t num_of_threads_;
  uint64_t num_of_seconds_;
  uint64_t num_of_loaders_;
  uint64_t num_of_operations_;
  YCSBWorkload workload_;
  bool verify_;
  uint64_t print_interval_ms_;
  uint64_t key_size_;
  noname::benchmark::ProfilingOption profiling_option_;
  WorkloadOption option_;

  static inline std::unordered_map<YCSBWorkload, WorkloadPercentage>
      preset_workloads = {
          // Operation type:                       insert, read, update, scan, rwm
          {YCSBWorkload::A, WorkloadPercentage('A',     0,   50,     50,    0,   0)},
          {YCSBWorkload::B, WorkloadPercentage('B',     0,   95,      5,    0,   0)},
          {YCSBWorkload::C, WorkloadPercentage('C',     0,  100,      0,    0,   0)},
          {YCSBWorkload::D, WorkloadPercentage('D',     5,   95,      0,    0,   0)},
          {YCSBWorkload::E, WorkloadPercentage('E',     5,    0,      0,   95,   0)},
          {YCSBWorkload::F, WorkloadPercentage('F',     0,    0,    100,    0,   0)},
          {YCSBWorkload::G, WorkloadPercentage('G',     0,    80,    20,    0,   0)},
          {YCSBWorkload::H, WorkloadPercentage('H',     0,    20,    80,    0,   0)},
  };
};

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

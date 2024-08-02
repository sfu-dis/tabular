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

#include <vector>
#include <string>

#include "thread/thread.h"

namespace noname {
namespace benchmark {

extern uint64_t print_interval_ms;

enum class ProfilingOption {
  None, PerfStat, PerfRecord, VTune
};

void validate(boost::any &v, const std::vector<std::string> &xs,  // NOLINT(runtime/references)
              ProfilingOption *, long);  // NOLINT(runtime/int)

struct PerformanceTest {
  // Constructor
  // @threads: number of benchmark worker threads
  // @seconds: benchmark duration in seconds
  PerformanceTest(uint32_t threads, uint32_t seconds,
                  uint64_t number_of_operations, uint64_t print_interval_ms,
                  ProfilingOption profiling_option);

  // Destructor
  ~PerformanceTest();

  // Virtual method interface for worker threads
  virtual void WorkerRun(uint32_t thread_id, uint64_t number_of_operations) = 0;

  // Entrance function to run a benchmark
  void Run();

  // One entry per thread to record the number of finished operations
  std::vector<uint64_t> ncommits;
  std::vector<uint64_t> naborts;
  std::vector<uint64_t> num_system_aborts;
  std::vector<uint64_t> num_system_read_aborts;
  std::vector<uint64_t> num_system_update_aborts;
  std::vector<uint64_t> num_system_insert_aborts;
  std::vector<uint64_t> num_system_scan_aborts;
  std::vector<uint64_t> part_naborts[5];

  // Benchmark start barrier: worker threads can only proceed if set to true
  std::atomic<bool> bench_start_barrier;

  // Thread start barrier: a counter of "ready-to-start" threads
  std::atomic<uint32_t> thread_start_barrier;

  // Whether the benchmark should stop and worker threads should shutdown
  std::atomic<bool> shutdown;

  // List of all worker threads
  thread::ThreadPool thread_pool;

  // Number of worker threads
  uint32_t nthreads;

  // Benchmark duration in seconds
  uint32_t seconds;

  // Benchmark stop after finish specific number of operations
  uint64_t number_of_operations;

  // Interval between each real-time throughput report in microseconds
  uint64_t print_interval_ms;

  // Profiling mode for workloads
  ProfilingOption profiling_option;

  // Synchronization variables related to number-of-operation based benchmarking
  std::condition_variable finish_cv;
  std::mutex finish_mx;
  uint32_t num_of_finished_threads = 0;
};

}  // namespace benchmark
}  // namespace noname

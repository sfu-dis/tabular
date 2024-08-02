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

#include <sys/wait.h>

#include <iomanip>
#include <chrono>
#include <vector>
#include <string>

#include <boost/program_options/value_semantic.hpp>

#include "perf.h"
#include "util.h"

namespace noname {
namespace benchmark {

uint64_t print_interval_ms = 0;

PerformanceTest::PerformanceTest(uint32_t threads, uint32_t seconds,
                                 uint64_t number_of_operations,
                                 uint64_t print_interval_ms,
                                 ProfilingOption profiling_option)
    : bench_start_barrier(false), thread_start_barrier(0), shutdown(false),
      nthreads(threads), seconds(seconds),
      number_of_operations(number_of_operations),
      print_interval_ms(print_interval_ms), profiling_option(profiling_option),
      thread_pool(thread::config_t(true)) {}

PerformanceTest::~PerformanceTest() {
  // Destructor - nothing needed here
}

void PerformanceTest::Run() {
  // 1. Start threads and initialize the commit/abort stats for each thread to 0
  std::vector<thread::Thread*> threads;
  uint64_t nops = number_of_operations / nthreads;
  uint64_t last_nops = number_of_operations - nops * (nthreads - 1);
  for (uint32_t i = 0; i < nthreads; ++i) {
    ncommits.emplace_back(0);
    naborts.emplace_back(0);
    num_system_aborts.emplace_back(0);
    num_system_read_aborts.emplace_back(0);
    num_system_update_aborts.emplace_back(0);
    num_system_insert_aborts.emplace_back(0);
    num_system_scan_aborts.emplace_back(0);
    for (int t = 0; t< 5 ; ++t) {
      part_naborts[t].emplace_back(0);
    }
    thread::Thread* t = thread_pool.GetThread(false);
    if (i == nthreads - 1) {
      t->StartTask([this, i, last_nops] { WorkerRun(i, last_nops); });
    } else {
      t->StartTask([this, i, nops] { WorkerRun(i, nops); });
    }
    threads.push_back(t);
  }

  // 2. Wait for all threads to become ready
  while (thread_start_barrier != nthreads) {}

  pid_t vtune_pid;
  pid_t perf_pid;
  pid_t pid;

  std::stringstream benchmark_pid;
  benchmark_pid << getpid();

  switch (profiling_option) {
  case ProfilingOption::VTune:
    std::cerr << "start vtune profiler..." << std::endl;

    pid = fork();
    // Launch profiler
    if (pid == 0) {
      // disable mem bandwidth measurement for any analysis target need ram
      // bandwidth measurement (performance-snapshot, uarch-exploration) because
      // it may affect the running benchmark
      exit(execl("/opt/intel/oneapi/vtune/latest/bin64/vtune", "vtune",
                 "-collect", "uarch-exploration",
                 "-knob", "enable-stack-collection=true", "-knob",
                 "dram-bandwidth-limits=false", "-target-pid",
                 benchmark_pid.str().c_str(), nullptr));
    } else {
      vtune_pid = pid;
    }
    break;

  case ProfilingOption::PerfStat:
    std::cerr << "start perf stat..." << std::endl;

    pid = fork();
    // Launch profiler
    if (pid == 0) {
      exit(execl(
          "/usr/bin/perf", "perf", "stat", "-B", "-e",
          "cache-references,cache-misses,cycles:P,instructions:P,branches:P,faults:P",
          "-p", benchmark_pid.str().c_str(), nullptr));
    } else {
      perf_pid = pid;
    }
    break;

  case ProfilingOption::PerfRecord:
    std::cerr << "start perf record..." << std::endl;

    pid = fork();
    // Launch profiler
    if (pid == 0) {
      /*
       exit(execl(
          "/usr/bin/perf", "perf", "record", "-F", "99", "-e",
          "cache-references,cache-misses,cycles:P,instructions:P,branches:P,faults:P",
          "-p", benchmark_pid.str().c_str(), nullptr));
	  */
       exit(execl(
          "/usr/bin/perf", "perf", "record", "-F", "99", "-g", "-e", 
	 "cycles:P", "-p", benchmark_pid.str().c_str(), nullptr));
    } else {
      perf_pid = pid;
    }
    break;

  default:
    break;
  }

  // 3. Allow everyone to go ahead
  auto start = util::GetCurrentUSec();
  bench_start_barrier = true;

  // 4. Sleep for the benchmark duration
  std::cout << "Benchmarking..." << std::endl;
  if (number_of_operations) {
    // number-of-operation-based
    // TODO(ziyi)
    std::cout << "nops-based run: " << number_of_operations << std::endl;
    {
      std::unique_lock lk(finish_mx);
      finish_cv.wait(lk, [&] { return num_of_finished_threads == nthreads; });
    }
  } else {
    // time-based
    if (print_interval_ms) {
      // Taken from bench.cc in ERMIA
      uint64_t slept = 0;
      uint64_t last_commits = 0, last_aborts = 0;
      uint64_t sleep_time = print_interval_ms * 1000;
      auto gather_stats = [&]() {
        usleep(sleep_time);
        uint64_t sec_commits = 0, sec_aborts = 0;
        for (uint32_t i = 0; i < ncommits.size(); ++i) {
          sec_commits += ncommits[i];
          sec_aborts += naborts[i];
        }
        sec_commits -= last_commits;
        sec_aborts -= last_aborts;
        last_commits += sec_commits;
        last_aborts += sec_aborts;
        printf("%.2f,%lu,%lu\n", static_cast<float>(slept + sleep_time) / 1000000,
               sec_commits, sec_aborts);
        slept += sleep_time;
      };

      while (slept < seconds * 1000000) {
        gather_stats();
      }
    } else {
      sleep(seconds);
    }
    shutdown = true;
  }

  // 6. Wait for all workers to finish
  for (const auto& thread : threads) {
    thread->Join();
  }
  auto end = util::GetCurrentUSec();

  switch (profiling_option) {
  case ProfilingOption::VTune:
    {
      // stop vtune profiler
      auto dir_name =
        util::Execute("ls -1 | grep -E 'r[0-9]{3}' | tail -1");  // find latest vtune directory name
      util::StripNewline(dir_name);
      std::cerr << "stop vtune(" << dir_name << ")..." << std::endl;
      const auto CMD_MAX_SIZE = 128;
      char stop_vtune_cmd[CMD_MAX_SIZE];
      auto ret = snprintf(stop_vtune_cmd, CMD_MAX_SIZE,
                          "/opt/intel/oneapi/vtune/latest/bin64/vtune -r %s -command stop",
                          dir_name.c_str());
      CHECK_GT(ret, 0);
      std::cerr << "stop vtune command: " << stop_vtune_cmd << std::endl;
      system(stop_vtune_cmd);
      waitpid(vtune_pid, nullptr, 0);
    }
    break;

  case ProfilingOption::PerfStat:
  case ProfilingOption::PerfRecord:
    std::cerr << "stop perf..." << std::endl;
    kill(perf_pid, SIGINT);
    waitpid(perf_pid, nullptr, 0);
    break;

  default:
    break;
  }

  for (const auto& thread : threads) {
    thread_pool.PutThread(thread);
  }
  // 7. Dump stats
  auto elapsed_sec = (end - start) / 1000000.0;
  std::cout << "=====================" << std::endl;
  std::cout << "running time: " << elapsed_sec << std::endl;

  std::cout << std::fixed;
  std::cout << std::setprecision(3);

  uint32_t total_commits = 0;
  uint32_t total_aborts = 0;
  uint32_t total_system_aborts = 0;
  uint32_t total_system_read_aborts = 0;
  uint32_t total_system_update_aborts = 0;
  uint32_t total_system_insert_aborts = 0;
  uint32_t total_system_scan_aborts = 0;

  for (uint32_t i = 0; i < ncommits.size(); ++i) {
    std::cout << i << "," << ncommits[i] / elapsed_sec
              << "," << naborts[i] / elapsed_sec
              << "," << num_system_aborts[i] / elapsed_sec
              << "," << num_system_read_aborts[i] / elapsed_sec
              << "," << num_system_update_aborts[i] / elapsed_sec
              << "," << num_system_insert_aborts[i] / elapsed_sec
              << "," << num_system_scan_aborts[i] / elapsed_sec;
    total_commits += ncommits[i];
    total_aborts += naborts[i];
    total_system_aborts += num_system_aborts[i];
    total_system_read_aborts += num_system_read_aborts[i];
    total_system_update_aborts += num_system_update_aborts[i];
    total_system_insert_aborts += num_system_insert_aborts[i];
    total_system_scan_aborts += num_system_scan_aborts[i];
    std::cout << std::endl;
  }

  std::cout << "Thread,Commits/s,Aborts/s"
               ",SystemAborts/s"
               ",SystemInsertAborts/s"
               ",SystemReadAborts/s"
               ",SystemUpdateAborts/s"
               ",SystemScanAborts/s"
            << std::endl;
  std::cout << "---------------------" << std::endl;
  std::cout << "All," << total_commits / elapsed_sec << ","
            << total_aborts / elapsed_sec
            << "," << total_system_aborts / elapsed_sec
            << "," << total_system_insert_aborts / elapsed_sec
            << "," << total_system_read_aborts / elapsed_sec
            << "," << total_system_update_aborts / elapsed_sec
            << "," << total_system_scan_aborts / elapsed_sec
            << std::endl;
  const std::vector<std::string> txn = {"new_order ",
	  "payement " , "delivery " , "stock_level " ," order_status "};
  /*for(int t = 0; t<5;++t) {
    uint32_t total_aborts = 0;
    std::cout << "Abort stats for txn : " << txn[t] << std::endl; 
    for (uint32_t i = 0; i < ncommits.size(); ++i) {
      // std::cout << i << "," << part_naborts[t][i] / elapsed_sec;
      total_aborts += part_naborts[t][i];
    }
    std::cout << "All," << total_aborts / elapsed_sec << std::endl;
  }*/
}

// Custom validator for ProfilingOption option
void validate(boost::any &v, const std::vector<std::string> &xs,  // NOLINT(runtime/references)
              ProfilingOption *, long) {  // NOLINT(runtime/int)
  boost::program_options::check_first_occurrence(v);
  std::string s(boost::program_options::get_single_string(xs));
  if (s == "perf-stat") {
    v = boost::any(ProfilingOption::PerfStat);
  } else if (s == "perf-record") {
    v = boost::any(ProfilingOption::PerfRecord);
  } else if (s == "vtune") {
    v = boost::any(ProfilingOption::VTune);
  } else if (s == "none") {
    v = boost::any(ProfilingOption::None);
  } else {
    throw boost::program_options::invalid_option_value(s);
  }
}

}  // namespace benchmark
}  // namespace noname

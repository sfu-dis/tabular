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

#include <tbb/tbb.h>

#include <cstdlib>
#include <chrono>
#include <iostream>

#include "tabular/btree.h"

// TODO(ziyi) change it to gtest
// Example usage:
//   ./tests/tabular/tabular_btree_multithread_test 10000 2 4

void multithreaded(int argc, char **argv) {
  std::cout << "multi threaded:" << std::endl;

  uint64_t n = std::atoll(argv[1]);
  uint64_t *keys = new uint64_t[n];

  // Generate keys
  for (uint64_t i = 0; i < n; i++)
    // dense, sorted
    keys[i] = i + 1;
  if (std::atoi(argv[2]) == 1) {
    // dense, random
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys, keys + n, g);
  }
  if (std::atoi(argv[2]) == 2) {
    // "pseudo-sparse"
    for (uint64_t i = 0; i < n; i++) {
      keys[i] =
          (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());
    }
  }

  int num_threads = (argc == 3) ? -1 : std::atoi(argv[3]);
  if (num_threads < 1) {
    num_threads = tbb::info::default_concurrency();
  }
  tbb::global_control global_limit(tbb::global_control::max_allowed_parallelism, num_threads);

  std::printf("operation,n,ops/s\n");
  constexpr uint64_t kPageSize = 64 * 1024;
  noname::table::config_t config(
      false,
      false,  // no hugepages
      align_up(n * sizeof(uint64_t), kPageSize));  // capacity
  noname::tabular::BTree<uint64_t, uint64_t> tree(config);
  // Build tree
  {
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          auto ival = __builtin_bswap64(keys[i]);
                          auto ok = tree.insert(keys[i], ival);
                          if (!ok) {
                            std::cout << "key insertion failed: " << keys[i] << std::endl;
                            throw;
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    std::printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(
        tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
          for (uint64_t i = range.begin(); i != range.end(); i++) {
            uint64_t val = 0;
            bool found = tree.lookup(keys[i], val);
            if (!found) {
              std::cout << "key not found: " << keys[i] << std::endl;
              throw;
            }
            auto ival = __builtin_bswap64(keys[i]);
            if (val != ival) {
              std::cout << "wrong key read: " << val << " expected:" << ival << std::endl;
              throw;
            }
          }
        });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    std::printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Update
    auto starttime = std::chrono::system_clock::now();

    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                      [&](const tbb::blocked_range<uint64_t> &range) {
                        for (uint64_t i = range.begin(); i != range.end(); i++) {
                          auto ok = tree.update(keys[i], keys[i]);
                          if (!ok) {
                            std::cout << "key update failed: " << keys[i] << std::endl;
                            throw;
                          }
                        }
                      });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    std::printf("update,%ld,%f\n", n, (n * 1.0) / duration.count());
  }

  {
    // Lookup
    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(
        tbb::blocked_range<uint64_t>(0, n),
        [&](const tbb::blocked_range<uint64_t> &range) {
          for (uint64_t i = range.begin(); i != range.end(); i++) {
            uint64_t val = 0;
            bool found = tree.lookup(keys[i], val);
            if (!found) {
              std::cout << "key not found: " << keys[i] << std::endl;
              throw;
            }
            auto ival = keys[i];
            if (val != ival) {
              std::cout << "wrong key read: " << val << " expected:" << ival << std::endl;
              throw;
            }
          }
        });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - starttime);
    std::printf("lookup,%ld,%f\n", n, (n * 1.0) / duration.count());
  }
  delete[] keys;
}

int main(int argc, char **argv) {
  if (argc != 3 && argc != 4) {
    std::printf(
        "usage: %s n 0|1|2 <threads>\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse "
        "keys\n",
        argv[0]);
    return 1;
  }

  multithreaded(argc, argv);

  return 0;
}

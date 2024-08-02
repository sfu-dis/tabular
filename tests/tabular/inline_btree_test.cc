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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>

#include "tabular/inline_btree.h"

TEST(InlineBTreeTest, SingleThreadInsertRead) {
  noname::table::config_t config(false,
                                 false,    // no hugepages
                                 1000000,  // capacity
                                 500000);  // initial size

  constexpr int MAX_NUM_KEYS = 1024;
  int NUM_KEYS = noname::tabular::BTreeLeafNode<uint64_t, uint64_t>::maxEntries;
  for (; NUM_KEYS < MAX_NUM_KEYS; NUM_KEYS++) {
    noname::tabular::InlineBTree<uint64_t, uint64_t> tree(config, false);

    bool ok;
    for (uint64_t k = 0; k < NUM_KEYS; k++) {
      ok = tree.insert(k, k);
      ASSERT_TRUE(ok);
    }

    for (uint64_t k = 0; k < NUM_KEYS; k++) {
      uint64_t result;
      ok = tree.lookup(k, result);
      ASSERT_TRUE(ok);
      ASSERT_EQ(result, k);
    }
  }
}

TEST(InlineBTreeTest, SingleThreadedScan) {
  noname::table::config_t config(false,
                                 false,    // no hugepages
                                 1000000,  // capacity
                                 500000);  // initial size

  constexpr int NUM_KEYS = 1024;
  noname::tabular::InlineBTree<uint64_t, uint64_t> tree(config, false);
  bool ok;
  for (uint64_t k = 0; k < NUM_KEYS; k++) {
    ok = tree.insert(k, k);
    ASSERT_TRUE(ok);
  }

  auto result = new uint64_t[NUM_KEYS];
  auto count = tree.scan(0, NUM_KEYS, result);
  CHECK_EQ(count, NUM_KEYS);
  for (auto i = 0; i < NUM_KEYS; i++) {
    CHECK_EQ(result[i], i);
  }
}

// TODO(ziyi) add occ test cases for delta overflow
TEST(InlineBTreeTest, DISABLED_SingleThreadLargeTreeInsertRead) {
  noname::table::config_t config(false,
                                 false,    // no hugepages
                                 1024 * 1024 * 1024,  // capacity
                                 1024 * 1024 * 1024);  // initial size

  constexpr int NUM_KEYS = 287674 + 1;

  noname::tabular::InlineBTree<uint64_t, uint64_t> tree(config, false);

  bool ok;
  for (uint64_t k = 0; k < NUM_KEYS; k++) {
    ok = tree.insert(k, k);
    ASSERT_TRUE(ok);
  }

  for (uint64_t k = 0; k < NUM_KEYS; k++) {
    uint64_t result;
    ok = tree.lookup(k, result);
    ASSERT_TRUE(ok);
    ASSERT_EQ(result, k);
  }
}

TEST(InlineBTreeTest, MultiThreadInsertRead) {
  noname::table::config_t config(false,
                                 false,    // no hugepages
                                 1000000,  // capacity
                                 500000);  // initial size

  constexpr int NUM_THREADS = 4;

  constexpr int MAX_NUM_KEYS = 1024;
  int NUM_KEYS = noname::tabular::BTreeLeafNode<uint64_t, uint64_t>::maxEntries;
  for (; NUM_KEYS < MAX_NUM_KEYS; NUM_KEYS++) {
    noname::tabular::InlineBTree<uint64_t, uint64_t> tree(config, false);
    auto insert_key_range = [&tree](uint64_t start, uint64_t end) {
      for (uint64_t k = start; k < end; k++) {
        auto ok = tree.insert(k, k);
        ASSERT_TRUE(ok);
      }
    };
    auto lookup_key_range = [&tree](uint64_t start, uint64_t end) {
      for (uint64_t k = start; k < end; k++) {
        uint64_t result;
        auto ok = tree.lookup(k, result);
        ASSERT_TRUE(ok);
        ASSERT_EQ(result, k);
      }
    };
    auto step = NUM_KEYS / NUM_THREADS;
    std::vector<std::thread> threads;
    // Insert
    for (auto i = 0; i < NUM_THREADS; i++) {
      auto start = i * step;
      auto end = start + step;
      if (i == NUM_THREADS - 1) {
        end = NUM_KEYS;
      }
      threads.emplace_back(insert_key_range, start, end);
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();
    // Lookup
    for (auto i = 0; i < NUM_THREADS; i++) {
      auto start = i * step;
      auto end = start + step;
      if (i == NUM_THREADS - 1) {
        end = NUM_KEYS;
      }
      threads.emplace_back(lookup_key_range, start, end);
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();
  }
}

TEST(InlineBTreeTest, MultipleThreadUpdate) {
  noname::table::config_t config(false,
                                 false,    // no hugepages
                                 1000000,  // capacity
                                 500000);  // initial size

  constexpr int NUM_THREADS = 4;
  constexpr uint64_t UPDATE_DIFF = 1;

  constexpr int MAX_NUM_KEYS = 1024;
  int NUM_KEYS = noname::tabular::BTreeLeafNode<uint64_t, uint64_t>::maxEntries;
  for (; NUM_KEYS < MAX_NUM_KEYS; NUM_KEYS++) {
    noname::tabular::InlineBTree<uint64_t, uint64_t> tree(config, false);
    auto insert_key_range = [&tree](uint64_t start, uint64_t end) {
      for (uint64_t k = start; k < end; k++) {
        auto ok = tree.insert(k, k);
        ASSERT_TRUE(ok);
      }
    };
    auto update_key_range = [&tree](uint64_t start, uint64_t end) {
      for (uint64_t k = start; k < end; k++) {
        auto ok = tree.update(k, k + UPDATE_DIFF);
        ASSERT_TRUE(ok);
      }
    };
    auto lookup_key_range = [&tree](uint64_t start, uint64_t end) {
      for (uint64_t k = start; k < end; k++) {
        uint64_t result;
        auto ok = tree.lookup(k, result);
        ASSERT_TRUE(ok);
        ASSERT_EQ(result, k + UPDATE_DIFF);
      }
    };
    auto step = NUM_KEYS / NUM_THREADS;
    std::vector<std::thread> threads;
    // Insert
    for (auto i = 0; i < NUM_THREADS; i++) {
      auto start = i * step;
      auto end = start + step;
      if (i == NUM_THREADS - 1) {
        end = NUM_KEYS;
      }
      threads.emplace_back(insert_key_range, start, end);
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();

    // Update
    for (auto i = 0; i < NUM_THREADS; i++) {
      auto start = i * step;
      auto end = start + step;
      if (i == NUM_THREADS - 1) {
        end = NUM_KEYS;
      }
      threads.emplace_back(update_key_range, start, end);
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();

    // Lookup
    for (auto i = 0; i < NUM_THREADS; i++) {
      auto start = i * step;
      auto end = start + step;
      if (i == NUM_THREADS - 1) {
        end = NUM_KEYS;
      }
      threads.emplace_back(lookup_key_range, start, end);
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

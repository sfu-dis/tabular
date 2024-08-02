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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>

#include "index/hash_table.h"
#include "index/hash_table_lc.h"
#include "tabular/hash_table.h"
#include "tabular/mvcc_hash_table.h"

TEST(NativeHashTableTest, OptiLockTest) {
  noname::index::HashTable<uint64_t, uint64_t> index;

  constexpr uint64_t VALUE_DIFF = 233;

  uint64_t value = 0;
  uint64_t bigger_value = VALUE_DIFF;
  auto add_one_by_n_times = [&](uint64_t n) {
    for (size_t i = 0; i < n; i++) {
      index.directory_lock.Lock();
      value += 1;
      bigger_value += 1;
      index.directory_lock.Unlock();
    }
  };

  bool consistency_check_stop = false;
  auto consistency_check = [&]() {
    uint64_t local_value, local_bigger_value;
    while (!__atomic_load_n(&consistency_check_stop, __ATOMIC_ACQUIRE)) {
      bool ok;
      do {
        auto lock_word = index.directory_lock.RLock();
        local_value = value;
        local_bigger_value = bigger_value;
        ok = index.directory_lock.RUnlock(lock_word);
      } while (!ok);
      CHECK_EQ(local_bigger_value - local_value, VALUE_DIFF);
    }
  };

  std::vector<std::thread> threads;
  constexpr uint64_t NUM_THREADS = 4;
  constexpr uint64_t NUM_ADDITIONS = 1000000;

  std::thread consistency_check_thread(consistency_check);
  for (uint64_t i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(add_one_by_n_times, NUM_ADDITIONS);
  }

  for (auto &thread : threads) {
    thread.join();
  }
  __atomic_store_n(&consistency_check_stop, true, __ATOMIC_RELEASE);
  consistency_check_thread.join();

  CHECK_EQ(value, NUM_THREADS * NUM_ADDITIONS);
}

template <typename T>
struct HashTableTest : public testing::Test {
  T* CreateIndex() {
    if constexpr (std::is_same_v<T, noname::index::HashTable<uint64_t, uint64_t>> || std::is_same_v<T, noname::index::HashTableLC<uint64_t, uint64_t>>) {
      return new T;
    } else {
      noname::table::config_t config(false,
                                     false,   // no hugepages
                                     10000000, // capacity
                                     5000000); // initial size
      if constexpr (std::is_same_v<T, noname::tabular::HashTable<uint64_t, uint64_t>>) {
        return new T(config, false);
      } else {
        return new T(config);
      }
    }
  }
};

using HashTableTypes =
    ::testing::Types<noname::index::HashTable<uint64_t, uint64_t>,
                     noname::index::HashTableLC<uint64_t, uint64_t>,
                     noname::tabular::MVCCHashTable<uint64_t, uint64_t>,
                     noname::tabular::HashTable<uint64_t, uint64_t>>;
TYPED_TEST_SUITE(HashTableTest, HashTableTypes);

TYPED_TEST(HashTableTest, SingleThreadedInsertReadUpdate) {
  constexpr size_t MAX_NUM_KEY = 46;
  // constexpr size_t MAX_NUM_KEY = 128;

  // TODO(ziyi) parameterized this
  for (size_t NUM_KEYS = 1; NUM_KEYS < MAX_NUM_KEY + 1; NUM_KEYS++) {
    auto index = this->CreateIndex();

    bool ok;
    for (size_t i = 0; i < NUM_KEYS; i++) {
      ok = index->insert(i, i);
      CHECK(ok);
    }

    for (size_t i = 0; i < NUM_KEYS; i++) {
      uint64_t value;
      ok = index->lookup(i, value);
      CHECK(ok);
      CHECK_EQ(value, i);
    }

    for (size_t i = 0; i < NUM_KEYS; i++) {
      ok = index->update(i, i + 1);
      CHECK(ok);
    }

    for (size_t i = 0; i < NUM_KEYS; i++) {
      uint64_t value;
      ok = index->lookup(i, value);
      CHECK(ok);
      CHECK_EQ(value, i + 1);
    }
  }
}

TYPED_TEST(HashTableTest, MultiThreadedInsertReadUpdate) {
  constexpr size_t NUM_KEYS = 1024 * 10;

  auto index = this->CreateIndex();

  bool ok;

  auto insert = [&index](uint64_t start, uint64_t end) {
    for (size_t i = start; i < end; i++) {
      auto ok = index->insert(i, i);
      CHECK(ok);
    }
  };

  auto lookup = [&index](uint64_t start, uint64_t end, uint64_t addition) {
    for (size_t i = start; i < end; i++) {
      uint64_t value;
      auto ok = index->lookup(i, value);
      CHECK(ok);
      CHECK_EQ(value, i + addition);
    }
  };

  auto update = [&index](uint64_t start, uint64_t end, uint64_t addition) {
    for (size_t i = start; i < end; i++) {
      auto ok = index->update(i, i + addition);
      CHECK(ok);
    }
  };

  constexpr size_t NUM_THREADS = 8;
  constexpr size_t ADDITION = 1;
  auto step = NUM_KEYS / NUM_THREADS;
  std::vector<std::thread> threads;

  // Insert
  for (auto i = 0; i < NUM_THREADS; i++) {
    auto start = i * step;
    auto end = start + step;
    if (i == NUM_THREADS - 1) {
      end = NUM_KEYS;
    }
    threads.emplace_back(insert, start, end);
  }
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Lookup
  for (size_t i = 0; i < NUM_KEYS; i++) {
    uint64_t value;
    ok = index->lookup(i, value);
    CHECK(ok);
    CHECK_EQ(value, i);
  }

  // Multithreaded Lookup
  for (auto i = 0; i < NUM_THREADS; i++) {
    auto start = i * step;
    auto end = start + step;
    if (i == NUM_THREADS - 1) {
      end = NUM_KEYS;
    }
    threads.emplace_back(lookup, start, end, 0);
  }
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();

  // Update
  for (auto i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(update, 0, NUM_KEYS, ADDITION);
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
    threads.emplace_back(lookup, start, end, ADDITION);
  }
  for (auto &thread : threads) {
    thread.join();
  }
  threads.clear();
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

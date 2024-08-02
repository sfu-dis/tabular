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

#include "table/config.h"
#include "table/inline_table.h"
#include "table/object.h"
#include "transaction/occ.h"

noname::table::config_t config(false,
                               false,    // no hugepages
                               1000000,  // capacity
                               500000);  // initial size

const size_t NUM_THREADS = 4;

TEST(OCCTest, RecordSpinLock) {
  noname::table::InlineTable::Record record;
  record.id = 0;  // initial state is unlocked

  const size_t NUM_ADDITION = 100000;
  const size_t INITIAL_VALUE = 0;

  uint64_t val = INITIAL_VALUE;
  auto add_one = [&]() {
    for (auto i = 0; i < NUM_ADDITION; i++) {
      record.Lock();
      val += 1;
      record.Unlock();
    }
  };

  std::vector<std::thread> threads;
  for (auto i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(add_one);
  }
  for (auto &thread : threads) {
    thread.join();
  }

  CHECK_EQ(val, INITIAL_VALUE + NUM_THREADS * NUM_ADDITION);
}

TEST(OCCTest, ConcurrentUpdate) {
  noname::tabular::TableGroup group(config, false);
  auto table = group.GetTable(0, false);

  const size_t NUM_RECORDS = 1;
  const size_t NUM_ADDITION = 10000;
  const size_t INITIAL_VALUE = 0;
  // Load
  noname::table::OID oid;
  {
    auto t = noname::transaction::occ::Transaction::BeginSystem(&group);
    oid = t->Insert(table, &INITIAL_VALUE);
    auto ok = t->PreCommit();
    CHECK(ok);
    t->PostCommit();
  }

  auto add_n_to_all = [&]() {
    for (auto n_time = 0; n_time < NUM_ADDITION; n_time++) {
      while (true) {
        auto t = noname::transaction::occ::Transaction::BeginSystem(&group);
        auto value = t->arena->NextValue<size_t>();
        t->Read(table, oid, value);
        *value += 1;
        t->Update(table, oid, value);
        auto ok = t->PreCommit();
        if (!ok) {
          t->Rollback();
          continue;
        }
        t->PostCommit();
        break;
      }
    }
  };

  std::vector<std::thread> threads;
  for (auto i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(add_n_to_all);
  }
  for (auto &thread : threads) {
    thread.join();
  }

  {
    auto t = noname::transaction::occ::Transaction::BeginSystem(&group);
    uint64_t val;
    t->Read(table, oid, &val);
    CHECK_EQ(val, INITIAL_VALUE + NUM_THREADS * NUM_ADDITION);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

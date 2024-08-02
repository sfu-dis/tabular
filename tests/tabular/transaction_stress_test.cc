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

#include <vector>
#include <thread>

#include "table/ia_table.h"
#include "tabular/object.h"
#include "transaction/transaction.h"

using noname::transaction::mvcc::Transaction;
using noname::tabular::TypedObject;
using noname::table::OID;

// TODO(ziyi) change it to gtest
// Example usage:
//  ./tests/tabular/tabular_transaction_stress_test 1 10000 4
int main(int argc, char **argv) {
  if (argc != 3 && argc != 4) {
    printf(
        "usage: %s n add <threads>\nn: number of records\nadd: number of addition"
        "keys\n",
        argv[0]);
    return 1;
  }

  size_t NUM_RECORDS = std::atoll(argv[1]);
  size_t NUM_ADDITION = std::atoll(argv[2]);
  size_t NUM_THREADS = std::atoll(argv[3]);
  const size_t INITIAL_VALUE = 0;

  noname::table::config_t config(false, false /* no hugepages */,
                                   100000 /* capacity */);

  noname::table::IndirectionArrayTable table(config);

  // single-threaded loading
  for (size_t i = 0; i < NUM_RECORDS; i++) {
    auto t = Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
    auto integer = TypedObject<size_t>::Make();
    *integer->Data() = INITIAL_VALUE;
    auto oid = t->Insert(&table, integer->Obj());
    CHECK_EQ(oid, OID{i});
    t->PreCommit();
    t->PostCommit();
  }

  // concurrent update
  auto add_n_to_all_records = [&table, &NUM_RECORDS, &NUM_ADDITION]() {
    for (size_t i = 0; i < NUM_RECORDS; i++) {
      for (auto n_time = 0; n_time < NUM_ADDITION; n_time++) {
        bool ok = false;
        while (!ok) {
          auto t = Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
          auto oid = OID{i};
          auto obj = t->Read(&table, oid);
          auto old_val = *reinterpret_cast<size_t *>(obj->data);
          auto new_val = old_val + 1;
          auto integer = TypedObject<size_t>::Make();
          *integer->Data() = new_val;
          ok = t->Update(&table, oid, integer->Obj());
          if (!ok) {
            t->Rollback();
            std::free(reinterpret_cast<void *>(integer));
            continue;
          }
          CHECK_EQ(integer->Obj()->next, obj);
          t->PreCommit();
          t->PostCommit();
        }
      }
    }
  };
  std::vector<std::thread> threads;
  for (auto i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back(add_n_to_all_records);
  }
  for (auto &thread : threads) {
    thread.join();
  }

  // verify
  for (size_t i = 0; i < NUM_RECORDS; i++) {
    auto oid = OID{i};
    auto obj = *table.GetEntryPtr(oid);
    auto val = *reinterpret_cast<size_t *>(obj->data);
    CHECK_EQ(val, INITIAL_VALUE + NUM_THREADS * NUM_ADDITION);
  }

  // verify with transaction read
  for (size_t i = 0; i < NUM_RECORDS; i++) {
    auto t = Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
    auto oid = OID{i};
    auto obj = t->Read(&table, oid);
    auto val = *reinterpret_cast<size_t *>(obj->data);
    CHECK_EQ(val, INITIAL_VALUE + NUM_THREADS * NUM_ADDITION);
  }
}

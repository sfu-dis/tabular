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

#include "table/ia_table.h"
#include "transaction/transaction.h"

noname::table::config_t table_config(false,
                               false, /* no hugepages */
                               100000 /* capacity */);


// Repeated updates + reader should see committed version (w -> r).
TEST(TransactionTest, SingleThreadSnapshotInsertRead) {
  noname::table::IndirectionArrayTable table(table_config);

  // Initialize a transaction
  auto current_global_csn = noname::transaction::mvcc::csn_counter.load();
  auto t = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
  ASSERT_EQ(t->begin_ts, current_global_csn);
  ASSERT_EQ(t->commit_ts, noname::transaction::mvcc::kInvalidCSN);
  ASSERT_EQ(t->write_set.count, 0);

  // Insert one version
  auto v1 = std::make_unique<noname::table::Object>();
  v1.get()->id = reinterpret_cast<uint64_t>(t) | (1UL <<63);
  auto oid = t->Insert(&table, v1.get());
  ASSERT_EQ(oid, 0);

  // Now read, should see my own version. Note here we directly return the pointer and transactions
  // don't make a local copy if it's read-only operation (instead of for example RMW).
  noname::table::Object *obj = t->Read(&table, oid);
  ASSERT_EQ(obj, v1.get());
  ASSERT_EQ(obj->id, reinterpret_cast<uint64_t>(t) | (1UL << 63));

  // Check write set
  ASSERT_EQ(t->write_set.count, 1);
  ASSERT_EQ(t->write_set.entries[0], table.backing_array.GetEntryPtr(oid));

  // Make a repeated update
  auto v2 = std::make_unique<noname::table::Object>();
  v2.get()->id = reinterpret_cast<uint64_t>(t) | (1UL <<63);
  bool success = t->Update(&table, oid, v2.get());
  ASSERT_TRUE(success);

  // Check write set again, now should see one more entry but repeated
  ASSERT_EQ(t->write_set.count, 2);
  ASSERT_EQ(t->write_set.entries[0], t->write_set.entries[1]);

  // Now read, should see v2
  obj = t->Read(&table, oid);
  ASSERT_EQ(obj, v2.get());
  ASSERT_EQ(obj->id, reinterpret_cast<uint64_t>(t) | (1UL << 63));

  // Commit the transaction
  success = t->PreCommit();
  ASSERT_TRUE(success);
  ASSERT_EQ(t->commit_ts, current_global_csn);  // Assuming no concurrency
  success = t->PostCommit();
  ASSERT_TRUE(success);
  // v1 was abondaned, so the id should remain t | (1 << 63).
  ASSERT_EQ(v1.get()->id, reinterpret_cast<uint64_t>(t) | (1UL <<63));
  ASSERT_EQ(v2.get()->id, t->commit_ts);

  // Start a new transaction, should see v2
  noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable> t2;
  ASSERT_GT(t2.begin_ts, t->begin_ts);
  ASSERT_EQ(t2.begin_ts, t->commit_ts + 1);  // Holds without concurrency
  obj = t2.Read(&table, oid);
  ASSERT_EQ(obj, v2.get());
}

// TODO(ziyi) disabled since current transaction assumes at most one active transaction per thread
// Reader cannot see concurrent update
TEST(TransactionTest, DISABLED_SingleThreadInsertReadIsolation) {
  noname::table::IndirectionArrayTable table(table_config);

  // Initialize a transaction
  auto current_global_csn = noname::transaction::mvcc::csn_counter.load();
  auto t1 = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
  ASSERT_EQ(t1->begin_ts, current_global_csn);
  ASSERT_EQ(t1->commit_ts, noname::transaction::mvcc::kInvalidCSN);
  ASSERT_EQ(t1->write_set.count, 0);

  // Insert one version
  auto v1 = std::make_unique<noname::table::Object>();
  v1.get()->id = reinterpret_cast<uint64_t>(t1) | (1UL <<63);
  auto oid = t1->Insert(&table, v1.get());
  ASSERT_EQ(oid, 0);

  // Now read using t1, should see my own version. Note here we directly return
  // the pointer and transactions don't make a local copy if it's read-only
  // operation (instead of for example RMW).
  noname::table::Object *obj = t1->Read(&table, oid);
  ASSERT_EQ(obj, v1.get());
  ASSERT_EQ(obj->id, reinterpret_cast<uint64_t>(t1) | (1UL << 63));

  // Check write set
  ASSERT_EQ(t1->write_set.count, 1);
  ASSERT_EQ(t1->write_set.entries[0], table.backing_array.GetEntryPtr(oid));

  // Now start another transaction t2 to read [oid], should see nothing
  auto t2 = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
  ASSERT_EQ(t2->begin_ts, current_global_csn);
  ASSERT_EQ(t2->commit_ts, noname::transaction::mvcc::kInvalidCSN);
  ASSERT_EQ(t2->write_set.count, 0);
  noname::table::Object *obj_t2 = t2->Read(&table, oid);
  ASSERT_EQ(obj_t2, nullptr);

  // Commit t1
  bool success = t1->PreCommit();
  ASSERT_TRUE(success);
  success = t1->PostCommit();
  ASSERT_TRUE(success);

  // Now issue the read again by t2, should still not see it
  obj_t2 = t2->Read(&table, oid);
  ASSERT_EQ(obj_t2, nullptr);

  // Start a t3, should see the version
  auto t3 = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
  ASSERT_GT(t3->begin_ts, t1->begin_ts);
  ASSERT_EQ(t3->begin_ts, t1->commit_ts + 1);  // Holds without concurrency
  obj = t3->Read(&table, oid);
  ASSERT_EQ(obj, v1.get());
}

// Multi-threaded insert + verify read
TEST(TransactionTest, ConcurrentSIInsert) {
  noname::table::IndirectionArrayTable table(table_config);

  uint32_t kThreads = 32;
  std::vector<std::thread> threads;
  std::atomic<uint64_t> ts_sum = 0;
  auto func_insert = [&table, &ts_sum]() {
    auto t = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
    auto *obj = new noname::table::Object;
    obj->id = reinterpret_cast<uint64_t>(t) | (1UL <<63);
    auto oid = t->Insert(&table, obj);
    ASSERT_NE(oid, noname::table::kInvalidOID);
    ASSERT_EQ(t->write_set.count, 1);
    bool success = t->PreCommit();
    ASSERT_TRUE(success);
    success = t->PostCommit();
    ASSERT_TRUE(success);
    ts_sum += t->commit_ts;
  };

  for (auto i = 0; i < kThreads; ++i) {
    threads.emplace_back(func_insert);
  }

  for (auto &t : threads) {
    t.join();
  }

  // Verify table status
  uint64_t ts_sum_verify = ts_sum;
  ASSERT_EQ(table.next_oid, kThreads);
  for (auto i = 0; i < table.next_oid; ++i) {
    auto *obj = table.backing_array[i];
    ts_sum_verify -= obj->id;
  }
  ASSERT_EQ(ts_sum_verify, 0);

  // Issue transactional reads to see
  auto t = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
  for (auto oid = 0; oid < table.next_oid; ++oid) {
    auto *obj = t->Read(&table, oid);
    ASSERT_NE(obj, nullptr);
    ts_sum -= obj->id;
    delete obj;
  }
  ASSERT_EQ(ts_sum, 0);
}

TEST(TransactionTest, ConcurrentSIUpdate) {
  size_t NUM_RECORDS = 1;
  size_t NUM_ADDITION = 10000;
  size_t NUM_THREADS = 4;
  const size_t INITIAL_VALUE = 0;

  noname::table::IndirectionArrayTable table(table_config);

  // single-threaded loading
  for (size_t i = 0; i < NUM_RECORDS; i++) {
    auto t = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
    auto integer = noname::table::TypedObject<size_t>::Make();
    *integer->Data() = INITIAL_VALUE;
    auto oid = t->Insert(&table, integer->Obj());
    CHECK_EQ(oid, i);
    t->PreCommit();
    t->PostCommit();
  }

  // concurrent update
  auto add_n_to_all_records = [&table, &NUM_RECORDS, &NUM_ADDITION]() {
    for (size_t i = 0; i < NUM_RECORDS; i++) {
      for (auto n_time = 0; n_time < NUM_ADDITION; n_time++) {
        bool ok = false;
        while (!ok) {
          auto t = noname::transaction::mvcc::Transaction<
              noname::table::IndirectionArrayTable>::BeginSystem();
          auto obj = t->Read(&table, i);
          auto old_val = *reinterpret_cast<size_t *>(obj->data);
          auto new_val = old_val + 1;
          auto integer = noname::table::TypedObject<size_t>::Make();
          *integer->Data() = new_val;
          ok = t->Update(&table, i, integer->Obj());
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
    auto obj = *table.GetEntryPtr(i);
    auto val = *reinterpret_cast<size_t *>(obj->data);
    CHECK_EQ(val, INITIAL_VALUE + NUM_THREADS * NUM_ADDITION);
  }

  // verify with transaction read
  for (size_t i = 0; i < NUM_RECORDS; i++) {
    auto t = noname::transaction::mvcc::Transaction<noname::table::IndirectionArrayTable>::BeginSystem();
    auto obj = t->Read(&table, i);
    auto val = *reinterpret_cast<size_t *>(obj->data);
    CHECK_EQ(val, INITIAL_VALUE + NUM_THREADS * NUM_ADDITION);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

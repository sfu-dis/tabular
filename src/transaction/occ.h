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

#include <tuple>
#include <vector>

#include "table/inline_table.h"
#include "tabular/table_group.h"

#include "string_arena.h"

// TODO(ziyi) declaration to avoid cycle header dependency
// #include "log/dlog/log.h"
namespace noname {
namespace dlog {
struct Log;
}
}  // namespace noname

namespace noname {
namespace transaction {
namespace occ {

inline static constexpr uint64_t INVALID_TID = ~(0ULL);

struct Transaction {
  static const int RECORD_SET_MAX_SIZE = 256;
  struct ReadSet {
    struct Entry {
      table::InlineTable::Record *record;
      uint64_t tid;
    };
    Entry entries[RECORD_SET_MAX_SIZE];
    uint8_t count;

    ReadSet();
    void Add(table::InlineTable::Record *record, uint64_t tid);
  };

  struct WriteSet {
    struct Entry {
      table::InlineTable *table;
      table::InlineTable::Record *record;
      const uint8_t *data;
      size_t size;
      uint64_t tid;
      table::OID oid;
      bool is_insert;
      bool is_callback;
      std::function<void(uint8_t *)> callback;
      Entry() : is_callback(false) {}
    };
    static bool RecordAddressLess(const Entry &lhs, const Entry &rhs) {
      return lhs.record < rhs.record;
    }
    Entry entries[RECORD_SET_MAX_SIZE];
    Entry sorted_entries[RECORD_SET_MAX_SIZE];
    uint8_t count;

    WriteSet();
    void Add(table::InlineTable *table, table::InlineTable::Record *record,
             const uint8_t *data, size_t size, table::OID oid, bool is_insert);

    void AddCallback(table::InlineTable *table, table::InlineTable::Record *record,
                     table::OID oid, std::function<void(uint8_t *)> callback,
                     size_t record_size) {
      auto &new_entry = entries[count++];
      new_entry.table = table;
      new_entry.record = record;
      new_entry.size = record_size;
      new_entry.oid = oid;
      new_entry.is_insert = false;
      new_entry.is_callback = true;
      new_entry.callback = callback;
    }
  };

  table::OID InsertData(table::InlineTable *table, const uint8_t *data, size_t size);

  template <typename T>
  inline table::OID Insert(table::InlineTable *table, const T *value, size_t size = sizeof(T)) {
    auto data = reinterpret_cast<const uint8_t *>(value);
    return InsertData(table, data, size);
  }

  std::tuple<uint8_t *, size_t> ReadRecord(table::InlineTable *table,
                                           table::OID oid,
                                           uint8_t *out_data = nullptr);

  // Caveat: size of record data (sizeof(T)) cannot be too large which exceeds
  // stacks. Use ReadRecord() instead for such cases.
  template <typename T>
  inline void Read(table::InlineTable *table, table::OID oid, T *out_value) {
    auto out_data = reinterpret_cast<uint8_t *>(out_value);
    ReadRecord(table, oid, out_data);
  }

  // Assume oid is first-time accessed in current transaction
  // TODO(ziyi): add a require clause for type Callback to be a callable<void(T*)>
  template <typename T, typename Callback>
  inline void ReadCallback(table::InlineTable *table, table::OID oid,
                           const Callback &callback) {
    auto record = table->GetRecord(oid);
    while (true) {
      // Spin till the record is unlock
      uint64_t tid;
      do {
        tid = __atomic_load_n(&record->id, __ATOMIC_ACQUIRE);
      } while (tid & table::InlineTable::LOCK_BIT);

      // Execute the callback on the single copy
      callback(reinterpret_cast<T *>(record->data));

      // Check current TID
      auto curr = __atomic_load_n(&record->id, __ATOMIC_ACQUIRE);
      if (curr == tid) {
        // TID match which means the data read is not partially updated
        read_set.Add(record, tid);
        return;
      }
      // TID changed. Retry till it succeeds
    }
  }

  template <typename Callback>
  inline void ReadRecordCallback(table::InlineTable *table, table::OID oid,
                                 const Callback &callback) {
    auto record = table->GetRecord(oid);
    while (true) {
      // Spin till the record is unlock
      uint64_t tid;
      do {
        tid = __atomic_load_n(&record->id, __ATOMIC_ACQUIRE);
      } while (tid & table::InlineTable::LOCK_BIT);

      // Execute the callback on the single copy
      callback(record->data, record->size);

      // Check current TID
      auto curr = __atomic_load_n(&record->id, __ATOMIC_ACQUIRE);
      if (curr == tid) {
        // TID match which means the data read is not partially updated
        read_set.Add(record, tid);
        return;
      }
      // TID changed. Retry till it succeeds
    }
  }

  void UpdateRecord(table::InlineTable *table, table::OID oid, const uint8_t *data, size_t size);

  template <typename T>
  inline void Update(table::InlineTable *table, table::OID oid, const T *value) {
    auto data = reinterpret_cast<const uint8_t *>(value);
    UpdateRecord(table, oid, data, sizeof(T));
  }

  template <typename Callback>
  void UpdateRecordCallback(table::InlineTable *table, table::OID oid,
    const Callback &callback, size_t record_size = 0) {
    auto *record = table->GetRecord(oid);
    write_set.AddCallback(table, record, oid, callback, record_size);
    // Logging delayed to commit time
  }

  bool PreCommit();
  bool PostCommit();
  void Rollback();

  static thread_local Transaction user_thread_local_txn;
  static Transaction *BeginUser(tabular::TableGroup *group);

  static thread_local Transaction system_thread_local_txn;
  static Transaction *BeginSystem(tabular::TableGroup *group);

  struct Worker {
    static std::atomic<uint64_t> ID;

    uint64_t id;
    uint64_t last_commit_tid;

    Worker();
  };
  Worker *worker;

  Transaction();

  transaction::StringArena *arena;

  ReadSet read_set;
  WriteSet write_set;

  uint64_t commit_tid;

  uint64_t worker_epoch;

  uint64_t log_size;

  tabular::TableGroup *group;
};

}  // namespace occ
}  // namespace transaction
}  // namespace noname

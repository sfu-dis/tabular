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

#include <tuple>
#include <algorithm>
#include <thread>
#include <chrono>

#include "table/inline_table.h"
#include "util.h"
#include "log/dlog/log.h"

#include "occ.h"

namespace noname {
namespace transaction {
namespace occ {

Transaction::ReadSet::ReadSet() : count(0) {}

void Transaction::ReadSet::Add(table::InlineTable::Record *record,
                               uint64_t tid) {
  auto &new_entry = entries[count++];
  new_entry.record = record;
  new_entry.tid = tid;
}

Transaction::WriteSet::WriteSet() : count(0) {}

void Transaction::WriteSet::Add(table::InlineTable *table,
                                table::InlineTable::Record *record,
                                const uint8_t *data, size_t size,
                                table::OID oid, bool is_insert) {
  auto &new_entry = entries[count++];
  new_entry.table = table;
  new_entry.record = record;
  new_entry.data = data;
  new_entry.size = size;
  new_entry.tid = INVALID_TID;
  new_entry.oid = oid;
  new_entry.is_insert = is_insert;
  new_entry.is_callback = false;
}

table::OID Transaction::InsertData(table::InlineTable *table, const uint8_t *data, size_t size) {
  auto oid = table->AllocateRecord(size);
  auto record = table->GetRecord(oid);
  write_set.Add(table, record, data, size, oid, true);
  if (table->is_persistent) {
    log_size += log::LogRecord::GetExpectSize(size);
  }
  return oid;
}

std::tuple<uint8_t *, size_t> Transaction::ReadRecord(table::InlineTable *table,
                                                      table::OID oid,
                                                      uint8_t *out_data) {
  auto record = table->GetRecord(oid);
  uint8_t *data = arena->Next(0);
  auto prev_size = 0;
  while (true) {
    // Spin till the record is unlock
    uint64_t tid;
    do {
      tid = __atomic_load_n(&record->id, __ATOMIC_ACQUIRE);
    } while (tid & table::InlineTable::LOCK_BIT);

    // TODO(ziyi) change ReadRecord and its usage to remove this branch
    // Copy record to *out_data
    auto record_size = record->size;
    if (!out_data) {
      if (record_size != prev_size) {
        CHECK_GT(record_size, prev_size);
        arena->Next(record_size - prev_size);
        prev_size = record_size;
      }
      std::memcpy(data, record->data, record_size);
    } else {
      std::memcpy(out_data, record->data, record_size);
    }

    // Check current TID
    auto curr = __atomic_load_n(&record->id, __ATOMIC_ACQUIRE);
    if (curr == tid) {
      // TID match which means the data read is not partially updated
      read_set.Add(record, tid);
      return std::make_tuple(data, record_size);
    }
    // TID changed. Retry till it succeeds
  }
}

// `data` should point to memory allocated from transaction-local arena.
void Transaction::UpdateRecord(table::InlineTable *table, table::OID oid,
                               const uint8_t *data, size_t size) {
  auto record = table->GetRecord(oid);

  write_set.Add(table, record, data, size, oid, false);

  if (table->is_persistent) {
    log_size += log::LogRecord::GetExpectSize(size);
  }
}

bool Transaction::PreCommit() {
  // Phase 1
  // Lock records in write set in the order of address ascending order

  // Retain the order of each write when they're added
  // for applying callbacks in the right order of dependency
  std::memcpy(write_set.sorted_entries, write_set.entries,
              write_set.count * sizeof(WriteSet::Entry));
  std::sort(write_set.sorted_entries,
            write_set.sorted_entries + write_set.count,
            WriteSet::RecordAddressLess);

  for (uint8_t i = 0; i < write_set.count; i++) {
    auto &entry = write_set.sorted_entries[i];
    entry.tid = entry.record->Lock();
  }

  COMPILER_MEMORY_FENCE;
  worker_epoch = group->epoch.load(std::memory_order::acquire);
  COMPILER_MEMORY_FENCE;

  // Phase 2
  // Validate read set
  for (uint8_t i = 0; i < read_set.count; i++) {
    auto &entry = read_set.entries[i];
    uint64_t tid_word = __atomic_load_n(&entry.record->id, __ATOMIC_ACQUIRE);
    auto tid = table::InlineTable::GetTID(tid_word);
    if (entry.tid != tid) {
      return false;
    }
    bool is_locked = tid_word & table::InlineTable::LOCK_BIT;
    if (is_locked) {
      // Check whether this record is locked by myself
      bool is_locked_by_myself = false;
      for (uint8_t i = 0; i < write_set.count; i++) {
        auto &write_entry = write_set.sorted_entries[i];
        if (write_entry.record == entry.record) {
          is_locked_by_myself = true;
          break;
        }
      }
      if (!is_locked_by_myself) {
        return false;
      }
    }
  }
  // Generate TID
  commit_tid = worker_epoch << (table::InlineTable::NUM_VERSION_BITS +
                                  table::InlineTable::NUM_STATUS_BITS);
  for (uint8_t i = 0; i < read_set.count; i++) {
    auto &entry = read_set.entries[i];
    commit_tid = std::max(
        commit_tid, entry.tid + (1UL << table::InlineTable::NUM_STATUS_BITS));
  }
  for (uint8_t i = 0; i < write_set.count; i++) {
    auto &entry = write_set.sorted_entries[i];
    commit_tid = std::max(
        commit_tid, entry.tid + (1UL << table::InlineTable::NUM_STATUS_BITS));
  }

  commit_tid = std::max(commit_tid, worker->last_commit_tid);
  worker->last_commit_tid = commit_tid;

  // Phase 3
  // Flush writes and unlock records in write set
  for (uint8_t i = 0; i < write_set.count; i++) {
    auto &entry = write_set.entries[i];
    if (entry.size > entry.record->size) {
      entry.table->EnsureRecordSize(entry.oid, entry.size);
      entry.record->size = entry.size;
    }
    if (entry.is_callback) {
      // Invoke the callback on the record data
      entry.callback(entry.record->data);
      // Copy after image after applying callback
      if (entry.table->is_persistent) {
        log_size += log::LogRecord::GetExpectSize(entry.record->size);
        auto data = arena->Next(entry.record->size);
        // TODO(ziyi) directly copy after image to log buffer
        std::memcpy(data, entry.record->data, entry.record->size);
        entry.data = data;
        entry.size = entry.record->size;
      }
    } else {
      std::memcpy(entry.record->data, entry.data, entry.size);
    }
    __atomic_store_n(&entry.record->id, commit_tid, __ATOMIC_RELEASE);
  }
  return true;
}

bool Transaction::PostCommit() {
  if (!group->logger) {
    // No need to persist
    return true;
  }

  if (write_set.count == 0) {
    // No logging for read-only transactions
    return true;
  }

  // Logging
  auto log = group->logger->LocalLog(worker->id);
  log::LSN lsn;
  auto block = log->AllocateLogBlock(log_size, &lsn, commit_tid);
  LOG_IF(FATAL, !block);

  for (uint8_t i = 0; i < write_set.count; ++i) {
    auto &entry = write_set.entries[i];
    CHECK_NE(entry.size, 0);
    if (!entry.table->is_persistent) {
      continue;
    }

    // Populate log block
    uint32_t offset = block->payload_size;
    if (entry.is_insert) {
      auto ret = log::LogRecord::LogInsert(
          block, entry.table->fid, entry.oid,
          reinterpret_cast<const char *>(entry.data), entry.size);
      LOG_IF(FATAL, ret != offset);
    } else {
      auto ret = log::LogRecord::LogUpdate(
          block, entry.table->fid, entry.oid,
          reinterpret_cast<const char *>(entry.data), entry.size);
      LOG_IF(FATAL, ret != offset);
    }
  }

  return true;
}

void Transaction::Rollback() {
  // At this point, Transaction::PreCommit() failed.
  // Unlock records in write set
  for (uint8_t i = 0; i < write_set.count; i++) {
    auto &entry = write_set.entries[i];
    entry.record->Unlock();
  }
}

Transaction::Transaction() : commit_tid(INVALID_TID), log_size(0) {}

inline constexpr uint64_t INVALID_WORKER_ID = ~0ull;
std::atomic<uint64_t> Transaction::Worker::ID = 0;
Transaction::Worker::Worker() : last_commit_tid(0), id(INVALID_WORKER_ID) {}

thread_local transaction::StringArena user_thread_local_arena;
thread_local Transaction Transaction::user_thread_local_txn;
thread_local Transaction::Worker user_thread_local_worker_ctx;
Transaction *Transaction::BeginUser(tabular::TableGroup *group) {
  user_thread_local_arena.Reset();
  if (user_thread_local_worker_ctx.id == INVALID_WORKER_ID) {
    user_thread_local_worker_ctx.id =
        Worker::ID.fetch_add(1, std::memory_order::acq_rel);
  }
  user_thread_local_txn.commit_tid = INVALID_TID;
  user_thread_local_txn.log_size = 0;
  user_thread_local_txn.arena = &user_thread_local_arena;
  user_thread_local_txn.worker = &user_thread_local_worker_ctx;
  user_thread_local_txn.group = group;
  user_thread_local_txn.write_set.count = 0;
  user_thread_local_txn.read_set.count = 0;
  return &user_thread_local_txn;
}

thread_local transaction::StringArena system_thread_local_arena;
thread_local Transaction Transaction::system_thread_local_txn;
thread_local Transaction::Worker system_thread_local_worker_ctx;
Transaction *Transaction::BeginSystem(tabular::TableGroup *group) {
  system_thread_local_arena.Reset();
  if (system_thread_local_worker_ctx.id == INVALID_WORKER_ID) {
    system_thread_local_worker_ctx.id =
        Worker::ID.fetch_add(1, std::memory_order::acq_rel);
  }
  system_thread_local_txn.commit_tid = INVALID_TID;
  system_thread_local_txn.log_size = 0;
  system_thread_local_txn.arena = &system_thread_local_arena;
  system_thread_local_txn.worker = &system_thread_local_worker_ctx;
  system_thread_local_txn.group = group;
  system_thread_local_txn.write_set.count = 0;
  system_thread_local_txn.read_set.count = 0;
  return &system_thread_local_txn;
}

}  // namespace occ
}  // namespace transaction
}  // namespace noname

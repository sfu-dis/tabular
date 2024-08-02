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

#include "table/ia_table.h"
#include "transaction-normal.h"

namespace noname {
namespace transaction {

std::atomic<CSN> Transaction::csn_counter(0);

Transaction::Transaction()
  : log_size(0)
  , state(State::ForwardProcessing)
  , begin_ts(csn_counter.load(std::memory_order_acquire))
  , commit_ts(kInvalidCSN) {
  thread_local uint32_t gen_num = 0;
  generation = ++gen_num;

  log = dlog::GetLog();
}

Transaction::~Transaction() {
}

table::OID Transaction::Insert(table::IndirectionArrayTable *table, table::Object *object) {
  object->id = reinterpret_cast<uint64_t>(this) | (1UL << 63);
  table::OID oid = table->Put(object);
  write_set.Add(table->backing_array.GetEntryPtr(oid), table->fid, oid, true);
  log_size += log::LogRecord::GetExpectSize(object->size);
  table_set[table->fid] = table;
  return oid;
}

table::Object *Transaction::Read(table::IndirectionArrayTable *table, table::OID oid) {
  table::Object *current = nullptr;
  current = table->Get(oid);
  while (current) {
    uint64_t id = current->id;

    // "id" will either contain a CSN or a transaction context pointer.
    if (id & (1UL << 63)) {
      // It's a transaction context pointer, so the generating transaction is
      // still in-progress. Then we check its CSN: if it already has a CSN, then
      // we use it, otherwise we proceed to the next version.
      Transaction *other_tx = reinterpret_cast<Transaction *>(id & ~(1UL << 63));

      // See first if it's myself - can definitely see it if so
      if (other_tx == this) {
        return current;
      }
      // Not myself, now must see if we can actually see the version

      // Capture the generation number here first. Later whenever we make a decision about whether
      // to use a version, check whether the transaction generation number still matches this one.
      auto other_gen = other_tx->generation;

      // Double check again the transaction is still there working on the version
      if (id == current->id) {
        // Version still carrying this transaction ID, now inspect the state of
        // the transaction:
        // - Forward processing: it's still yet to enter precommit, so I won't be
        //   able to see its updates because its commit timestamp is going to be
        //   at least my begin_ts
        // - Committing: the transaction is in the process of (1) obtaining a
        //   commit_ts, (2) determining whether it can commit (e.g., doing
        //   serializability check, which is noop for SI). I should then spin and
        //   wait for the transaction's commit decision to finalize (i.e., either
        //   becomes PreCommitted/Committed, or Aborted). Then, if it turns out
        //   my begin_ts is larger, I can go ahead to access the new versions.
        // - PreCommitted: the transaction will commit and its commit_ts is
        //   available. If its smaller than my begin_ts, I can access the data.
        // - Committed: transaction already committed (finished post commit),
        //   same rule as for PreCommitted State.
        auto other_state = other_tx->state;

        // At this point since we are already sure that the object still carries the same id, it is
        // guaranteed that the previous generation number we got for this transaction context should
        // not be invalid (i.e., impossible for it to have been assigned to another transaction
        // when we were getting other_gen, before the version id was changed which only happens
        // during post-commit and the re-assignment could only happen after post-commit).
        LOG_IF(FATAL, other_gen == 0);

        // Still, the transaction context could have been reassigned after we obtained other_gen, so
        // double check here
        if (other_gen == other_tx->generation) {
          if (other_state == State::ForwardProcessing) {
            // Continue to the next version
            current = current->next;
            continue;
          }
        } else {
          // Transaction context already changed - retry
          continue;
        }

        // Has to be PreCommit or a later stage
        // FIXME(tzwang): need pointer stability for [other_tx]
        while (other_tx->state == Committing) {}

        // Refresh to see the final state
        other_state = other_tx->state;

        if (other_state == State::Aborted) {
          // Retry from the beginning
          continue;
        } else {
          // Now should have a valid commit ts, but we need to verify its generation number is still
          // the same (i.e., this is still the guy we saw earlier).
          CSN other_csn = other_tx->commit_ts;
          if (other_gen == other_tx->generation) {
            if (other_csn <= begin_ts) {
              // Found our guy!
              return current;
            } else {
              // The version is too new for me
              current = current->next;
            }
          } else {
            // Already a different guy using this transaction object, retry
            continue;
          }
        }
      } else {
        // The transaction is already gone, so re-read the opaque id and
        // re-evaluate
        continue;
      }
    } else {
      // A normal committed version
      CSN other_csn = id;
      if (begin_ts >= other_csn) {
        return current;
      } else {
        current = current->next;
      }
    }
  }

  // There is no proper version to read if we reached here
  return nullptr;
}

bool Transaction::Update(table::IndirectionArrayTable *table, table::OID oid,
                         table::Object *new_object) {
  new_object->id = reinterpret_cast<uint64_t>(this) | (1UL << 63);
  table::Object *current = table->Get(oid);
  table::Object **ptr = table->backing_array.GetEntryPtr(oid);
  bool success = false;

  // Can only update if I can see the latest version and the latest version is
  // already committed (unless I'm doing repeated update)
  uint64_t id = current->id;
  if (id & (1UL << 63)) {
    Transaction *other_tx = reinterpret_cast<Transaction *>(id & ~(1UL << 63));
    // See if it's myself
    if (other_tx == this) {
      // My own update - retire the old object but still have to keep it for a
      // while in case other transactions may be peeking at it
      // FIXME(tzwang): implement EBR
      auto *old_obj = *ptr;

      // Install the new object
      new_object->next = current;
      *ptr = new_object;
      success = true;
      write_set.Add(ptr, table->fid, oid, false);
      log_size += log::LogRecord::GetExpectSize(new_object->size);
      table_set[table->fid] = table;
    }  // Otherwise it's not mine, have to abort
  } else {
    if (id <= begin_ts) {
      new_object->next = current;

      // Attempt a CAS to install the new object
      success = __atomic_compare_exchange(ptr, &current, &new_object, false,
                                          __ATOMIC_RELEASE, __ATOMIC_ACQUIRE);
      if (success) {
        write_set.Add(ptr, table->fid, oid, false);
        log_size += log::LogRecord::GetExpectSize(new_object->size);
        table_set[table->fid] = table;
      }
    }  // Otherwise can't see the latest value, abort
  }

  return success;
}

bool Transaction::PreCommit() {
  if (!write_set.count) {
    return true;
  }
  // Put myself into "committing" state first
  state = State::Committing;

  // Now acquire a commit timestamp - note that this must happen after state
  // becomes "committing" otherwise concurrent readers may miss my updates and
  // see an inconsistent snapshot.
  commit_ts = ++csn_counter;

  state = State::PreCommitted;

  return true;
}

bool Transaction::PostCommit() {
  if (!write_set.count) {
    return true;
  }
  log::LSN lb_lsn;
  log::LogBlock *lb = log->AllocateLogBlock(log_size, &lb_lsn, commit_ts);
  LOG_IF(FATAL, !lb);

  for (uint32_t i = 0; i < write_set.count; ++i) {
    auto &w = write_set.entries[i];
    auto obj = w.GetObject();

    // Populate log block
    uint32_t off = lb->payload_size;
    if(w.is_insert) {
      auto ret_off = log::LogRecord::LogInsert(lb, w.fid, w.oid, obj->GetData(), obj->size);
      LOG_IF(FATAL, ret_off != off);
    } else {
      auto ret_off = log::LogRecord::LogUpdate(lb, w.fid, w.oid, obj->GetData(), obj->size);
      LOG_IF(FATAL, ret_off != off);
    }
    obj->id = commit_ts;
  }
  state = State::Committed;
  return true;
}

void Transaction::Rollback() {
  for (uint32_t i = 0; i < write_set.count; ++i) {
    WriteSetEntry entry = write_set.entries[i];
    table::Object *obj = entry.GetObject();
    *entry.object = entry.GetObject()->next;
    table::Object::Free(obj);
  }
}

}  // namespace transaction
}  // namespace noname

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

#include "table/ia_table.h"
#include "transaction.h"
#include "occ.h"

namespace noname {
namespace transaction {
namespace mvcc {

std::atomic<CSN> csn_counter(0);

template <typename TABLE>
Transaction<TABLE>::Transaction()
  : begin_ts(csn_counter.load(std::memory_order::relaxed))
  , commit_ts(kInvalidCSN) {
    state.store(State::ForwardProcessing, std::memory_order::release);
}

template <typename TABLE>
Transaction<TABLE>::~Transaction() {
}

template <typename TABLE>
table::OID Transaction<TABLE>::Insert(TABLE *table, table::Object *object) {
  table::OID oid = table->Put(object);
  object->id = reinterpret_cast<uint64_t>(this) | (1UL <<63);
  write_set.Add(table->GetEntryPtr(oid));
  return oid;
}

template <typename TABLE>
table::Object *Transaction<TABLE>::Read(TABLE *table, table::OID oid) {
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
      auto other_xid = other_tx->xid.load(std::memory_order::acquire);
      if (other_xid == kInvalidXID) {
        // Transaction is under re-initialization
        // Retry till we can read a consistent state of a transaction
        continue;
      }

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
        if (other_tx->state.load(std::memory_order::acquire) == State::ForwardProcessing) {
          if (other_xid != other_tx->xid.load(std::memory_order::acquire)) {
            // Transaction is possible not consistent during read
            // Retry till we can read a consistent state of a transaction
            continue;
          }
          // Continue to the next version
          current = current->next;
          continue;
        }

        // Has to be PreCommit or later stage
        // FIXME(tzwang): need pointer stability for [other_tx]
        while (other_tx->state.load(std::memory_order::acquire) == Committing) {}

        if (other_tx->state.load(std::memory_order::acquire) == State::Aborted) {
          // Retry from the beginning
          continue;
        } else {
          CSN other_csn = other_tx->commit_ts;
          if (other_xid != other_tx->xid.load(std::memory_order::acquire)) {
            // Transaction is possible not consistent during read
            // Retry till we can read a consistent state of a transaction
            continue;
          }
          LOG_IF(FATAL, other_csn == kInvalidCSN);
          if (other_csn < begin_ts) {
            // Found our guy!
            return current;
          } else {
            // The version is too new for me
            current = current->next;
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
      if (begin_ts > other_csn) {
        return current;
      } else {
        current = current->next;
      }
    }
  }

  // There is no proper version to read if we reached here
  return nullptr;
}

template <typename TABLE>
bool Transaction<TABLE>::Update(TABLE *table, table::OID oid,
                         table::Object *new_object) {
  new_object->id = reinterpret_cast<uint64_t>(this) | (1UL <<63);
  table::Object *current = table->Get(oid);
  table::Object **ptr = table->GetEntryPtr(oid);
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
      write_set.Add(ptr);
    }  // Otherwise it's not mine, have to abort
  } else {
    if (id < begin_ts) {
      new_object->next = current;

      // Attempt a CAS to install the new object
      success = __atomic_compare_exchange(ptr, &current, &new_object, false,
                                          __ATOMIC_RELEASE, __ATOMIC_ACQUIRE);
      if (success) {
        write_set.Add(ptr);
      }
    }  // Otherwise can't see the latest value, abort
  }

  return success;
}

template <typename TABLE>
bool Transaction<TABLE>::PreCommit() {
  if (write_set.count == 0) {
    // No need to increase csn_counter nor update state for read-only transaction
    return true;
  }
  // Put myself into "committing" state first
  state.store(State::Committing, std::memory_order::release);

  // Now acquire a commit timestamp - note that this must happen after state
  // becomes "committing" otherwise concurrent readers may miss my updates and
  // see an inconsistent snapshot.
  commit_ts = csn_counter.fetch_add(1, std::memory_order::relaxed);

  state.store(State::PreCommitted, std::memory_order::release);
  return true;
}

template <typename TABLE>
bool Transaction<TABLE>::PostCommit() {
  if (write_set.count == 0) {
    // No need to update state for read-only transaction
    return true;
  }

  for (uint32_t i = 0; i < write_set.count; ++i) {
    auto &w = write_set.entries[i];
    auto obj = *w;
    obj->id = commit_ts;
  }

  state.store(State::Committed, std::memory_order::release);

  // TODO(tzwang): generate and flush log
  return true;
}

template <typename TABLE>
void Transaction<TABLE>::Rollback() {
  for (uint32_t i = 0; i < write_set.count; ++i) {
    table::Object **ptr = write_set.entries[i];
    table::Object *obj = *ptr;
    *ptr = obj->next;
    // FIXME(tzwang): the object now is leaking
  }
}

thread_local uint64_t user_xid_counter = 0;
template <typename TABLE>
thread_local Transaction<TABLE> Transaction<TABLE>::user_thread_local_txn {};

template <typename TABLE>
Transaction<TABLE> *Transaction<TABLE>::BeginUser() {
  user_thread_local_txn.xid.store(kInvalidXID, std::memory_order::release);
  new (&user_thread_local_txn) Transaction;
  user_thread_local_txn.xid.store(++user_xid_counter, std::memory_order::release);
  return &user_thread_local_txn;
}

thread_local uint64_t system_xid_counter = 0;
template <typename TABLE>
thread_local Transaction<TABLE> Transaction<TABLE>::system_thread_local_txn {};
template <typename TABLE>
Transaction<TABLE> *Transaction<TABLE>::BeginSystem() {
  system_thread_local_txn.xid.store(kInvalidXID, std::memory_order::release);
  new (&system_thread_local_txn) Transaction;
  system_thread_local_txn.xid.store(++system_xid_counter, std::memory_order::release);
  return &system_thread_local_txn;
}

template class Transaction<table::IndirectionArrayTable>;

}  // namespace mvcc
}  // namespace transaction
}  // namespace noname

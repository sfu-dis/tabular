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

// Very basic transaction abstraction

#pragma once

#include <glog/logging.h>

#include "table/object.h"

namespace noname {
namespace transaction {
namespace mvcc {

/*
struct WriteSetEntry {
  OID oid;
  Object *object;
  WriteSetEntry() : oid(kInvalidOID), object(nullptr) {}
  WriteSetEntry(OID oid, Object *object) : oid(oid), object(object) {}
  ~WriteSetEntry() {}
};
*/

// Write set which essentially is an array. Each entry is simply a pointer to the corresponding
// record's indirection array entry. Doing so allows us to handle duplicates (repeated updates)
// easily without having to dedup the write set, as upon post-commit the transaction can just learn
// about the state of the record by dereferencing the write-set entry.
//
// For "normal" OLTP workloads their write sets won't be too big, so here it's hard coded to be 256
// entries. Revisit if this changes.
struct WriteSet {
  // Max size of the write set
  static const uint32_t kWriteSetSize = 256;

  // Entries
  table::Object **entries[kWriteSetSize];

  // Current number of entries
  uint32_t count;

  // Add (append) a new entry
  // @obj: pointer to the indirection array (templated type is Object*) entry
  inline void Add(table::Object **obj) {
    uint32_t i = count++;
    LOG_IF(FATAL, count > kWriteSetSize);
    entries[i] = obj;
  }

  WriteSet() : count(0) {}
  ~WriteSet() {}
};

typedef uint64_t CSN;
extern std::atomic<CSN> csn_counter;
static const CSN kInvalidCSN = ~CSN{0};

static const uint64_t kInvalidXID = 0;

template <typename TABLE>
struct Transaction {

  Transaction();
  ~Transaction();

  table::OID Insert(TABLE *table, table::Object *object);
  table::Object *Read(TABLE *table, table::OID oid);
  bool Update(TABLE *table, table::OID oid, table::Object *new_object);

  bool PreCommit();
  bool PostCommit();
  void Rollback();

  enum State {
    ForwardProcessing,
    Committing,
    PreCommitted,
    Committed,
    Aborted
  };

  static thread_local Transaction<TABLE> user_thread_local_txn;
  static Transaction<TABLE> *BeginUser();

  static thread_local Transaction<TABLE> system_thread_local_txn;
  static Transaction<TABLE> *BeginSystem();

  std::atomic<uint64_t> xid;
  std::atomic<State> state;
  CSN begin_ts;
  CSN commit_ts;
  WriteSet write_set;
};

}  // namespace mvcc
}  // namespace transaction
}  // namespace noname

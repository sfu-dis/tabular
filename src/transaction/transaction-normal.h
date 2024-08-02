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

// Very basic transaction abstraction

#pragma once

#include <glog/logging.h>
#include "table/ia_table.h"
#include "log/dlog/log.h"

namespace noname {
namespace transaction {

typedef std::unordered_map<table::FID, table::IndirectionArrayTable *> TableSet;

struct WriteSetEntry {
  table::OID oid;
  table::FID fid;
  table::Object **object;
  bool is_insert;

  WriteSetEntry() : fid(table::kInvalidOID), oid(table::kInvalidOID), object(nullptr), is_insert(false) {}
  WriteSetEntry(table::Object **object, table::FID fid, table::OID oid, bool is_insert)
      : fid(fid), oid(oid), object(object), is_insert(is_insert) {}
  ~WriteSetEntry() {}

  inline table::Object *GetObject(){ return *object; }
};

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
  WriteSetEntry entries[kWriteSetSize];

  // Current number of entries
  uint32_t count;

  // Add (append) a new entry
  // @obj: pointer to the indirection array (templated type is Object*) entry 
  inline void Add(table::Object **obj, table::FID fid, table::OID oid, bool insert) {
    uint32_t i = count++;
    LOG_IF(FATAL, count > kWriteSetSize);
    new (&entries[i]) WriteSetEntry(obj, fid, oid, insert);
  }

  WriteSet() : count(0) {}
  ~WriteSet() {}
};

typedef uint64_t CSN;
static const CSN kInvalidCSN = ~CSN{0};

struct Transaction {
  static std::atomic<CSN> csn_counter;

  Transaction();
  ~Transaction();

  table::OID Insert(table::IndirectionArrayTable *table, table::Object *object);
  table::Object *Read(table::IndirectionArrayTable *table, table::OID oid);
  bool Update(table::IndirectionArrayTable *table, table::OID oid, table::Object *new_object);

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

  uint32_t generation;
  State state;
  CSN begin_ts;
  CSN commit_ts;
  WriteSet write_set;
  dlog::Log *log;
  uint64_t log_size;
  TableSet table_set;
};

}  // namespace table
}  // namespace noname

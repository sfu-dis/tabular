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

// Noname engine abstraction

#pragma once

#include "index/generic_index.h"
#include "table/tuple.h"
#include "transaction/transaction-normal.h"

namespace noname {

struct Engine {
  struct ScanIterator {
    ScanIterator() : table(nullptr), result(nullptr), cur(0), total(0) {}
    ScanIterator(table::IndirectionArrayTable *table)
      : table(table), result(nullptr), cur(0), total(0) {}
    ~ScanIterator() {}

    inline table::Object *Next() {
    fetch_next:
      if (cur >= total) {
        return nullptr;
      }
      table::OID oid = *reinterpret_cast<table::OID *>(result + (cur++) * sizeof(table::OID));
      table::Object *obj = txn->Read(table, oid);
      if (!obj) {
        // Empty, fetch next record
        goto fetch_next;
      }
      table::Tuple *tuple = reinterpret_cast<table::Tuple *>(obj->data);
      if (tuple->GetOID() != oid) {
        // Already deleted, fetch next record
        goto fetch_next;
      }
      return obj;
    }

    inline table::Object *Get(uint32_t index) {
      table::OID oid = *reinterpret_cast<table::OID *>(result + index * sizeof(table::OID));
      table::Object *obj = txn->Read(table, oid);
      if (!obj) {
        return nullptr;
      }
      table::Tuple *tuple = reinterpret_cast<table::Tuple *>(obj->data);
      if (tuple->GetOID() != oid) {
        return nullptr;
      }
      return obj;
    }

    transaction::Transaction *txn;
    table::IndirectionArrayTable *table;
    char *result;
    uint32_t cur;
    uint32_t total;
  };

  transaction::Transaction *NewTransaction();
  void TransactionCommit();
  void TransactionRollback();
  catalog::SchemaInfo *TransactionGetSchemaInfo(const std::string &table_name);
  table::Tuple *TransactionRead(index::GenericIndex *index,
                                const char *key, size_t key_size,
                                table::IndirectionArrayTable *table);
  bool TransactionUpdate(table::IndirectionArrayTable *table, table::OID oid,
                         table::Object *new_obj);
  bool TransactionInsert(table::IndirectionArrayTable *table,
                         const char *tuple_data, size_t tuple_data_size,
                         index::GenericIndex *index,
                         const char *key, size_t key_size);
  bool TransactionDelete(table::IndirectionArrayTable *table, table::Object *old_obj);
  void TransactionScan(index::GenericIndex *index,
                       const char *start_key, bool start_key_inclusive,
                       const char *end_key, bool end_key_inclusive,
                       size_t key_sz, int scan_sz, bool reverse, ScanIterator &si);
};

}  // namespace ermia

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

#include "noname.h"

namespace noname {

thread_local transaction::Transaction tls_tx;

transaction::Transaction *Engine::NewTransaction() {
  new (&tls_tx) transaction::Transaction;
  return &tls_tx;
}

void Engine::TransactionCommit() {
  tls_tx.PreCommit();
  tls_tx.PostCommit();
}

void Engine::TransactionRollback() {
  tls_tx.Rollback();
}

catalog::SchemaInfo *Engine::TransactionGetSchemaInfo(const std::string &table_name) {
  return catalog::GetSchemaInfo(table_name, &tls_tx);
}

table::Tuple *Engine::TransactionRead(index::GenericIndex *index, const char *key,
                                      size_t key_size, table::IndirectionArrayTable *table) {
  char *index_result;
  auto success = index->Search(key, key_size, index_result);
  if (!success) {
    return nullptr;
  }
  table::OID oid = *reinterpret_cast<table::OID *>(index_result);
  auto *obj = tls_tx.Read(table, oid);
  if (!obj) {
    return nullptr;
  }
  table::Tuple *tuple = reinterpret_cast<table::Tuple *>(obj->data);
  if (tuple->GetOID() != oid) {
    // Already deleted
    return nullptr;
  }
  return tuple;
}

bool Engine::TransactionUpdate(table::IndirectionArrayTable *table, table::OID oid,
                               table::Object *new_obj) {
  return tls_tx.Update(table, oid, new_obj);
}

bool Engine::TransactionInsert(table::IndirectionArrayTable *table,
                               const char *tuple_data, size_t tuple_data_size,
                               index::GenericIndex *index,
                               const char *key, size_t key_size) {
  uint32_t new_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + tuple_data_size;
  auto *new_obj = table::Object::Make(new_obj_size);
  table::Tuple *new_tuple = reinterpret_cast<table::Tuple *>(new_obj->data);
  new_tuple->SetSize(tuple_data_size);
  new_tuple->SetStandard(true);
  memcpy(new_tuple->GetTupleData(), tuple_data, tuple_data_size);
  auto oid = tls_tx.Insert(table, new_obj);
  if (oid == table::kInvalidOID) {
    return false;
  }
  new_tuple->SetOID(oid);
  if (index) {
    auto success = index->Insert(key, key_size, reinterpret_cast<char *>(&oid),
                                 sizeof(table::OID));
    if (!success) {
      return false;
    }
  }
  return true;
}

bool Engine::TransactionDelete(table::IndirectionArrayTable *table, table::Object *old_obj) {
  table::Tuple *old_tuple = reinterpret_cast<table::Tuple *>(old_obj->data);
  auto oid = old_tuple->GetOID();
  uint32_t obj_size = sizeof(table::Object) + sizeof(table::Tuple);
  auto new_obj = table::Object::Make(obj_size);
  table::Tuple *tuple = reinterpret_cast<table::Tuple *>(new_obj->data);
  memcpy(tuple, old_tuple, sizeof(table::Tuple));
  tuple->SetOID(table::kInvalidOID);
  return tls_tx.Update(table, oid, new_obj);
}

void Engine::TransactionScan(index::GenericIndex *index,
                             const char *start_key, bool start_key_inclusive,
                             const char *end_key, bool end_key_inclusive,
                             size_t key_sz, int scan_sz, bool reverse, ScanIterator &si) {
  auto result_sz = reverse ? index->ReverseScan(start_key, start_key_inclusive, end_key,
                                                end_key_inclusive, key_sz, scan_sz,
                                                si.result)
                             :
                             index->Scan(start_key, start_key_inclusive, end_key, end_key_inclusive,
                                         key_sz, scan_sz, si.result);
  si.total = result_sz;
  si.txn = &tls_tx;
}

}  // namespace ermia

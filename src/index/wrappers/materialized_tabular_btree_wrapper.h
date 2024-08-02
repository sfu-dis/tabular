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
#pragma once

#ifndef MATERIALIZED_READ
#define MATERIALIZED_READ 
#endif

#ifndef MATERIALIZED_INSERT
#define MATERIALIZED_INSERT
#endif

#ifndef MATERIALIZED_UPDATE
#define MATERIALIZED_UPDATE
#endif


#include "index/generic_index.h"
#include "index/generic_key.h"
#include "tabular/inline_btree.h"
// #include <duckdb/common/exception.hpp>

namespace BTree {

template<typename Key>
struct MaterializedInlineBTreeWrapper : noname::index::GenericIndex {

  using BTreeType = noname::tabular::InlineBTree<Key, noname::table::OID>;

  explicit MaterializedInlineBTreeWrapper(size_t key_size, noname::table::IndirectionArrayTable *table)
    : GenericIndex(key_size, sizeof(noname::table::OID), table) {
     noname::table::config_t config(false, true /* use huge pages*/
                                         , 17179869184  /* capacity 16GB*/ 
					 , 4294967296 /* initial size 4GB*/ ); 
     btree = new BTreeType(config, false);
  }


  inline bool Insert(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
  inline bool Search(const char *key, size_t key_sz, char *&value_out) override;
  inline int Scan(const char *start_key, bool start_key_inclusive,
                  const char *end_key, bool end_key_inclusive,
                  size_t key_sz, int scan_sz, char *&value_out) override;
  inline int ReverseScan(const char *start_key, bool start_key_inclusive,
                         const char *end_key, bool end_key_inclusive,
                         size_t key_sz, int scan_sz, char *&value_out) override;
  inline bool Update(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
  inline bool Delete(const char *key, size_t key_sz) override;

 private:
  BTreeType *btree;
};

static thread_local char results[MB];

template<typename Key>
inline bool MaterializedInlineBTreeWrapper<Key>::Insert(const char *key, size_t key_sz,
  const char *value, size_t value_sz) {
  // CHECK(key_sz <= 40);	
  // CHECK(sizeof(Key) >= key_sz);
  // CHECK(sizeof(noname::table::OID) == value_sz);
  auto key_ptr = reinterpret_cast<const Key *>(key);
  auto value_ptr = reinterpret_cast<const noname::table::OID *>(value);
  return btree->insert(*key_ptr, *value_ptr);
}


template<typename Key>
bool MaterializedInlineBTreeWrapper<Key>::Search(const char *key,
 size_t key_sz, char *&value_out) {
  // CHECK(key_sz <= 40);
  // CHECK(sizeof(Key) >= key_sz);
  value_out = results; 
  auto key_ptr = reinterpret_cast<const Key *>(key);
  auto oid_value = reinterpret_cast<noname::table::OID *>(value_out);
  const bool found = btree->lookup(*key_ptr, *oid_value);
  return found;
}



template<typename Key>
inline int MaterializedInlineBTreeWrapper<Key>::Scan(const char *start_key, bool start_key_inclusive,
                  const char *end_key, bool end_key_inclusive,
                  size_t key_sz, int scan_sz, char *&value_out) {
  // static_assert(sizeof(Key) == key_sz);
  // throw duckdb::NotImplementedException("Scan not implemented yet");
  // CHECK(key_sz <= 40);
  // CHECK(sizeof(Key) >= key_sz);
  
  value_out = results;
  auto start_key_ptr = reinterpret_cast<const Key *>(start_key);
  auto end_key_ptr = reinterpret_cast<const Key *>(end_key);

  auto values = reinterpret_cast<noname::table::OID *>(value_out);
  auto count = btree->scan(*start_key_ptr, scan_sz, values, end_key_ptr);
  // skip first value if not inclusive
  value_out += (!start_key_inclusive)*sizeof(noname::table::OID);
  return count - !start_key_inclusive - !end_key_inclusive ;
}

template<typename Key>
inline int MaterializedInlineBTreeWrapper<Key>::ReverseScan(const char *start_key, bool start_key_inclusive,
                         const char *end_key, bool end_key_inclusive,
                         size_t key_sz, int scan_sz, char *&value_out) {
  // throw duckdb::NotImplementedException("Reverse Scan not implemented yet");
  // Do normal scan with reversed end and start key, then reverse value_out
  auto count = Scan(end_key, end_key_inclusive, start_key, 
		  start_key_inclusive, key_sz, scan_sz, value_out);
  auto values = reinterpret_cast<noname::table::OID *>(value_out);
  std::reverse(values, &values[count]);
  return count;
}

template<typename Key>
bool MaterializedInlineBTreeWrapper<Key>::Update(const char *key, size_t key_sz
  , const char *value, size_t value_sz) {
  //static_assert(sizeof(Key) == key_sz);
  //static_assert(sizeof(Value) == value_sz);
  // CHECK(key_sz <= 40);
  // CHECK(sizeof(Key) >= key_sz);
  // CHECK(sizeof(noname::table::OID) == value_sz);
  auto key_ptr = reinterpret_cast<const Key *>(key);
  auto value_ptr= reinterpret_cast<const noname::table::OID *>(value);
  return btree->update(*key_ptr, *value_ptr);
}

template<typename Key>
inline bool MaterializedInlineBTreeWrapper<Key>::Delete(const char *key, size_t key_sz) {
  return false;
  // throw duckdb::NotImplementedException("Delete not implemented yet");
}

/*
template struct MaterializedInlineBTreeWrapper<GenericTPCCKey<4>>;
template struct MaterializedInlineBTreeWrapper<GenericTPCCKey<8>>;
template struct MaterializedInlineBTreeWrapper<GenericTPCCKey<12>>;
template struct MaterializedInlineBTreeWrapper<GenericTPCCKey<16>>;
template struct MaterializedInlineBTreeWrapper<GenericTPCCKey<40>>;
*/

}


/*
 * Copyright 2018-2024 Stichting DuckDB Foundation

 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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

#include "catalog/catalog.h"

#ifdef MASSTREE_INDEX
#include "index/wrappers/masstree_wrapper.h"
#elif defined(BTREE_INDEX)
  #ifdef BTREE_INLINE_TABULAR
  #include "index/wrappers/inline_btree_wrapper.h"
  #elif defined(BTREE_NATIVE_OLC)
  #include "index/wrappers/olc_btree_wrapper.h"
  #elif defined(BTREE_NAIVE_TABULAR)
  #include "index/wrappers/naive_btree_wrapper.h"
  #elif defined(BTREE_MATERIALIZED_TABULAR)
  #include "index/wrappers/materialized_tabular_btree_wrapper.h"
  #endif
#include "index/generic_key.h"
#endif



namespace noname {
namespace catalog {


table::config_t schema_table_config =
  table::config_t(false, true /* 1GB hugepages */, 10000000000 /* capacity */);
table::IndirectionArrayTable *schema_table;
std::unordered_map<std::string, table::FID> table_name_fid_map;

bool InsertTableInfo(const std::string &table_name,
                     transaction::Transaction *transaction,
                     catalog::TableInfo &&table_info) {
  auto table = table_info.table;
  catalog::SchemaInfo schema_info(1, std::move(table_info));
  uint32_t obj_size = sizeof(table::Object) + sizeof(catalog::SchemaInfo);
  auto obj = table::Object::Make(obj_size);
  catalog::SchemaInfo *schema_info_in_obj = new (obj->data) catalog::SchemaInfo();
  *schema_info_in_obj = std::move(schema_info);
  auto oid = transaction->Insert(catalog::schema_table, obj);
  if (oid == table::kInvalidOID) {
    return false;
  }
  catalog::table_name_fid_map.emplace(table_name, oid);
  table->fid = oid;
  return true;
}

bool InsertIndexInfo(const std::string &table_name,
                     transaction::Transaction *transaction,
                     std::unique_ptr<duckdb::CreateIndexInfo> &&create_index_info_ptr) {
  auto oid = catalog::table_name_fid_map[table_name];
  auto *obj = transaction->Read(catalog::schema_table, oid);
  auto *schema_info = reinterpret_cast<catalog::SchemaInfo *>(obj->data);
  auto *table_info = &schema_info->table_info;

  uint32_t key_size = 0;
  for (auto &column_id : create_index_info_ptr->column_ids) {
    key_size += table_info->column_sizes[column_id];
  }

#ifdef MASSTREE_INDEX
  auto index = new Masstree::ConcurrentMasstree(key_size, sizeof(table::OID), table_info->table);
#elif defined(BTREE_INLINE_TABULAR)
  
  index::GenericIndex *index;
  if ( key_size == 4 ) {
    index = new BTree::InlineBTreeWrapper<GenericTPCCKey<4>>(key_size, table_info->table);
  } else if ( key_size == 8 ) { 	    
    index = new BTree::InlineBTreeWrapper<GenericTPCCKey<8>>(key_size, table_info->table);
  } else if ( key_size == 12 ) {
    index = new BTree::InlineBTreeWrapper<GenericTPCCKey<12>>(key_size, table_info->table);
  } else if ( key_size == 16 ) {
    index = new BTree::InlineBTreeWrapper<GenericTPCCKey<16>>(key_size, table_info->table);
  } else if ( key_size <= 40 ) {
    CHECK(key_size > 16);	  
    index = new BTree::InlineBTreeWrapper<GenericTPCCKey<40>>(key_size, table_info->table);
  } else {	  
    // LOG(FATAL, "Maximum key size in TPP-C is 40");
    std::cout << "Maximum key size in TPP-C is 40" << std::endl;
    std::exit(1);
  }
#elif defined(BTREE_NATIVE_OLC) 
 index::GenericIndex *index;
  if ( key_size == 4 ) {
    index = new BTree::OLCBTreeWrapper<GenericTPCCKey<4>>(key_size, table_info->table);
  } else if ( key_size == 8 ) { 	    
    index = new BTree::OLCBTreeWrapper<GenericTPCCKey<8>>(key_size, table_info->table);
  } else if ( key_size == 12 ) {
    index = new BTree::OLCBTreeWrapper<GenericTPCCKey<12>>(key_size, table_info->table);
  } else if ( key_size == 16 ) {
    index = new BTree::OLCBTreeWrapper<GenericTPCCKey<16>>(key_size, table_info->table);
  } else if ( key_size <= 40 ) {
    CHECK(key_size > 16);	  
    index = new BTree::OLCBTreeWrapper<GenericTPCCKey<40>>(key_size, table_info->table);
  } else {	  
    // LOG(FATAL, "Maximum key size in TPP-C is 40");
    std::cout << "Maximum key size in TPP-C is 40" << std::endl;
    std::exit(1);
  }

#elif defined(BTREE_MATERIALIZED_TABULAR)
  index::GenericIndex *index;
  if ( key_size == 4 ) {
    index = new BTree::MaterializedInlineBTreeWrapper<GenericTPCCKey<4>>(key_size, table_info->table);
  } else if ( key_size == 8 ) { 	    
    index = new BTree::MaterializedInlineBTreeWrapper<GenericTPCCKey<8>>(key_size, table_info->table);
  } else if ( key_size == 12 ) {
    index = new BTree::MaterializedInlineBTreeWrapper<GenericTPCCKey<12>>(key_size, table_info->table);
  } else if ( key_size == 16 ) {
    index = new BTree::MaterializedInlineBTreeWrapper<GenericTPCCKey<16>>(key_size, table_info->table);
  } else if ( key_size <= 40 ) {
    CHECK(key_size > 16);	  
    index = new BTree::MaterializedInlineBTreeWrapper<GenericTPCCKey<40>>(key_size, table_info->table);
  } else {	  
    // LOG(FATAL, "Maximum key size in TPP-C is 40");
    std::cout << "Maximum key size in TPP-C is 40" << std::endl;
    std::exit(1);
  }

#elif defined(BTREE_NAIVE_TABULAR)
  index::GenericIndex *index;
  if ( key_size == 4 ) {
    index = new BTree::NaiveBTreeWrapper<GenericTPCCKey<4>>(key_size, table_info->table);
  } else if ( key_size == 8 ) { 	    
    index = new BTree::NaiveBTreeWrapper<GenericTPCCKey<8>>(key_size, table_info->table);
  } else if ( key_size == 12 ) {
    index = new BTree::NaiveBTreeWrapper<GenericTPCCKey<12>>(key_size, table_info->table);
  } else if ( key_size == 16 ) {
    index = new BTree::NaiveBTreeWrapper<GenericTPCCKey<16>>(key_size, table_info->table);
  } else if ( key_size <= 40 ) {
    CHECK(key_size > 16);	  
    index = new BTree::NaiveBTreeWrapper<GenericTPCCKey<40>>(key_size, table_info->table);
  } else {	  
    // LOG(FATAL, "Maximum key size in TPP-C is 40");
    std::cout << "Maximum key size in TPP-C is 40" << std::endl;
    std::exit(1);
  }
#endif

  catalog::IndexInfo index_info(index, std::move(create_index_info_ptr));


  uint32_t obj_size = sizeof(table::Object) + sizeof(catalog::SchemaInfo);
  auto new_obj = table::Object::Make(obj_size);
  catalog::SchemaInfo *schema_info_in_obj = new (new_obj->data) catalog::SchemaInfo();
  *schema_info_in_obj = *schema_info;
  schema_info_in_obj->schema_version++;
  schema_info_in_obj->index_infos.emplace_back(std::move(index_info));

  bool success = transaction->Update(catalog::schema_table, oid, new_obj);
  if (!success) {
    return false;
  }
  return true;
}

SchemaInfo *GetSchemaInfo(const std::string &table_name, transaction::Transaction *transaction,
                          bool blind_write, table::OID *out_oid) {
  auto oid = catalog::table_name_fid_map[table_name];
retry:  
  auto *obj = transaction->Read(catalog::schema_table, oid);
  auto *schema_info = reinterpret_cast<catalog::SchemaInfo *>(obj->data);
  if (!blind_write && !schema_info->ready) {
    goto retry;
  }
  if (out_oid) {
    *out_oid = oid;
  }
  return schema_info;
}

IndexInfo *GetIndexInfo(SchemaInfo *schema_info,
                        std::vector<std::unique_ptr<duckdb::Expression>> &filter_expressions) {
  catalog::IndexInfo *index_info = nullptr;
  auto &indexes_info = schema_info->index_infos;
  uint32_t index_column_total = 0;
  std::unordered_map<std::string, bool> column_names;
  for (auto &index_info_ : indexes_info) {
    auto create_index_info =
      reinterpret_cast<duckdb::CreateIndexInfo *>(index_info_.create_index_info.get());
    bool satisfied = true;
    for (auto &expression : filter_expressions) {
      auto &bound_comparison_expression = (duckdb::BoundComparisonExpression &)*(expression);
      auto &bound_ref_expression = (duckdb::BoundReferenceExpression &)*(bound_comparison_expression.left);
      column_names.emplace(bound_ref_expression.GetName(), true);
    }
    for (auto &column_id : create_index_info->column_ids) {
      if (!column_names[create_index_info->names[column_id]]) {
        satisfied = false;
        break;
      }
    }
    uint32_t column_ids_size = create_index_info->column_ids.size();
    // only the one with all index columns included in scan plan and
    // with the largest number of index column can be the winner of index info.
    if (satisfied && column_ids_size > index_column_total) {
      index_info = &index_info_;
      index_column_total = column_ids_size;
    }
  }
  return index_info;
}

void Initialize() {
  schema_table = new table::IndirectionArrayTable(schema_table_config);
}

void Uninitialize() {
  // FIXME(hutx): schema records on version chains will leak
  delete schema_table;
  table_name_fid_map.clear();
}

}  // namespace catalog
}  // namespace noname


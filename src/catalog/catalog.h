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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "index/generic_index.h"
#include "table/ia_table.h"
#include "table/tuple.h"
#include "transaction/transaction-normal.h"
#include "sql/query.h"

namespace noname {
namespace catalog {

struct TableInfo {
  TableInfo() {}
  TableInfo(table::IndirectionArrayTable *table_,
            duckdb::CreateTableInfo &create_table_info_,
            std::vector<uint32_t> &column_offsets_, std::vector<uint32_t> &column_sizes_,
            std::vector<duckdb::LogicalType> &column_types_, uint32_t fixed_size_)
      : table(table_), column_offsets(column_offsets_),
        column_sizes(column_sizes_), column_types(column_types_),
        fixed_size(fixed_size_) {
    create_table_info = create_table_info_.Copy();
  }
  TableInfo(table::IndirectionArrayTable *table_,
            duckdb::CreateTableInfo &create_table_info_,
            std::vector<uint32_t> &&column_offsets_, std::vector<uint32_t> &&column_sizes_,
            std::vector<duckdb::LogicalType> &&column_types_, uint32_t fixed_size_)
      : table(table_), column_offsets(std::move(column_offsets_)),
        column_sizes(std::move(column_sizes_)), column_types(std::move(column_types_)),
        fixed_size(fixed_size_) {
    create_table_info = create_table_info_.Copy();
  }
  ~TableInfo() {}

  TableInfo(const TableInfo &victim) noexcept
    : table(victim.table), column_offsets(victim.column_offsets),
      column_sizes(victim.column_sizes), column_types(victim.column_types),
      fixed_size(victim.fixed_size) {
    create_table_info = victim.create_table_info->Copy();
  }

  void operator=(const TableInfo &victim) noexcept {
    table = victim.table;
    column_offsets = victim.column_offsets;
    column_sizes = victim.column_sizes;
    column_types = victim.column_types;
    fixed_size = victim.fixed_size;
    create_table_info = victim.create_table_info->Copy();
  }

  TableInfo(TableInfo &&victim) noexcept
    : table(victim.table), column_offsets(std::move(victim.column_offsets)),
      column_sizes(std::move(victim.column_sizes)),
      column_types(std::move(victim.column_types)),
      fixed_size(victim.fixed_size) {
    create_table_info = victim.create_table_info->Copy();
  }

  TableInfo& operator=(TableInfo &&victim) noexcept {
    table = victim.table;
    column_offsets = std::move(victim.column_offsets);
    column_sizes = std::move(victim.column_sizes);
    column_types = std::move(victim.column_types);
    fixed_size = victim.fixed_size;
    create_table_info = victim.create_table_info->Copy();
    return *this;
  }

  table::IndirectionArrayTable *table;
  std::unique_ptr<duckdb::CreateInfo> create_table_info;
  std::vector<uint32_t> column_offsets;
  std::vector<uint32_t> column_sizes; // max size of each column
  std::vector<duckdb::LogicalType> column_types;
  uint32_t fixed_size;
};

struct IndexInfo {
  IndexInfo() {}
  IndexInfo(index::GenericIndex *index_,
            std::unique_ptr<duckdb::CreateInfo> &&create_index_info_)
      : index(index_), create_index_info(std::move(create_index_info_)) {}
  ~IndexInfo() {}

  IndexInfo(const IndexInfo &victim) noexcept
    : index(victim.index) {
    create_index_info = victim.create_index_info->Copy();
  }

  void operator=(const IndexInfo &victim) noexcept {
    index = victim.index;
    create_index_info = victim.create_index_info->Copy();
  }

  IndexInfo(IndexInfo &&victim) noexcept
    : index(std::move(victim.index)), create_index_info(std::move(victim.create_index_info)) {
  }

  IndexInfo& operator=(IndexInfo &&victim) noexcept {
    index = std::move(victim.index);
    create_index_info = std::move(victim.create_index_info);
    return *this;
  }

  index::GenericIndex *index;
  std::unique_ptr<duckdb::CreateInfo> create_index_info;
};

struct SchemaInfo {
  SchemaInfo() {}
  SchemaInfo(uint64_t schema_version, TableInfo &&table_info)
    : ready(true), schema_version(schema_version), table_info(std::move(table_info)) {}
  ~SchemaInfo() {}

  SchemaInfo(const SchemaInfo &victim) noexcept
    : ready(ready), schema_version(victim.schema_version), table_info(victim.table_info) {
    for (auto &index_info : victim.index_infos) {
      index_infos.push_back(index_info);
    }
  }

  void operator=(const SchemaInfo &victim) noexcept {
    ready = victim.ready;
    schema_version = victim.schema_version;
    table_info = victim.table_info;
    for (auto &index_info : victim.index_infos) {
      index_infos.push_back(index_info);
    }
  }

  SchemaInfo(SchemaInfo &&victim) noexcept
    : ready(ready), schema_version(schema_version), table_info(std::move(table_info)),
      index_infos(std::move(index_infos)) {
  }

  SchemaInfo& operator=(SchemaInfo &&victim) noexcept {
    ready = victim.ready;
    schema_version = victim.schema_version;
    table_info = std::move(victim.table_info);
    index_infos = std::move(victim.index_infos);
    return *this;
  }

  bool ready;
  uint64_t schema_version;
  TableInfo table_info;
  std::vector<IndexInfo> index_infos;
};

void Initialize();
void Uninitialize();
bool InsertTableInfo(const std::string &table_name,
                     transaction::Transaction *transaction,
                     catalog::TableInfo &&table_info);
bool InsertIndexInfo(const std::string &table_name,
                     transaction::Transaction *transaction,
                     std::unique_ptr<duckdb::CreateIndexInfo> &&create_index_info_ptr);
SchemaInfo *GetSchemaInfo(const std::string &table_name,
                          transaction::Transaction *transaction,
                          bool blind_write = false, table::OID *out_oid = nullptr);
IndexInfo *GetIndexInfo(SchemaInfo *schema_info,
                        std::vector<std::unique_ptr<duckdb::Expression>> &filter_expressions);

extern table::config_t schema_table_config;
extern table::IndirectionArrayTable *schema_table;
extern std::unordered_map<std::string, table::FID> table_name_fid_map;

}  // namespace catalog
}  // namespace noname

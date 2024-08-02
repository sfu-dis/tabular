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

// duckdb as a planner

#pragma once

#include <memory>
#include <string>

#include "sql/duckdb_internal.h"

#define DUCK_VALUE_SIZE 40
// 31 is the base size of a serializable duckdb constant-size value
#define DUCK_SERIAL_CONSTANT_VALUE_BASE_SIZE 31
// 35 is the base size of a serializable duckdb varchar value
#define DUCK_SERIAL_VAR_VALUE_BASE_SIZE 35
#define DUCK_SERIAL_VALUE_DATA_OFFSET 12

namespace noname {
namespace sql {

#ifdef RUN_BENCHMARK
extern duckdb::DuckDB db_;
#endif

struct DuckDbQuery {
  DuckDbQuery() {}
  ~DuckDbQuery() {}

  duckdb::vector<std::unique_ptr<duckdb::SQLStatement>> ExtractStatements(const std::string &sql);

  std::unique_ptr<duckdb::LogicalOperator> CreatePlan(const std::string &sql);

  // Exception thrown when create_index_info.Copy(), seems a bug in duckdb,
  // so just copy fields without create_index_info.expressions
  inline static std::unique_ptr<duckdb::CreateIndexInfo>
  CopyCreateIndexInfo(duckdb::CreateIndexInfo &create_index_info) {
    auto create_index_info_ptr = std::make_unique<duckdb::CreateIndexInfo>();
    create_index_info.CopyProperties(*create_index_info_ptr);
    create_index_info_ptr->index_type = create_index_info.index_type;
    create_index_info_ptr->index_name = create_index_info.index_name;
    create_index_info_ptr->constraint_type = create_index_info.constraint_type;
    create_index_info_ptr->table =
      duckdb::unique_ptr_cast<duckdb::TableRef, duckdb::BaseTableRef>(
        create_index_info.table->Copy()
      );
    // for (auto &expr : create_index_info.expressions) {
    //   create_index_info_ptr->expressions.push_back(expr->Copy());
    // }
    for (auto &expr : create_index_info.parsed_expressions) {
      create_index_info_ptr->parsed_expressions.push_back(expr->Copy());
    }
    create_index_info_ptr->scan_types = create_index_info.scan_types;
    create_index_info_ptr->names = create_index_info.names;
    create_index_info_ptr->column_ids = create_index_info.column_ids;
    return std::move(create_index_info_ptr);
  }

#ifndef RUN_BENCHMARK
  duckdb::DuckDB db_;
#endif
  duckdb::Connection conn_{db_};
};

}  // namespace sql
}  // namespace noname


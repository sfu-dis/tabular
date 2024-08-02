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

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement_data.hpp
//
//
//===----------------------------------------------------------------------===//

namespace duckdb {
class CatalogEntry;
class ClientContext;
class PhysicalOperator;
class SQLStatement;

class PreparedStatementData {
 public:
  DUCKDB_API explicit PreparedStatementData(StatementType type);
  DUCKDB_API ~PreparedStatementData();

  StatementType statement_type;
  //! The unbound SQL statement that was prepared
  unique_ptr<SQLStatement> unbound_statement;
  //! The fully prepared logical plan of the prepared statement
  //! XXX(hutx): In duckdb, here is physical plan
  unique_ptr<LogicalOperator> plan;
  //! The map of parameter index to the actual value entry
  bound_parameter_map_t value_map;

  //! The result names of the transaction
  vector<string> names;
  //! The result types of the transaction
  vector<LogicalType> types;

  //! The statement properties
  StatementProperties properties;

  //! The catalog version of when the prepared statement was bound
  //! If this version is lower than the current catalog version, we have to
  //! rebind the prepared statement
  idx_t catalog_version;

 public:
  void CheckParameterCount(idx_t parameter_count);
  //! Whether or not the prepared statement data requires the query to rebound
  //! for the given parameters
  bool RequireRebind(ClientContext &context, const vector<Value> &values);
  //! Bind a set of values to the prepared statement data
  DUCKDB_API void Bind(vector<Value> values);
  //! Get the expected SQL Type of the bound parameter
  DUCKDB_API LogicalType GetType(idx_t param_index);
  //! Try to get the expected SQL Type of the bound parameter
  DUCKDB_API bool TryGetType(idx_t param_idx, LogicalType &result);
};

}  // namespace duckdb

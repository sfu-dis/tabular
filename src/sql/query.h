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
#include <utility>
#include <glog/logging.h>

#include "sql/duckdb_query.h"
#include "sql/execution/execution_context.h"
#include "sql/execution/pull/pull_executor.h"
#include "sql/query_result.h"

namespace noname {
namespace sql {

struct PreparedStatement {
  std::shared_ptr<duckdb::PreparedStatementData> statement_data;
  std::unique_ptr<PullExecutor> executor;
  duckdb::vector<duckdb::Value> values;
};

struct SQLQuery {
  SQLQuery() : transaction(nullptr) {
    // Disable duckdb optimizer since it is cost-based,
    // but statistics are not maintained in duckdb side.
    // TODO(hutx): implemente an optimizer on our side.
    duckdb_query.conn_.Query("PRAGMA disable_optimizer");
  }
  SQLQuery(transaction::Transaction *transaction) : transaction(transaction) {
    duckdb_query.conn_.Query("PRAGMA disable_optimizer");
  }
  ~SQLQuery() {}

  void Prepare(const std::string &sql, PreparedStatement *prepared_statement);
  std::shared_ptr<duckdb::PreparedStatementData> PrepareInternal(const std::string &sql);
  bool ExecutePrepared(PreparedStatement *prepared_statement, QueryResult *query_result = nullptr);

  void BindBool(PreparedStatement *prepared_statement, uint64_t param_idx, bool val);
  void BindInt8(PreparedStatement *prepared_statement, uint64_t param_idx, int8_t val);
  void BindInt16(PreparedStatement *prepared_statement, uint64_t param_idx, int16_t val);
  void BindInt32(PreparedStatement *prepared_statement, uint64_t param_idx, int32_t val);
  void BindInt64(PreparedStatement *prepared_statement, uint64_t param_idx, int64_t val);
  void BindUInt8(PreparedStatement *prepared_statement, uint64_t param_idx, uint8_t val);
  void BindUInt16(PreparedStatement *prepared_statement, uint64_t param_idx, uint16_t val);
  void BindUInt32(PreparedStatement *prepared_statement, uint64_t param_idx, uint32_t val);
  void BindUInt64(PreparedStatement *prepared_statement, uint64_t param_idx, uint64_t val);
  void BindFloat(PreparedStatement *prepared_statement, uint64_t param_idx, float val);
  void BindDouble(PreparedStatement *prepared_statement, uint64_t param_idx, double val);
  void BindTimestamp(PreparedStatement *prepared_statement, uint64_t param_idx, duckdb::timestamp_t val);
  void BindVarchar(PreparedStatement *prepared_statement, uint64_t param_idx, const char *val);
  void BindVarchar(PreparedStatement *prepared_statement, uint64_t param_idx, std::string &val);
  void BindNull(PreparedStatement *prepared_statement, uint64_t param_idx);
  void BindValue(PreparedStatement *prepared_statement, uint64_t param_idx, duckdb::Value val);

  bool Query(const std::string &sql, QueryResult *query_result = nullptr);
  bool QueryInternal(const std::string &sql, duckdb::LogicalOperator &plan,
                     PullExecutor *pull_executor, PreparedStatement *prepared_statement,
                     QueryResult *query_result);

  bool CreateTable(const std::string &sql, duckdb::LogicalOperator &plan);
  bool InsertTable(const std::string &sql, duckdb::LogicalOperator &plan);
  bool CreateIndex(const std::string &sql, duckdb::LogicalOperator &plan);

  transaction::Transaction *transaction;
  DuckDbQuery duckdb_query;
};

}  // namespace sql
}  // namespace noname


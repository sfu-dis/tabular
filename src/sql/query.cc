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
#include "sql/execution/pull/pull_executor.h"
#include "sql/query.h"
#include "sql/query_result.h"
#include "table/ia_table.h"
#include "table/tuple.h"

namespace noname {
namespace sql {

thread_local transaction::StringArena arena(16);
thread_local std::string *tls_table_name;

void SQLQuery::Prepare(const std::string &sql, PreparedStatement *prepared_statement) {
  prepared_statement->statement_data = PrepareInternal(sql);
  prepared_statement->executor = nullptr;
}

std::shared_ptr<duckdb::PreparedStatementData> SQLQuery::PrepareInternal(const std::string &sql) {
  auto statements = duckdb_query.ExtractStatements(sql);
  duckdb::StatementType statement_type = statements[0]->type;
  auto result = std::make_shared<duckdb::PreparedStatementData>(statement_type);
  auto unbound_statement = statements[0]->Copy();
  result->unbound_statement = std::move(unbound_statement);
  std::unique_ptr<duckdb::LogicalOperator> plan;
  duckdb_query.conn_.context->RunFunctionInTransaction([&]() {
    duckdb::Planner planner(*duckdb_query.conn_.context);
    planner.CreatePlan(std::move(statements[0]));

    result->properties = planner.properties;
    result->names = planner.names;
    result->types = planner.types;
    result->value_map = std::move(planner.value_map);
    result->catalog_version = 0;

    plan = std::move(planner.plan);

    duckdb::ColumnBindingResolver resolver;
    resolver.Verify(*plan);
    resolver.VisitOperator(*plan);

    plan->ResolveOperatorTypes();
    result->plan = std::move(plan);
  });
  return result;
}

void SQLQuery::BindBool(PreparedStatement *prepared_statement, uint64_t param_idx, bool val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::BOOLEAN(val));
}

void SQLQuery::BindInt8(PreparedStatement *prepared_statement, uint64_t param_idx, int8_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::TINYINT(val));
}

void SQLQuery::BindInt16(PreparedStatement *prepared_statement, uint64_t param_idx, int16_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::SMALLINT(val));
}

void SQLQuery::BindInt32(PreparedStatement *prepared_statement, uint64_t param_idx, int32_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::INTEGER(val));
}

void SQLQuery::BindInt64(PreparedStatement *prepared_statement, uint64_t param_idx, int64_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::BIGINT(val));
}

void SQLQuery::BindUInt8(PreparedStatement *prepared_statement, uint64_t param_idx, uint8_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::UTINYINT(val));
}

void SQLQuery::BindUInt16(PreparedStatement *prepared_statement, uint64_t param_idx, uint16_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::USMALLINT(val));
}

void SQLQuery::BindUInt32(PreparedStatement *prepared_statement, uint64_t param_idx, uint32_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::UINTEGER(val));
}

void SQLQuery::BindUInt64(PreparedStatement *prepared_statement, uint64_t param_idx, uint64_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::UBIGINT(val));
}

void SQLQuery::BindFloat(PreparedStatement *prepared_statement, uint64_t param_idx, float val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::FLOAT(val));
}

void SQLQuery::BindDouble(PreparedStatement *prepared_statement, uint64_t param_idx, double val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::DOUBLE(val));
}

void SQLQuery::BindTimestamp(PreparedStatement *prepared_statement,
                             uint64_t param_idx, duckdb::timestamp_t val) {
  BindValue(prepared_statement, param_idx, duckdb::Value::TIMESTAMP(val));
}

void SQLQuery::BindVarchar(PreparedStatement *prepared_statement,
                           uint64_t param_idx, const char *val) {
  BindValue(prepared_statement, param_idx, duckdb::Value(val));
}

void SQLQuery::BindVarchar(PreparedStatement *prepared_statement,
                           uint64_t param_idx, std::string &val) {
  BindValue(prepared_statement, param_idx, duckdb::Value(val));
}

void SQLQuery::BindNull(PreparedStatement *prepared_statement, uint64_t param_idx) {
  BindValue(prepared_statement, param_idx, duckdb::Value());
}

void SQLQuery::BindValue(PreparedStatement *prepared_statement, uint64_t param_idx,
                          duckdb::Value val) {
  LOG_IF(FATAL, !prepared_statement || !prepared_statement->statement_data || param_idx <= 0 ||
    param_idx > prepared_statement->statement_data->unbound_statement->n_param) <<
    "Please check your prepared statement or parameters";
  if (param_idx > prepared_statement->values.size()) {
    prepared_statement->values.resize(param_idx);
  }
  prepared_statement->values[param_idx - 1] = val;
}

bool SQLQuery::ExecutePrepared(PreparedStatement *prepared_statement, QueryResult *query_result) {
  prepared_statement->statement_data->Bind(prepared_statement->values);
  return QueryInternal(prepared_statement->statement_data->unbound_statement->query,
                       *prepared_statement->statement_data->plan,
                       prepared_statement->executor.get(),
                       prepared_statement, query_result);
}

bool SQLQuery::Query(const std::string &sql, QueryResult *query_result) {
  auto plan_ptr = duckdb_query.CreatePlan(sql);
  return QueryInternal(sql, *plan_ptr, nullptr, nullptr, query_result);
}

bool SQLQuery::QueryInternal(const std::string &sql, duckdb::LogicalOperator &plan,
                             PullExecutor *pull_executor, PreparedStatement *prepared_statement,
                             QueryResult *query_result) {
  arena.Reset();

  switch (plan.type) {
  case duckdb::LogicalOperatorType::LOGICAL_CREATE_TABLE:
    return CreateTable(sql, plan);
  case duckdb::LogicalOperatorType::LOGICAL_INSERT:
    if (prepared_statement) {
      tls_table_name = &reinterpret_cast<duckdb::InsertStatement *>(
        prepared_statement->statement_data->unbound_statement.get())->table;
    } else {
      auto &insert_plan = (duckdb::LogicalInsert &)plan;
      tls_table_name = &insert_plan.table->name;
    }
    return InsertTable(sql, plan);
  case duckdb::LogicalOperatorType::LOGICAL_CREATE_INDEX:
    return CreateIndex(sql, plan);
  default:
    return false;
  }
}

bool SQLQuery::CreateTable(const std::string &sql, duckdb::LogicalOperator &plan) {
  auto &create_table_plan = (duckdb::LogicalCreateTable &)plan;
  auto &create_table_info = (duckdb::CreateTableInfo &)*create_table_plan.info->base;

  uint32_t offset = 0;
  std::vector<uint32_t> column_offsets;
  std::vector<uint32_t> column_sizes;
  for (auto &type : create_table_info.columns.GetColumnTypes()) {
    const auto internal_type = type.InternalType();
    if (duckdb::TypeIsConstantSize(internal_type)) {
      column_offsets.emplace_back(offset);
      uint32_t size = duckdb::GetTypeIdSize(internal_type);
      offset += size;
      column_sizes.emplace_back(size);
    } else if (internal_type == duckdb::PhysicalType::VARCHAR) {
      column_offsets.emplace_back(offset);
      offset += sizeof(uint32_t);
      column_sizes.emplace_back(0);
    } else {
      return false;
    }
  }

  std::vector<uint32_t>::iterator it;
  uint32_t tuple_size = offset;

  std::string sql_ = sql;
  std::string del = "char(";
  transform(sql_.begin(), sql_.end(), sql_.begin(), ::tolower);
  int end = sql_.find(del);
  while (end != -1) {
    int sub_size = 0;
    end += del.size();
    while (std::isdigit(sql_.at(end + sub_size))) {
      sub_size++;
    }
    if (sub_size) {
      uint32_t size = stoull(sql_.substr(end, end + sub_size - 1));
      tuple_size += size;
      it = find(column_sizes.begin(), column_sizes.end(), 0);
      if (it != column_sizes.end()) {
        *it = size;
      }
    }
    end += sub_size;
    sql_.erase(sql_.begin(), sql_.begin() + end);
    end = sql_.find(del);
  }

  table::config_t config(false, true /* 1GB hugepages */, 10000000000 /* capacity */);
  auto &table_name = create_table_info.table;
  auto table = new table::IndirectionArrayTable(config);
  catalog::TableInfo table_info(
    std::move(table), create_table_info, std::move(column_offsets), std::move(column_sizes),
    std::move(create_table_info.columns.GetColumnTypes()), offset
  );
  bool success = catalog::InsertTableInfo(table_name, transaction, std::move(table_info));
  if (!success) {
    return false;
  }
  auto result = duckdb_query.conn_.Query(sql);
  return !result->HasError();
}

bool SQLQuery::InsertTable(const std::string &sql, duckdb::LogicalOperator &plan) {
  auto &insert_plan = (duckdb::LogicalInsert &)plan;
  auto *schema_info = catalog::GetSchemaInfo(*tls_table_name, transaction, true);
  auto *table_info = &schema_info->table_info;
  auto *table = table_info->table;
  auto &expression_get_plan = (duckdb::LogicalExpressionGet &)*insert_plan.children[0]->children[0];

  for (auto &expressions : expression_get_plan.expressions) {
    arena.Reset();
    char *tuple_data = reinterpret_cast<char *>(arena.Next(1 << 14));
    char *key_data = reinterpret_cast<char *>(arena.Next(1 << 14));

    table::Tuple *tuple = reinterpret_cast<table::Tuple *>(tuple_data);
    tuple->CreateTuple(expressions, table_info);

    uint32_t obj_size = sizeof(table::Object) + sizeof(table::Tuple) + tuple->GetSize();
    auto obj = table::Object::Make(obj_size);
    memcpy(&obj->data[0], tuple, sizeof(table::Tuple) + tuple->GetSize());
    auto oid = transaction->Insert(table, obj);
    if (oid == table::kInvalidOID) {
      return false;
    }
    table::Tuple *tuple_in_obj = reinterpret_cast<table::Tuple *>(&obj->data);
    tuple_in_obj->SetOID(oid);

    for (auto &index_info : schema_info->index_infos) {
      table::Tuple *key = reinterpret_cast<table::Tuple *>(key_data);
      key->CreateKeyFromTuple(tuple, &index_info, table_info);
      auto index = index_info.index;
      auto success = index->Insert(key->GetTupleData(), key->GetSize(),
                                   reinterpret_cast<char *>(&oid), sizeof(table::OID));
      if (!success) {
        return false;
      }
#ifndef NDEBUG
      char *oid_out;
      success = index->Search(key->GetTupleData(), key->GetSize(), oid_out);
      if (!success || *reinterpret_cast<table::OID *>(oid_out) != oid) {
        return false;
      }
#endif
    }
  }

  return true;
}

bool SQLQuery::CreateIndex(const std::string &sql, duckdb::LogicalOperator &plan) {
  auto &create_index_plan = (duckdb::LogicalCreateIndex &)plan;
  auto &create_index_info = (duckdb::CreateIndexInfo &)*create_index_plan.info;

  auto create_index_info_ptr = DuckDbQuery::CopyCreateIndexInfo(create_index_info);

  auto &table_name = create_index_info.table->table_name;
  auto *schema_info = catalog::GetSchemaInfo(table_name, transaction);
  auto *table_info = &schema_info->table_info;
  auto *table = table_info->table;

  bool success = catalog::InsertIndexInfo(table_name, transaction,
                                          std::move(create_index_info_ptr));
  if (!success) {
    return false;
  }

  // XXX(hutx): In DuckDB, no idea why table cannot be altered after creating an index,
  //   CREATE TABLE t0(i INTEGER, j INTEGER, k INTEGER)
  //   CREATE INDEX t0_i ON t0 (i)
  //   ALTER TABLE t0 ADD COLUMN m INTEGER DEFAULT 10 ------> Dependency Error:
  //     Cannot alter entry "t0" because there are entries that depend on it.
  //   For now, it is OK if we do not register index in DuckDB as we do not have optimizer.
  // auto result = duckdb_query.conn_.Query(sql);
  // return !result->HasError();
  return true;
}

}  // namespace sql
}  // namespace noname


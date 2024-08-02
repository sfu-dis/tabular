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

#include "table/tuple.h"

namespace noname {
namespace table {

void Tuple::CreateTuple(std::vector<std::unique_ptr<duckdb::Expression>> &expressions,
                        catalog::TableInfo *table_info) {
  uint32_t column_idx = 0;
  uint32_t variable_length_payload_offset = table_info->fixed_size;
  for (auto &expression : expressions) {
    duckdb::Value value;
    if (expression->GetExpressionType() == duckdb::ExpressionType::VALUE_CONSTANT) {
      auto &bound_constant_expression = (duckdb::BoundConstantExpression &)*expression;
      value = bound_constant_expression.value;
    } else if (expression->GetExpressionType() == duckdb::ExpressionType::VALUE_PARAMETER) {
      auto &bound_param_expression = (duckdb::BoundParameterExpression &)*expression;
      value = bound_param_expression.parameter_data->value;
    }

    if (expression->GetExpressionType() == duckdb::ExpressionType::VALUE_CONSTANT ||
        expression->GetExpressionType() == duckdb::ExpressionType::VALUE_PARAMETER) {
      const auto &type = value.type();
      const auto &internal_type = type.InternalType();
      if (duckdb::TypeIsConstantSize(internal_type)) {
      } else if (internal_type == duckdb::PhysicalType::VARCHAR) {
        auto str_value = duckdb::StringValue::Get(value);
        uint32_t str_value_size = (uint32_t)str_value.size();
        memcpy(tuple_data + variable_length_payload_offset, &str_value_size, sizeof(uint32_t));
        memcpy(tuple_data + variable_length_payload_offset + sizeof(uint32_t),
               str_value.c_str(), str_value_size);
        duckdb::Value value_;
        value = std::move(value_.CreateValue(variable_length_payload_offset));
        variable_length_payload_offset += sizeof(uint32_t) + str_value_size;
      } else { 
        LOG(FATAL) << "Not supported yet!";
      }
    } else if (expression->GetExpressionType() == duckdb::ExpressionType::OPERATOR_CAST) {
      /// Not figure out how to do casting in duckdb, just cast with strings here
      auto &bound_cast_expression = (duckdb::BoundCastExpression &)*expression;
      switch (bound_cast_expression.return_type.id()) {
      case duckdb::LogicalTypeId::FLOAT: {
        std::string float_str = bound_cast_expression.child->GetName();
        float v = stof(float_str);
        duckdb::Value value_(v);
        value = std::move(value_);
        break;
      }
      case duckdb::LogicalTypeId::DOUBLE: {
        std::string double_str = bound_cast_expression.child->GetName();
        double v = stod(double_str);
        duckdb::Value value_(v);
        value = std::move(value_);
        break;
      }
      case duckdb::LogicalTypeId::TIMESTAMP: {
        std::string ts_str = bound_cast_expression.child->GetName();
        ts_str.erase(std::remove(ts_str.begin(), ts_str.end(), 39), ts_str.end());
        duckdb::timestamp_t ts = duckdb::Timestamp::FromString(ts_str);
        value = std::move(duckdb::Value::TIMESTAMP(ts));
        break;
      }
      default:
        LOG(FATAL) << "Not supported yet!";
      }
    }
  
    memcpy(tuple_data + table_info->column_offsets[column_idx], reinterpret_cast<char *>(&value) + 32,
           duckdb::GetTypeIdSize(value.type().InternalType()));
    column_idx++;
  }
  SetSize(variable_length_payload_offset);
}

void Tuple::CreateKeyFromTuple(Tuple *tuple, catalog::IndexInfo *index_info,
                               catalog::TableInfo *table_info) {
  auto &column_types = table_info->column_types;
  auto create_index_info_ptr = index_info->create_index_info.get();
  auto &create_index_info = (duckdb::CreateIndexInfo &)*create_index_info_ptr;
  uint64_t offset = 0;
  for (auto &column_id : create_index_info.column_ids) {
    const auto &type = column_types[column_id];
    const auto &internal_type = type.InternalType();
    auto column_offset = table_info->column_offsets[column_id];
    if (duckdb::TypeIsConstantSize(internal_type)) {
      uint32_t payload_size = duckdb::GetTypeIdSize(internal_type);
      memcpy(tuple_data + offset, tuple->GetTupleData() + column_offset, payload_size);
      offset += payload_size;
    } else if (internal_type == duckdb::PhysicalType::VARCHAR) {
      uint32_t payload_offset = tuple->GetVarValueOffset(column_offset);
      uint32_t payload_size = tuple->GetVarValueSize_(payload_offset);
      memcpy(tuple_data + offset, tuple->GetTupleData() + payload_offset + sizeof(uint32_t),
             payload_size);
      offset += payload_size;
    } else {
      LOG(FATAL) << "Not supported yet!";
    }
  }
  SetSize(offset);
}

void Tuple::CreateProjectedTuple(Tuple *tuple, catalog::TableInfo *table_info,
                                 duckdb::LogicalGet &scan_plan) {
  uint32_t column_ids_size = scan_plan.column_ids.size();
  *(uint32_t *)GetTupleData() = column_ids_size;
  uint32_t projection_offsets_size = sizeof(uint32_t) * (column_ids_size + 1);
  uint32_t offset = 0;
  for (uint32_t i = 0; i < column_ids_size; i++) {
    auto &column_id = scan_plan.column_ids[i];
    auto &column_type = scan_plan.returned_types[column_id];
    const auto &internal_type = column_type.InternalType();
    uint32_t column_offset = table_info->column_offsets[column_id];
    uint32_t true_offset = offset + projection_offsets_size;
    if (duckdb::TypeIsConstantSize(internal_type)) {
      uint32_t payload_size = duckdb::GetTypeIdSize(internal_type);
      memcpy(GetTupleData() + true_offset, (char *)&payload_size, sizeof(uint32_t));
      memcpy(GetTupleData() + true_offset + sizeof(uint32_t),
             tuple->GetTupleData() + column_offset, payload_size);
      memcpy(GetTupleData() + sizeof(uint32_t) * (i + 1),
             (char *)&true_offset, sizeof(uint32_t));
      offset += payload_size + sizeof(uint32_t);
    } else if (internal_type == duckdb::PhysicalType::VARCHAR) {
      uint32_t payload_offset = tuple->GetVarValueOffset(column_offset);
      uint32_t payload_size = tuple->GetVarValueSize_(payload_offset);
      memcpy(GetTupleData() + true_offset, tuple->GetTupleData() + payload_offset,
             payload_size + sizeof(uint32_t));
      memcpy(GetTupleData() + sizeof(uint32_t) * (i + 1),
             (char *)&true_offset, sizeof(uint32_t));
      offset += payload_size + sizeof(uint32_t);
    } else {
      LOG(FATAL) << "Not supported yet!";
    }
  }
  SetSize(offset + projection_offsets_size);
}

void Tuple::Merge(Tuple *left_tuple, Tuple *right_tuple) {
  uint32_t left_column_total = *(uint32_t *)left_tuple->GetTupleData();
  uint32_t right_column_total = *(uint32_t *)right_tuple->GetTupleData();
  uint32_t merged_column_total = left_column_total + right_column_total;
  *(uint32_t *)GetTupleData() = merged_column_total;

  memcpy(GetTupleData() + sizeof(uint32_t) * (merged_column_total + 1),
    left_tuple->GetTupleData() + sizeof(uint32_t) * (left_column_total + 1),
    left_tuple->GetSize() - sizeof(uint32_t) * (left_column_total + 1));

  memcpy(GetTupleData() + sizeof(uint32_t) * (merged_column_total + 1) +
    left_tuple->GetSize() - sizeof(uint32_t) * (left_column_total + 1),
    right_tuple->GetTupleData() + sizeof(uint32_t) * (right_column_total + 1),
    right_tuple->GetSize() - sizeof(uint32_t) * (right_column_total + 1));

  for (uint32_t i = 0; i < left_column_total; i++) {
    *(uint32_t *)(left_tuple->GetTupleData() + sizeof(uint32_t) * (i + 1)) +=
      sizeof(uint32_t) * right_column_total;
  }

  memcpy(GetTupleData() + sizeof(uint32_t),
    left_tuple->GetTupleData() + sizeof(uint32_t), sizeof(uint32_t) * left_column_total);

  for (uint32_t i = 0; i < right_column_total; i++) {
    *(uint32_t *)(right_tuple->GetTupleData() + sizeof(uint32_t) * (i + 1)) +=
      left_tuple->GetSize() - sizeof(uint32_t);
  }

  memcpy(GetTupleData() + sizeof(uint32_t) + sizeof(uint32_t) * left_column_total,
    right_tuple->GetTupleData() + sizeof(uint32_t), sizeof(uint32_t) * right_column_total);
}

}  // namespace table
}  // namespace noname


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

#include <algorithm>
#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "sql/duckdb_query.h"
#include "sql/duckdb_internal.h"
#include "noname_defs.h"

#define TUPLE_VAR_VALUE_OFFSET_SIZE 39

namespace noname {

namespace catalog {
struct TableInfo;
struct IndexInfo;
}

namespace table {

struct Tuple {
  OID oid;

  uint32_t size;

  // standard: normal tuple, nonstandard: projected tuple
  bool standard;

  char tuple_data[0];

  Tuple() {}
  ~Tuple() {}

  inline OID GetOID() { return oid; }
  inline void SetOID(OID oid_) { oid = oid_; }
  inline char *GetTupleData() { return tuple_data; }
  inline bool GetStandard() { return standard; }
  inline void SetStandard(bool standard_) { standard = standard_; }
  inline uint32_t GetSize() { return size; }
  inline void SetSize(uint32_t size_) { size = size_; }

  inline char *GetConstValue(uint32_t column_offset) {
    return tuple_data + column_offset;
  }

  // get variable-length value payload (no size included) via column offset
  inline char *GetVarValuePayload(uint32_t column_offset) {
    uint32_t payload_offset = GetVarValueOffset(column_offset);
    return tuple_data + payload_offset + sizeof(uint32_t);
  }

  // get variable-length value size via column offset
  inline uint32_t GetVarValueSize(uint32_t column_offset) {
    uint32_t payload_offset = GetVarValueOffset(column_offset);
    return GetVarValueSize_(payload_offset);
  }

  // get variable-length value size via real payload offset
  inline uint32_t GetVarValueSize_(uint32_t column_offset) {
    return *(uint32_t *)(tuple_data + column_offset);
  }

  // get variable-length value payload offset via column offset
  inline uint32_t GetVarValueOffset(uint32_t column_offset) {
    return *(uint32_t *)(tuple_data + column_offset);
  }

  // used to get a projected value
  inline char *GetProjectedValue(uint32_t column_idx) {
    LOG_IF(FATAL, standard) << "Not for standard tuple type";
    uint32_t column_offset = *((uint32_t *)tuple_data + column_idx + 1);
    return tuple_data + column_offset + sizeof(uint32_t);
  }

  inline uint32_t GetProjectedValueSize(uint32_t column_idx) {
    LOG_IF(FATAL, standard) << "Not for standard tuple type";
    uint32_t column_offset = *((uint32_t *)tuple_data + column_idx + 1);
    return *(uint32_t *)(tuple_data + column_offset);
  }

  void Merge(Tuple *left_tuple, Tuple *right_tuple);
  void CreateProjectedTuple(Tuple *tuple, catalog::TableInfo *table_info,
                            duckdb::LogicalGet &scan_plan);
  void CreateTuple(std::vector<std::unique_ptr<duckdb::Expression>> &expressions,
                   catalog::TableInfo *table_info);
  // build a key from an existing tuple
  void CreateKeyFromTuple(Tuple *tuple, catalog::IndexInfo *index_info,
                          catalog::TableInfo *table_info);
};

}  // namespace table
}  // namespace noname

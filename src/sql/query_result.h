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

// query result

#pragma once

#include <iostream>
#include <unordered_map>
#include <vector>

#include "sql/duckdb_query.h"

namespace noname {
namespace sql {

struct QueryResult {
  std::vector<std::unordered_map<std::string, char *>> results;

  inline void clear() { results.clear(); }

  inline void Print() {}

  static inline uint32_t GetValueSize(char *value_ptr) {
    return *(uint32_t *)(value_ptr - sizeof(uint32_t));
  }
};

}  // namespace sql
}  // namespace noname


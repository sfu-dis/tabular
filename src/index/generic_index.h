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

#include "table/ia_table.h"

namespace noname {
namespace index {

struct GenericIndex {
  virtual bool Insert(const char *key, size_t key_sz, const char *value, size_t value_sz) = 0;
  virtual bool Search(const char *key, size_t key_sz, char *&value_out) = 0;
  virtual int Scan(const char *start_key, bool start_key_inclusive,
                  const char *end_key, bool end_key_inclusive,
                  size_t key_sz, int scan_sz, char *&value_out) = 0;
  virtual int ReverseScan(const char *start_key, bool start_key_inclusive,
                         const char *end_key, bool end_key_inclusive,
                         size_t key_sz, int scan_sz, char *&value_out) = 0;
  virtual bool Update(const char *key, size_t key_sz, const char *value, size_t value_sz) = 0;
  virtual bool Delete(const char *key, size_t key_sz) = 0;

  GenericIndex(size_t key_size, size_t value_size, noname::table::IndirectionArrayTable *table)
    : key_size(key_size), value_size(value_size), table(table) {}

  size_t key_size;
  size_t value_size;
  noname::table::IndirectionArrayTable *table;
};

} //namespace index
} //namespace noname
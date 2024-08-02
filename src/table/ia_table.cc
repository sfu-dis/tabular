/*
 * Copyright (C) 2023 Data-Intensive Systems Lab, Simon Fraser University.
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

#include "ia_table.h"

namespace noname {
namespace table {

IndirectionArrayTable::IndirectionArrayTable(config_t config)
  : config(config), next_oid(0)
  , backing_array(config.ia_table_dynarray_use_1gb_hugepages, config.ia_table_capacity, 64) {
}

OID IndirectionArrayTable::Put(Object *object) {
  OID oid = next_oid.fetch_add(1, std::memory_order::relaxed);
  backing_array.EnsureSize((oid + 1) * sizeof(Object *));
  backing_array[oid] = object;
  return oid;
}

Object *IndirectionArrayTable::Get(OID oid) {
  LOG_IF(FATAL, oid >= next_oid.load(std::memory_order_relaxed));
  return backing_array[oid];
}

Object **IndirectionArrayTable::GetEntryPtr(OID oid) {
  return backing_array.GetEntryPtr(oid);
}

}  // namespace table
}  // namespace noname

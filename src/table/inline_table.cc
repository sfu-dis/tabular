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

#include "config.h"

#include "inline_table.h"

namespace noname {
namespace table {

InlineTable::InlineTable(config_t config, bool is_persistent)
    : config(config), next_oid(0), is_persistent(is_persistent),
      backing_array(config.ia_table_dynarray_use_1gb_hugepages,
                    config.ia_table_capacity,
                    config.inline_table_initial_size) {}

OID InlineTable::AllocateRecord(uint64_t size) {
  auto record_size = align_up(size + sizeof(Record), RECORD_ALIGNMENT);
  OID oid = next_oid.fetch_add(record_size, std::memory_order::relaxed);
  backing_array.EnsureSize(oid + record_size);
  auto record = reinterpret_cast<Record *>(&backing_array[oid]);
  record->id = 0;
  record->size = size;
  return oid;
}

OID InlineTable::PutData(const uint8_t *data, uint64_t size) {
  auto record_size = align_up(size + sizeof(Record), RECORD_ALIGNMENT);
  OID oid = next_oid.fetch_add(record_size, std::memory_order::relaxed);
  backing_array.EnsureSize(oid + record_size);
  auto record = reinterpret_cast<Record *>(&backing_array[oid]);
  record->id = 0;
  record->size = size;
  memcpy(record->data, data, size);
  return oid;
}

void InlineTable::EnsureRecordSize(OID oid, uint64_t new_size) {
  auto record = reinterpret_cast<Record *>(&backing_array[oid]);
  auto record_size = align_up(record->size + sizeof(Record), RECORD_ALIGNMENT);
  auto new_record_size = align_up(new_size + sizeof(Record), RECORD_ALIGNMENT);
  if (new_record_size > record_size) {
    auto curr_next_oid = next_oid.load(std::memory_order::relaxed);
    CHECK_EQ(oid + record_size, curr_next_oid);
    auto new_next_oid = oid + new_record_size;
    auto ok = next_oid.compare_exchange_strong(curr_next_oid, new_next_oid);
    CHECK(ok);
    backing_array.EnsureSize(new_next_oid);
  }
}

// Return the entry at the specified OID entry
InlineTable::Record *InlineTable::GetRecord(OID oid) {
  DCHECK_LT(oid, next_oid.load(std::memory_order_relaxed))
      << "oid is out of range.";
  return reinterpret_cast<Record *>(&backing_array[oid]);
}

}  // namespace table
}  // namespace noname

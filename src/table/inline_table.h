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

// An object/table abstraction based on ERMIA's indirection array idea

#pragma once

#include <glog/logging.h>
#include <atomic>
#include <tuple>

#include "config.h"
#include "dynarray.h"
#include "object.h"
#include "noname_defs.h"

namespace noname {
namespace table {

struct InlineTable {
  inline static constexpr uint64_t NUM_STATUS_BITS = 1;
  inline static constexpr uint64_t LOCK_BIT = 1UL << 0;

  inline static constexpr uint64_t NUM_VERSION_BITS = 53;
  inline static constexpr uint64_t NUM_EPOCH_BITS = 10;

  static_assert(NUM_STATUS_BITS + NUM_VERSION_BITS + NUM_EPOCH_BITS == 8 * sizeof(uint64_t));

  // GetTID only clear all the status bits instead of shifting it
  inline static uint64_t GetTID(uint64_t tid_word) {
    return tid_word & ~LOCK_BIT;
  }

  struct Record {
    uint64_t id;
    size_t size;
    uint8_t data[0];

    // Returns TID while locking write set
    inline uint64_t Lock() {
      uint64_t curr = __atomic_load_n(&id, __ATOMIC_ACQUIRE);
      do {
        while (true) {
          bool is_locked = curr & LOCK_BIT;
          if (is_locked) {
            curr = __atomic_load_n(&id, __ATOMIC_ACQUIRE);
          } else {
            break;
          }
        }
      } while (!__atomic_compare_exchange_n(
          &id, &curr, curr | LOCK_BIT, false,
          __ATOMIC_RELEASE, __ATOMIC_ACQUIRE));

      return curr;
    }

    void Unlock() {
      __atomic_store_n(&id, id & ~LOCK_BIT, __ATOMIC_RELEASE);
    }
  };

  // Constructor
  // @config: config_t struct for parameters
  explicit InlineTable(config_t config, bool is_persistent = false);

  // Parameters
  config_t config;

  // Centralised OID counter for now - may need to get a TLS allocator/cache later
  std::atomic<OID> next_oid;

  // Whether this table is persistent
  bool is_persistent;

  // The dynarray that's backing the indirection entries of this table
  DynamicArray<uint8_t> backing_array;

  // Integer identifier for a table in a table group
  table::FID fid;

  // Unit size for record allocation
  static const size_t RECORD_ALIGNMENT = 2 * CACHELINE_SIZE;

  // Allocate space for a record with certain size.
  OID AllocateRecord(uint64_t size);

  // Append a record
  OID PutData(const uint8_t *data, uint64_t size);

  void EnsureRecordSize(OID oid, uint64_t size);

  template <typename T>
  OID Put(const T &value) {
    auto data = reinterpret_cast<const uint8_t *>(&value);
    return PutData(data, sizeof(T));
  }

  // Return the entry at the specified OID entry
  Record *GetRecord(OID oid);

  template <typename T>
  T *Get(OID oid) {
    return reinterpret_cast<T *>(GetRecord(oid)->data);
  }
};

}  // namespace table
}  // namespace noname

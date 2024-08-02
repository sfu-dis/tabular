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

#include "hash_table_common.h"

#include "table/dynarray.h"
#include "util.h"

// Add RWLOCK definition to make OMCSLock a read-write lock
#include "index/latches/OMCS.h"

namespace noname {
namespace index {

// Extendible Hash Table with Pessimistic Lock Coupling
template <typename Key, typename Value>
struct HashTableLC {
  // TODO(ziyi) print hash table number of keyval pair in bucket
  using RWLock = OMCSLock;
  struct ConcurrentBucket {
    RWLock lock;
    HashTableBucket<Key, Value> bucket;
  };

  struct Entry {
    uint8_t local_depth;
    ConcurrentBucket *concurrent_bucket;
  };

  HashTableLC() : global_depth(0) {
    entries = new Entry[1];
    entries[0].concurrent_bucket = new ConcurrentBucket();
    entries[0].local_depth = 0;
  }

  Entry GetEntry(uint64_t hash) {
    auto offset = hash & ~((~0ULL) << global_depth);
    return entries[offset];
  }

  void UpdateEntries(uint8_t local_depth, uint64_t hash,
                     ConcurrentBucket *new_bucket) {
    auto first_idx = hash & ~(~0ULL << local_depth);
    auto step = 1ULL << local_depth;
    auto dir_size = 1ULL << global_depth;
    for (auto i = first_idx; i < dir_size; i += step) {
      entries[i].local_depth += 1;
      if (i & (1ULL << local_depth)) {
        entries[i].concurrent_bucket = new_bucket;
      }
    }
  }

  bool insert(Key key, Value value) {
    auto hash = Hash(key);
    // TODO(ziyi) add a test case of multiple splits for one insert.
    while (true) {
      directory_lock.writeLock();
      auto [local_depth, concurrent_bucket] = GetEntry(hash);
      concurrent_bucket->lock.writeLock();
      if (concurrent_bucket->bucket.Find(key)) {
        concurrent_bucket->lock.writeUnlock();
        directory_lock.writeUnlock();
        return false;
      }

      if (!concurrent_bucket->bucket.IsFull()) {
        directory_lock.writeUnlock();
        concurrent_bucket->bucket.Insert(key, value);
        concurrent_bucket->lock.writeUnlock();
        return true;
      }

      // Need to split bucket and update directory.
      auto new_bucket = new ConcurrentBucket();
      concurrent_bucket->bucket.SplitTo(local_depth, &(new_bucket->bucket));

      if (local_depth < global_depth) {
        UpdateEntries(local_depth, hash, new_bucket);
      } else {
        auto dir_size = 1ULL << global_depth;
        auto offset = hash & ~((~0ULL) << global_depth);
        auto new_entries = new Entry[2 * dir_size];
        memcpy(&new_entries[0], entries, dir_size * sizeof(Entry));
        memcpy(&new_entries[dir_size], entries, dir_size * sizeof(Entry));

        new_entries[offset].local_depth += 1;

        new_entries[offset + dir_size].local_depth += 1;
        new_entries[offset + dir_size].concurrent_bucket = new_bucket;

        delete[] entries;
        entries = new_entries;

        global_depth += 1;
      }
      directory_lock.writeUnlock();
      concurrent_bucket->lock.writeUnlock();
    }
  }

  bool lookup(Key key, Value &out_value) {
    // TODO(ziyi) add epoch announcement for the overhead
    auto hash = Hash(key);

    directory_lock.readLock();
    auto offset = hash & ~(~0ULL << global_depth);
    auto [local_depth, concurrent_bucket] = entries[offset];

    concurrent_bucket->lock.readLock();
    directory_lock.readUnlock();
    auto ok = concurrent_bucket->bucket.Find(key, &out_value);
    concurrent_bucket->lock.readUnlock();

    return ok;
  }

  bool update(Key key, Value value) {
    auto hash = Hash(key);
    directory_lock.readLock();
    auto offset = hash & ~(~0ULL << global_depth);
    auto [local_depth, concurrent_bucket] = entries[offset];

    concurrent_bucket->lock.writeLock();
    directory_lock.readUnlock();
    auto ok = concurrent_bucket->bucket.Update(key, value);
    concurrent_bucket->lock.writeUnlock();

    return ok;
  }

  uint64_t scan(Key k, int64_t range, Value *output) {
    LOG(FATAL) << "Hash table doesn't support scan";
    return 0;
  }

  RWLock directory_lock;
  uint8_t global_depth;

  Entry *entries;
};

} // namespace index
} // namespace noname

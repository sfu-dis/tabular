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

namespace noname {
namespace index {

// Optimisitc Lock
struct OptiLock {
  inline static constexpr uint64_t LOCK_BIT = 1;

  uint64_t lock_word;

  OptiLock() : lock_word(0) {} // Initial value is unlocked

  uint64_t SetLockedBit(uint64_t version) { return version | LOCK_BIT; }

  bool IsLocked(uint64_t version) { return version & LOCK_BIT; }

  uint64_t RLock() {
    uint64_t version;
    do {
      version = __atomic_load_n(&lock_word, __ATOMIC_ACQUIRE);
    } while (IsLocked(version));
    return version;
  }

  bool Check(uint64_t version) {
    return version == __atomic_load_n(&lock_word, __ATOMIC_ACQUIRE);
  }

  bool RUnlock(uint64_t version) { return Check(version); }

  bool Upgrade(uint64_t version) {
    return __atomic_compare_exchange_n(&lock_word, &version,
                                       SetLockedBit(version), false,
                                       __ATOMIC_RELEASE, __ATOMIC_ACQUIRE);
  }

  void Lock() {
    uint64_t version;
    do {
      version = RLock();
    } while (!Upgrade(version));
  }

  void Unlock() {
    // TODO(ziyi) FAA or LDST
    __atomic_store_n(&lock_word, lock_word + LOCK_BIT, __ATOMIC_RELEASE);
  }
};

// Extendible Hash Table
template <typename Key, typename Value>
struct HashTable {
  struct ConcurrentBucket {
    OptiLock lock;
    HashTableBucket<Key, Value> bucket;
  };

  struct Entry {
    uint8_t local_depth;
    ConcurrentBucket *concurrent_bucket;
  };

  HashTable() : global_depth(0) {
    entries = new Entry[1];
    entries[0].concurrent_bucket = new ConcurrentBucket();
    entries[0].local_depth = 0;
  }

  Entry GetEntry(uint64_t hash) {
    bool ok = false;
    Entry entry;
    do {
      auto version = directory_lock.RLock();
      auto offset = hash & ~((~0ULL) << global_depth);
      entry = entries[offset];
      ok = directory_lock.Check(version);
    } while (!ok);
    return entry;
  }

  // UpdateEntries will lock the directory and update affected entries of a
  // bucket split.
  // It assumed that the lock of the bucket being splitted is acquired.
  template <bool IsDirectoryLocked>
  void UpdateEntries(uint8_t local_depth, uint64_t hash,
                     ConcurrentBucket *new_bucket) {
    auto first_idx = hash & ~(~0ULL << local_depth);
    auto step = 1ULL << local_depth;
    if constexpr (!IsDirectoryLocked) {
      directory_lock.Lock();
    }
    auto dir_size = 1ULL << global_depth;
    for (auto i = first_idx; i < dir_size; i += step) {
      entries[i].local_depth += 1;
      if (i & (1ULL << local_depth)) {
        entries[i].concurrent_bucket = new_bucket;
      }
    }
    if constexpr (!IsDirectoryLocked) {
      directory_lock.Unlock();
    }
  }

  bool insert(Key key, Value value) {
    auto hash = Hash(key);
  restart:
    // TODO(ziyi) add a test case of multiple splits for one insert.
    while (true) {
      auto [local_depth, concurrent_bucket] = GetEntry(hash);
      concurrent_bucket->lock.Lock();
      auto [curr_depth, curr_bucket] = GetEntry(hash);
      if (curr_depth != local_depth) {
        concurrent_bucket->lock.Unlock();
        goto restart;
      }

      if (concurrent_bucket->bucket.Find(key)) {
        concurrent_bucket->lock.Unlock();
        return false;
      }

      if (!concurrent_bucket->bucket.IsFull()) {
        concurrent_bucket->bucket.Insert(key, value);
        concurrent_bucket->lock.Unlock();
        return true;
      }

      // Need to split bucket and update directory.
      auto new_bucket = new ConcurrentBucket();
      concurrent_bucket->bucket.SplitTo(local_depth, &(new_bucket->bucket));

      if (local_depth < global_depth) {
        UpdateEntries</*IsDirectoryLocked=*/false>(local_depth, hash,
                                                   new_bucket);
      } else {
        directory_lock.Lock();
        if (local_depth < global_depth) {
          UpdateEntries</*IsDirectoryLocked=*/true>(local_depth, hash,
                                                    new_bucket);
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
        directory_lock.Unlock();
      }
      concurrent_bucket->lock.Unlock();
    }
  }

  bool lookup(Key key, Value &out_value) {
    // TODO(ziyi) add epoch announcement for the overhead
    auto hash = Hash(key);
    bool ok;
    bool directory_ok;
    bool bucket_ok;
    do {
      auto lock_word = directory_lock.RLock();
      auto offset = hash & ~(~0ULL << global_depth);
      auto [local_depth, concurrent_bucket] = entries[offset];

      auto bucket_lock_word = concurrent_bucket->lock.RLock();
      ok = concurrent_bucket->bucket.Find(key, &out_value);
      bucket_ok = concurrent_bucket->lock.RUnlock(bucket_lock_word);

      directory_ok = directory_lock.RUnlock(lock_word);
    } while (!(bucket_ok && directory_ok));

    return ok;
  }

  bool update(Key key, Value value) {
    auto hash = Hash(key);
    bool ok;
    uint64_t lock_word;
    do {
      lock_word = directory_lock.RLock();
      auto offset = hash & ~(~0ULL << global_depth);
      auto [local_depth, concurrent_bucket] = entries[offset];

      concurrent_bucket->lock.Lock();
      if (directory_lock.RUnlock(lock_word)) {
        ok = concurrent_bucket->bucket.Update(key, value);
        concurrent_bucket->lock.Unlock();
        return ok;
      }
      concurrent_bucket->lock.Unlock();
    } while (true);
  }

  uint64_t scan(Key k, int64_t range, Value *output) {
    LOG(FATAL) << "Hash table doesn't support scan";
    return 0;
  }

  OptiLock directory_lock;
  uint8_t global_depth;

  Entry *entries;
};

} // namespace index
} // namespace noname

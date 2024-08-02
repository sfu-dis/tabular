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

#include <glog/logging.h>

#include <cstdint>

#include <bits/hash_bytes.h>

#ifndef HASHTABLE_PAGE_SIZE
#define HASHTABLE_PAGE_SIZE 256
#endif

namespace noname {
namespace index {

static inline constexpr uint64_t kHashTablePageSize = HASHTABLE_PAGE_SIZE;

template <typename Key>
inline static uint64_t Hash(Key key) {
  return std::_Hash_bytes(&key, sizeof(Key), 0);
}

template <typename Key, typename Value>
struct HashTableBucket {
  struct Header {
    size_t size;
    Header() : size(0) {}
  };
  struct Entry {
    Key key;
    Value value;
  };

  static constexpr size_t kEntrySize = sizeof(Entry);
  static constexpr uint64_t kMaxEntries =
      (kHashTablePageSize - sizeof(Header)) / kEntrySize;

  Header header;
  Entry entries[kMaxEntries];

  bool Find(Key key, Value *out_value) {
    for (auto i = 0; i < header.size; i++) {
      auto &entry = entries[i];
      if (entry.key == key) {
        *out_value = entry.value;
        return true;
      }
    }
    return false;
  }

  bool Find(Key key) {
    for (auto i = 0; i < header.size; i++) {
      auto &entry = entries[i];
      if (entry.key == key) {
        return true;
      }
    }
    return false;
  }

  bool IsFull() {
    return header.size == kMaxEntries;
  }

  void Insert(Key key, const Value &value) {
    DCHECK(header.size < kMaxEntries);
    auto &entry = entries[header.size++];
    entry.key = key;
    entry.value = value;
  }

  bool Update(Key key, const Value &value) {
    for (auto i = 0; i < header.size; i++) {
      auto &entry = entries[i];
      if (entry.key == key) {
        entry.value = value;
        return true;
      }
    }
    return false;
  }

  // Split this bucket into a new bucket.
  // The new bucket will have the entries whose additional bit is one.
  void SplitTo(uint64_t local_depth, HashTableBucket *new_bucket) {
    auto curr_idx = 0;
    auto other_idx = 0;
    for (size_t i = 0; i < header.size; i++) {
      auto &entry = entries[i];
      // mask to extract only the additional bit from old to new local depth.
      auto mask = 1ULL << local_depth;
      auto hash = Hash(entry.key);
      if (hash & mask) {  // non-zero
        // move to new bucket
        new_bucket->entries[other_idx].key = entry.key;
        new_bucket->entries[other_idx].value = entry.value;
        other_idx += 1;
      } else {  // zero
        // stay in old bucket
        if (i > curr_idx) {
          entries[curr_idx].key = entry.key;
          entries[curr_idx].value = entry.value;
        }
        curr_idx += 1;
      }
    }
    header.size = curr_idx;
    new_bucket->header.size = other_idx;
  }
};

} // namespace index
} // namespace noname

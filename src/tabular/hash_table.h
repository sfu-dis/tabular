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

#include <optional>

#include "index/hash_table_common.h"
#include "table_group.h"
#include "transaction/occ.h"
#include "table/object.h"

namespace noname {
namespace tabular {

using noname::transaction::occ::Transaction;

// Store the split_bucket_oid from last failed transaction of insert()
static thread_local table::OID split_bucket_oid{table::kInvalidOID};

// Extendible Hash Table implemented in tabular manner.
// One of the two tables with only one record as the directory that we extend
// the size of it when hash table is extended. Another table consists of records
// that each of records represents a bucket.
template <typename Key, typename Value> struct HashTable {
  using Bucket = index::HashTableBucket<Key, Value>;

  TableGroup group;
  table::InlineTable *directory_table;
  table::InlineTable *buckets;

  struct Entry {
    uint8_t local_depth;
    table::OID bucket_oid;
  };
  // Directory is an array of entries being stored in one record.
  inline static constexpr table::OID DIRECTORY_OID = 0;

  HashTable(table::config_t config, bool is_persistent,
            const std::filesystem::path &logging_directory = "",
            size_t num_of_workers = 0)
      : group(config, is_persistent, logging_directory, num_of_workers) {
    group.StartEpochDaemon();

    directory_table = group.GetTable(0, is_persistent);
    buckets = group.GetTable(1, is_persistent);

    auto t = Transaction::BeginSystem(&group);

    auto initial_bucket = t->arena->NextValue<Bucket>();
    auto initial_bucket_oid = t->Insert(buckets, initial_bucket);

    auto entry = t->arena->NextValue<Entry>();
    *entry = {.local_depth = 0, .bucket_oid = initial_bucket_oid};
    auto directory_oid = t->Insert(directory_table, entry);
    CHECK_EQ(directory_oid, DIRECTORY_OID);

    auto ok = t->PreCommit();
    CHECK(ok);
    t->PostCommit();
  }

  ~HashTable() { group.StopEpochDaemon(); }

  bool insert(Key key, Value value) {
    std::optional<bool> ret;
    auto hash = index::Hash(key);
    do {
      ret = insert_internal_callback(hash, key, value);
    } while (!ret);
    split_bucket_oid = table::kInvalidOID;
    CHECK(ret.value());
    return ret.value();
  }

  std::optional<bool> insert_internal_callback(uint64_t hash, Key key, Value value) {
    while (true) {
      auto t = Transaction::BeginSystem(&group);

      Entry entry;
      uint32_t offset;
      uint32_t num_entries = 0;
      t->ReadRecordCallback(
          directory_table, DIRECTORY_OID,
          [&num_entries, &entry, &offset, hash](uint8_t *data, uint64_t size) {
            DCHECK_EQ(size % sizeof(Entry), 0);
            num_entries = size / sizeof(Entry);
            auto offset_mask = num_entries - 1;
            offset = hash & offset_mask;
            auto entries = reinterpret_cast<Entry *>(data);
            entry = entries[offset];
          });

      // Existence and capacity checks
      bool found = false;
      bool full = false;
      t->ReadRecordCallback(buckets, entry.bucket_oid,
                            [&found, &full, key](uint8_t *data, uint64_t size) {
                              auto bucket = reinterpret_cast<Bucket *>(data);
                              found = bucket->Find(key);
                              full = bucket->IsFull();
                            });

      if (found) {
        t->Rollback();
        return false;
      }

      if (!full) {
        auto bucket_insert_cb = [this, key, value](uint8_t *data) -> void {
          auto bucket = reinterpret_cast<Bucket *>(data);
          bucket->Insert(key, value);
        };
        t->UpdateRecordCallback(buckets, entry.bucket_oid, bucket_insert_cb);
        if (!t->PreCommit()) {
          t->Rollback();
          return std::nullopt;
        }
        t->PostCommit();
        return true;
      }

      auto *new_bucket = t->arena->NextValue<Bucket>();
      auto bucket_split_cb = [this, new_bucket, entry](uint8_t *data) -> void {
        auto bucket = reinterpret_cast<Bucket *>(data);
        bucket->SplitTo(entry.local_depth, new_bucket);
      };
      t->UpdateRecordCallback(buckets, entry.bucket_oid, bucket_split_cb);
      table::OID new_bucket_oid = split_bucket_oid;
      if (split_bucket_oid == table::kInvalidOID) {
        new_bucket_oid = t->Insert(buckets, new_bucket);
        split_bucket_oid = new_bucket_oid;
      } else {
        t->Update(buckets, split_bucket_oid, new_bucket);
      }

      if (1 << entry.local_depth == num_entries) {  // local_depth == global_depth
        auto dir_double_cb = [this, offset, num_entries,
                              new_bucket_oid](uint8_t *data) -> void {
          const size_t directory_size = num_entries * sizeof(Entry);
          auto entries = reinterpret_cast<Entry *>(data);
          std::memcpy(&entries[num_entries], entries, directory_size);
          entries[offset].local_depth += 1;

          entries[offset + num_entries].local_depth += 1;
          entries[offset + num_entries].bucket_oid = new_bucket_oid;
        };
        t->UpdateRecordCallback(directory_table, DIRECTORY_OID, dir_double_cb,
                                2 * num_entries * sizeof(Entry));
        if (!t->PreCommit()) {
          t->Rollback();
          return std::nullopt;
        }
        t->PostCommit();
        split_bucket_oid = table::kInvalidOID;
      } else { // local_depth < global_depth
        CHECK_LT(1 << entry.local_depth, num_entries);
        auto dir_update_cb = [this, hash, num_entries, entry,
                              new_bucket_oid](uint8_t *data) -> void {
          auto first_idx = hash & ~(~0ULL << entry.local_depth);
          auto step = 1 << entry.local_depth;
          auto entries = reinterpret_cast<Entry *>(data);
          for (auto i = first_idx; i < num_entries; i += step) {
            entries[i].local_depth += 1;
            if (i & (1ULL << entry.local_depth)) {
              entries[i].bucket_oid = new_bucket_oid;
            }
          }
        };
        t->UpdateRecordCallback(directory_table, DIRECTORY_OID, dir_update_cb);
        if (!t->PreCommit()) {
          t->Rollback();
          return std::nullopt;
        }
        t->PostCommit();
        split_bucket_oid = table::kInvalidOID;
      }
    }
  }

  std::optional<bool> insert_internal(uint64_t hash, Key key, Value value) {
    while (true) {
      auto t = Transaction::BeginSystem(&group);

      auto [directory, directory_size] =
          t->ReadRecord(directory_table, DIRECTORY_OID);
      DCHECK_EQ(directory_size % sizeof(Entry), 0);
      auto entries = reinterpret_cast<Entry *>(directory);
      auto num_entries = directory_size / sizeof(Entry);

      auto offset_mask = num_entries - 1;
      auto offset = hash & offset_mask;
      auto entry = entries[offset];

      Bucket bucket;
      t->Read(buckets, entry.bucket_oid, &bucket);
      if (bucket.Find(key)) {
        // Key already exists
        t->Rollback();
        return false;
      }

      if (!bucket.IsFull()) {
        bucket.Insert(key, value);
        t->Update(buckets, entry.bucket_oid, &bucket);
        if (!t->PreCommit()) {
          t->Rollback();
          return std::nullopt;
        }
        t->PostCommit();
        return true;
      }

      auto new_bucket = t->arena->NextValue<Bucket>();
      bucket.SplitTo(entry.local_depth, new_bucket);
      t->Update(buckets, entry.bucket_oid, &bucket);
      auto new_bucket_oid = t->Insert(buckets, new_bucket);

      if ((1 << entry.local_depth) <
          num_entries) { // because num_entries == 1 << global_depth
        auto first_idx = hash & ~(~0ULL << entry.local_depth);
        auto step = 1 << entry.local_depth;
        for (auto i = first_idx; i < num_entries; i += step) {
          entries[i].local_depth += 1;
          if (i & (1ULL << entry.local_depth)) {
            entries[i].bucket_oid = new_bucket_oid;
          }
        }
        t->UpdateRecord(directory_table, DIRECTORY_OID, directory,
                        directory_size);
      } else {
        auto new_directory_size = 2 * directory_size;
        auto new_directory = t->arena->Next(new_directory_size);
        auto new_entries = reinterpret_cast<Entry *>(new_directory);
        memcpy(&new_entries[0], entries, directory_size);
        memcpy(&new_entries[num_entries], entries, directory_size);

        new_entries[offset].local_depth += 1;

        new_entries[offset + num_entries].local_depth += 1;
        new_entries[offset + num_entries].bucket_oid = new_bucket_oid;

        t->UpdateRecord(directory_table, DIRECTORY_OID, new_directory,
                        new_directory_size);
      }

      auto ok = t->PreCommit();
      if (!ok) {
        t->Rollback();
        return std::nullopt;
      }
      t->PostCommit();
    }
  }

  bool lookup(Key key, Value &result) {
    std::optional<bool> ret;
    auto hash = index::Hash(key);
    do {
      ret = lookup_internal(hash, key, result);
    } while (!ret);

    return ret.value();
  }

  std::optional<bool> lookup_internal(uint64_t hash, Key key, Value &out_value) {
    auto t = Transaction::BeginSystem(&group);

    #if defined(HASHTABLE_MATERIALIZED_READ)
    auto [directory, directory_size] =
        t->ReadRecord(directory_table, DIRECTORY_OID);
    DCHECK_EQ(directory_size % sizeof(Entry), 0);
    auto entries = reinterpret_cast<Entry *>(directory);
    auto num_entries = directory_size / sizeof(Entry);

    auto offset_mask = num_entries - 1;
    auto offset = hash & offset_mask;

    Bucket bucket;
    t->Read(buckets, entries[offset].bucket_oid, &bucket);

    auto found = bucket.Find(key, &out_value);
    #else
    Entry entry;
    t->ReadRecordCallback(directory_table, DIRECTORY_OID,
                          [&entry, hash](uint8_t *data, uint64_t size) {
                            DCHECK_EQ(size % sizeof(Entry), 0);
                            auto num_entries = size / sizeof(Entry);
                            auto offset_mask = num_entries - 1;
                            auto offset = hash & offset_mask;
                            auto entries = reinterpret_cast<Entry *>(data);
                            entry = entries[offset];
                          });

    bool found;
    t->ReadCallback<Bucket>(buckets, entry.bucket_oid,
                    [&found, &out_value, key](Bucket *bucket) {
                      found = bucket->Find(key, &out_value);
                    });
    #endif

    auto ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();

    return found;
  }

  bool update(Key key, Value value) {
    std::optional<bool> ret;
    auto hash = index::Hash(key);
    do {
      ret = update_internal(hash, key, value);
    } while (!ret);

    return ret.value();
  }

  std::optional<bool> update_internal(uint64_t hash, Key key, Value value) {
    auto t = Transaction::BeginSystem(&group);

    #if defined(HASHTABLE_MATERIALIZED_READ)
    auto [directory, directory_size] =
        t->ReadRecord(directory_table, DIRECTORY_OID);
    DCHECK_EQ(directory_size % sizeof(Entry), 0);
    auto entries = reinterpret_cast<Entry *>(directory);
    auto num_entries = directory_size / sizeof(Entry);

    auto offset_mask = num_entries - 1;
    auto offset = hash & offset_mask;
    auto entry = entries[offset];
    #else
    Entry entry;
    t->ReadRecordCallback(directory_table, DIRECTORY_OID,
                          [&entry, hash](uint8_t *data, uint64_t size) {
                            DCHECK_EQ(size % sizeof(Entry), 0);
                            auto num_entries = size / sizeof(Entry);
                            auto offset_mask = num_entries - 1;
                            auto offset = hash & offset_mask;
                            auto entries = reinterpret_cast<Entry *>(data);
                            entry = entries[offset];
                          });
    #endif

    #if defined(HASHTABLE_MATERIALIZED_UPDATE)
    Bucket bucket;
    t->Read(buckets, entry.bucket_oid, &bucket);

    auto success = bucket.Update(key, value);

    t->Update(buckets, entry.bucket_oid, &bucket);

    auto ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();

    return success;
    #else
    bool found = false;
    t->ReadRecordCallback(buckets, entry.bucket_oid,
                          [&found, key](uint8_t *data, uint64_t size) {
                            auto bucket = reinterpret_cast<Bucket *>(data);
                            found = bucket->Find(key);
                          });
    if (!found) {
      t->Rollback();
      return false;
    }

    auto bucket_update_cb = [this, key, value](uint8_t *data) -> void {
      auto bucket = reinterpret_cast<Bucket *>(data);
      bucket->Update(key, value);
    };
    t->UpdateRecordCallback(buckets, entry.bucket_oid, bucket_update_cb);

    auto ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();

    return true;
    #endif
  }

  uint64_t scan(Key key, int64_t range, Value *output) {
    LOG(FATAL) << "Hash table doesn't support scan";
    return 0;
  }
};

} // namespace tabular
} // namespace noname

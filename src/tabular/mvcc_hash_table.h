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

#include <cstddef>
#include <cstdlib>
#include <optional>
#include <tuple>

#include "index/hash_table_common.h"
#include "tabular/object.h"
#include "table/ia_table.h"
#include "table/object.h"
#include "transaction/transaction.h"

namespace noname {
namespace tabular {

using transaction::mvcc::Transaction;

template <class Key, class Value>
struct MVCCHashTable {
  using Bucket = index::HashTableBucket<Key, Value>;

  table::IndirectionArrayTable directory_table;
  table::IndirectionArrayTable buckets_table;

  // This won't cause extra space and global depth
  // field is meaningful only is the first entry of directory
  // This is to keep memory layout same with tabular and hand-crafted.
  struct Entry {
    uint32_t global_depth{0};
    uint32_t local_depth;
    table::OID bucket_oid;

    Entry(const uint32_t local_depth, const table::OID bucket_oid)
      : local_depth(local_depth), bucket_oid(bucket_oid) {}
  };

  static constexpr table::OID DIRECTORY_OID = 0;

  explicit MVCCHashTable(table::config_t config) :
    directory_table(config), buckets_table(config) {
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();

    table::Object *initial_bucket_obj = TypedObject<Bucket>::Make()->Obj();
    auto initial_bucket_oid = t->Insert(&buckets_table, initial_bucket_obj);

    Entry entry{0, initial_bucket_oid};
    auto entry_data = reinterpret_cast<char *>(&entry);
    table::Object *entry_obj = TypedObject<Entry>::MakeArray(1, entry_data)->Obj();
    auto directory_oid = t->Insert(&directory_table, entry_obj);
    CHECK_EQ(directory_oid, DIRECTORY_OID);

    const bool ok = t->PreCommit();
    CHECK(ok);
    t->PostCommit();
  }


  bool insert(Key key, Value value) {
    std::optional<bool> ret;
    auto hash = index::Hash(key);
    do {
      ret = insert_internal(hash, key, value);
    } while (!ret);
    CHECK(ret.value());
    return ret.value();
  }

  std::optional<bool> insert_internal(uint64_t hash, Key key, Value value) {
    while (true) {
      auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();

      table::Object *directory = t->Read(&directory_table, DIRECTORY_OID);
      auto entries = reinterpret_cast<Entry *>(directory->data);
      // reading the global depth from first entry
      const uint32_t global_depth = entries->global_depth;
      auto num_entries = 1ULL << global_depth;
      auto offset_mask = num_entries - 1;
      auto offset = hash & offset_mask;
      auto entry = entries[offset];

      table::Object *bucket_obj = t->Read(&buckets_table, entry.bucket_oid);
      auto bucket = reinterpret_cast<Bucket *>(bucket_obj->data);
      if (bucket->Find(key)) {
        // Key already exists
        t->Rollback();
        return false;
      }

      table::Object *new_version_bucket_obj = TypedObject<Bucket>::Make()->Obj();
      auto new_version_bucket = reinterpret_cast<Bucket *>(new_version_bucket_obj->data);
      std::memcpy(new_version_bucket_obj->data, bucket_obj->data, sizeof(Bucket));

      if (!bucket->IsFull()) {
        new_version_bucket->Insert(key, value);
        bool updated = t->Update(&buckets_table, entry.bucket_oid, new_version_bucket_obj);
        if (!updated) {
          std::free((void *)new_version_bucket_obj);
          t->Rollback();
          return std::nullopt;
        }
        if (!t->PreCommit()) {
          t->Rollback();
          return std::nullopt;
        }
        t->PostCommit();
        return true;
      }

      table::Object *new_bucket_obj = TypedObject<Bucket>::Make()->Obj();
      auto new_bucket = reinterpret_cast<Bucket *>(new_bucket_obj->data);

      new_version_bucket->SplitTo(entry.local_depth, new_bucket);
      bool bucket_updated = t->Update(&buckets_table, entry.bucket_oid, new_version_bucket_obj);
      if (!bucket_updated) { // someone else is currently updating the same
                             // bucket, retry
        std::free((void *)new_version_bucket_obj);
        std::free((void *)new_bucket_obj);
        t->Rollback();
        return std::nullopt;
      }
      auto new_bucket_oid = t->Insert(&buckets_table, new_bucket_obj);

      if ((1 << entry.local_depth) <
          num_entries) { // because num_entries == 1 << global_depth
        table::Object *new_directory_obj =
            TypedObject<Entry>::MakeArray(num_entries, directory->data)->Obj();
        auto new_entries = reinterpret_cast<Entry *>(new_directory_obj->data);
        auto first_idx = hash & ~(~0ULL << entry.local_depth);
        auto step = 1 << entry.local_depth;
        for (auto i = first_idx; i < num_entries; i += step) {
          new_entries[i].local_depth += 1;
          if (i & (1ULL << entry.local_depth)) {
            new_entries[i].bucket_oid = new_bucket_oid;
          }
        }
        bool updated = t->Update(&directory_table, DIRECTORY_OID, new_directory_obj);
        if (!updated) {
          std::free((void *)new_directory_obj);
          t->Rollback();
          return std::nullopt;
        }
      } else {
        auto directory_size = num_entries*sizeof(Entry);
        auto new_directory_size = 2 * directory_size;

        table::Object *new_directory_obj =
            TypedObject<Entry>::MakeArray(2 * num_entries)->Obj();
        auto new_entries = reinterpret_cast<Entry *>(new_directory_obj->data);
        memcpy(&new_entries[0], entries, directory_size);
        memcpy(&new_entries[num_entries], entries, directory_size);

        // update global depth
        new_entries[0].global_depth += 1;

        new_entries[offset].local_depth += 1;

        new_entries[offset + num_entries].local_depth += 1;
        new_entries[offset + num_entries].bucket_oid = new_bucket_oid;

        bool updated = t->Update(&directory_table, DIRECTORY_OID, new_directory_obj);
        if (!updated) {
          std::free((void *)new_directory_obj);
          t->Rollback();
          return std::nullopt;
        }
      }

      auto ok = t->PreCommit();
      if (!ok) {
        t->Rollback();
        return std::nullopt;
      }
      t->PostCommit();
    }
    return std::nullopt; // unreachable
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
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();

    table::Object *directory = t->Read(&directory_table, DIRECTORY_OID);
    auto entries = reinterpret_cast<Entry *>(directory->data);
    // reading the global depth from first entry
    const uint32_t global_depth = entries->global_depth;
    auto num_entries = 1ULL << global_depth;
    auto offset_mask = num_entries - 1;
    auto offset = hash & offset_mask;
    auto entry = entries[offset];

    table::Object *bucket_obj = t->Read(&buckets_table, entry.bucket_oid);
    auto bucket = reinterpret_cast<Bucket *>(bucket_obj->data);

    bool found = bucket->Find(key, &out_value);
    bool ok = t->PreCommit();
    if (!ok) {
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
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();

    table::Object *directory = t->Read(&directory_table, DIRECTORY_OID);
    auto entries = reinterpret_cast<Entry *>(directory->data);
    // reading the global depth from first entry
    const uint32_t global_depth = entries->global_depth;
    auto num_entries = 1ULL << global_depth;
    auto offset_mask = num_entries - 1;
    auto offset = hash & offset_mask;
    auto entry = entries[offset];

    table::Object *bucket_obj = t->Read(&buckets_table, entry.bucket_oid);
    auto bucket = reinterpret_cast<Bucket *>(bucket_obj->data);

    table::Object *new_version_bucket_obj = TypedObject<Bucket>::Make()->Obj();
    auto new_version_bucket = reinterpret_cast<Bucket *>(new_version_bucket_obj->data);
    std::memcpy(new_version_bucket_obj->data, bucket_obj->data, sizeof(Bucket));

    bool exists = new_version_bucket->Update(key, value);
    bool updated = t->Update(&buckets_table, entry.bucket_oid, new_version_bucket_obj);
    if (!updated) {
      std::free((void *) new_version_bucket_obj);
      t->Rollback();
      return std::nullopt;
    }
    bool ok = t->PreCommit();
    if (!ok) {
      return std::nullopt;
    }
    t->PostCommit();
    return updated;
  }

  uint64_t scan(Key key, int64_t range, Value *output) {
    LOG(FATAL) << "Hash table doesn't support scan";
    return 0;
  }
};

} // namespace tabular
} // namespace noname

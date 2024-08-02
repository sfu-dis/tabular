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

#include "config.h"
#include "dynarray.h"
#include "object.h"


namespace noname {
namespace table {

// File (table) ID which itself is an OID as all tables are indexed by an
// indirection array as well
typedef OID FID;


template <typename T>
struct TypedObject {
  Object object;

  static TypedObject *Make() {
    auto obj = static_cast<TypedObject *>(std::malloc(sizeof(T) + sizeof(Object)));
    auto ptr = obj->Data();
    // TODO(ziyi) forward ctor list from Make to new T
    new (ptr) T;
    return obj;
  }

  static TypedObject *Make(T &data) {
    auto obj = static_cast<TypedObject *>(std::malloc(sizeof(T) + sizeof(Object)));
    *obj->Data() = data;
    return obj;
  }

  T *Data() {
    return reinterpret_cast<T *>(&object.data);
  }

  Object *Obj() {
    return &object;
  }
};


// Nearly a POD, nothing transactional and to be used as a static data structure
// that is manipulated by the trasancation processing code and is **unaware** of
// any multiversioning or concurrency control concepts. It only allows appending
// and reading entries indexed by OIDs.
struct IndirectionArrayTable {
  // Constructor
  // @config: config_t struct for parameters
  IndirectionArrayTable(config_t config);

  ~IndirectionArrayTable() {}

  // Parameters
  config_t config;

  // File id
  FID fid;

  // Centralised OID counter for now - may need to get a TLS allocator/cache later
  std::atomic<OID> next_oid;

  // The dynarray that's backing the indirection entries of this table
  DynamicArray<Object *> backing_array;

  // Append an object (record)
  // @object: the new object created by the caller
  OID Put(Object *object);

  // Return the entry at the specified OID entry
  Object *Get(OID oid);

  Object **GetEntryPtr(OID oid);
};

}  // namespace table
}  // namespace noname

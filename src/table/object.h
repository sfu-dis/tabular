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

#pragma once

#include <cstdint>
#include <cstdlib>
#include <new>

#include "noname_defs.h"

namespace noname {
namespace table {

// Per-table object (record) ID
typedef uint64_t OID;
typedef OID FID;
extern const uint64_t kInvalidOID;
extern const uint64_t kInvalidFID;

struct Object {
  Object();

  // An opaque "identification" area that is used by other components.
  // Object itself doesn't try to interpret it, nor cares about its use.
  // A typical use is for the transaction subsystem to use it to store CSN or
  // transaction ID (before commit). This is made opaque here to improve
  // modularity, as an object itself doesn't (and shouldn't) know about
  // transactions or even logging related issues.
  uint64_t id;

  // Next (older) version object
  Object *next;

  // Current data size
  uint32_t size;

  // Actual data, must be the last field
  char data[];

  inline uint32_t TotalSize() { return sizeof(*this) + size; }
  inline char *GetData() { return &data[0]; }

  static Object *Make(uint32_t obj_size) {
    auto obj = reinterpret_cast<table::Object *>(malloc(align_up(obj_size)));
    new (obj) table::Object();
    obj->size = obj_size - sizeof(table::Object);
    return obj;
  }

  static void Free(Object *obj) {
    free(obj);
  }
};

}  // namespace table
}  // namespace noname

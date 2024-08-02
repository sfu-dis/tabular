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
#include <cstring>

#include "table/object.h"

namespace noname {
namespace tabular {

using noname::table::Object;

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

  // create an object of type T[size]
  static TypedObject *MakeArray(const size_t size, const char *data = nullptr) {
    auto obj = static_cast<TypedObject *>(
        std::malloc(size * sizeof(T) + sizeof(Object)));
    if (data) {
      auto ptr = reinterpret_cast<char *>(obj->object.data);
      std::memcpy(ptr, data, size*sizeof(T));
    }
    return obj;
  } 

  T *Data() {
    return reinterpret_cast<T *>(&object.data);
  }

  Object *Obj() {
    return &object;
  }
};

}  // namespace tabular
}  // namespace noname

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

#include <cstdlib>

#include "noname_defs.h"

namespace noname {
namespace transaction {

class StringArena {
 public:
  static const uint64_t RESERVED_BYTES = (512 + 1024) * 1024 * 1024;
  StringArena() : n(0) {
    str = reinterpret_cast<uint8_t *>(
        aligned_alloc(DEFAULT_ALIGNMENT, RESERVED_BYTES));
    CHECK(str);
  }
  StringArena(uint32_t size_mb) : n(0) {
    str = reinterpret_cast<uint8_t *>(
        aligned_alloc(DEFAULT_ALIGNMENT, size_mb * MB));
    CHECK(str);
  }

  // non-copyable/non-movable for the time being
  StringArena(StringArena &&) = delete;
  StringArena(const StringArena &) = delete;
  StringArena &operator=(const StringArena &) = delete;

  inline void Reset() {
    CHECK(n < RESERVED_BYTES);
    n = 0;
  }

  uint8_t *Next(uint64_t size) {
    uint64_t off = n;
    n += align_up(size);
    CHECK_LE(n, RESERVED_BYTES);
    return str + off;
  }

  template <typename T>
  T *NextValue(size_t size = sizeof(T)) {
    auto val = reinterpret_cast<T *>(Next(size));
    new (val) T;
    return val;
  }

 private:
  uint8_t *str;
  size_t n;
};

}  // namespace transaction
}  // namespace noname

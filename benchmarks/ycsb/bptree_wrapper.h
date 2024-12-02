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

#include <cstdlib>
#include <functional>
#include <memory>

#include "third-party/bptree/tlx-leafds/container/btree_map.hpp"

#include "noname_defs.h"

namespace noname {
namespace benchmark {
namespace ycsb {

template <typename Key, typename Value, uint32_t internal_bytes = 1024,
          uint32_t log_size = 32, uint32_t block_size = 32>
struct BPTreeWrapper {
  tlx::btree_map<Key, Value, std::less<Key>,
                 tlx::btree_default_traits<Key, Value, internal_bytes, log_size,
                                           block_size>,
                 std::allocator<Value>,
                 /*concurrent=*/true>
      tree;

  uint64_t scan(const Key &k, int64_t range, Value *output,
                const Key *end_key = nullptr) {
    // NOTE(ziyi) not implemented
    return 0;
  }

  bool insert(const Key &k, Value v) {
    auto [it, ok] = tree.insert({k, k});
    return ok;
  }

  bool update(const Key &k, Value v) {
    return tree.update({k, v});
  }

  bool lookup(const Key &k, Value &result) {
    result = tree.value(k);
    return true;
  }
};

} // namespace ycsb
} // namespace benchmark
} // namespace noname

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

#include "noname_defs.h"
#if defined(ARTOLC)
#include "index/indexes/ARTOLC/Tree.h"
#elif defined(ART_LC)
#include "index/indexes/ARTLC/Tree.h"
#elif defined(ART_TABULAR)
#include "tabular/art/Tree.h"
#else
#error "ART variant is not defined"
#endif

namespace noname {
namespace benchmark {
namespace ycsb {

// Following implementation assumes KEY is uint64_t and VALUE is uint64_t
template <typename ART, typename KEY, typename VALUE>
struct ARTWrapper {
  ART *tree;

  struct Record {
    KEY key;
    uint8_t padding[2 * CACHELINE_SIZE - sizeof(KEY)];
  };
  static_assert(sizeof(Record) == 2 * CACHELINE_SIZE);

  static void loadKey(TID tid, Key &key) {
    auto record = reinterpret_cast<Record *>(tid);
    key.set(reinterpret_cast<const char *>(&record->key), sizeof(KEY));
  }

  static void removeNode(void *node) {
    // NOTE(ziyi) not implmeneted
  }

  explicit ARTWrapper(ART *tree) : tree(tree) {}

  ~ARTWrapper() {
  }

  uint64_t scan(const KEY &k, int64_t range, VALUE *output,
                const KEY *end_key = nullptr) {
    // NOTE(ziyi) not implmeneted
    return 0;
  }

  bool insert(const KEY &k, VALUE v) {
    Key key;
    key.set(reinterpret_cast<const char *>(&k), sizeof(KEY));
    return tree->insert(key, v);
  }

  bool update(const KEY &k, VALUE v) {
    Key key;
    key.set(reinterpret_cast<const char *>(&k), sizeof(KEY));
    TID tid = v;
    return tree->update(key, v);
  }

  bool lookup(const KEY &k, VALUE &result) {
    Key key;
    key.set(reinterpret_cast<const char *>(&k), sizeof(KEY));
    auto tid = tree->lookup(key);
    if (tid == 0) {
      // key not found
      return false;
    }
    result = tid;
    return true;
  }
};


} // namespace ycsb
} // namespace benchmark
} // namespace noname

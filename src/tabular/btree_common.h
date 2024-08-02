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

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <utility>

#include "table/object.h"

namespace noname {
namespace tabular {

enum class NodeType : uint8_t { Inner = 1, Leaf = 2 };

static inline constexpr uint64_t kPageSizeLinearSearchCutoff = 256;  // Use binary search if larger
#ifndef BTREE_PAGE_SIZE
#define BTREE_PAGE_SIZE 256
#endif
// make BTree page size configurable through compiling macros
static inline constexpr uint64_t kPageSize = BTREE_PAGE_SIZE - 8;  // Match memory layout of OptiQL BTree
static inline constexpr uint64_t kMaxLevels = 16;

struct NodeBase {
  inline NodeType getType() const { return (level == 1) ? NodeType::Leaf : NodeType::Inner; }

  uint8_t level;
  uint16_t count;
};

struct BTreeNode : public NodeBase {
  uint8_t _data[kPageSize - sizeof(NodeBase)];
};

template <typename Key, typename Value>
struct BTreeLeafNode : public NodeBase {
  struct KeyValueType {
    Key key;
    Value value;
  };
  static constexpr size_t entrySize = sizeof(KeyValueType);
  // XXX(shiges): one spot less to accept the new key-val pair when splitting
  static const uint64_t maxEntries =
      (kPageSize - sizeof(NodeBase) - sizeof(table::OID)) / entrySize - 1;

  // Singly linked list pointer to my sibling
  table::OID next_leaf;

  // This is the array that we perform search on
  KeyValueType data[maxEntries + 1];

  BTreeLeafNode() {
    level = 1;
    count = 0;
    next_leaf = table::kInvalidOID;
  }
  // generate copy ctor for in-memory copying
  BTreeLeafNode(BTreeLeafNode<Key, Value> &) = default;

  bool isFull() { return count == maxEntries; }

  unsigned lowerBound(const Key &k) {
    if constexpr (kPageSize <= kPageSizeLinearSearchCutoff) {
      unsigned lower = 0;
      while (lower < count) {
        const Key &next_key = data[lower].key;

        if (k <= next_key) {
          return lower;
        } else {
          lower++;
        }
      }
      return lower;
    } else {
      unsigned lower = 0;
      unsigned upper = count;
      do {
        unsigned mid = ((upper - lower) / 2) + lower;
        // This is the key at the pivot position
        const Key &middle_key = data[mid].key;

        if (k < middle_key) {
          upper = mid;
        } else if (k > middle_key) {
          lower = mid + 1;
        } else {
          return mid;
        }
      } while (lower < upper);
      return lower;
    }
  }

  bool key_exists(const Key &k) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].key == k)) {
        // key already exists
        return true;
      }
    }
    return false;
  }

  bool insert_no_existence_check(const Key &k, Value p) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      memmove(data + pos + 1, data + pos, sizeof(KeyValueType) * (count - pos));
      // memmove(payloads+pos+1,payloads+pos,sizeof(Value)*(count-pos));
      data[pos].key = k;
      data[pos].value = p;
    } else {
      data[0].key = k;
      data[0].value = p;
    }
    count++;
    return true;
  }

  bool insert(const Key &k, Value p) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].key == k)) {
        // key already exists
        return false;
      }
      memmove(data + pos + 1, data + pos, sizeof(KeyValueType) * (count - pos));
      // memmove(payloads+pos+1,payloads+pos,sizeof(Value)*(count-pos));
      data[pos].key = k;
      data[pos].value = p;
    } else {
      data[0].key = k;
      data[0].value = p;
    }
    count++;
    return true;
  }

  bool remove(const Key &k) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].key == k)) {
        // key found
        memmove(data + pos, data + pos + 1, sizeof(KeyValueType) * (count - pos));
        count--;
        return true;
      }
    }
    return false;
  }

  bool update(const Key &k, Value p) {
    assert(count <= maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].key == k)) {
        // Update
        data[pos].value = p;
        return true;
      }
    }
    return false;
  }

  void split_to(BTreeLeafNode *newLeaf, Key &sep) {
    newLeaf->count = count - (count / 2);
    count = count - newLeaf->count;
    memcpy(newLeaf->data, data + count, sizeof(KeyValueType) * newLeaf->count);
    newLeaf->next_leaf = next_leaf;
    sep = data[count - 1].key;
  }

};

template <typename Key>
struct BTreeInnerNode : public NodeBase {
  static constexpr size_t entrySize = sizeof(Key) + sizeof(table::OID);
  // XXX(shiges): one spot less to accept the new key-val pair when splitting
  static const uint64_t maxEntries = (kPageSize - sizeof(NodeBase)) / entrySize - 1;

  table::OID children[maxEntries + 1];
  Key keys[maxEntries + 1];

  BTreeInnerNode() {
    level = 1;
    count = 0;
  }
  explicit BTreeInnerNode(uint8_t node_level) {
    level = node_level;
    count = 0;
  }
  // generate copy ctor for in-memory copying
  BTreeInnerNode(BTreeInnerNode<Key> &) = default;

  bool isFull() { return count >= (maxEntries - 1); }

  unsigned lowerBoundBF(const Key &k) {
    auto base = keys;
    unsigned n = count;
    while (n > 1) {
      const unsigned half = n / 2;
      base = (base[half] < k) ? (base + half) : base;
      n -= half;
    }
    return (*base < k) + base - keys;
  }

  unsigned lowerBound(const Key &k) {
    if constexpr (kPageSize <= kPageSizeLinearSearchCutoff) {
      unsigned lower = 0;
      while (lower < count) {
        const Key &next_key = keys[lower];

        if (k <= next_key) {
          return lower;
        } else {
          lower++;
        }
      }
      return lower;
    } else {
      unsigned lower = 0;
      unsigned upper = count;
      do {
        unsigned mid = ((upper - lower) / 2) + lower;
        if (k < keys[mid]) {
          upper = mid;
        } else if (k > keys[mid]) {
          lower = mid + 1;
        } else {
          return mid;
        }
      } while (lower < upper);
      return lower;
    }
  }

  void split_to(BTreeInnerNode *newInner, Key &sep) {
    newInner->level = level;
    newInner->count = count - (count / 2);
    count = count - newInner->count - 1;
    sep = keys[count];
    memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
    memcpy(newInner->children, children + count + 1, sizeof(NodeBase *) * (newInner->count + 1));
  }

  void insert(const Key &k, table::OID child_id) {
    assert(count <= maxEntries - 1);
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
    memmove(children + pos + 1, children + pos, sizeof(NodeBase *) * (count - pos + 1));
    keys[pos] = k;
    children[pos] = child_id;
    std::swap(children[pos + 1], children[pos]);
    count++;
  }
};

static_assert(sizeof(BTreeNode) == kPageSize);
static_assert(sizeof(BTreeInnerNode<uint64_t>) <= kPageSize);
static_assert(sizeof(BTreeLeafNode<uint64_t, uint64_t>) <= kPageSize);
// TODO(ziyi) size of inner and leaf node may exceed kPageSize
// static_assert(sizeof(BTreeInnerNode<uint8_t>) <= kPageSize);
// static_assert(sizeof(BTreeLeafNode<uint8_t, uint8_t>) <= kPageSize);

}  // namespace tabular
}  // namespace noname

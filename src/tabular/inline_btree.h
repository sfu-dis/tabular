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

#include <iostream>
#include <tuple>
#include <optional>

#include "btree_common.h"
#include "table/inline_table.h"
#include "transaction/occ.h"

namespace noname {
namespace tabular {

using noname::transaction::occ::Transaction;
template <class Key, class Value>
struct InlineBTree {
  tabular::TableGroup group;
  table::InlineTable *nodes;
  bool is_persistent;

  static const size_t nodes_table_id = 0;

  static const table::OID root_id = table::OID{0};

  explicit InlineBTree(table::config_t config, bool is_persistent,
                       const std::filesystem::path &logging_directory = "",
                       size_t num_of_workers = 0)
      : group(config, is_persistent, logging_directory, num_of_workers) {
    group.StartEpochDaemon();
    nodes = group.GetTable(nodes_table_id, is_persistent);
    auto t = Transaction::BeginSystem(&group);
    auto initial_node = t->arena->NextValue<BTreeLeafNode<Key, Value>>(
        sizeof(BTreeNode)); // for accommodating both leaf and inner node
    auto initial_root_id = t->Insert(nodes, initial_node, sizeof(BTreeNode));
    assert(initial_root_id == root_id);
    auto ok = t->PreCommit();
    CHECK(ok);
    t->PostCommit();
    std::cout << "====================================" << std::endl;
    std::cout << "Inline Tabular BTree:" << std::endl;
    std::cout << "Max #entries: Leaf: " << BTreeLeafNode<Key, Value>::maxEntries
              << " Inner: " << BTreeInnerNode<Key>::maxEntries << std::endl;
    std::cout << "====================================" << std::endl;
  }

  ~InlineBTree() {
    group.StopEpochDaemon();
  }

  uint64_t scan(const Key &k, int64_t range, Value *output, 
		  const Key *end_key = nullptr) {
    std::optional<uint64_t> ret;
    do {
      ret = scan_internal(k, range, output, end_key);
    } while (!ret);

    return ret.value();
  }

  std::optional<uint64_t> scan_internal(const Key &k, int64_t range, Value *output,
		  const Key *end_key = nullptr) {
    auto t = Transaction::BeginSystem(&group);

    table::OID next_oid = root_id;
    auto node = t->arena->NextValue<BTreeNode>();
    #if defined(MATERIALIZED_READ)
    t->Read(nodes, next_oid, node);

    while (node->getType() == NodeType::Inner) {
      auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);

      next_oid = inner->children[inner->lowerBound(k)];
      t->Read(nodes, next_oid, node);
    }
    #else
    NodeType last_node = NodeType::Inner;

    auto read_node_cb = [&k, &last_node, &next_oid](NodeBase *node) {
      auto node_type = node->getType();
      if (node_type == NodeType::Inner) {
        auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);
        next_oid = inner->children[inner->lowerBound(k)];
      } else {  // node_type == NodeType::Leaf
        last_node = NodeType::Leaf;
      }
    };

    do  {
      t->ReadCallback<NodeBase>(nodes, next_oid, read_node_cb);
    } while (last_node == NodeType::Inner);

    t->Read(nodes, next_oid, node);
    #endif

    auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    uint64_t count = 0;

    // range is int64_t and count is uint64_t
    // -1 for range is a special value that marks infinite range
    // the comparaison will change range to uint64_t maximum value
    bool finish = false; 
    while (leaf && count < range) {
      for (size_t i = pos; i < leaf->count && count < range; i++) {
	if ( end_key == nullptr || leaf->data[i].key <= *end_key ) {
          output[count++] = leaf->data[i].value;
	} else {
	  finish = true; 
	  break;
	}
      }

      if (finish || count == range) {
        // scan() finishes at [leaf]
        break;
      } else {
        // proceed with next leaf
        if (leaf->next_leaf == table::kInvalidOID) {
          // scan() finishes at [leaf]
          break;
        }
        auto next_leaf = leaf->next_leaf;
        t->Read(nodes, next_leaf, leaf);
        pos = 0;
      }
    }
    if (!t->PreCommit()) {
        t->Rollback();
        return std::nullopt; 
    }
    t->PostCommit();
    return count;
  }

  struct UnsafeNodeStack {
    // When used in pessimistic top-down insertion, the stack holds references
    // to exclusively latched nodes.
   private:
    std::tuple<table::OID, NodeBase *> nodes[kMaxLevels];
    size_t top = 0;

   public:
    void Push(table::OID oid, NodeBase * node) {
      assert(top < kMaxLevels);
      nodes[top++] = std::make_tuple(oid, node);
    }
    std::tuple<table::OID, NodeBase *> Pop() {
      assert(top > 0);
      return nodes[--top];
    }
    std::tuple<table::OID, NodeBase *> Top() const {
      assert(top > 0);
      return nodes[top - 1];
    }
    size_t Size() const { return top; }
  };

  enum class Result {
    SUCCEED,
    FAILED,
    TRANSACTION_FAILED,
  };

  bool insert(const Key &k, Value v) {
    Result result = Result::TRANSACTION_FAILED;
    while (result == Result::TRANSACTION_FAILED) {
      #if defined(MATERIALIZED_INSERT)
      result = insert_internal(k, v);
      #else
      result = insert_internal_callback(k, v);
      #endif
    }
    assert(result != Result::TRANSACTION_FAILED);
    return result == Result::SUCCEED;
  }

  Result insert_internal_callback(const Key &k, Value v) {
    auto t = Transaction::BeginSystem(&group);

    // Fast path that doesn't split anything
    table::OID next_oid = root_id;
    bool key_exists = false;
    BTreeLeafNode<Key, Value> *leaf = nullptr;

    auto find_leaf_cb = [&k, &next_oid, &key_exists, &leaf](NodeBase *node) {
      auto node_type = node->getType();
      if (node_type == NodeType::Inner) {
        auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);
        next_oid = inner->children[inner->lowerBound(k)];
      } else {  // node_type == NodeType::Leaf
        leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
        if (leaf->key_exists(k)) {
          key_exists = true;
        }
      }
    };

    do  {
      t->ReadCallback<NodeBase>(nodes, next_oid, find_leaf_cb);
    } while (!leaf);

    if (key_exists) {
      t->Rollback();
      return Result::FAILED;
    }

    // Now next_oid points to the leaf
    auto leaf_oid = next_oid;
    if (!leaf->isFull()) {
      // no need to split, just insert into [leaf]
      auto leaf_insert_cb = [leaf, k, v](uint8_t *data) -> void {
        leaf->insert_no_existence_check(k, v);
      };

      t->UpdateRecordCallback(nodes, leaf_oid, leaf_insert_cb);
      if (!t->PreCommit()) {
        t->Rollback();
        return Result::TRANSACTION_FAILED;
      }
      t->PostCommit();
      return Result::SUCCEED;
    } else {
      // Fallback to slow path
      return insert_internal_slow(k, v);
    }
  }

  BTreeInnerNode<Key> *MakeRoot(Transaction *t, Key sep, table::OID left_child_id, table::OID right_child_id, uint8_t child_level) {
    auto root = t->arena->NextValue<BTreeInnerNode<Key>>();
    root->count = 1;
    root->keys[0] = sep;
    root->children[0] = left_child_id;
    root->children[1] = right_child_id;
    root->level = child_level + 1;
    return root;
  }

  Result insert_internal_slow(const Key &k, Value v) {
    auto t = Transaction::BeginSystem(&group);
    UnsafeNodeStack stashed_nodes;
    table::OID node_id = root_id;
    auto node = t->arena->NextValue<BTreeNode>();
    t->Read(nodes, node_id, node);
    stashed_nodes.Push(node_id, node);

    while (node->getType() == NodeType::Inner) {
      auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);

      node_id = inner->children[inner->lowerBound(k)];
      auto next = t->arena->NextValue<BTreeNode>();
      t->Read(nodes, node_id, next);

      bool release_ancestors = false;
      if (next->getType() == NodeType::Inner) {
        // [next] is an inner node
        auto next_inner = reinterpret_cast<BTreeInnerNode<Key> *>(next);
        if (!next_inner->isFull()) {
          release_ancestors = true;
        }
      } else {
        // [next] is a leaf node
        auto next_leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(next);
        if (!next_leaf->isFull()) {
          release_ancestors = true;
        }
      }

      if (release_ancestors) {
        while (stashed_nodes.Size() > 0) {
          stashed_nodes.Pop();
        }
      }
      stashed_nodes.Push(node_id, next);
      node = next;
    }

    auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
    auto leaf_id = node_id;
    if (!leaf->isFull()) {
      // no need to split, just insert into [leaf]
      assert(stashed_nodes.Size() == 1);
      bool exists = leaf->key_exists(k);
      if (exists) {
        return Result::FAILED;
      }
      auto leaf_insert_cb = [&k, &v](uint8_t *data) {
        auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(data);
        leaf->insert_no_existence_check(k, v);
      };
      t->UpdateRecordCallback(nodes, leaf_id, leaf_insert_cb);
      auto ok = t->PreCommit();
      if (!ok) {
        t->Rollback();
        return Result::TRANSACTION_FAILED;
      }
      t->PostCommit();
      return Result::SUCCEED;
    }

    // handle splits
    bool ok = leaf->insert(k, v);  // insert new pair to additional space
    if (!ok) {
      return Result::FAILED;
    }
    auto [top_node_id, top_node] = stashed_nodes.Top();
    assert(leaf_id == top_node_id);
    stashed_nodes.Pop();
    Key sep;
    auto split_leaf = t->arena->NextValue<BTreeLeafNode<Key, Value>>();
    leaf->split_to(split_leaf, sep);
    auto new_node_id = t->Insert(nodes, split_leaf);
    leaf->next_leaf = new_node_id;
    if (stashed_nodes.Size() == 0) {
      // [leaf] has to be root
      // insert leaf into a new record
      // and update a newly created inner root node to root_id record
      assert(root_id == leaf_id);
      auto new_leaf_id = t->Insert(nodes, leaf);
      auto new_root = MakeRoot(t, sep, new_leaf_id, new_node_id, leaf->level);
      // TODO(ziyi) cannot apply in-place update as the same reason as L310
      t->Update(nodes, root_id, new_root);
    } else {
      // [leaf] has a parent
      // update leaf and insert it to its parent
      // TODO(ziyi) cannot simply change this to in-place update
      //   because leaf may be inserted at L265
      //   and t->Insert doesn't support callback yet.
      t->Update(nodes, leaf_id, leaf);
      while (stashed_nodes.Size() > 1) {
        auto [parent_id, stashed_node] = stashed_nodes.Pop();
        assert(stashed_node->getType() == NodeType::Inner);
        auto parent = reinterpret_cast<BTreeInnerNode<Key> *>(stashed_node);
        parent->insert(sep, new_node_id);
        auto split_inner = t->arena->NextValue<BTreeInnerNode<Key>>();
        parent->split_to(split_inner, sep);
        t->Update(nodes, parent_id, parent);
        new_node_id = t->Insert(nodes, split_inner);
      }
      assert(stashed_nodes.Size() == 1);
      auto [parent_id, stashed_node] = stashed_nodes.Pop();
      assert(stashed_node->getType() == NodeType::Inner);
      auto parent = reinterpret_cast<BTreeInnerNode<Key> *>(stashed_node);
      if (parent->isFull()) {
        assert(parent_id == root_id);
        auto split_inner = t->arena->NextValue<BTreeInnerNode<Key>>();
        parent->insert(sep, new_node_id);
        parent->split_to(split_inner, sep);
        auto new_split_inner_id = t->Insert(nodes, split_inner);
        // insert updated parent into a new record
        // and update newly created inner node to root_id record
        auto new_parent_id = t->Insert(nodes, parent);
        auto new_root = MakeRoot(t, sep, new_parent_id, new_split_inner_id, parent->level);
        // TODO(ziyi) also cannot simply change this because parent was inserted at L307
        t->Update(nodes, root_id, new_root);
      } else {
        // We have finally found some space
        parent->insert(sep, new_node_id);
        t->Update(nodes, parent_id, parent);
      }
    }
    ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return Result::TRANSACTION_FAILED;
    }
    t->PostCommit();
    return Result::SUCCEED;
  }

  Result insert_internal(const Key &k, Value v) {
    auto t = Transaction::BeginSystem(&group);
    UnsafeNodeStack stashed_nodes;
    table::OID node_id = root_id;
    auto node = t->arena->NextValue<BTreeNode>();
    t->Read(nodes, node_id, node);
    stashed_nodes.Push(node_id, node);

    while (node->getType() == NodeType::Inner) {
      auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);

      node_id = inner->children[inner->lowerBound(k)];
      auto next = t->arena->NextValue<BTreeNode>();
      t->Read(nodes, node_id, next);

      bool release_ancestors = false;
      if (next->getType() == NodeType::Inner) {
        // [next] is an inner node
        auto next_inner = reinterpret_cast<BTreeInnerNode<Key> *>(next);
        if (!next_inner->isFull()) {
          release_ancestors = true;
        }
      } else {
        // [next] is a leaf node
        auto next_leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(next);
        if (!next_leaf->isFull()) {
          release_ancestors = true;
        }
      }

      if (release_ancestors) {
        while (stashed_nodes.Size() > 0) {
          stashed_nodes.Pop();
        }
      }
      stashed_nodes.Push(node_id, next);
      node = next;
    }

    auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
    auto leaf_id = node_id;
    if (!leaf->isFull()) {
      // no need to split, just insert into [leaf]
      assert(stashed_nodes.Size() == 1);
      bool ok = leaf->insert(k, v);
      if (!ok) {
        return Result::FAILED;
      }
      // TODO(ziyi) seem to have too much copying: from record to local var and to write set buffer
      t->Update(nodes, leaf_id, leaf);
      ok = t->PreCommit();
      if (!ok) {
        t->Rollback();
        return Result::TRANSACTION_FAILED;
      }
      t->PostCommit();
      return Result::SUCCEED;
    }

    // handle splits
    bool ok = leaf->insert(k, v);  // insert new pair to additional space
    if (!ok) {
      return Result::FAILED;
    }
    auto [top_node_id, top_node] = stashed_nodes.Top();
    assert(leaf_id == top_node_id);
    stashed_nodes.Pop();
    Key sep;
    auto split_leaf = t->arena->NextValue<BTreeLeafNode<Key, Value>>();
    leaf->split_to(split_leaf, sep);
    auto new_node_id = t->Insert(nodes, split_leaf);
    leaf->next_leaf = new_node_id;
    if (stashed_nodes.Size() == 0) {
      // [leaf] has to be root
      // insert leaf into a new record
      // and update a newly created inner root node to root_id record
      assert(root_id == leaf_id);
      auto new_leaf_id = t->Insert(nodes, leaf);
      auto new_root = MakeRoot(t, sep, new_leaf_id, new_node_id, leaf->level);
      t->Update(nodes, root_id, new_root);
    } else {
      // [leaf] has a parent
      // update leaf and insert it to its parent
      t->Update(nodes, leaf_id, leaf);
      while (stashed_nodes.Size() > 1) {
        auto [parent_id, stashed_node] = stashed_nodes.Pop();
        assert(stashed_node->getType() == NodeType::Inner);
        auto parent = reinterpret_cast<BTreeInnerNode<Key> *>(stashed_node);
        parent->insert(sep, new_node_id);
        auto split_inner = t->arena->NextValue<BTreeInnerNode<Key>>();
        parent->split_to(split_inner, sep);
        t->Update(nodes, parent_id, parent);
        new_node_id = t->Insert(nodes, split_inner);
      }
      assert(stashed_nodes.Size() == 1);
      auto [parent_id, stashed_node] = stashed_nodes.Pop();
      assert(stashed_node->getType() == NodeType::Inner);
      auto parent = reinterpret_cast<BTreeInnerNode<Key> *>(stashed_node);
      if (parent->isFull()) {
        assert(parent_id == root_id);
        auto split_inner = t->arena->NextValue<BTreeInnerNode<Key>>();
        parent->insert(sep, new_node_id);
        parent->split_to(split_inner, sep);
        auto new_split_inner_id = t->Insert(nodes, split_inner);
        // insert updated parent into a new record
        // and update newly created inner node to root_id record
        auto new_parent_id = t->Insert(nodes, parent);
        auto new_root = MakeRoot(t, sep, new_parent_id, new_split_inner_id, parent->level);
        t->Update(nodes, root_id, new_root);
      } else {
        // We have finally found some space
        parent->insert(sep, new_node_id);
        t->Update(nodes, parent_id, parent);
      }
    }
    ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return Result::TRANSACTION_FAILED;
    }
    t->PostCommit();
    return Result::SUCCEED;
  }

  bool lookup(const Key &k, Value &result) {
    std::optional<bool> ret;
    do {
      ret = lookup_internal(k, result);
    } while (!ret);

    return ret.value();
  }

  std::optional<bool> lookup_internal(const Key &k, Value &result) {
    auto t = Transaction::BeginSystem(&group);

    table::OID next_oid = root_id;
    bool success = false;
    #if defined(MATERIALIZED_READ)
    BTreeNode node;
    t->Read(nodes, next_oid, &node);

    while (node.getType() == NodeType::Inner) {
      auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(&node);

      next_oid = inner->children[inner->lowerBound(k)];
      t->Read(nodes, next_oid, &node);
    }

    auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(&node);
    unsigned pos = leaf->lowerBound(k);
    if ((pos < leaf->count) && (leaf->data[pos].key == k)) {
      success = true;
      result = leaf->data[pos].value;
    }
    #else
    NodeType last_node = NodeType::Inner;

    auto read_node_cb = [&k, &last_node, &next_oid,
                         &success, &result](NodeBase *node) {
      auto node_type = node->getType();
      if (node_type == NodeType::Inner) {
        auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);
        next_oid = inner->children[inner->lowerBound(k)];
      } else {  // node_type == NodeType::Leaf
        auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
        unsigned pos = leaf->lowerBound(k);
        if ((pos < leaf->count) && (leaf->data[pos].key == k)) {
          success = true;
          result = leaf->data[pos].value;
        }
        last_node = NodeType::Leaf;
      }
    };

    do  {
      t->ReadCallback<NodeBase>(nodes, next_oid, read_node_cb);
    } while (last_node == NodeType::Inner);
    #endif

    auto ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();
    return success;
  }

  bool update(const Key &k, Value v) {
    std::optional<bool> ret;
    do {
      #if defined(MATERIALIZED_UPDATE)
      ret = update_internal(k, v);
      #else
      ret = update_internal_callback(k, v);
      #endif
    } while (!ret);

    return ret.value();
  }

  std::optional<bool> update_internal_callback(const Key &k, Value v) {
    auto t = Transaction::BeginSystem(&group);

    table::OID next_oid = root_id;
    auto node = t->arena->NextValue<BTreeNode>();
    NodeType last_node = NodeType::Inner;
    bool key_exists = false;
    auto read_node_cb = [&k, &last_node, &next_oid, &key_exists](NodeBase *node) {
      auto node_type = node->getType();
      if (node_type == NodeType::Inner) {
        auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);
        next_oid = inner->children[inner->lowerBound(k)];
      } else {  // node_type == NodeType::Leaf
        last_node = NodeType::Leaf;
        auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
        if (leaf->key_exists(k)) {
          key_exists = true;
        }
      }
    };

    do {
      t->ReadCallback<NodeBase>(nodes, next_oid, read_node_cb);
    } while (last_node == NodeType::Inner);

    if (!key_exists) {
      t->Rollback();
      return false;
    }

    auto leaf_update_cb = [k, v](uint8_t *data) -> void {
      auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(data);
      auto ok = leaf->update(k, v);
      CHECK(ok);
    };

    t->UpdateRecordCallback(nodes, next_oid, leaf_update_cb);
    if (!t->PreCommit()) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();
    return true;
  }

  std::optional<bool> update_internal(const Key &k, Value v) {
    auto t = Transaction::BeginSystem(&group);

    table::OID next_oid = root_id;
    auto node = t->arena->NextValue<BTreeNode>();
    #if defined(MATERIALIZED_READ)
    t->Read(nodes, next_oid, node);

    while (node->getType() == NodeType::Inner) {
      auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);

      next_oid = inner->children[inner->lowerBound(k)];
      t->Read(nodes, next_oid, node);
    }
    #else
    NodeType last_node = NodeType::Inner;
    auto read_node_cb = [&k, &last_node, &next_oid](NodeBase *node) {
      auto node_type = node->getType();
      if (node_type == NodeType::Inner) {
        auto inner = reinterpret_cast<BTreeInnerNode<Key> *>(node);
        next_oid = inner->children[inner->lowerBound(k)];
      } else {  // node_type == NodeType::Leaf
        last_node = NodeType::Leaf;
      }
    };

    do {
      t->ReadCallback<NodeBase>(nodes, next_oid, read_node_cb);
    } while (last_node == NodeType::Inner);

    // TODO(ziyi) re-read a tuple, add tid verification in read_set.Add().
    // Or not, correctness is still intact but there won't be early exit here.
    t->Read(nodes, next_oid, node);
    #endif

    auto leaf = reinterpret_cast<BTreeLeafNode<Key, Value> *>(node);
    bool ok = leaf->update(k, v);
    if (!ok) {
      t->Rollback();
      return false;
    }
    t->Update(nodes, next_oid, leaf);
    ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();
    return true;
  }
};

}  // namespace tabular
}  // namespace noname

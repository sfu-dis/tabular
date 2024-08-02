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

#include <cstddef>
#include <cstdlib>
#include <tuple>
#include <optional>

#include "table/ia_table.h"
#include "transaction/transaction.h"
#include "table/object.h"
#include "object.h"
#include "btree_common.h"

namespace noname {
namespace tabular {

using noname::transaction::mvcc::Transaction;

template <class Key, class Value>
struct BTree {
  table::IndirectionArrayTable nodes;

  static const table::OID root_id = table::OID{0};

  bool lookup(const Key &k, Value &result) {
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();
    auto root = t->Read(&nodes, root_id);
    auto node = reinterpret_cast<NodeBase *>(root->data);

    while (node->getType() == NodeType::Inner) {
      auto inner = static_cast<BTreeInnerNode<Key> *>(node);

      auto next_oid = inner->children[inner->lowerBound(k)];
      auto next = t->Read(&nodes, next_oid);
      node = reinterpret_cast<NodeBase *>(next->data);
    }

    auto leaf = static_cast<BTreeLeafNode<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->data[pos].key == k)) {
      success = true;
      result = leaf->data[pos].value;
    }
    t->PreCommit();
    t->PostCommit();

    return success;
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
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();
    auto root = t->Read(&nodes, root_id);
    auto node = reinterpret_cast<NodeBase *>(root->data);

    while (node->getType() == NodeType::Inner) {
      auto inner = static_cast<BTreeInnerNode<Key> *>(node);
      auto next_oid = inner->children[inner->lowerBound(k)];
      auto next = t->Read(&nodes, next_oid);
      node = reinterpret_cast<NodeBase *>(next->data);
    }

    auto leaf = static_cast<BTreeLeafNode<Key, Value> *>(node);
    unsigned pos = leaf->lowerBound(k);
    size_t count = 0;

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
        auto next_leaf_obj = t->Read(&nodes, leaf->next_leaf);
        auto next_leaf = reinterpret_cast<NodeBase *>(next_leaf_obj->data);
        leaf = static_cast<BTreeLeafNode<Key, Value> *>(next_leaf);
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
    void Push(table::OID oid, NodeBase *node) {
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
      result = insert_internal(k, v);
    }
    assert(result != Result::TRANSACTION_FAILED);
    return result == Result::SUCCEED;
  }

  TypedObject<BTreeInnerNode<Key>> *MakeRoot(Key sep, table::OID left_child_id, table::OID right_child_id, uint8_t child_level) {
    auto root = TypedObject<BTreeInnerNode<Key>>::Make();
    auto root_node = root->Data();
    root_node->count = 1;
    root_node->keys[0] = sep;
    root_node->children[0] = left_child_id;
    root_node->children[1] = right_child_id;
    root_node->level = child_level + 1;
    return root;
  }

  Result insert_internal(const Key &k, Value v) {
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();
    UnsafeNodeStack stashed_nodes;
    table::OID node_id = root_id;
    auto root = t->Read(&nodes, node_id);
    auto old_node_id = root_id;
    auto old_node_obj = root;
    auto node = reinterpret_cast<NodeBase *>(root->data);
    stashed_nodes.Push(node_id, node);

    while (node->getType() == NodeType::Inner) {
      auto inner = static_cast<BTreeInnerNode<Key> *>(node);

      auto next_node_id = inner->children[inner->lowerBound(k)];
      auto next_node = t->Read(&nodes, next_node_id);
      auto next = reinterpret_cast<NodeBase *>(next_node->data);

      bool release_ancestors = false;
      if (next->getType() == NodeType::Inner) {
        // [node] is an inner node
        auto next_inner = static_cast<BTreeInnerNode<Key> *>(next);
        if (!next_inner->isFull()) {
          release_ancestors = true;
        }
      } else {
        // [next] is a leaf node
        auto next_leaf = static_cast<BTreeLeafNode<Key, Value> *>(next);
        if (!next_leaf->isFull()) {
          release_ancestors = true;
        }
      }

      if (release_ancestors) {
        while (stashed_nodes.Size() > 0) {
          stashed_nodes.Pop();
        }
      }
      stashed_nodes.Push(next_node_id, next);

      node = next;
      node_id = next_node_id;
    }

    auto leaf = static_cast<BTreeLeafNode<Key, Value> *>(node);
    auto leaf_id = node_id;
    auto new_leaf_node = TypedObject<BTreeLeafNode<Key, Value>>::Make(*leaf);
    auto new_leaf = new_leaf_node->Data();
    bool ok = new_leaf->insert(k, v);
    if (!ok) {
      // key exists in leaf node
      std::free((void *) new_leaf_node);
      t->Rollback();
      return Result::FAILED;
    }
    if (!leaf->isFull()) {
      // no need to split, just insert into [leaf]
      assert(stashed_nodes.Size() == 1);
      auto ok = t->Update(&nodes, leaf_id, new_leaf_node->Obj());
      if (!ok) {
        std::free((void *)new_leaf_node);
        t->Rollback();
        return Result::TRANSACTION_FAILED;
      }
      t->PreCommit();
      t->PostCommit();
      return Result::SUCCEED;
    }

    // handle splits
    auto [top_node_id, top_node] = stashed_nodes.Top();
    assert(leaf_id == top_node_id);
    stashed_nodes.Pop();
    Key sep;
    auto split_leaf_node = TypedObject<BTreeLeafNode<Key, Value>>::Make();
    auto split_leaf = split_leaf_node->Data();
    new_leaf->split_to(split_leaf, sep);
    auto new_node_id = t->Insert(&nodes, split_leaf_node->Obj());
    new_leaf->next_leaf = new_node_id;
    if (stashed_nodes.Size() == 0) {
      // [leaf] has to be root
      // insert leaf into a new record
      // and update a newly created inner root node to root_id record
      assert(root_id == leaf_id);
      auto new_leaf_id = t->Insert(&nodes, new_leaf_node->Obj());
      auto root = MakeRoot(sep, new_leaf_id, new_node_id, new_leaf->level);
      auto ok = t->Update(&nodes, root_id, root->Obj());
      if (!ok) {
        std::free((void *) root);
        t->Rollback();
        return Result::TRANSACTION_FAILED;
      }
    } else {
      // [leaf] has a parent
      // update leaf and insert it to its parent
      auto ok = t->Update(&nodes, leaf_id, new_leaf_node->Obj());
      if (!ok) {
        std::free((void *) new_leaf_node);
        std::free((void *) split_leaf_node);
        t->Rollback();
        return Result::TRANSACTION_FAILED;
      }
      while (stashed_nodes.Size() > 1) {
        auto [parent_id, node] = stashed_nodes.Pop();
        assert(node->getType() == NodeType::Inner);
        auto parent = static_cast<BTreeInnerNode<Key> *>(node);
        auto new_parent_node = TypedObject<BTreeInnerNode<Key>>::Make(*parent);
        auto new_parent = new_parent_node->Data();
        new_parent->insert(sep, new_node_id);
        auto split_inner_node = TypedObject<BTreeInnerNode<Key>>::Make();
        auto split_inner = split_inner_node->Data();
        new_parent->split_to(split_inner, sep);
        auto ok = t->Update(&nodes, parent_id, new_parent_node->Obj());
        if (!ok) {
          std::free((void *) new_parent_node);
          std::free((void *) split_inner_node);
          t->Rollback();
          return Result::TRANSACTION_FAILED;
        }
        new_node_id = t->Insert(&nodes, split_inner_node->Obj());
      }
      assert(stashed_nodes.Size() == 1);
      auto [parent_id, node] = stashed_nodes.Pop();
      assert(node->getType() == NodeType::Inner);
      auto parent = static_cast<BTreeInnerNode<Key> *>(node);
      auto new_parent_node = TypedObject<BTreeInnerNode<Key>>::Make(*parent);
      auto new_parent = new_parent_node->Data();
      new_parent->insert(sep, new_node_id);
      if (parent->isFull()) {
        assert(parent_id == root_id);
        auto split_inner_node = TypedObject<BTreeInnerNode<Key>>::Make();
        auto split_inner = split_inner_node->Data();
        new_parent->split_to(split_inner, sep);
        auto new_split_inner_id = t->Insert(&nodes, split_inner_node->Obj());
        // insert updated parent into a new record
        // and update newly created inner node to root_id record
        auto new_parent_id = t->Insert(&nodes, new_parent_node->Obj());
        auto root = MakeRoot(sep, new_parent_id, new_split_inner_id, new_parent->level);
        auto ok = t->Update(&nodes, root_id, root->Obj());
        if (!ok) {
          std::free((void *) root);
          t->Rollback();
          return Result::TRANSACTION_FAILED;
        }
      } else {
        // We have finally found some space
        auto ok = t->Update(&nodes, parent_id, new_parent_node->Obj());
        if (!ok) {
          std::free((void *) new_parent_node);
          t->Rollback();
          return Result::TRANSACTION_FAILED;
        }
      }
    }
    t->PreCommit();
    t->PostCommit();
    return Result::SUCCEED;
  }

  bool remove(const Key &k) {
    Result result = Result::TRANSACTION_FAILED;
    while (result == Result::TRANSACTION_FAILED) {
      result = remove_internal(k);
    }
    assert(result != Result::TRANSACTION_FAILED);
    return result == Result::SUCCEED;
  }

  Result remove_internal(const Key &k) {
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();
    auto [ leaf_id, leaf ] = traverseToLeaf(t, k);
    auto new_leaf_node = TypedObject<BTreeLeafNode<Key, Value>>::Make(*leaf);
    auto new_leaf = new_leaf_node->Data();
    bool ok = new_leaf->remove(k);
    if (!ok) {
      t->Rollback();
      return Result::FAILED;
    }
    ok = t->Update(&nodes, leaf_id, new_leaf_node->Obj());
    if (!ok) {
      t->Rollback();
      return Result::TRANSACTION_FAILED;
    }
    t->PreCommit();
    t->PostCommit();
    return true;
  }

  bool update(const Key &k, Value v) {
    Result result = Result::TRANSACTION_FAILED;
    while (result == Result::TRANSACTION_FAILED) {
      result = update_internal(k, v);
    }
    assert(result != Result::TRANSACTION_FAILED);
    return result == Result::SUCCEED;
  }

  Result update_internal(const Key &k, Value v) {
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();
    auto [ leaf_id, leaf ] = traverseToLeaf(t, k);
    auto new_leaf_node = TypedObject<BTreeLeafNode<Key, Value>>::Make(*leaf);
    auto new_leaf = new_leaf_node->Data();
    bool ok = new_leaf->update(k, v);
    if (!ok) {
      t->Rollback();
      return Result::FAILED;
    }
    ok = t->Update(&nodes, leaf_id, new_leaf_node->Obj());
    if (!ok) {
      t->Rollback();
      return Result::TRANSACTION_FAILED;
    }
    t->PreCommit();
    t->PostCommit();
    return Result::SUCCEED;
  }

  std::tuple<table::OID, BTreeLeafNode<Key, Value> *> traverseToLeaf(Transaction<table::IndirectionArrayTable> *t, Key k) {
    auto next_id = root_id;
    auto next = t->Read(&nodes, next_id);
    auto node = reinterpret_cast<NodeBase *>(next->data);

    while (node->getType() == NodeType::Inner) {
      auto inner = static_cast<BTreeInnerNode<Key> *>(node);
      next_id = inner->children[inner->lowerBound(k)];
      next = t->Read(&nodes, next_id);
      node = reinterpret_cast<NodeBase *>(next->data);
    }

    return { next_id, static_cast<BTreeLeafNode<Key, Value> *>(node) };
  }

  BTree(table::config_t config) : nodes(config) {
    auto t = Transaction<table::IndirectionArrayTable>::BeginSystem();
    auto root_node = TypedObject<BTreeLeafNode<Key, Value>>::Make();
    auto initial_root_id = t->Insert(&nodes, root_node->Obj());
    assert(initial_root_id == root_id);
    t->PreCommit();
    t->PostCommit();
  }
};

} // namespace tabular
} // namespace noname

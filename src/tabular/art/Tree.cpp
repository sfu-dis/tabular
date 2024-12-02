#include "Tree.h"

#include <assert.h>

#include <algorithm>
#include <functional>

#include "Key.h"
#include "N.h"

namespace noname {
namespace tabular {
namespace art {

using noname::transaction::occ::Transaction;

Tree::Tree(LoadKeyFunction loadKey, RemoveNodeFunction removeNode,
           table::config_t config, bool is_persistent,
           const std::filesystem::path &logging_directory,
           size_t num_of_workers)
    : group(config, is_persistent, logging_directory, num_of_workers),
      loadKey(loadKey), removeNode(removeNode) {
  group.StartEpochDaemon();
  nodes = group.GetTable(nodes_table_id, is_persistent);
  auto t = Transaction::BeginSystem(&group);
  auto initial_node = t->arena->NextValue<N256>(nullptr, 0);
  auto initial_root_id = t->Insert(nodes, initial_node);
  assert(initial_root_id == root_id);
  auto ok = t->PreCommit();
  CHECK(ok);
  t->PostCommit();
}

Tree::~Tree() { group.StopEpochDaemon(); }

TID Tree::lookup(const Key &k) {
  std::optional<TID> ret;
  do {
    #ifdef MATERIALIZED_READ
    ret = lookup_internal_materialized(k);
    #else
    ret = lookup_internal(k);
    #endif
  } while (!ret);

  return ret.value();
}

std::optional<TID> Tree::lookup_internal(const Key &k) {
  auto t = Transaction::BeginSystem(&group);
  TID tid = 0;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  auto nodeID = root_id;
  auto node = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));

  bool done = false;
  do {
    t->ReadCallback<N>(nodes, nodeID, [&done, &k, &level, &optimisticPrefixMatch, &tid, &nodeID, this](N *node){
      switch (checkPrefix(node, k, level)) {  // increases level
        case CheckPrefixResult::NoMatch:
          done = true;
          return;
        case CheckPrefixResult::OptimisticMatch:
          optimisticPrefixMatch = true;
          // fallthrough
        case CheckPrefixResult::Match:
          if (k.getKeyLen() <= level) {
            done = true;
            return;
          }
          auto nextNodeID = N::getChild(k[level], node);
          if (nextNodeID == table::kInvalidOID) {
            done = true;
            return;
          }

          if (N::isLeaf(nextNodeID)) {
            tid = N::getLeaf(nextNodeID);
            if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
              tid = checkKey(tid, k);
            }
            done = true;
            return;
          }
          nodeID = nextNodeID;
          level++;
      }
    });
  } while (!done);

  auto ok = t->PreCommit();
  if (!ok) {
    t->Rollback();
    return std::nullopt;
  }
  t->PostCommit();
  return tid;
}

std::optional<TID> Tree::lookup_internal_materialized(const Key &k) {
  TID tid = 0;
  N *node;
  N *parentNode = nullptr;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  auto t = Transaction::BeginSystem(&group);
  node = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
  t->Read(nodes, root_id, node);

  while (true) {
    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        goto commit;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
        // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          goto commit;
        }
        parentNode = node;
        auto nodeID = N::getChild(k[level], parentNode);
        if (nodeID == table::kInvalidOID) {
          goto commit;
        }

        if (N::isLeaf(nodeID)) {
          tid = N::getLeaf(nodeID);
          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            tid = checkKey(tid, k);
          }
          goto commit;
        }
        t->Read(nodes, nodeID, node);
        level++;
    }
  }
commit:
  auto ok = t->PreCommit();
  if (!ok) {
    t->Rollback();
    return std::nullopt;
  }
  t->PostCommit();
  return tid;
}

TID Tree::checkKey(const TID tid, const Key &k) const {
  Key kt;
  this->loadKey(tid, kt);
  if (k == kt) {
    return tid;
  }
  return 0;
}

bool Tree::insert(const Key &k, TID tid) {
  std::optional<TID> ret;
  do {
    ret = insert_internal(k, tid);
  } while (!ret);

  return ret.value();
}

std::optional<bool> Tree::insert_internal(const Key &k, TID tid) {
  bool needRestart = false;

  auto t = Transaction::BeginSystem(&group);

  N *node = nullptr;
  table::OID nodeID = table::kInvalidOID;
  table::OID nextNodeID = root_id;
  N *nextNode = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
  t->Read(nodes, nextNodeID, nextNode);
  N *parentNode = nullptr;
  table::OID parentNodeID = table::kInvalidOID;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNodeID = nodeID;
    parentNode = node;
    parentKey = nodeKey;

    nodeID = nextNodeID;
    node = nextNode;

    uint64_t v;

    uint32_t nextLevel = level;

    uint8_t nonMatchingKey;
    Prefix remainingPrefix;
    auto res = checkPrefixPessimistic(t, nodes, node, v, k, nextLevel, nonMatchingKey, remainingPrefix,
                                      this->loadKey, needRestart);  // increases level
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        // 1) Create new node which will be parent of node, Set common prefix, level to this node
        auto newNode = t->arena->NextValue<N4>(node->getPrefix(), nextLevel - level);
        auto newNodeID = t->Insert(nodes, newNode);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[nextLevel], N::setLeaf(tid));
        newNode->insert(nonMatchingKey, nodeID);

        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
        N::change_txn(t, nodes, parentNodeID, parentNode, parentKey, newNodeID);

        // 4) update prefix of node, unlock
        node->setPrefix(remainingPrefix, node->getPrefixLength() - ((nextLevel - level) + 1));
        t->Update(nodes, nodeID, node);

        auto ok = t->PreCommit();
        if (!ok) {
          t->Rollback();
          return std::nullopt;
        }
        t->PostCommit();
        return true;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = nextLevel;
    nodeKey = k[level];
    nextNodeID = N::getChild(nodeKey, node);

    if (nextNodeID == table::kInvalidOID) {
      N *obsoleteN = nullptr;
      N::insertAndUnlock(t, nodes, nodeID, node, v, parentNodeID, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid),
                         needRestart, obsoleteN);
      if (obsoleteN) {
        this->removeNode(obsoleteN);
      }
      auto ok = t->PreCommit();
      if (!ok) {
        t->Rollback();
        return std::nullopt;
      }
      t->PostCommit();
      return true;
    }

    if (N::isLeaf(nextNodeID)) {
      Key key;
      loadKey(N::getLeaf(nextNodeID), key);

      if (key == k) {
        // key already exists
        return false;
      }

      level++;
      uint32_t prefixLength = 0;
      while (key[level + prefixLength] == k[level + prefixLength]) {
        prefixLength++;
      }

      auto n4 = t->arena->NextValue<N4>(&k[level], prefixLength);
      n4->insert(k[level + prefixLength], N::setLeaf(tid));
      n4->insert(key[level + prefixLength], nextNodeID);
      auto n4ID = t->Insert(nodes, n4);
      N::change_txn(t, nodes, nodeID, node, k[level - 1], n4ID);
      auto ok = t->PreCommit();
      if (!ok) {
        t->Rollback();
        return std::nullopt;
      }
      t->PostCommit();
      return true;
    }

    nextNode = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
    t->Read(nodes, nextNodeID, nextNode);

    level++;
    parentVersion = v;
  }
}

bool Tree::traverseToLeafUpgradeEx(Transaction *t, const Key &k, table::OID &parentNodeID_out, uint32_t &level_out) {
  table::OID nodeID = table::kInvalidOID;
  N *node;
  table::OID parentNodeID = table::kInvalidOID;
  N *parentNode = nullptr;
  uint64_t v;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;
  bool matched = false;
  bool done = false;

  nodeID = root_id;
  node = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
  do {
    t->ReadCallback<N>(nodes, nodeID, [&done, &matched, &k, &level, &optimisticPrefixMatch, &parentNodeID_out, &level_out, &nodeID, this](N *node){
      switch (checkPrefix(node, k, level)) {  // increases level
        case CheckPrefixResult::NoMatch:
          done = true;
          return;
        case CheckPrefixResult::OptimisticMatch:
          optimisticPrefixMatch = true;
          // fallthrough
        case CheckPrefixResult::Match:
          if (k.getKeyLen() <= level) {
            done = true;
            return;
          }
          auto nextNodeID = N::getChild(k[level], node);

          if (nextNodeID == table::kInvalidOID) {
            done = true;
            return;
          }
          if (N::isLeaf(nextNodeID)) {
            TID old_tid = N::getLeaf(nextNodeID);
            if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
              if (checkKey(old_tid, k) != old_tid) {
                done = true;
                return;
              }
            }

            parentNodeID_out = nodeID;
            level_out = level;
            done = true;
            matched = true;
            return;
          }
          nodeID = nextNodeID;
          level++;
      }
    });
  } while (!done);
  return matched;
}

bool Tree::traverseToLeafUpgradeEx_materialized(Transaction *t, const Key &k, table::OID &parentNodeID_out, N *&parentNode_out, uint32_t &level_out) {
  table::OID nodeID = table::kInvalidOID;
  N *node;
  table::OID parentNodeID = table::kInvalidOID;
  N *parentNode = nullptr;
  uint64_t v;
  uint32_t level = 0;
  bool optimisticPrefixMatch = false;

  nodeID = root_id;
  node = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
  t->Read(nodes, nodeID, node);
  while (true) {
    switch (checkPrefix(node, k, level)) {  // increases level
      case CheckPrefixResult::NoMatch:
        return false;
      case CheckPrefixResult::OptimisticMatch:
        optimisticPrefixMatch = true;
        // fallthrough
      case CheckPrefixResult::Match:
        if (k.getKeyLen() <= level) {
          return false;
        }
        parentNodeID = nodeID;
        parentNode = node;
        nodeID = N::getChild(k[level], parentNode);

        if (nodeID == table::kInvalidOID) {
          return false;
        }
        if (N::isLeaf(nodeID)) {
          TID old_tid = N::getLeaf(nodeID);
          if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
            if (checkKey(old_tid, k) != old_tid) {
              return false;
            }
          }

          parentNodeID_out = parentNodeID;
          parentNode_out = parentNode;
          level_out = level;
          return true;
        }
        node = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
        t->Read(nodes, nodeID, node);
        level++;
    }
  }
}

bool Tree::update(const Key &k, TID new_tid) {
  std::optional<TID> ret;
  do {
    #ifdef MATERIALIZED_UPDATE
    ret = update_internal_materialized(k, new_tid);
    #else
    ret = update_internal(k, new_tid);
    #endif
  } while (!ret);

  return ret.value();
}

std::optional<bool> Tree::update_internal(const Key &k, TID new_tid) {
  auto t = Transaction::BeginSystem(&group);
  N *parentNode = nullptr;
  table::OID parentNodeID = table::kInvalidOID;
  uint32_t level = 0;
  if (!traverseToLeafUpgradeEx(t, k, parentNodeID, level)) {
    auto ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();
    return false;
  }

  auto tid = N::setLeaf(new_tid);
  t->UpdateRecordCallback(nodes, parentNodeID, [&k, level, tid](uint8_t *data) {
    auto node = reinterpret_cast<N *>(data);
    N::change(node, k[level], tid);
  });
  auto ok = t->PreCommit();
  if (!ok) {
    t->Rollback();
    return std::nullopt;
  }
  t->PostCommit();
  return true;
}

std::optional<bool> Tree::update_internal_materialized(const Key &k, TID new_tid) {
  auto t = Transaction::BeginSystem(&group);
  N *parentNode = nullptr;
  table::OID parentNodeID = table::kInvalidOID;
  uint32_t level = 0;
  if (!traverseToLeafUpgradeEx_materialized(t, k, parentNodeID, parentNode, level)) {
    auto ok = t->PreCommit();
    if (!ok) {
      t->Rollback();
      return std::nullopt;
    }
    t->PostCommit();
    return false;
  }

  auto tid = N::setLeaf(new_tid);
  N::change_txn(t, nodes, parentNodeID, parentNode, k[level], tid);
  auto ok = t->PreCommit();
  if (!ok) {
    t->Rollback();
    return std::nullopt;
  }
  t->PostCommit();
  return true;
}

#if 0
bool Tree::update(const Key &k, const char *value, size_t value_sz) {
#if defined(IS_CONTEXTFUL)
  OMCSLock::Context q;
#endif
  N *parentNode = nullptr;
  uint32_t level = 0;
#if defined(ART_OLC_UPGRADE)
#if defined(IS_CONTEXTFUL)
  if (!traverseToLeafUpgradeEx(k, q, parentNode, level)) {
#else
  if (!traverseToLeafUpgradeEx(k, parentNode, level)) {
#endif
#else
#if defined(IS_CONTEXTFUL)
  if (!traverseToLeafEx(k, q, parentNode, level)) {
#else
  if (!traverseToLeafEx(k, parentNode, level)) {
#endif
#endif
    return false;
  }

  TID tid = N::getLeaf(N::getChild(k[level], parentNode));
  const char *record = reinterpret_cast<const char *>(tid);

  memcpy((void *)(record + k.getKeyLen()), value, value_sz);
#if defined(IS_CONTEXTFUL)
  parentNode->writeUnlock(&q);
#else
  parentNode->writeUnlock();
#endif
  return true;
}

bool Tree::update(const Key &k) {
  // XXX(shiges): Do not use
#if defined(IS_CONTEXTFUL)
  OMCSLock::Context q;
#endif
  N *parentNode = nullptr;
  uint32_t level = 0;
#if defined(ART_OLC_UPGRADE)
#if defined(IS_CONTEXTFUL)
  if (!traverseToLeafUpgradeEx(k, q, parentNode, level)) {
#else
  if (!traverseToLeafUpgradeEx(k, parentNode, level)) {
#endif
#else
#if defined(IS_CONTEXTFUL)
  if (!traverseToLeafEx(k, q, parentNode, level)) {
#else
  if (!traverseToLeafEx(k, parentNode, level)) {
#endif
#endif
    return false;
  }

  TID tid = N::getLeaf(N::getChild(k[level], parentNode));
  // Value updates are ignored, and [record] is not dereferenced either.
  N::change(parentNode, k[level], N::setLeaf(tid));
#if defined(IS_CONTEXTFUL)
  parentNode->writeUnlock(&q);
#else
  parentNode->writeUnlock();
#endif
  return true;
}
#endif

#ifdef ART_UPSERT
bool Tree::upsert(const Key &k, TID tid) {
restart:
  bool needRestart = false;

  N *node = nullptr;
  N *nextNode = root;
  N *parentNode = nullptr;
  uint8_t parentKey, nodeKey = 0;
  uint64_t parentVersion = 0;
  uint32_t level = 0;

  while (true) {
    parentNode = node;
    parentKey = nodeKey;
    node = nextNode;
    auto v = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;

    uint32_t nextLevel = level;

    uint8_t nonMatchingKey;
    Prefix remainingPrefix;
    auto res = checkPrefixPessimistic(node, v, k, nextLevel, nonMatchingKey, remainingPrefix,
                                      this->loadKey, needRestart);  // increases level
    if (needRestart) goto restart;
    switch (res) {
      case CheckPrefixPessimisticResult::NoMatch: {
        parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
        if (needRestart) goto restart;

        node->upgradeToWriteLockOrRestart(v, needRestart);
        if (needRestart) {
          parentNode->writeUnlock();
          goto restart;
        }
        // 1) Create new node which will be parent of node, Set common prefix, level to this node
        auto newNode = new N4(node->getPrefix(), nextLevel - level);

        // 2)  add node and (tid, *k) as children
        newNode->insert(k[nextLevel], N::setLeaf(tid));
        newNode->insert(nonMatchingKey, node);

        // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
        N::change(parentNode, parentKey, newNode);
        parentNode->writeUnlock();

        // 4) update prefix of node, unlock
        node->setPrefix(remainingPrefix, node->getPrefixLength() - ((nextLevel - level) + 1));

        node->writeUnlock();
        return true;
      }
      case CheckPrefixPessimisticResult::Match:
        break;
    }
    level = nextLevel;
    nodeKey = k[level];
    nextNode = N::getChild(nodeKey, node);
    node->checkOrRestart(v, needRestart);
    if (needRestart) goto restart;

    if (nextNode == nullptr) {
      N *obsoleteN = nullptr;
      N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid),
                         needRestart, obsoleteN);
      if (needRestart) goto restart;
      if (obsoleteN) {
        this->removeNode(obsoleteN);
      }
      return true;
    }

    if (parentNode != nullptr) {
      parentNode->readUnlockOrRestart(parentVersion, needRestart);
      if (needRestart) goto restart;
    }

    if (N::isLeaf(nextNode)) {
      node->upgradeToWriteLockOrRestart(v, needRestart);
      if (needRestart) goto restart;

      Key key;
      loadKey(N::getLeaf(nextNode), key);

      if (key == k) {
        // upsert
        N::change(node, k[level], N::setLeaf(tid));
        node->writeUnlock();
        return true;
      }

      level++;
      uint32_t prefixLength = 0;
      while (key[level + prefixLength] == k[level + prefixLength]) {
        prefixLength++;
      }

      auto n4 = new N4(&k[level], prefixLength);
      n4->insert(k[level + prefixLength], N::setLeaf(tid));
      n4->insert(key[level + prefixLength], nextNode);
      N::change(node, k[level - 1], n4);
      node->writeUnlock();
      return true;
    }
    level++;
    parentVersion = v;
  }
}
#endif

inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
  if (n->hasPrefix()) {
    if (k.getKeyLen() <= level + n->getPrefixLength()) {
      return CheckPrefixResult::NoMatch;
    }
    for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
      if (n->getPrefix()[i] != k[level]) {
        return CheckPrefixResult::NoMatch;
      }
      ++level;
    }
    if (n->getPrefixLength() > maxStoredPrefixLength) {
      level = level + (n->getPrefixLength() - maxStoredPrefixLength);
      return CheckPrefixResult::OptimisticMatch;
    }
  }
  return CheckPrefixResult::Match;
}

typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(
    Transaction *t, table::InlineTable *nodes, N *n, uint64_t v, const Key &k, uint32_t &level, uint8_t &nonMatchingKey,
    Prefix &nonMatchingPrefix, LoadKeyFunction loadKey, bool &needRestart) {
  uint32_t prefixLen = n->getPrefixLength();
  if (prefixLen) {
    uint32_t prevLevel = level;
    Key kt;
    for (uint32_t i = 0; i < prefixLen; ++i) {
      if (i == maxStoredPrefixLength) {
        auto anyTID = N::getAnyChildTid(t, nodes, n, needRestart);
        loadKey(anyTID, kt);
      }
      uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
      if (curKey != k[level]) {
        nonMatchingKey = curKey;
        if (prefixLen > maxStoredPrefixLength) {
          if (i < maxStoredPrefixLength) {
            auto anyTID = N::getAnyChildTid(t, nodes, n, needRestart);
            loadKey(anyTID, kt);
          }
          memcpy(nonMatchingPrefix, &kt[0] + level + 1,
                 std::min((prefixLen - (level - prevLevel) - 1), maxStoredPrefixLength));
        } else {
          memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, prefixLen - i - 1);
        }
        return CheckPrefixPessimisticResult::NoMatch;
      }
      ++level;
    }
  }
  return CheckPrefixPessimisticResult::Match;
}

} // namespace art
} // namespace tabular
} // namespace noname

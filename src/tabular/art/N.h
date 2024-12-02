//
// Created by florian on 05.08.15.
//

#pragma once
//#define ART_NOREADLOCK
//#define ART_NOWRITELOCK
#include <stdint.h>
#include <string.h>

#include <atomic>

#include "Key.h"
#include "index/latches/OMCS.h"
#include "index/latches/OMCSOffset.h"

#include "table/object.h"
#include "transaction/occ.h"
#include "table/inline_table.h"

using TID = uint64_t;

#if defined(IS_CONTEXTFUL)
#if defined(OMCS_OFFSET)
#define DEFINE_CONTEXT(q, i) OMCSLock::Context &q = *offset::get_qnode(i)
#else
#define DEFINE_CONTEXT(q, i) OMCSLock::Context q
#endif
#define LOCK_NODE(n) n->writeLockOrRestart(&q, needRestart)
#define UPGRADE_NODE(n) n->upgradeToWriteLockOrRestart(v, &q, needRestart)
#define UPGRADE_PARENT() \
  parentNode->upgradeToWriteLockOrRestart(parentVersion, &parentQ, needRestart)
#define UNLOCK_NODE(n) n->writeUnlock(&q)
#define UNLOCK_PARENT() parentNode->writeUnlock(&parentQ)
#else
#define DEFINE_CONTEXT(q, i)
#define LOCK_NODE(n) n->writeLockOrRestart(needRestart)
#define UPGRADE_NODE(n) n->upgradeToWriteLockOrRestart(v, needRestart)
#define UPGRADE_PARENT() parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart)
#define UNLOCK_NODE(n) n->writeUnlock()
#define UNLOCK_PARENT() parentNode->writeUnlock()
#endif

namespace noname {
namespace tabular {
namespace art {

using noname::transaction::occ::Transaction;

/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */

enum class NTypes : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

static constexpr uint32_t maxStoredPrefixLength = 15;

using Prefix = uint8_t[maxStoredPrefixLength];

class N {
 public:
  using Lock = OMCSLock;

 protected:
  N(NTypes type, const uint8_t *prefix, uint32_t prefixLength) {
    setType(type);
    setPrefix(prefix, prefixLength);
  }

  N(const N &) = delete;

  N(N &&) = delete;

  struct TaggedPrefixCount {
    // 2b type 1b obsolete 29b prefixCount
    static const uint32_t kMaxPrefixCount = (1 << 29) - 1;
    static const uint32_t kObsoleteBit = 1 << 29;
    static const uint32_t kBitsMask = 0xE0000000;
    static const uint32_t kTypeBitsMask = 0xC0000000;

    uint32_t value;

    TaggedPrefixCount() = default;
    TaggedPrefixCount(uint32_t v) : value(v) {}

    TaggedPrefixCount &operator=(const uint32_t v) {
      assert(v < kMaxPrefixCount);
      value = v | (value & kBitsMask);
      return *this;
    }

    TaggedPrefixCount &operator+=(const uint32_t v) {
      uint32_t newv = get() + v;
      assert(newv < kMaxPrefixCount);
      value = newv | (value & kBitsMask);
      return *this;
    }

    uint32_t get() const { return value & (~kBitsMask); }

    uint32_t getRaw() const { return value; }

    uint32_t getObsolete() const { return value & kObsoleteBit; }

    void setType(uint32_t type) { value = (value & (~kTypeBitsMask)) | type; }

    void setObsolete() { value |= kObsoleteBit; }
  };

  TaggedPrefixCount prefixCount = 0;

  uint8_t count = 0;
  Prefix prefix;

 protected:
  void setType(NTypes type);

  static uint32_t convertTypeToPrefixCount(NTypes type);

 public:
  NTypes getType() const;

  uint32_t getCount() const;

  bool isLocked() const { 
    return false; }

  inline void checkObsoleteOrRestart(bool &needRestart) const {
    return;
  }

  void writeLockOrRestart(bool &needRestart) {
    return;
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    return;
  }

  void writeUnlock() {
    return;
  }

  void writeLockOrRestart(Lock::Context *q, bool &needRestart) {
    return;
  }

  void upgradeToWriteLockOrRestart(uint64_t &version, Lock::Context *q, bool &needRestart) {
    return;
  }

  void writeUnlock(Lock::Context *q) {
    return;
  }

  uint64_t readLockOrRestart(bool &needRestart) const {
    return 0;
  }

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    return;
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    return;
  }

  bool isObsolete() const { return prefixCount.getObsolete(); }

  void setObsolete() { prefixCount.setObsolete(); }

  static table::OID getChild(const uint8_t k, const N *node);

  static void insertAndUnlock(Transaction *t, table::InlineTable *nodes, table::OID nodeID, N *node, uint64_t v, table::OID parentNodeID, N *parentNode, uint64_t parentVersion,
                              uint8_t keyParent, uint8_t key, table::OID val, bool &needRestart,
                              N *&obsoleteN);

  static bool change(N *node, uint8_t key, table::OID val);
  static bool change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, N *node, uint8_t key, table::OID val);

  bool hasPrefix() const;

  const uint8_t *getPrefix() const;

  void setPrefix(const uint8_t *prefix, uint32_t length);

  void addPrefixBefore(N *node, uint8_t key);

  uint32_t getPrefixLength() const;

  static TID getLeaf(const table::OID n);

  static bool isLeaf(const table::OID n);

  static table::OID setLeaf(TID tid);

  static table::OID getAnyChild(const N *n);

  static TID getAnyChildTid(Transaction *t, table::InlineTable *nodes, N *n, bool &needRestart);

  template <typename curN, typename biggerN>
  static void insertGrow(Transaction *t, table::InlineTable *nodes, table::OID nodeID, curN *n, uint64_t v, table::OID parentNodeID, N *parentNode, uint64_t parentVersion,
                         uint8_t keyParent, uint8_t key, table::OID val, bool &needRestart, N *&obsoleteN);
};

class N4 : public N {
 public:
  uint8_t keys[4];
  table::OID children[4];

 public:
  N4(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N4, prefix, prefixLength) {}

  void insert(uint8_t key, table::OID n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, table::OID val);
  bool change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID val);

  table::OID getChild(const uint8_t k) const;

  table::OID getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;

  std::tuple<N *, uint8_t> getSecondChild(const uint8_t key) const;
};

static_assert(sizeof(N) == 20);

class N16 : public N {
 public:
  uint8_t keys[16];
  table::OID children[16];

  static uint8_t flipSign(uint8_t keyByte) {
    // Flip the sign bit, enables signed SSE comparison of unsigned values, used by Node16
    return keyByte ^ 128;
  }

  static inline unsigned ctz(uint16_t x) {
    // Count trailing zeros, only defined for x>0
#ifdef __GNUC__
    return __builtin_ctz(x);
#else
    // Adapted from Hacker's Delight
    unsigned n = 1;
    if ((x & 0xFF) == 0) {
      n += 8;
      x = x >> 8;
    }
    if ((x & 0x0F) == 0) {
      n += 4;
      x = x >> 4;
    }
    if ((x & 0x03) == 0) {
      n += 2;
      x = x >> 2;
    }
    return n - (x & 1);
#endif
  }

  table::OID const *getChildPos(const uint8_t k) const;

 public:
  N16(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N16, prefix, prefixLength) {
    memset(keys, 0, sizeof(keys));
  }

  void insert(uint8_t key, table::OID n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, table::OID val);
  bool change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID val);

  table::OID getChild(const uint8_t k) const;

  table::OID getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;
};

class N48 : public N {
  uint8_t childIndex[256];
  table::OID children[48];

 public:
  static const uint8_t emptyMarker = 48;

  N48(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N48, prefix, prefixLength) {
    memset(childIndex, emptyMarker, sizeof(childIndex));
    for (size_t i = 0; i < 48; i++) {
      children[i] = table::kInvalidOID;
    }
  }

  void insert(uint8_t key, table::OID n);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, table::OID val);
  bool change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID val);

  table::OID getChild(const uint8_t k) const;

  table::OID getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;
};

class N256 : public N {
  table::OID children[256];

 public:
  // default ctor for allocating space for a node from string arena
  N256() : N256(nullptr, 0) {}

  N256(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N256, prefix, prefixLength) {
    for (size_t i = 0; i < 256; i++) {
      children[i] = table::kInvalidOID;
    }
  }

  void insert(uint8_t key, table::OID val);

  template <class NODE>
  void copyTo(NODE *n) const;

  bool change(uint8_t key, table::OID val);
  bool change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID n);

  table::OID getChild(const uint8_t k) const;

  table::OID getAnyChild() const;

  bool isFull() const;

  bool isUnderfull() const;
};

inline table::OID N::getChild(const uint8_t k, const N *node) {
  switch (node->getType()) {
  case NTypes::N4: {
    auto n = static_cast<const N4 *>(node);
    return n->getChild(k);
  }
  case NTypes::N16: {
    auto n = static_cast<const N16 *>(node);
    return n->getChild(k);
  }
  case NTypes::N48: {
    auto n = static_cast<const N48 *>(node);
    return n->getChild(k);
  }
  case NTypes::N256: {
    auto n = static_cast<const N256 *>(node);
    return n->getChild(k);
  }
  }
  assert(false);
  __builtin_unreachable();
}

} // namespace art
} // namespace tabular
} // namespace noname

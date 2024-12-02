#include "N.h"

#include <assert.h>

#include <algorithm>

#include "N16.cpp"
#include "N256.cpp"
#include "N4.cpp"
#include "N48.cpp"

#include "transaction/occ.h"

namespace noname {
namespace tabular {
namespace art {

using noname::transaction::occ::Transaction;

void N::setType(NTypes type) { prefixCount.setType(convertTypeToPrefixCount(type)); }

uint32_t N::convertTypeToPrefixCount(NTypes type) { return (static_cast<uint32_t>(type) << 30); }

NTypes N::getType() const { return static_cast<NTypes>(prefixCount.getRaw() >> 30); }

table::OID N::getAnyChild(const N *node) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<const N4 *>(node);
      return n->getAnyChild();
    }
    case NTypes::N16: {
      auto n = static_cast<const N16 *>(node);
      return n->getAnyChild();
    }
    case NTypes::N48: {
      auto n = static_cast<const N48 *>(node);
      return n->getAnyChild();
    }
    case NTypes::N256: {
      auto n = static_cast<const N256 *>(node);
      return n->getAnyChild();
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N::change(N *node, uint8_t key, table::OID val) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->change(key, val);
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      return n->change(key, val);
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      return n->change(key, val);
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      return n->change(key, val);
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N::change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, N *node, uint8_t key, table::OID val) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      return n->change_txn(t, nodes, nodeID, key, val);
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      return n->change_txn(t, nodes, nodeID, key, val);
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      return n->change_txn(t, nodes, nodeID, key, val);
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      return n->change_txn(t, nodes, nodeID, key, val);
    }
  }
  assert(false);
  __builtin_unreachable();
}

template <typename curN, typename biggerN>
void N::insertGrow(Transaction *t, table::InlineTable *nodes, table::OID nodeID, curN *n, uint64_t v, table::OID parentNodeID, N *parentNode, uint64_t parentVersion, uint8_t keyParent,
                   uint8_t key, table::OID val, bool &needRestart, N *&obsoleteN) {
  if (!n->isFull()) {
    n->insert(key, val);
    t->Update(nodes, nodeID, n);
    return;
  }

  auto nBig = t->arena->NextValue<biggerN>(n->getPrefix(), n->getPrefixLength());
  n->copyTo(nBig);
  nBig->insert(key, val);

  auto nBigID = t->Insert(nodes, nBig);

  N::change_txn(t, nodes, parentNodeID, parentNode, keyParent, nBigID);

  n->setObsolete();
  obsoleteN = n;
}

void N::insertAndUnlock(Transaction *t, table::InlineTable *nodes, table::OID nodeID, N *node, uint64_t v, table::OID parentNodeID, N *parentNode, uint64_t parentVersion,
                        uint8_t keyParent, uint8_t key, table::OID val, bool &needRestart, N *&obsoleteN) {
  switch (node->getType()) {
    case NTypes::N4: {
      auto n = static_cast<N4 *>(node);
      insertGrow<N4, N16>(t, nodes, nodeID, n, v, parentNodeID, parentNode, parentVersion, keyParent, key, val, needRestart,
                          obsoleteN);
      break;
    }
    case NTypes::N16: {
      auto n = static_cast<N16 *>(node);
      insertGrow<N16, N48>(t, nodes, nodeID, n, v, parentNodeID, parentNode, parentVersion, keyParent, key, val, needRestart,
                           obsoleteN);
      break;
    }
    case NTypes::N48: {
      auto n = static_cast<N48 *>(node);
      insertGrow<N48, N256>(t, nodes, nodeID, n, v, parentNodeID, parentNode, parentVersion, keyParent, key, val, needRestart,
                            obsoleteN);
      break;
    }
    case NTypes::N256: {
      auto n = static_cast<N256 *>(node);
      insertGrow<N256, N256>(t, nodes, nodeID, n, v, parentNodeID, parentNode, parentVersion, keyParent, key, val, needRestart,
                             obsoleteN);
      break;
    }
  }
}

uint32_t N::getPrefixLength() const { return prefixCount.get(); }

bool N::hasPrefix() const { return prefixCount.get() > 0; }

uint32_t N::getCount() const { return count; }

const uint8_t *N::getPrefix() const { return prefix; }

void N::setPrefix(const uint8_t *prefix, uint32_t length) {
  if (length > 0) {
    memcpy(this->prefix, prefix, std::min(length, maxStoredPrefixLength));
    prefixCount = length;
  } else {
    prefixCount = 0;
  }
}

void N::addPrefixBefore(N *node, uint8_t key) {
  uint32_t prefixCopyCount = std::min(maxStoredPrefixLength, node->getPrefixLength() + 1);
  memmove(this->prefix + prefixCopyCount, this->prefix,
          std::min(this->getPrefixLength(), maxStoredPrefixLength - prefixCopyCount));
  memcpy(this->prefix, node->prefix, std::min(prefixCopyCount, node->getPrefixLength()));
  if (node->getPrefixLength() < maxStoredPrefixLength) {
    this->prefix[prefixCopyCount - 1] = key;
  }
  this->prefixCount += node->getPrefixLength() + 1;
}

bool N::isLeaf(const table::OID n) {
  return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << 63)) ==
         (static_cast<uint64_t>(1) << 63);
}

table::OID N::setLeaf(TID tid) { return reinterpret_cast<table::OID>(tid | (static_cast<uint64_t>(1) << 63)); }

TID N::getLeaf(const table::OID n) {
  return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << 63) - 1));
}

TID N::getAnyChildTid(Transaction *t, table::InlineTable *nodes, N *n, bool &needRestart) {
  N *nextNode = n;

  while (true) {
    const N *node = nextNode;

    auto nextNodeID = getAnyChild(node);

    assert(nextNodeID != table::kInvalidOID);
    if (isLeaf(nextNodeID)) {
      return getLeaf(nextNodeID);
    }
    nextNode = reinterpret_cast<N *>(t->arena->Next(sizeof(N256)));
    t->Read(nodes, nextNodeID, nextNode);
  }
}

} // namespace art
} // namespace tabular
} // namespace noname

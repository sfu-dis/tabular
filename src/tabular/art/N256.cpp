#include <assert.h>

#include <algorithm>

#include "N.h"

namespace noname {
namespace tabular {
namespace art {


bool N256::isFull() const { return false; }

bool N256::isUnderfull() const { return count == 37; }

void N256::insert(uint8_t key, table::OID val) {
  children[key] = val;
  count++;
}

template <class NODE>
void N256::copyTo(NODE *n) const {
  for (int i = 0; i < 256; ++i) {
    if (children[i] != table::kInvalidOID) {
      n->insert(i, children[i]);
    }
  }
}

bool N256::change(uint8_t key, table::OID n) {
  children[key] = n;
  return true;
}

bool N256::change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID n) {
  children[key] = n;
  t->Update(nodes, nodeID, this);
  return true;
}

table::OID N256::getChild(const uint8_t k) const { return children[k]; }

table::OID N256::getAnyChild() const {
  auto anyChild = table::kInvalidOID;
  for (uint64_t i = 0; i < 256; ++i) {
    if (children[i] != table::kInvalidOID) {
      if (N::isLeaf(children[i])) {
        return children[i];
      } else {
        anyChild = children[i];
      }
    }
  }
  return anyChild;
}

} // namespace art
} // namespace tabular
} // namespace noname

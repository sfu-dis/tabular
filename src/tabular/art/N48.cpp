#include <assert.h>

#include <algorithm>

#include "N.h"

namespace noname {
namespace tabular {
namespace art {


bool N48::isFull() const { return count == 48; }

bool N48::isUnderfull() const { return count == 12; }

void N48::insert(uint8_t key, table::OID n) {
  unsigned pos = count;
  if (children[pos] != table::kInvalidOID) {
    for (pos = 0; children[pos] != table::kInvalidOID; pos++)
      ;
  }
  children[pos] = n;
  childIndex[key] = (uint8_t)pos;
  count++;
}

template <class NODE>
void N48::copyTo(NODE *n) const {
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      n->insert(i, children[childIndex[i]]);
    }
  }
}

bool N48::change(uint8_t key, table::OID val) {
  children[childIndex[key]] = val;
  return true;
}

bool N48::change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID val) {
  children[childIndex[key]] = val;
  t->Update(nodes, nodeID, this);
  return true;
}

table::OID N48::getChild(const uint8_t k) const {
  if (childIndex[k] == emptyMarker) {
    return table::kInvalidOID;
  } else {
    return children[childIndex[k]];
  }
}

table::OID N48::getAnyChild() const {
  table::OID anyChild = table::kInvalidOID;
  for (unsigned i = 0; i < 256; i++) {
    if (childIndex[i] != emptyMarker) {
      if (N::isLeaf(children[childIndex[i]])) {
        return children[childIndex[i]];
      } else {
        anyChild = children[childIndex[i]];
      }
    }
  }
  return anyChild;
}

} // namespace art
} // namespace tabular
} // namespace noname

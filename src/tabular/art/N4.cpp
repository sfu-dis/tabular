#include <assert.h>

#include <algorithm>

#include "N.h"

#include "table/object.h"

namespace noname {
namespace tabular {
namespace art {

bool N4::isFull() const { return count == 4; }

bool N4::isUnderfull() const { return false; }

void N4::insert(uint8_t key, table::OID n) {
  unsigned pos;
  for (pos = 0; (pos < count) && (keys[pos] < key); pos++)
    ;
  memmove(keys + pos + 1, keys + pos, count - pos);
  memmove(children + pos + 1, children + pos, (count - pos) * sizeof(N *));
  keys[pos] = key;
  children[pos] = n;
  count++;
}

template <class NODE>
void N4::copyTo(NODE *n) const {
  for (uint32_t i = 0; i < count; ++i) {
    n->insert(keys[i], children[i]);
  }
}

bool N4::change(uint8_t key, table::OID val) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == key) {
      children[i] = val;
      return true;
    }
  }
  assert(false);
  __builtin_unreachable();
}

bool N4::change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID val) {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == key) {
      children[i] = val;
      t->Update(nodes, nodeID, this);
      return true;
    }
  }
  assert(false);
  __builtin_unreachable();
}

table::OID N4::getChild(const uint8_t k) const {
  for (uint32_t i = 0; i < count; ++i) {
    if (keys[i] == k) {
      return children[i];
    }
  }
  return table::kInvalidOID;
}

table::OID N4::getAnyChild() const {
  table::OID anyChild = table::kInvalidOID;
  for (uint32_t i = 0; i < count; ++i) {
    if (N::isLeaf(children[i])) {
      return children[i];
    } else {
      anyChild = children[i];
    }
  }
  return anyChild;
}

} // namespace art
} // namespace tabular
} // namespace noname

#include <assert.h>
#include <emmintrin.h>  // x86 SSE intrinsics

#include <algorithm>

#include "N.h"

namespace noname {
namespace tabular {
namespace art {


bool N16::isFull() const { return count == 16; }

bool N16::isUnderfull() const { return count == 3; }

void N16::insert(uint8_t key, table::OID n) {
  uint8_t keyByteFlipped = flipSign(key);
  __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(keyByteFlipped),
                               _mm_loadu_si128(reinterpret_cast<__m128i *>(keys)));
  uint16_t bitfield = _mm_movemask_epi8(cmp) & (0xFFFF >> (16 - count));
  unsigned pos = bitfield ? ctz(bitfield) : count;
  memmove(keys + pos + 1, keys + pos, count - pos);
  memmove(children + pos + 1, children + pos, (count - pos) * sizeof(uintptr_t));
  keys[pos] = keyByteFlipped;
  children[pos] = n;
  count++;
}

template <class NODE>
void N16::copyTo(NODE *n) const {
  for (unsigned i = 0; i < count; i++) {
    n->insert(flipSign(keys[i]), children[i]);
  }
}

bool N16::change(uint8_t key, table::OID val) {
  table::OID *childPos = const_cast<table::OID *>(getChildPos(key));
  assert(childPos != nullptr);
  *childPos = val;
  return true;
}

bool N16::change_txn(Transaction *t, table::InlineTable *nodes, table::OID nodeID, uint8_t key, table::OID val) {
  table::OID *childPos = const_cast<table::OID *>(getChildPos(key));
  assert(childPos != nullptr);
  *childPos = val;
  t->Update(nodes, nodeID, this);
  return true;
}

table::OID const *N16::getChildPos(const uint8_t k) const {
  __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(flipSign(k)),
                               _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
  unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << count) - 1);
  if (bitfield) {
    return &children[ctz(bitfield)];
  } else {
    return nullptr;
  }
}

table::OID N16::getChild(const uint8_t k) const {
  table::OID const *childPos = getChildPos(k);
  if (childPos == nullptr) {
    return table::kInvalidOID;
  } else {
    return *childPos;
  }
}

table::OID N16::getAnyChild() const {
  for (int i = 0; i < count; ++i) {
    if (N::isLeaf(children[i])) {
      return children[i];
    }
  }
  return children[0];
}

} // namespace art
} // namespace tabular
} // namespace noname

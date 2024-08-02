/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */

/*
 * Copyright (C) 2024 Data-Intensive Systems Lab, Simon Fraser University. 
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

// Implements a wrapper for Masstree index

#pragma once

#include "kvrow.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "noname_defs.h"
#include "table/ia_table.h"
#include "index/generic_index.h"

namespace Masstree {

class SimpleThreadInfo {
public:
  SimpleThreadInfo(EpochNum e) : ts_(0), epoch_(e) {}
  class mrcu_callback {
  public:
    virtual void operator()(SimpleThreadInfo &ti) = 0;
    virtual ~mrcu_callback() {}
  };

public:
  // XXX Correct node timstamps are needed for recovery, but for no other
  // reason.
  kvtimestamp_t operation_timestamp() const { return 0; }
  kvtimestamp_t update_timestamp() const { return ts_; }
  kvtimestamp_t update_timestamp(kvtimestamp_t x) const {
    if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
      // x might be a marker timestamp; ensure result is not
      ts_ = (x | 1) + 1;
    return ts_;
  }
  kvtimestamp_t update_timestamp(kvtimestamp_t x, kvtimestamp_t y) const {
    if (circular_int<kvtimestamp_t>::less(x, y))
      x = y;
    if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
      // x might be a marker timestamp; ensure result is not
      ts_ = (x | 1) + 1;
    return ts_;
  }
  void increment_timestamp() { ts_ += 2; }
  void advance_timestamp(kvtimestamp_t x) {
    if (circular_int<kvtimestamp_t>::less(ts_, x))
      ts_ = x;
  }

  /** @brief Return a function object that calls mark(ci); relax_fence().
   *
   * This function object can be used to count the number of relax_fence()s
   * executed. */
  relax_fence_function accounting_relax_fence(threadcounter) {
    return relax_fence_function();
  }

  class accounting_relax_fence_function {
  public:
    template <typename V> void operator()(V) { relax_fence(); }
  };
  /** @brief Return a function object that calls mark(ci); relax_fence().
   *
   * This function object can be used to count the number of relax_fence()s
   * executed. */
  accounting_relax_fence_function stable_fence() {
    return accounting_relax_fence_function();
  }

  relax_fence_function lock_fence(threadcounter) {
    return relax_fence_function();
  }

  // memory allocation
  void *allocate(size_t sz, memtag m) {
    auto *obj = noname::table::Object::Make(sz + sizeof(noname::table::Object));
    // obj->data is NOT equal to (obj + sizeof(table::Object))
    return reinterpret_cast<void *>(reinterpret_cast<char *>(obj) + sizeof(noname::table::Object));
  }
  void deallocate(void *p, size_t sz, memtag m) {
    noname::table::Object::Free(reinterpret_cast<noname::table::Object *>(
      reinterpret_cast<char *>(p) - sizeof(noname::table::Object)));
  }
  void *pool_allocate(size_t sz, memtag m) {
    return allocate(sz, m);
  }
  void pool_deallocate(void *p, size_t sz, memtag m) {
    deallocate(p, sz, m); // FIXME(tzwang): add rcu callback support
  }
  void deallocate_rcu(void *p, size_t sz, memtag m) {
    deallocate(p, sz, m); // FIXME(tzwang): add rcu callback support
  }
  void pool_deallocate_rcu(void *p, size_t sz, memtag m) {
    deallocate(p, sz, m); // FIXME(tzwang): add rcu callback support
  }
  void rcu_register(mrcu_callback *cb) { (void)cb; }
  void mark(threadcounter ci) { (void)ci; }

private:
  mutable kvtimestamp_t ts_;
  EpochNum epoch_;
};

struct MasstreeParams : public nodeparams<> {
  typedef noname::table::OID value_type;
  typedef value_print<value_type> value_print_type;
  typedef SimpleThreadInfo threadinfo_type;
};

static thread_local char results[MB];

template <typename P>
struct MasstreeWrapper : public noname::index::GenericIndex {
  typedef P parameters_type;
  typedef node_base<P> node_type;
  typedef leaf<P> leaf_type;
  typedef typename P::threadinfo_type threadinfo;
  typedef unlocked_tcursor<P> unlocked_cursor_type;
  typedef tcursor<P> cursor_type;
  typedef typename P::value_type value_type;

  struct Scanner {
    Scanner(const char *start_key, const char *end_key, bool end_key_inclusive,
            size_t key_sz, int scan_sz, bool reverse = false)
      : start_key(start_key), end_key(end_key), end_key_inclusive(end_key_inclusive)
      , key_sz(key_sz), scan_sz(scan_sz), reverse(reverse), count(0) {}

    template <typename SS, typename K>
    inline void visit_leaf(const SS&, const K&, threadinfo&) {
    }

    inline bool visit_value(Str key, noname::table::OID oid, threadinfo&) {
      if (scan_sz >= 0 && count >= scan_sz) {
        return false;
      }
      auto cmp = memcmp(key.s, end_key, key_sz);
      if ((!reverse && ((end_key_inclusive && cmp <= 0) || (!end_key_inclusive && cmp < 0)))
          || (reverse && ((end_key_inclusive && cmp >= 0) || (!end_key_inclusive && cmp > 0)))) {
        *reinterpret_cast<noname::table::OID *>(results + count * sizeof(noname::table::OID)) = oid;
        count++;
        return true;
      }
      return false;
    }

    const char *start_key;
    const char *end_key;
    bool end_key_inclusive;
    size_t key_sz;
    int scan_sz;
    bool reverse;
    uint32_t count;
  };

  MasstreeWrapper(size_t key_size, size_t value_size, noname::table::IndirectionArrayTable *table)
    : noname::index::GenericIndex(key_size, value_size, table) {
    threadinfo ti(0);
    table_.initialize(ti);
  }

  ~MasstreeWrapper() {
    threadinfo ti(0);
    table_.destroy(ti);
  }

  // interface implementations
  inline bool Insert(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
  inline bool Search(const char *key, size_t key_sz, char *&value_out) override;
  inline int Scan(const char *start_key, bool start_key_inclusive,
                  const char *end_key, bool end_key_inclusive,
                  size_t key_sz, int scan_sz, char *&value_out) override;
  inline int ReverseScan(const char *start_key, bool start_key_inclusive,
                         const char *end_key, bool end_key_inclusive,
                         size_t key_sz, int scan_sz, char *&value_out) override;
  inline bool Update(const char *key, size_t key_sz, const char *value, size_t value_sz) override;
  inline bool Delete(const char *key, size_t key_sz) override;

  // TODO(hutx): support update and delete
  inline bool Insert(const char *key, size_t key_sz, const char *value, size_t value_sz, EpochNum e);
  inline bool Search(const char *key, size_t key_sz, char *&value_out, EpochNum e);
  inline int Scan(const char *start_key, bool start_key_inclusive,
                  const char *end_key, bool end_key_inclusive,
                  size_t key_sz, int scan_sz, char *&value_out, EpochNum e);
  inline int ReverseScan(const char *start_key, bool start_key_inclusive,
                         const char *end_key, bool end_key_inclusive,
                         size_t key_sz, int scan_sz, char *&value_out, EpochNum e);

  basic_table<P> table_;
};

//update and delete aren't supported yet
template <typename P>
inline bool MasstreeWrapper<P>::Update(const char *key, size_t key_sz, 
                                        const char *value, size_t value_sz) {
  return false;
}
template <typename P>
inline bool MasstreeWrapper<P>::Delete(const char *key, size_t key_sz) {
  return false;
}

template <typename P>
inline bool MasstreeWrapper<P>::Insert(const char *key, size_t key_sz, const char *value, size_t value_sz) {
  return MasstreeWrapper<P>::Insert(key, key_sz, value, value_sz, 0);
}

template <typename P>
inline bool MasstreeWrapper<P>::Search(const char *key, size_t key_sz, char *&value_out) {
  return MasstreeWrapper<P>::Search(key, key_sz, value_out, 0);
}

template <typename P>
inline int MasstreeWrapper<P>::Scan(const char *start_key, bool start_key_inclusive,
                  const char *end_key, bool end_key_inclusive,
                  size_t key_sz, int scan_sz, char *&value_out) {
  return MasstreeWrapper<P>::Scan(start_key, start_key_inclusive,
                                    end_key, end_key_inclusive,
                                    key_sz, scan_sz, value_out, 0);
}

template <typename P>
inline int MasstreeWrapper<P>::ReverseScan(const char *start_key, bool start_key_inclusive,
                         const char *end_key, bool end_key_inclusive,
                         size_t key_sz, int scan_sz, char *&value_out) {
  return MasstreeWrapper<P>::ReverseScan(start_key, start_key_inclusive,
                                          end_key, end_key_inclusive,
                                          key_sz, scan_sz, value_out, 0);
}

template <typename P>
inline bool MasstreeWrapper<P>::Insert(const char *key, size_t key_sz, const char *value,
                                       size_t value_sz, EpochNum e) {
  threadinfo ti(e);
  tcursor<P> lp(table_, Str(key, key_sz));
  bool found = lp.find_insert(ti);
  if (!found) {
  insert_new:
    found = false;
    lp.value() = *(noname::table::OID *)(value);
  } else {
    // we have two cases: 1) predecessor's inserts are still remaining in tree,
    // even though version chain is empty or 2) somebody else are making dirty
    // data in this chain. If it's the first case, version chain is considered
    // empty, then we retry insert.
    noname::table::OID oid = lp.value();
    if (!table->Get(oid)) {
      goto insert_new;
    }
  }
  lp.finish(!found, ti);
  return !found;
}

template <typename P>
inline bool MasstreeWrapper<P>::Search(const char *key, size_t key_sz, char *&value_out,
                                       EpochNum e) {
  threadinfo ti(e);
  unlocked_tcursor<P> lp(table_, Str(key, key_sz));
  bool found = lp.find_unlocked(ti);
  if (found) {
    *(noname::table::OID *)(results) = lp.value();
  }
  value_out = results;
  return found;
}

template <typename P>
inline int MasstreeWrapper<P>::Scan(const char *start_key, bool start_key_inclusive,
                                    const char *end_key, bool end_key_inclusive,
                                    size_t key_sz, int scan_sz, char *&value_out, EpochNum e) {
  threadinfo ti(e);
  value_out = results;
  Scanner scanner(start_key, end_key, end_key_inclusive, key_sz, scan_sz);
  table_.scan(Str(start_key, key_sz), start_key_inclusive, scanner, ti);
  return scanner.count;
}

template <typename P>
inline int MasstreeWrapper<P>::ReverseScan(const char *start_key, bool start_key_inclusive,
                                           const char *end_key, bool end_key_inclusive,
                                           size_t key_sz, int scan_sz, char *&value_out,
                                           EpochNum e) {
  threadinfo ti(e);
  value_out = results;
  Scanner scanner(start_key, end_key, end_key_inclusive, key_sz, scan_sz, true);
  table_.rscan(Str(start_key, key_sz), start_key_inclusive, scanner, ti);
  return scanner.count;
}

typedef MasstreeWrapper<MasstreeParams> ConcurrentMasstree;

}  // namespace Masstree

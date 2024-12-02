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

#pragma once

#include <cstdlib>

#include "kvrow.hh"
#include "masstree.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"

#include "noname_defs.h"

namespace noname {
namespace benchmark {
namespace ycsb {

using namespace Masstree;

class SimpleThreadInfo {
 public:
  explicit SimpleThreadInfo(uint64_t e) : ts_(0), epoch_(e) {}
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
    return malloc(sz);
  }
  void deallocate(void *p, size_t sz, memtag m) {
    free(p);
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
  uint64_t epoch_;
};

template <typename Key, typename Value>
struct MasstreeWrapper {
  struct Params : public nodeparams<> {
    typedef Value value_type;
    typedef value_print<value_type> value_print_type;
    typedef SimpleThreadInfo threadinfo_type;
  };

  typedef typename Params::threadinfo_type threadinfo;

  basic_table<Params> table_;

  MasstreeWrapper() {
    threadinfo ti(0);
    table_.initialize(ti);
  }

  ~MasstreeWrapper() {
    threadinfo ti(0);
    table_.destroy(ti);
  }

  uint64_t scan(const Key &k, int64_t range, Value *output,
                const Key *end_key = nullptr) {
    return 0;
  }

  bool insert(const Key &k, Value v) {
    threadinfo ti(0);
    tcursor<Params> lp(table_, Str(reinterpret_cast<const char *>(&k), sizeof(Key)));
    bool found = lp.find_insert(ti);
    if (!found) {
      lp.value() = v;
    }
    lp.finish(!found, ti);
    return !found;
  }

  bool update(const Key &k, Value v) {
    threadinfo ti(0);
    tcursor<Params> lp(table_, Str(reinterpret_cast<const char *>(&k), sizeof(Key)));
    bool found = lp.find_insert(ti);
    if (!found) {
      return false;
    }
    lp.value() = v;
    lp.finish(1, ti);
    return true;
  }

  bool lookup(const Key &k, Value &result) {
    threadinfo ti(0);
    unlocked_tcursor<Params> lp(table_, Str(reinterpret_cast<const char *>(&k), sizeof(Key)));
    bool found = lp.find_unlocked(ti);
    if (found) {
      result = lp.value();
    }
    return found;
  }
};

} // namespace ycsb
} // namespace benchmark
} // namespace noname

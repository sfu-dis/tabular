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

#include "ycsb_loader.h"
#ifdef OMCS_OFFSET
#include "index/latches/OMCSOffset.h"
#endif

namespace noname {
namespace benchmark {
namespace ycsb {

template <class IndexType, class TableType>
void YCSBLoad<IndexType, TableType>::Load(uint32_t thread_id,
                                          uint64_t key_start,
                                          uint64_t key_end) {
#ifdef OMCS_OFFSET
  offset::reset_tls_qnodes();
#endif
  uint64_t nb_aborts = 0;
  for (uint64_t key = key_start; key < key_end; key++) {
    bool ok;
#if defined(HASHTABLE_INLINE_TABULAR)
    std::optional<bool> ret;
    auto hash = index::Hash(key);
    do {
      ret = index_->insert_internal_callback(hash, key, key);
      nb_aborts++;
    } while (!ret);
    nb_aborts--;
    ok = ret.value();
#elif defined(HASHTABLE_MVCC_TABULAR)
    std::optional<bool> ret;
    auto hash = index::Hash(key);
    do {
      ret = index_->insert_internal(hash, key, key);
      nb_aborts++;
    } while (!ret);
    nb_aborts--;
    ok = ret.value();
#elif defined(BTREE_INLINE_TABULAR)
    auto result = IndexType::Result::TRANSACTION_FAILED;
    while (result == IndexType::Result::TRANSACTION_FAILED) {
      result = index_->insert_internal_callback(key, key);
      nb_aborts += 1;
    }
    nb_aborts -= 1;
    ok = result == IndexType::Result::SUCCEED;
#elif defined(BTREE_TABULAR)
    auto result = IndexType::Result::TRANSACTION_FAILED;
    while (result == IndexType::Result::TRANSACTION_FAILED) {
      result = index_->insert_internal(key, key);
      nb_aborts += 1;
    }
    nb_aborts -= 1;
    ok = result == IndexType::Result::SUCCEED;
#else
    ok = index_->insert(key, key);
#endif
    CHECK(ok);
  }
  auto num_attempts = (nb_aborts + key_end - key_start);
  std::cout << "Thread " << thread_id
            << " insert finished. Abort rate: " << nb_aborts << " / "
            << num_attempts << " = " << nb_aborts * 1.0 / num_attempts
            << std::endl;
  {
    std::lock_guard lk(finish_mx_);
    num_of_finished_threads_ += 1;
  }
  finish_cv_.notify_one();
}

template <class IndexType, class TableType>
void YCSBLoad<IndexType, TableType>::Run() {
  auto step = num_of_records_ / num_of_loaders_;
  std::vector<thread::Thread*> threads;
  for (int i = 0; i < num_of_loaders_; ++i) {
    auto start = i * step;
    auto end = start + step;
    if (i == num_of_loaders_ - 1) {
      end = num_of_records_;
    }
    thread::Thread* t = thread_pool_->GetThread(false);
    t->StartTask([this, i, start, end] { Load(i, start, end); });
    threads.push_back(t);
  }

  {
    std::unique_lock lk(finish_mx_);
    finish_cv_.wait(lk, [&] { return num_of_finished_threads_ == num_of_loaders_; });
  }

  for (const auto& thread : threads) {
    thread->Join();
  }
  for (const auto& thread : threads) {
    thread_pool_->PutThread(thread);
  }

  std::cout << "Loading Done" << std::endl;
}

template <class IndexType, class TableType>
void YCSBLoad<IndexType, TableType>::Verify() {
  for (size_t i = 0; i < num_of_records_; i++) {
    uint64_t value;
    auto ok = index_->lookup(i, value);
    LOG_IF(FATAL, !ok) << "Missing key: " << i;
  }

  std::cout << "Verification Done" << std::endl;
}

#ifdef BTREE_OPTIQL
template class YCSBLoad<btreeolc::BTreeOMCSLeaf<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef BTREE_OLC
template class YCSBLoad<btreeolc::BTreeOLC<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef BTREE_LC_STDRW
template class YCSBLoad<btreeolc::BTreeLC<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef BTREE_LC_TBBRW
template class YCSBLoad<btreeolc::BTreeLC<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef BTREE_TABULAR
template class YCSBLoad<tabular::BTree<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef BTREE_INLINE_TABULAR
template class YCSBLoad<tabular::InlineBTree<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef HASHTABLE_OPTIMISTIC_LC
template class YCSBLoad<noname::index::HashTable<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef HASHTABLE_LC_STDRW
template class YCSBLoad<noname::index::HashTableLC<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef HASHTABLE_LC_TBBRW
template class YCSBLoad<noname::index::HashTableLC<uint64_t, uint64_t>, table::IndirectionArrayTable>;
#endif
#ifdef HASHTABLE_INLINE_TABULAR
template class YCSBLoad<noname::tabular::HashTable<uint64_t, uint64_t>,
                        table::IndirectionArrayTable>;
#endif
#ifdef HASHTABLE_MVCC_TABULAR
template class YCSBLoad<noname::tabular::MVCCHashTable<uint64_t, uint64_t>,
                        table::IndirectionArrayTable>;
#endif

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

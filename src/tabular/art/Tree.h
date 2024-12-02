#pragma once

#include <limits>

#include "N.h"
#include "index/indexes/common/random.h"

#include "table/inline_table.h"
#include "tabular/table_group.h"
#include "transaction/occ.h"

namespace noname {
namespace tabular {
namespace art {

class Tree {
 public:
  using LoadKeyFunction = void (*)(TID tid, Key &key);
  using RemoveNodeFunction = void (*)(void *);

 private:
  tabular::TableGroup group;
  table::InlineTable *nodes;

  static const size_t nodes_table_id = 0;
  // N *const root;
  static const table::OID root_id = 0;

  TID checkKey(const TID tid, const Key &k) const;

  LoadKeyFunction loadKey;
  RemoveNodeFunction removeNode;
  uniform_random_generator rng;

 public:
  enum class CheckPrefixResult : uint8_t { Match, NoMatch, OptimisticMatch };

  enum class CheckPrefixPessimisticResult : uint8_t {
    Match,
    NoMatch,
  };

  enum class PCCompareResults : uint8_t {
    Smaller,
    Equal,
    Bigger,
  };
  enum class PCEqualsResults : uint8_t { BothMatch, Contained, NoMatch };
  static CheckPrefixResult checkPrefix(N *n, const Key &k, uint32_t &level);

  static CheckPrefixPessimisticResult checkPrefixPessimistic(
      Transaction *t, table::InlineTable *nodes, N *n, uint64_t v, const Key &k, uint32_t &level, uint8_t &nonMatchingKey,
      Prefix &nonMatchingPrefix, LoadKeyFunction loadKey, bool &needRestart);

  static PCCompareResults checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey,
                                             uint32_t &level, LoadKeyFunction loadKey,
                                             bool &needRestart);

  static PCEqualsResults checkPrefixEquals(const N *n, uint32_t &level, const Key &start,
                                           const Key &end, LoadKeyFunction loadKey,
                                           bool &needRestart);

#if defined(IS_CONTEXTFUL)
  bool traverseToLeafEx(const Key &k, OMCSLock::Context &q, N *&parentNode, uint32_t &level,
                        bool &upgraded);
  bool traverseToLeafUpgradeEx(const Key &k, OMCSLock::Context &q, N *&parentNode, uint32_t &level);
  bool traverseToLeafAcquireEx(const Key &k, OMCSLock::Context qnodes[], N *&parentNode,
                               uint32_t &level, OMCSLock::Context *&q);
#else
  bool traverseToLeafEx(const Key &k, N *&parentNode, uint32_t &level, bool &upgraded);
  bool traverseToLeafUpgradeEx(Transaction *t, const Key &k, table::OID &parentNodeID, uint32_t &level);
  bool traverseToLeafUpgradeEx_materialized(Transaction *t, const Key &k, table::OID &parentNodeID, N *&parentNode, uint32_t &level);
  bool traverseToLeafAcquireEx(const Key &k, N *&parentNode, uint32_t &level);
#endif

 public:
  Tree(LoadKeyFunction loadKey, RemoveNodeFunction removeNode, table::config_t config, bool is_persistent,
                       const std::filesystem::path &logging_directory = "",
                       size_t num_of_workers = 0);

  Tree(const Tree &) = delete;

  ~Tree();

  TID lookup(const Key &k);
  std::optional<TID> lookup_internal(const Key &k);
  std::optional<TID> lookup_internal_materialized(const Key &k);

  bool insert(const Key &k, TID tid);
  std::optional<bool> insert_internal(const Key &k, TID tid);

  bool update(const Key &k, TID tid);
  std::optional<bool> update_internal(const Key &k, TID tid);
  std::optional<bool> update_internal_materialized(const Key &k, TID tid);

#if 0
  bool update(const Key &k, const char *value, size_t value_sz);

  bool update(const Key &k);
#endif

#ifdef ART_UPSERT
  bool upsert(const Key &k, TID tid);
#endif

  bool remove(const Key &k, TID tid);
};
} // namespace art
} // namespace tabular
} // namespace noname

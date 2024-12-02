#pragma once

#include "parallel.h"
#include "reducer.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wshadow"
#include "flat_hash_map.hpp"
#pragma clang diagnostic pop
#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ParallelTools {
template <class Key, class T, class Hash = std::hash<Key>,
          class KeyEqual = std::equal_to<Key>>
class concurrent_hash_map {
private:
#ifdef __cpp_lib_hardware_interference_size
  static constexpr std::size_t hardware_constructive_interference_size =
      std::hardware_constructive_interference_size;
  static constexpr std::size_t hardware_destructive_interference_size =
      std::hardware_destructive_interference_size;
#else
  // 64 bytes on x86-64 │ L1_CACHE_BYTES │ L1_CACHE_SHIFT │ __cacheline_aligned
  // │
  // ...
  static constexpr std::size_t hardware_constructive_interference_size = 64;
  static constexpr std::size_t hardware_destructive_interference_size = 64;
#endif
  using Map = std::pair<ska::flat_hash_map<Key, T, Hash, KeyEqual>, std::mutex>;
  struct aligned_map {
    alignas(hardware_destructive_interference_size) Map m;
  };
  std::vector<aligned_map> maps;

  static size_t bucket_hash(Key k) { return Hash{}(k) >> 10UL; }

  template <class U> size_t log2_up(U i) {
    size_t a = 0;
    U b = i - 1;
    while (b > 0) {
      b = b >> 1U;
      a++;
    }
    return a;
  }

public:
  concurrent_hash_map(int blow_up_factor = (PARALLEL == 1) ? 10 : 1)
      : maps(1UL << log2_up(ParallelTools::getWorkers() * blow_up_factor)) {}

  std::pair<bool, T *> insert(Key k, T value) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    auto pair = maps[bucket].m.first.insert({k, value});
    maps[bucket].m.second.unlock();
    return {pair.second, &(pair.first->second)};
  }

  std::pair<bool, T *> insert_or_assign(Key k, T value) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    auto pair = maps[bucket].m.first.insert_or_assign(k, value);
    maps[bucket].m.second.unlock();
    return {pair.second, &(pair.first->second)};
  }

  void remove(Key k) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    maps[bucket].m.first.erase(k);
    maps[bucket].m.second.unlock();
  }

  T value(Key k, T null_value) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    auto it = maps[bucket].m.first.find(k);
    T value;
    if (it == maps[bucket].m.first.end()) {
      value = null_value;
    } else {
      value = it->second;
    }
    maps[bucket].m.second.unlock();
    return value;
  }
  T unlocked_value(Key k, T null_value) const {
    size_t bucket = bucket_hash(k) % maps.size();
    auto it = maps[bucket].m.first.find(k);
    T value;
    if (it == maps[bucket].m.first.end()) {
      value = null_value;
    } else {
      value = it->second;
    }
    return value;
  }
  bool contains(Key k) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    bool has = maps[bucket].m.first.contains(k);
    maps[bucket].m.second.unlock();
    return has;
  }

  template <typename F> void for_each(F f) {
    ParallelTools::parallel_for(0, maps.size(), [&](size_t i) {
      for (auto &[key, value] : maps[i].m.first) {
        f(key, value);
      }
    });
  }
  bool unlocked_empty() const {
    for (const auto &map : maps) {
      if (!map.m.first.empty()) {
        return false;
      }
    }
    return true;
  }
  std::vector<std::pair<Key, T>> unlocked_entries() const {
    std::vector<uint64_t> sizes;
    sizes.push_back(maps[0].m.first.size());
    for (size_t i = 1; i < maps.size(); i++) {
      sizes.push_back(sizes[i - 1] + maps[i].m.first.size());
    }
    std::vector<std::pair<Key, T>> entries(sizes.back());
    ParallelTools::parallel_for(0, maps.size(), [&](size_t i) {
      size_t j = 0;
      if (i > 0) {
        j = sizes[i - 1];
      }
      for (auto &[key, value] : maps[i].m.first) {
        entries[j++] = {key, value};
      }
    });
    return entries;
  }

  void clear() { maps.clear(); }
};
template <class Key, class T, class Hash = std::hash<Key>,
          class KeyEqual = std::equal_to<Key>>
class concurrent_hash_multimap {
private:
#ifdef __cpp_lib_hardware_interference_size
  static constexpr std::size_t hardware_constructive_interference_size =
      std::hardware_constructive_interference_size;
  static constexpr std::size_t hardware_destructive_interference_size =
      std::hardware_destructive_interference_size;
#else
  // 64 bytes on x86-64 │ L1_CACHE_BYTES │ L1_CACHE_SHIFT │ __cacheline_aligned
  // │
  // ...
  static constexpr std::size_t hardware_constructive_interference_size = 64;
  static constexpr std::size_t hardware_destructive_interference_size = 64;
#endif
  using Map =
      std::pair<std::unordered_multimap<Key, T, Hash, KeyEqual>, std::mutex>;
  using iterator =
      typename std::unordered_multimap<Key, T, Hash, KeyEqual>::iterator;
  struct aligned_map {
    alignas(hardware_destructive_interference_size) Map m;
  };
  std::vector<aligned_map> maps;

  static size_t bucket_hash(Key k) { return Hash{}(k) >> 32UL; }

  template <class U> size_t log2_up(U i) {
    size_t a = 0;
    U b = i - 1;
    while (b > 0) {
      b = b >> 1U;
      a++;
    }
    return a;
  }

public:
  concurrent_hash_multimap(int blow_up_factor = 10)
      : maps(1UL << log2_up(ParallelTools::getWorkers() * blow_up_factor)) {}

  void insert(Key k, T value) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    maps[bucket].m.first.insert({k, value});
    maps[bucket].m.second.unlock();
  }

  std::pair<iterator, iterator> equal_range(Key k) {
    size_t bucket = bucket_hash(k) % maps.size();
    maps[bucket].m.second.lock();
    auto p = maps[bucket].m.first.equal_range(k);
    maps[bucket].m.second.unlock();
    return p;
  }
};
} // namespace ParallelTools

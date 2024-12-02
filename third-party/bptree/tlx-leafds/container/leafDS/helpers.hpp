#pragma once
#include <concepts>
#include <cstdint>
#include <random>
#include <sys/time.h>
#include <tuple>
#include <vector>

static constexpr uint32_t bsr_word_constexpr(uint32_t word) {
  if (word == 0) {
    return 0;
  }
  if (word & (1U << 31U)) {
    return 31;
  } else {
    return bsr_word_constexpr(word << 1U) - 1;
  }
}

template <typename T1, typename... Ts>
std::tuple<Ts...> leftshift_tuple(const std::tuple<T1, Ts...> &tuple) {
  return std::apply([](auto &&, auto &...args) { return std::tie(args...); },
                    tuple);
}

template <std::integral T>
std::vector<T> create_random_data(size_t n,
                                  T max_val = std::numeric_limits<T>::max(),
                                  uint32_t seed = 0) {
  std::mt19937 rng(seed);

  std::uniform_int_distribution<T> dist(0, max_val);
  std::vector<T> v(n);

  generate(begin(v), end(v), bind(dist, rng));
  return v;
}

template <std::floating_point T>
std::vector<T> create_random_data(size_t n,
                                  T max_val = std::numeric_limits<T>::max(),
                                  uint32_t seed = 0) {
  std::mt19937 rng(seed);

  std::uniform_real_distribution<T> dist(0, max_val);
  std::vector<T> v(n);

  generate(begin(v), end(v), bind(dist, rng));
  return v;
}

/*
uint64_t get_usecs() {
  struct timeval st {};
  gettimeofday(&st, nullptr);
  return st.tv_sec * 1000000 + st.tv_usec;
}
*/

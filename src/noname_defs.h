#pragma once

#include <cassert>
#include <type_traits>

#define MB (1024 * 1024)

#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

#define DEFAULT_ALIGNMENT_BITS 4
#define DEFAULT_ALIGNMENT (1 << DEFAULT_ALIGNMENT_BITS)

// TODO(hutx): this should be in epoch manager, write it here for now
typedef uint64_t EpochNum;

/* Align integers up/down to the nearest alignment boundary, and check
 * for existing alignment
 */
template <typename T, typename U = int>
static constexpr typename std::common_type<T, U>::type align_down(
    T val, U amount = DEFAULT_ALIGNMENT) {
  return val & -amount;
}

template <typename T, typename U = int>
static constexpr typename std::common_type<T, U>::type align_up(
    T val, U amount = DEFAULT_ALIGNMENT) {
  return align_down(val + amount - 1, amount);
}

template <typename T, typename U = int>
static constexpr bool is_aligned(T val, U amount = DEFAULT_ALIGNMENT) {
  return not(val & (amount - 1));
}

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

// cache alignment defs 
#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHE_PADOUT  \
    char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))

#ifndef ALWAYS_INLINE
#define ALWAYS_INLINE __attribute__((always_inline)) inline
#endif


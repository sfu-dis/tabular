#pragma once

#include "parallel.h"
#include <algorithm>
#include <functional>

#if PARLAY == 1
#include "parlay/primitives.h"
#endif

namespace ParallelTools {

template <class InputIt, class OutputIt, class Compare = std::less<>>
void merge(InputIt first1, InputIt last1, InputIt first2, InputIt last2,
           OutputIt d_first, Compare comp = std::less<>()) {
  /*
algorithm merge(A[i...j], B[k...ℓ], C[p...q]) is
inputs A, B, C : array
i, j, k, ℓ, p, q : indices

let m = j - i,
n = ℓ - k

if m < n then
swap A and B  // ensure that A is the larger array: i, j still belong to A; k, ℓ
to B swap m and n

if m ≤ 0 then
return  // base case, nothing to merge

let r = ⌊(i + j)/2⌋
let s = binary-search(A[r], B[k...ℓ])
let t = p + (r - i) + (s - k)
C[t] = A[r]

in parallel do
merge(A[i...r], B[k...s], C[p...t])
merge(A[r+1...j], B[s...ℓ], C[t+1...q])
  */
  size_t first_length = last1 - first1;
  size_t second_length = last2 - first2;

  InputIt longer_start = (first_length >= second_length) ? first1 : first2;
  InputIt longer_last = (first_length >= second_length) ? last1 : last2;

  InputIt shorter_start = (first_length < second_length) ? first1 : first2;
  InputIt shorter_last = (first_length < second_length) ? last1 : last2;
  if (shorter_last - shorter_start < 10000) {
    std::merge(first1, last1, first2, last2, d_first, comp);
    return;
  }
  InputIt r = longer_start + ((longer_last - longer_start) / 2);
  InputIt s = std::lower_bound(shorter_start, shorter_last, *r, comp);
  OutputIt out_mid = d_first + (r - longer_start) + (s - shorter_start);
  *out_mid = *r;
  ParallelTools::par_do(
      [&]() {
        ParallelTools::merge(longer_start, r, shorter_start, s, d_first, comp);
      },
      [&]() {
        ParallelTools::merge(r + 1, longer_last, s, shorter_last, out_mid + 1,
                             comp);
      });
}

// TODO(wheatman) make a better parallel sort
template <class RandomIt, class Compare = std::less<>>
void sort(RandomIt first, RandomIt last, Compare comp = std::less<>()) {
#if PARALLEL == 0
  std::sort(first, last, comp);
  return;
#endif
#if PARLAY == 1
  if constexpr (parlay::is_random_access_iterator_v<RandomIt>) {
    parlay::sort_inplace(parlay::make_slice(first, last), comp);
    return;
  }
#endif
  using E = typename std::iterator_traits<RandomIt>::value_type;
  if (last - first < 10000) {
    std::sort(first, last, comp);
  } else {
    RandomIt mid = first + ((last - first) / 2);
    E *tmp = nullptr;
    ParallelTools::par_do([&]() { ParallelTools::sort(first, mid, comp); },
                          [&]() {
                            ParallelTools::sort(mid, last, comp);
                            tmp = (E *)malloc((last - first) * sizeof(E));
                          });

    merge(first, mid, mid, last, tmp, comp);
    ParallelTools::parallel_for(0, last - first,
                                [&](size_t i) { first[i] = tmp[i]; });
    free(tmp);
  }
}

} // namespace ParallelTools

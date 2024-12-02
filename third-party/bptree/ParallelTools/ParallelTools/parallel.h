#pragma once
#include <cstddef>
#include <cstdlib>

#if CILK == 1
#define PARALLEL 1
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#endif

#if PARLAY == 1
#define PARALLEL 1

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow"
#include "parlay/parallel.h"
#pragma clang diagnostic pop
#endif

#if !defined(PARALLEL)
#define PARALLEL 0
#endif

namespace ParallelTools {

// these can be useful for debugging since they can be easily switched with the
// parallel ones even when compiling with CILK==1
template <typename F>
inline void serial_for(size_t start, size_t end, F f,
                       [[maybe_unused]] const size_t chunksize = 0) {
  for (size_t i = start; i < end; i++)
    f(i);
}

template <typename F>
inline void serial_for(size_t start, size_t end, size_t step, F f) {
  for (size_t i = start; i < end; i += step)
    f(i);
}

// intel cilk+
#if CILK == 1

template <typename F> inline void parallel_for(size_t start, size_t end, F f) {
  cilk_for(size_t i = start; i < end; i++) f(i);
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F f,
                         const size_t chunksize) {
  if ((end - start) <= chunksize) {
    for (size_t i = start; i < end; i++) {
      f(i);
    }
  } else {
    cilk_for(size_t i = start; i < end; i += chunksize) {
      size_t local_end = i + chunksize;
      if (local_end > end) {
        local_end = end;
      }
      for (size_t j = i; j < local_end; j++) {
        f(j);
      }
    }
  }
}
template <typename F>
inline void parallel_for(size_t start, size_t end, size_t step, F f) {
  cilk_for(size_t i = start; i < end; i += step) f(i);
}

template <typename F, typename RAC>
inline void parallel_for_each(RAC &container, F f, const size_t chunksize = 0) {
  if (chunksize == 0) {
    cilk_for(size_t i = 0; i < container.size(); i++) f(container[i]);
  } else if (container.size() <= chunksize) {
    for (size_t i = 0; i < container.size(); i++) {
      f(container[i]);
    }
  } else {
    cilk_for(size_t i = 0; i < container.size(); i += chunksize) {
      size_t local_end = i + chunksize;
      if (local_end > container.size()) {
        local_end = container.size();
      }
      for (size_t j = i; j < local_end; j++) {
        f(container[j]);
      }
    }
  }
}

// running the function returns a vector
// each element in the vector is run with the same function as the original
// vector
template <typename F, typename RAC>
inline void parallel_for_each_spawn(RAC &container, F f,
                                    const size_t chunksize = 0) {
  if (chunksize == 0) {
    cilk_for(size_t i = 0; i < container.size(); i++) {
      auto vec = f(container[i]);
      cilk_spawn parallel_for_each_spawn(vec, f, chunksize);
    }
  } else if (container.size() <= chunksize) {
    for (size_t i = 0; i < container.size(); i++) {
      auto vec = f(container[i]);
      parallel_for_each_spawn(vec, f, chunksize);
    }
  } else {
    cilk_for(size_t i = 0; i < container.size(); i += chunksize) {
      size_t local_end = i + chunksize;
      if (local_end > container.size()) {
        local_end = container.size();
      }
      for (size_t j = i; j < local_end; j++) {
        auto vec = f(container[j]);
        cilk_spawn parallel_for_each_spawn(vec, f, chunksize);
      }
    }
  }
  cilk_sync;
}

[[maybe_unused]] static int getWorkers() { return __cilkrts_get_nworkers(); }

[[maybe_unused]] static int getWorkerNum() {
  return __cilkrts_get_worker_number();
}

template <typename Lf, typename Rf> inline void par_do(Lf left, Rf right) {
  cilk_spawn right();
  left();
  cilk_sync;
}

#elif PARLAY == 1

template <typename F> inline void parallel_for(size_t start, size_t end, F f) {
  parlay::parallel_for(start, end, f);
}

template <typename F>
inline void parallel_for(size_t start, size_t end, size_t step, F f) {
  size_t last = (end - start) / step;
  if ((end - start) % step != 0) {
    last += 1;
  }
  parlay::parallel_for(0, last, [&](size_t i) { f(start + i * step); });
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F f,
                         const size_t chunksize) {
  if ((end - start) <= chunksize) {
    for (size_t i = start; i < end; i++) {
      f(i);
    }
  } else {
    parallel_for(start, end, chunksize, [&](auto i) {
      size_t local_end = i + chunksize;
      if (local_end > end) {
        local_end = end;
      }
      for (size_t j = i; j < local_end; j++) {
        f(j);
      }
    });
  }
}

template <typename F, typename RAC>
inline void parallel_for_each(RAC &container, F f, const size_t chunksize = 0) {
  parlay::parallel_for(
      0, container.size(), [&](size_t i) { f(container[i]); }, chunksize);
}

[[maybe_unused]] static int getWorkers() { return parlay::num_workers(); }

[[maybe_unused]] static int getWorkerNum() { return parlay::worker_id(); }

template <typename Lf, typename Rf> inline void par_do(Lf left, Rf right) {
  parlay::par_do(left, right);
}

// c++
#else

template <typename F>
inline void parallel_for(size_t start, size_t end, F f,
                         [[maybe_unused]] const size_t chunksize = 0) {
  for (size_t i = start; i < end; i++) {
    f(i);
  }
}

template <typename F>
inline void parallel_for(size_t start, size_t end, size_t step, F f) {
  for (size_t i = start; i < end; i += step) {
    f(i);
  }
}

template <typename F, typename RAC>
inline void parallel_for_each(RAC &container, F f,
                              [[maybe_unused]] const size_t chunksize = 0) {
  for (size_t i = 0; i < container.size(); i++) {
    f(container[i]);
  }
}

template <typename F, typename RAC>
inline void parallel_for_each_spawn(RAC &container, F f,
                                    const size_t chunksize = 0) {
  for (size_t i = 0; i < container.size(); i++) {
    auto vec = f(container[i]);
    parallel_for_each_spawn(vec, f, chunksize);
  }
}

[[maybe_unused]] static int getWorkers() { return 1; }
[[maybe_unused]] static int getWorkerNum() { return 0; }

template <typename Lf, typename Rf> inline void par_do(Lf left, Rf right) {
  right();
  left();
}

#endif

template <bool parallel, typename F>
inline void For(size_t start, size_t end, F f) {
  if constexpr (parallel) {
    return parallel_for(start, end, f);
  } else {
    return serial_for(start, end, f);
  }
}

template <bool parallel, typename F>
inline void For(size_t start, size_t end, size_t step, F f) {
  if constexpr (parallel) {
    return parallel_for(start, end, step, f);
  } else {
    return serial_for(start, end, step, f);
  }
}

} // namespace ParallelTools

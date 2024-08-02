#pragma once

#include <oneapi/tbb.h>

namespace noname {
namespace index {
class TBBRWLock {
 public:
  TBBRWLock() {}

  inline void lock(uint64_t * = nullptr) { lock_.lock(); }

  inline void unlock(uint64_t * = nullptr) { lock_.unlock(); }

  inline void read_lock(uint64_t * = nullptr) { lock_.lock_shared(); }

  inline void read_unlock(uint64_t * = nullptr) { lock_.unlock_shared(); }

 private:
  oneapi::tbb::spin_rw_mutex lock_;
};
} // namespace index
} // namespace noname

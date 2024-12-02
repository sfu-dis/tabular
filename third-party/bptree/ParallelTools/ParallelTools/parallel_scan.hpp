#pragma once

#include "parallel.h"
#include <vector>

namespace ParallelTools {

template <typename Range, typename Value, typename Scan, typename Combine>
Value parallel_scan(const Range &range, const Value &identity, const Scan &scan,
                    const Combine &combine, size_t min_serial_size) {
  // TODO
}
} // namespace ParallelTools

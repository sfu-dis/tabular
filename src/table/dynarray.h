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

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

// A dynamic array (re)implementation based on the one in ERMIA/Shore-MT.

#pragma once

#include <linux/mman.h>  // for MAP_HUGE_1GB
#include <sys/mman.h>

#include <glog/logging.h>

#include <cstdint>
#include <utility>
#include <mutex>

#include "noname_defs.h"

namespace noname {
namespace table {

/* A memory-mapped array which exploits the capabilities provided by
   mmap in order to grow dynamically without moving existing data or
   wasting memory.

   Ideal for situations where you don't know the final size of the
   array, the potential maximum is known but very large, and a
   threaded environment makes it unsafe to resize by reallocating.

   NOTE: the array only supports growing, under the assumption that
   any array which can shrink safely at all can shrink safely to size
   zero (with the data copied to a new, smaller DynamicArray)

   This approach makes the most sense in a 64-bit environment where
   address space is cheap.

   Note that most systems cannot reserve more than 2-8TB of
   contiguous address space (32-128TB total), most likely because most
   machines don't have that much swap space anyway.

   NOTE: the DynamicArray is not POD (because it has non-trivial
   constructor/destructor, etc.), but that is only to prevent
   accidental copying and moving (= world's biggest buffer
   overflow!). The class is designed so that its contents can be
   copied easily to buffers, written to disk, etc.
 */
template <class ElementType> struct DynamicArray {
  inline uint64_t PageBits() { return use_1gb_hugepages ? 30 : 16; }
  inline uint64_t PageSize() { return uint64_t{1} << PageBits(); }

  // Default constructor - Create a useless array with maximum capacity of zero
  // bytes. It can only become useful by plundering an rvalue reference of a
  // useful array.
  //
  // This method exists only so we can satisfy std::is_move_constructible and
  // std::is_move_assignable.
  DynamicArray() : capacity(0), size(0), data(nullptr) {}

  // Create a new array of [size] bytes that can grow to a maximum of [capacity]
  // bytes.
  DynamicArray(bool use_1gb_hugepages, uint64_t capacity,
               uint64_t initial_size = 0)
      : use_1gb_hugepages(use_1gb_hugepages), capacity(capacity), size(0) {
    // round up to the nearest page boundary
    capacity = align_up(capacity, PageSize());
    initial_size = align_up(initial_size, PageSize());

    LOG_IF(FATAL, capacity < initial_size)
        << "Initial size cannot be larger than capacity (" << initial_size
        << "/" << capacity << ")";

    /*
      The magical incantation below tells mmap to reserve address
      space within the process without actually allocating any
      memory. We then call mprotect() to make bits of that address
      space usable.

      Note that mprotect() is smart enough to mix and match different
      sets of permissions, so we can extend the array simply by
      updating 0..new_size to have R/W permissions. Similarly, we can
      blow everything away by unmapping 0..reserved_size.

      Tested on Cygwin/x86_64, Linux-2.6.18/x86 and Solaris-10/Sparc.
    */

    int flags = MAP_NORESERVE | MAP_ANON | MAP_PRIVATE;

    // See if we need to use 1GB hugepages - Linux only for now. E.g., enable
    // 10GB of 1GB hugepages using: sudo sh -c 'echo 10 >
    // /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages'
    if (use_1gb_hugepages) {
      flags |= (MAP_HUGETLB | MAP_HUGE_1GB);
    }

    data = reinterpret_cast<char *>(
        mmap(nullptr, capacity, PROT_NONE, flags, -1, 0));
    LOG_IF(FATAL, !data || data == MAP_FAILED)
        << "Unable to reserve " << capacity
        << " bytes of address space: " << errno;

    // Make the initial size of array available
    ExtendTo(initial_size);
  }

  // Destructor
  ~DynamicArray() {
    if (capacity) {
      int err = munmap(data, capacity);
      LOG_IF(FATAL, err) << "Unable to unmap " << capacity
                         << " bytes starting at " << data;
    }
  }

  // Ensures that at least [min_size] bytes are ready to use. Unlike Resize(),
  // this function accepts any value of [min_size] (doing nothing if the array
  // is already big enough).
  void EnsureSize(uint64_t min_size) {
    if (size < min_size) {
      const std::lock_guard<std::mutex> lock(mx);
      if (size < min_size) {
        min_size = align_up(min_size, PageSize());
        ExtendTo(min_size);
      }
    }
  }

  // Return the i-th element, depending on the request type
  ElementType &operator[](uint64_t i) {
    return reinterpret_cast<ElementType *>(data)[i];
  }

  // Return the i-th element, depending on the request type
  ElementType const &operator[](uint64_t i) const {
    return reinterpret_cast<ElementType *>(data)[i];
  }

  ElementType *GetEntryPtr(uint64_t i) {
    return &(reinterpret_cast<ElementType *>(data)[i]);
  }

  // Not copyable, but is movable
  DynamicArray(DynamicArray const &) = delete;
  void operator=(DynamicArray const &) = delete;

  DynamicArray(DynamicArray &&victim) noexcept : DynamicArray() {
    *this = std::move(victim);
  }

  DynamicArray &operator=(DynamicArray &&victim) noexcept {
    std::swap(*this, victim);
    return *this;
  }

  // Extend the array to [new_size] bytes
  void ExtendTo(uint64_t new_size) {
    // Round up to the nearest page boundary
    new_size = align_up(new_size, PageSize());

    LOG_IF(FATAL, new_size < size) << "Cannot resize to a smaller size";

    // Mark the new range as RW. Don't mess w/ the existing region!!
    ExtendBy(new_size - size);
  }

  // Extend the array by [extend_size] bytes
  void ExtendBy(uint64_t extend_size) {
    uint64_t new_size = size + extend_size;
    LOG_IF(FATAL, new_size > capacity)
        << "Cannot extend beyond capacity (" << new_size << " bytes requested, "
        << capacity << " possible)";
    LOG_IF(FATAL, !is_aligned(new_size, PageSize()));

    if (extend_size) {
      int err = mprotect(data + size, extend_size, PROT_READ | PROT_WRITE);
      LOG_IF(FATAL, err) << "Unable to resize DynamicArray: " << errno;
      // prefault the space
      mlock(data + size, extend_size);
      memset(data + size, 0, extend_size);
      size = new_size;
    }
  }

  // Whether to use 1GB hugepages
  bool use_1gb_hugepages;

  // Total capacity in bytes
  uint64_t capacity;

  // Current size of the array in bytes
  uint64_t size;
  // Mutex to extend size thread-safely
  std::mutex mx;

  // The memory block holding this array
  char *data;
};

} // namespace table
} // namespace noname

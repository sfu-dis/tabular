/*
 * Copyright (C) 2022 Data-Intensive Systems Lab, Simon Fraser University. 
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

// Implements a centralized log that uses a dedicated flusher daemon.

#pragma once
#include <atomic>
#include <condition_variable>
#include <vector>
#include "../defs.h"
#include "../segment.h"

namespace noname {
namespace clog {

// Same as Log but supports concurrency. Can be used as a "scalable" centralized
// log, similar to the one described in ERMIA. 
struct Log {
  // Source for generating unique Segment IDs - only one thread (the flusher) will increment it, so
  // not an atomic.
  static uint64_t segment_id_counter;

  // Segment file name template: log-segid
  char segment_name_buf[SEGMENT_FILE_NAME_BUFSZ];

  // Directory storing the segments belonging to this log
  std::string dir;

  // All segments belonging to this log. segments[segments.size()-1] is the currently open segment
  // To be manipulated by the only log flusher. Not protected.
  std::vector<noname::log::Segment* > segments;

  // Whether to O_DIRECT for writing segments
  bool direct_io;

  // Segment size
  uint64_t segment_size;

  // Size of a log buffer (in bytes)
  uint32_t log_buffer_size;

  // One ring log buffer
  char *log_buffer;

  // Current offset (treating log space as a linear space without segmentation)
  std::atomic<uint64_t> reserve_offset;

  // How many bytes have been filled with actual log data; always <= reserved_offset
  std::atomic<uint64_t> filled_offset;

  // Also treating log space as a linear space, but tracks the durable portion.
  // To be manipulated only by the only flusher, so not an atomic.
  uint64_t durable_offset;

  // Condition variable for log flush
  std::condition_variable flush_cond;

  // Mutex for flush_cond
  std::mutex flush_mutex;

  // Log flusher thread (background)
  std::thread flusher;

  // Condition variable for whether the log should be running or not
  std::atomic<bool> thread_flushing;

  // Constructor
  Log(std::string &dir, uint64_t segment_size, uint32_t log_buffer_size, bool dio);

  // Destructor
  ~Log();

  // Create a new segment when the current segment is about to exceed the max segment size.
  // Returns reference to the newly created segment.
  noname::log::Segment *CreateSegment();

  // Get the currently open segment
  inline noname::log::Segment *CurrentSegment() { return segments[segments.size() - 1]; }

  // Allocate a log block in-place on the currently active log buffer
  // @payload_size: number of bytes (payload) to allocate for this log block
  // Returns a pointer to the allocated log block which is directly instantiated on the allocated
  // space in-place.
  noname::log::LogBlock *AllocateLogBlock(uint32_t payload_size);

  // Flush the log buffer
  void Flush();

  // Flusher daemon function
  void FlushDaemon();

  // Copy log data to a previously reserved log block in the log buffer
  // @block: a previously allocated log block in the buffer
  // @payload: log data
  // @payload_size: size of payload in bytes
  void PopulateLogBlock(noname::log::LogBlock *block, char *payload, uint32_t payload_size);
};

}  // namespace clog
}  // namespace noname

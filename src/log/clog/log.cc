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

#include <glog/logging.h>
#include <algorithm>
#include <utility>
#include <string>
#include "log.h"

namespace noname {
namespace clog {

uint64_t noname::clog::Log::segment_id_counter = 0;

Log::Log(std::string &dir, uint64_t segment_size, uint32_t log_buffer_size, bool dio)
  : dir(dir)
  , direct_io(dio)
  , segment_size(segment_size)
  , log_buffer_size(log_buffer_size)
  , reserve_offset(0)
  , filled_offset(0)
  , durable_offset(0)
  , flusher(&Log::FlushDaemon, this) {
  // Segment size must of multiples of log buffer size
  LOG_IF(FATAL, segment_size % log_buffer_size != 0)
    << "Segment size must be multiples of log buffer size";

  thread_flushing.store(true, std::memory_order_relaxed);

  int ret = posix_memalign(reinterpret_cast<void **>(&log_buffer), 4096, log_buffer_size);
  LOG_IF(FATAL, ret) << "Unable to allocate log buffer";

  auto *seg = CreateSegment();
}

Log::~Log() {
  thread_flushing.store(false, std::memory_order_relaxed);
  flush_cond.notify_all();
  flusher.join();

  for (auto s : segments) {
    delete s;
  }
  segments.clear();
}

noname::log::Segment *Log::CreateSegment() {
  uint64_t sid = segment_id_counter++;
  size_t n = snprintf(segment_name_buf, sizeof(segment_name_buf), SEGMENT_FILE_NAME_FMT, sid);
  LOG_IF(FATAL, n != sizeof(segment_name_buf) - 1) << "Error generating log segment file name.";

  std::string filename = dir + "/" + std::string(segment_name_buf);
  auto *seg =  new noname::log::Segment(filename, segment_size, sid, direct_io);
  segments.push_back(seg);
  return seg;
}

noname::log::LogBlock *Log::AllocateLogBlock(uint32_t payload_size) {
  // Very basic check - log record can't be larger than the log buffer
  uint32_t alloc_size = payload_size + sizeof(noname::log::LogBlock);
  if (alloc_size > log_buffer_size) {
    return nullptr;
  }

  // Try to reserve log buffer space - here we need to make sure a log block doesn't cross segment
  // boundary. If so, we append dummy record and start with a "new" segment, but do so only
  // physically. The actualy segment open/close business is done by the flusher when it drains the
  // log buffer.

  // Reseve the space in the "logical" log
  auto start_off = reserve_offset.fetch_add(alloc_size);
  while (start_off + alloc_size - durable_offset > log_buffer_size) {
    // Notify the flusher to flush until flush_begin/curr_off
    flush_cond.notify_all();
  }
  noname::log::LogBlock* new_block =
    reinterpret_cast<noname::log::LogBlock *>(log_buffer + start_off % log_buffer_size);
  new_block->offset = start_off;
  return new_block;
}

void Log::PopulateLogBlock(noname::log::LogBlock *block, char *payload, uint32_t payload_size) {
  // TODO(tzwang): initialize log block
  uint32_t to_copy = payload_size;
  while (to_copy > 0) {
    auto remaining_buffer_space = log_buffer_size - (reinterpret_cast<char *>(block) - log_buffer);
    auto copy_size = std::min<uint32_t>(to_copy, remaining_buffer_space);
    // Bypass the metadata stored in the log block
    memcpy(block + 1, payload, copy_size);
    to_copy -= copy_size;
    payload += copy_size;
  }
  uint64_t filled = block->offset;
  do {
    filled = block->offset;
  } while (!filled_offset.compare_exchange_strong(filled,
    filled + payload_size + sizeof(noname::log::LogBlock)));
}

void Log::Flush() {
  auto *seg = CurrentSegment();
  auto flush_size = filled_offset - durable_offset;
  if (flush_size > 0) {
    bool seg_space_ok = (seg->current_size + flush_size) <= segment_size;

    if (!seg_space_ok) {
      seg = CreateSegment();
    }

    bool success = seg->SyncAppend(log_buffer + (durable_offset % log_buffer_size), flush_size);
    LOG_IF(FATAL, !success);
    durable_offset += flush_size;
  }
}

void Log::FlushDaemon() {
  while (thread_flushing.load(std::memory_order_relaxed)) {
    std::unique_lock lk(flush_mutex);
    flush_cond.wait(lk);
    Flush();
  }
}

}  // namespace clog
}  // namespace noname

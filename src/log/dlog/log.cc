/*
 * Copyright (C) 2024 Data-Intensive Systems Lab, Simon Fraser University. 
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

#include <unistd.h>
#include <sched.h>
#include <numa.h>
#include <glog/logging.h>

#include <string>
#include <filesystem>

#include "log.h"

namespace noname {
namespace dlog {

thread_local Log dlog;
std::mutex dlog_lock;
std::vector<Log *> dlogs;
std::vector<log::LSN> lsns;

Log *GetLog() {
  thread_local bool initialized = false;
  if (!initialized) {
    std::lock_guard<std::mutex> guard(dlog_lock);
    if (!initialized) {
      initialized = true;

      constexpr int USERNAME_MAX_SIZE = 32;
      char user[USERNAME_MAX_SIZE];
      getlogin_r(user, USERNAME_MAX_SIZE);

      std::string log_dir = std::filesystem::path("/dev/shm") / user / "noname-log";
      log::config_t config(false, log_dir, (uint64_t)64 * MB, 1 * MB, false);
      new (&dlog) dlog::Log(config);
      dlogs.push_back(&dlog);
    }
  }
  return &dlog;
}

void GetLSNs() {
  // Get durable log offsets so that CDC can start from there instead from the beginning
  for (auto &dlog : dlogs) {
    dlog->LastFlush();
    lsns.push_back(dlog->durable_lsn);
  }
}

std::atomic<uint64_t> Log::segment_id_counter(0);

Log::Log(const std::string &dir, uint64_t segment_size, uint32_t log_buffer_size, bool dio)
  : dir(dir)
  , direct_io(dio)
  , segment_size(segment_size)
  , log_buffer_size(log_buffer_size)
  , active_log_buffer(nullptr)
  , log_buffer_offset(0) {
  int ret = posix_memalign(reinterpret_cast<void **>(&log_buffers[0]),
                           4096, log_buffer_size * 2);

  log_buffers[1] = log_buffers[0] + log_buffer_size;
  active_log_buffer = log_buffers[0];

  auto *seg = CreateSegment();
  new (&current_lsn) noname::log::LSN(seg->id, 0);
  new (&durable_lsn) noname::log::LSN(seg->id, 0);
}

Log::Log(log::config_t config)
  : Log(config.log_dir, config.segment_size, config.log_buffer_size, config.dio) {}

Log::~Log() {
  LastFlush();

  for (auto s : segments) {
    delete s;
  }

  std::lock_guard<std::mutex> guard(dlog_lock);
  auto found = std::find(dlogs.begin(), dlogs.end(), this);
  if (found != dlogs.end()) {
    dlogs.erase(found);
  }
}

noname::log::Segment *Log::CreateSegment() {
  uint64_t sid = segment_id_counter++;
  size_t n = snprintf(segment_name_buf, sizeof(segment_name_buf),
                      SEGMENT_FILE_NAME_FMT, sid);
  LOG_IF(FATAL, n != sizeof(segment_name_buf) - 1) << "Error generating log segment file name.";

  std::string filename = dir + "/" + std::string(segment_name_buf);
  segments.push_back(new noname::log::Segment(filename, segment_size, sid, direct_io));

  return segments.back();
}

void Log::PollFlush() {
  uint32_t i = 0;
  for (auto &seg : segments) {
    if (seg->flushing) {
      LOG_IF(FATAL, seg->expected_size == 0);
      uint64_t flushed_bytes = seg->PollAsyncAppend();
      LOG_IF(FATAL, flushed_bytes <= 0);

      if (durable_lsn.segment_id != seg->id) {
        durable_lsn.segment_id = seg->id;
      }
      durable_lsn.segment_offset = seg->current_size;
    }
    ++i;
  }
}

noname::log::LogBlock *Log::AllocateLogBlock(uint32_t payload_size,
                                             noname::log::LSN *out_lsn,
                                             uint64_t tid) {
  // No need since thread-local logging
  // std::lock_guard<std::mutex> lg(lock);

  noname::log::Segment *seg;
  seg = CurrentSegment();
  uint32_t alloc_size = payload_size + sizeof(noname::log::LogBlock);
  if (alloc_size > log_buffer_size || alloc_size > seg->capacity) {
    return nullptr;
  }

  bool seg_space_ok = seg->expected_size + log_buffer_offset + alloc_size <= seg->capacity;
  bool buf_space_ok = log_buffer_offset + alloc_size <= log_buffer_size;

  // In either case we need to flush the current segment
  if (!buf_space_ok || !seg_space_ok) {
    FlushLogBuffer();
    // Now buffer space is ok, see if we have enough space in the segment
    if (!seg_space_ok) {
      seg = CreateSegment();
    }
  }

  // Now guaranteed to have free space in both the segment and log buffer
  uint64_t off = log_buffer_offset;
  out_lsn->segment_id = seg->id;
  out_lsn->segment_offset = seg->expected_size + log_buffer_offset;
  log_buffer_offset += alloc_size;

  auto new_block =
      reinterpret_cast<noname::log::LogBlock *>(active_log_buffer + off);
  new (new_block) noname::log::LogBlock(payload_size, off);
  new_block->tid = tid;
  return new_block;
}

void Log::FlushLogBuffer() {
  uint64_t log_size;
  if (direct_io) {
    log_size = align_up(log_buffer_offset, 4096);
  } else {
    log_size = log_buffer_offset;
  }
  bool success =
      CurrentSegment()->AsyncAppend(active_log_buffer, log_size);
  LOG_IF(FATAL, !success);
  active_log_buffer =
      (active_log_buffer == log_buffers[0]) ? log_buffers[1] : log_buffers[0];
  log_buffer_offset = 0;
}

void Log::LastFlush() {
  std::lock_guard<std::mutex> lg(lock);
  PollFlush();
  // Last round of flush to make sure any remaining data in the buffer is persisted
  if (log_buffer_offset > 0) {
    FlushLogBuffer();
    PollFlush();
  }
}

}  // namespace dlog
}  // namespace noname

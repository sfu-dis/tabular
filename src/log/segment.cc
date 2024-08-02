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

// Implements an append-only segment.

#include <glog/logging.h>
#include <string>

#include "segment.h"

#if defined(LOG_NOP)
#include "asi/nop_storage.h"
#else
#include "asi/io_uring_storage.h"
#endif

namespace noname {
namespace log {

Segment::Segment(const std::string &filename, uint64_t cap, uint64_t id, bool dio)
    : capacity(cap)
    , current_size(0)
    , expected_size(0)
    , id(id)
    , flushing(false) {
  #if defined(LOG_NOP)
  storage_interface = new asi::NOPStorage();
  #else
  storage_interface = new asi::IOUringStorage(filename, dio);
  #endif
}

Segment::~Segment() {
  LOG_IF(FATAL, flushing);
  delete storage_interface;
}

bool Segment::AsyncAppend(const char *src, const uint64_t size) {
  if (flushing) {
    PollAsyncAppend();
  }

  if (current_size + size > capacity) {
    return false;
  }

  flushing = true;

  storage_interface->AsyncWrite(src, size, current_size);

  expected_size += size;

  return true;
}

uint64_t Segment::PollAsyncAppend() {
  LOG_IF(FATAL, !flushing);
  uint64_t size = storage_interface->PollAsyncWrite();

  current_size += size;
  LOG_IF(FATAL, current_size > expected_size);
  flushing = false;
  return size;
}

bool Segment::SyncAppend(const char *src, const uint64_t size) {
  if (!AsyncAppend(src, size)) {
    return false;
  }

  PollAsyncAppend();
  return true;
}

bool Segment::PeekAsyncAppend(uint64_t *out_size) {
  if (!storage_interface->PeekAsyncWrite(out_size)) {
    return false;
  }

  current_size += *out_size;
  flushing = false;
  return true;
}

int Segment::GetFD() {
  #if defined(LOG_NOP)
  return 0;
  #else
  return reinterpret_cast<asi::IOUringStorage *>(storage_interface)->fd;
  #endif
}

uint32_t LogRecord::PopulateLogRecord(LogRecord::LogRecordType type,
                                      LogBlock *block, table::FID fid,
                                      table::OID oid, const char *after_image,
                                      const uint32_t size) {
  LOG_IF(FATAL, type != LogRecordType::INSERT && type != LogRecordType::UPDATE)
      << "Wrong log record type";
  LOG_IF(FATAL, block->payload_size + size > block->capacity) << "No enough space in log block";

  uint32_t offset = block->payload_size;
  auto record = reinterpret_cast<LogRecord *>(&block->GetPayload()[offset]);

  record->type = type;
  record->oid = oid;
  record->fid = fid;
  record->tid = block->tid;

  memcpy(record->GetPayload(), after_image, size);
  record->payload_size = size;
  block->payload_size += log::LogRecord::GetExpectSize(size);

  return offset;
}

}  // namespace log
}  // namespace noname

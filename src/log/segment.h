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

// Implements an append-only segment. 

#pragma once

#include <cstdint>
#include <string>

#include "asi/asi.h"
#include "table/object.h"

namespace noname {
namespace log {

// A block of data in the log
struct LogBlock {
  // The start position in log_buff, &log_buff[offset] == &log_block
  uint64_t offset;

  // TID of the transaction who created this log block (Silo OCC)
  uint64_t tid;

  // Size of this log block in bytes
  uint32_t payload_size;

  // Total capacity of this log block (could be greater than payload_size)
  uint32_t capacity;

  // Actual data, which in turn is an array of log records;
  // must be the last element
  char payload[0];

  LogBlock(uint32_t cap, uint64_t off)
      : payload_size(0), capacity(cap), offset(off) {}
  ~LogBlock() {}

  // Size of this whole log block
  inline uint32_t TotalSize() { return sizeof(*this) + payload_size; }
  inline char *GetPayload() { return &payload[0]; }
};

struct LogRecord {
  enum LogRecordType {
    INSERT,
    UPDATE,
  };

  LogRecordType type;
  table::FID fid;
  table::OID oid;
  uint64_t tid;
  uint32_t payload_size;
  char payload[0];

  inline char *GetPayload() { return &payload[0]; }
  inline uint32_t TotalSize() { return sizeof(*this) + payload_size; }
  static inline uint32_t GetExpectSize(uint32_t expect_payload_size) {
    return sizeof(LogRecord) + expect_payload_size;
  }
  static inline uint32_t LogInsert(LogBlock *block, table::FID fid,
                                   table::OID oid, const char *after_image,
                                   uint32_t size) {
    return PopulateLogRecord(INSERT, block, fid, oid, after_image, size);
  }
  static inline uint32_t LogUpdate(LogBlock *block, table::FID fid,
                                   table::OID oid, const char *after_image,
                                   uint32_t size) {
    return PopulateLogRecord(UPDATE, block, fid, oid, after_image, size);
  }
  // Append a new LogRecord to given log block
  static uint32_t PopulateLogRecord(LogRecordType type,
                                      LogBlock *block,
                                      table::FID fid, table::OID oid,
                                      const char *after_image,
                                      uint32_t size);
};

// Filename of segment files is "wal-<sequence-number>".
#define SEGMENT_FILE_NAME_FMT "wal-%08x"
#define SEGMENT_FILE_NAME_BUFSZ sizeof("wal-01234567")

// A persistent "segment" of "the log" that stores log records (LogBlock's).
//
// This structure only describes the persistent portion of the log (by definition), without handling
// log buffers, which is done by the user.
//
// The user can be creative in using a segment as:
// - a thread-local/transaction-local log
// - a log thread-local/transaction-local log partition
//
// Concurrency control is offloaded to the user - there is no CC whatsoever guaranteed by the
// segment itself.
struct Segment {
  // ID of this segment
  uint64_t id;

  // Max size of this segment in bytes
  uint64_t capacity;

  // Current size (in bytes) of the segment
  uint64_t current_size;

  // Current size but including size of data that's being flushed asynchronously
  uint64_t expected_size;

  // Whether there is an append I/O operation going on
  bool flushing;

  // Abstract storage interface
  struct asi::ASI *storage_interface;

  // Constructors
  Segment(const std::string &filename, uint64_t cap, uint64_t id, bool dio);

  // Destructor
  ~Segment();

  // Synchronously append data to the end of this segment
  // @src: pointer to source data
  // @size: number of bytes to write
  // Returns whether the request succeeded.
  bool SyncAppend(const char *src, const uint64_t size);

  // Asynchronously append data to the end of this segment. We allow multiple outstanding I/Os.
  // @src: pointer to source data
  // @size: number of bytes to write
  // Returns whether the request was successfully submitted.
  bool AsyncAppend(const char *src, const uint64_t size);

  // Poll for completion of the previous async-append request. Blocks until the
  // request is completed.
  // Returns the number of bytes written
  uint64_t PollAsyncAppend();

  // Query the current completion status of the previous async-append request.
  // Returns true if the I/O has concluded (not necessarly successful).
  // @out_size: number of bytes written
  bool PeekAsyncAppend(uint64_t *out_size);

  // Get fd
  int GetFD();
};

}  // namespace log
}  // namespace noname

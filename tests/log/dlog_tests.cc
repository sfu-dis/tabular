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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <noname_defs.h>

#include <log/dlog/log.h>
#include "config/config.h"

TEST(LogTest, Create) {
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  noname::dlog::Log log(dirname, 16384, 1024, false);
  ASSERT_EQ(noname::dlog::Log::segment_id_counter, 1);
  ASSERT_EQ(log.segments.size(), 1);
  ASSERT_FALSE(log.direct_io);
  ASSERT_EQ(log.segment_size, 16384);
  ASSERT_EQ(log.log_buffer_size, 1024);
  ASSERT_EQ(log.active_log_buffer, log.log_buffers[0]);
  ASSERT_EQ(log.current_lsn.segment_id, 0);
  ASSERT_EQ(log.durable_lsn.segment_id, 0);
  ASSERT_EQ(log.current_lsn.segment_offset, 0);
  ASSERT_EQ(log.durable_lsn.segment_offset, 0);

  system("rm -rf ./logtest");
}

TEST(LogTest, Append) {
  // Create a log with small log buffer but large enough segment
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  {
    noname::dlog::Log log(dirname, 16384, 256, false);

    // Keep inserting - should flush four times to one segment
    for (uint32_t i = 0; i < 4; ++i) {
      noname::log::LSN lsn;
      auto *block = log.AllocateLogBlock(130, &lsn, 0);
      uint64_t alloc_sz = 130 + sizeof(noname::log::LogBlock);
      ASSERT_EQ(log.log_buffer_offset, alloc_sz);
      ASSERT_EQ(lsn.segment_id, log.segments[log.segments.size() - 1]->id);
      ASSERT_EQ(lsn.segment_offset, i * alloc_sz);
    }
  }
  system("rm -rf ./logtest");
}

TEST(LogTest, SegmentsWithExtraSpace) {
  // Create a log with small log buffer and small segment
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  {
    noname::dlog::Log log(dirname, 512, 256, false);

    // Keep inserting - should flush twice into one segment.
    // Four segments with exta space should be created.
    uint32_t payload_size = 256 - sizeof(noname::log::LogBlock);
    uint64_t alloc_sz = payload_size + sizeof(noname::log::LogBlock);
    for (uint32_t i = 0; i < 8; ++i) {
      noname::log::LSN lsn;
      auto *block = log.AllocateLogBlock(payload_size, &lsn, 0);
      if (log.segments.size() > 1) {
        auto prevSegment = log.segments[log.segments.size() - 2];
        ASSERT_EQ(prevSegment->expected_size, alloc_sz*2);
      }
      // Segment has 512 bytes and buffer has 256 bytes
      // First insert, 1 segment, segment offset = 0, buffer = 256
      // Second insert, 1 segment, segment offset = 256, buffer = 256
      // Third insert, 2 segment, first flush buffer to segment,
      //    then create a new segment, since the segment doesn't have more space for a new block
      //    after create the new segment, data is written to buffer
      //    so 2 segment, segment offset = 0, buffer = 256
      if (i % 2 == 0) {
        ASSERT_EQ(lsn.segment_offset, 0);
      } else {
        ASSERT_EQ(lsn.segment_offset, 256);
      }
      ASSERT_EQ(log.log_buffer_offset, 256);
    }
    ASSERT_EQ(4, log.segments.size());
  }
  system("rm -rf ./logtest");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

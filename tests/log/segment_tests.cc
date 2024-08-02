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
#include <gtest/gtest.h>

#include <noname_defs.h>

#include <log/segment.h>
#include "config/config.h"
#include "asi/io_uring_storage.h"

TEST(SegmentTest, Create) {
  std::string filename("seg-test");
  noname::log::Segment seg(filename, 4096, 0, false);
  ASSERT_EQ(seg.capacity, 4096);
  ASSERT_EQ(seg.id, 0);
  ASSERT_FALSE(((noname::asi::IOUringStorage *)seg.storage_interface)->direct_io);
  ASSERT_EQ(((noname::asi::IOUringStorage *)seg.storage_interface)->filename, filename);
  ASSERT_GT(((noname::asi::IOUringStorage *)seg.storage_interface)->fd, 0);
  ASSERT_FALSE(seg.flushing);
  ASSERT_EQ(seg.current_size, 0);
  ASSERT_EQ(seg.expected_size, 0);
  system("rm -rf seg-test");
}

TEST(SegmentTest, AsyncAppendPoll) {
  std::string filename("seg-test");
  {
    noname::log::Segment seg(filename, 4096, 0, false);

    for (uint32_t i = 0; i < 10; ++i) {
      bool success = seg.AsyncAppend(reinterpret_cast<const char *>(&i), sizeof(i));
      ASSERT_TRUE(success);

      uint64_t written = seg.PollAsyncAppend();
      ASSERT_EQ(written, sizeof(i));
      ASSERT_EQ(seg.current_size, (i + 1) * sizeof(i));
      ASSERT_EQ(seg.current_size, seg.expected_size);
      ASSERT_FALSE(seg.flushing);
    }
  }

  // Verify the actual file content
  int fd = open(filename.c_str(), O_RDWR);
  for (uint32_t i = 0; i < 10; ++i) {
    uint32_t val = -1;
    auto ret = pread(fd, reinterpret_cast<void *>(&val), sizeof(i), i * sizeof(i));
    ASSERT_EQ(ret, sizeof(i));
    ASSERT_EQ(val, i);
  }
  close(fd);
  system("rm -rf seg-test");
}

TEST(SegmentTest, AsyncAppendPeek) {
  std::string filename("seg-test");
  {
    noname::log::Segment seg(filename, 4096, 0, false);

    for (uint32_t i = 0; i < 10; ++i) {
      bool success = seg.AsyncAppend(reinterpret_cast<const char *>(&i), sizeof(i));
      ASSERT_TRUE(success);

      uint64_t size = 0;
      do {
        success = seg.PeekAsyncAppend(&size);
      } while (!success);
      ASSERT_EQ(size, sizeof(i));
      ASSERT_EQ(seg.current_size, (i + 1) * sizeof(i));
    }
  }

  // Verify the actual file content
  int fd = open(filename.c_str(), O_RDWR);
  for (uint32_t i = 0; i < 10; ++i) {
    uint32_t val = -1;
    auto ret = pread(fd, reinterpret_cast<void *>(&val), sizeof(i), i * sizeof(i));
    ASSERT_EQ(ret, sizeof(i));
    ASSERT_EQ(val, i);
  }
  close(fd);
  system("rm -rf seg-test");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


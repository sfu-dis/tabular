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

#include <log/clog/log.h>
#include <config/config.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <fcntl.h>
#include <atomic>

#include "noname_defs.h"

TEST(LogTest, Create) {
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  noname::clog::Log log(dirname, 16384, 1024, false);
  ASSERT_EQ(noname::clog::Log::segment_id_counter, 1);
  ASSERT_EQ(log.segments.size(), 1);
  ASSERT_FALSE(log.direct_io);
  ASSERT_EQ(log.segment_size, 16384);
  ASSERT_EQ(log.log_buffer_size, 1024);
  ASSERT_NE(log.log_buffer, nullptr);
  ASSERT_EQ(log.reserve_offset, 0);
  ASSERT_EQ(log.filled_offset, 0);
  ASSERT_EQ(log.durable_offset, 0);
  system("rm -rf ./logtest");
}

TEST(LogTest, Append) {
  // Create a log with small log buffer but large enough segment
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  {
    noname::clog::Log log(dirname, 16384, 1024, false);

    // Keep inserting - should flush four times to one segment
    auto alloc_size = 0;
    auto payload_size = sizeof(uint32_t);
    for (uint32_t i = 0; i < 4; ++i) {
      uint32_t val = i;
      auto *block = log.AllocateLogBlock(payload_size);
      alloc_size += payload_size + sizeof(noname::log::LogBlock);
      ASSERT_EQ(alloc_size, log.reserve_offset);
      ASSERT_EQ(log.segments.size(), 1);

      log.PopulateLogBlock(block, reinterpret_cast<char *>(&val), payload_size);
      ASSERT_EQ(log.filled_offset, (payload_size+sizeof(noname::log::LogBlock))*(i+1));
    }
  }
  system("rm -rf ./logtest");
}


TEST(LogTest, Persistence) {
  // Create a log with small log buffer same as before
  // This time check the underlying file for the correct bits
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  uint64_t segment_id;
  auto payload_size = sizeof(uint64_t);
  {
    noname::clog::Log log(dirname, 16384, 1024, false);
    segment_id = log.segment_id_counter;
    // Keep inserting - should flush four times to one segment
    auto alloc_size = 0;
    for (uint32_t i = 0; i < 4; ++i) {
      uint64_t val = i+1;
      auto *block = log.AllocateLogBlock(payload_size);
      alloc_size += payload_size + sizeof(noname::log::LogBlock);
      ASSERT_EQ(alloc_size, log.reserve_offset);
      ASSERT_EQ(log.segments.size(), 1);

      log.PopulateLogBlock(block, reinterpret_cast<char *>(&val), payload_size);
      ASSERT_EQ(log.filled_offset, (payload_size+sizeof(noname::log::LogBlock))*(i+1));
    }
    log.Flush();
  }

  // Open the file for the 3rd test
  std::string filename("./logtest/wal-0000000");
  filename += std::to_string(segment_id - 1);
  auto fd = open(filename.c_str(), O_RDWR);
  ASSERT_NE(fd, -1);

  // Ensure the size and file are able to be opened and check for data
  auto fsize = lseek(fd, 0, SEEK_END);
  ASSERT_NE(fsize, 0);
  for (uint32_t i = 0; i < 4; ++i) {
    uint64_t val =-1;
    auto log_size = sizeof(noname::log::LogBlock);
    auto offset = i * (log_size + payload_size) + log_size;
    auto ret = pread(fd, &val, payload_size, offset);
    ASSERT_EQ(ret, payload_size);
    ASSERT_EQ(val, i+1);
  }
  // Check that the metadata was copied correctly
  auto alloc_size = 0;
  for (uint32_t i = 0; i < 4; ++i) {
    uint64_t val =-1;
    auto ret = pread(fd, &val, payload_size, i * (sizeof(noname::log::LogBlock) + payload_size));
    ASSERT_EQ(ret, payload_size);
    ASSERT_EQ(val, alloc_size);
    alloc_size += sizeof(noname::log::LogBlock) + payload_size;
  }
  close(fd);
  system("rm -rf ./logtest");
}



TEST(LogTest, CreateNewSegments) {
  // Create a log with a small log and segment size
  // Testing if multiple segments are created properly
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  noname::clog::Log log(dirname, 24, 12, false);

  // Keep inserting should flush twice to a segment
  // Ex. If there are 10 insertions than 5 segments should be created
  auto alloc_size = 0;
  auto payload_size = sizeof(uint32_t);

  for (uint32_t i = 0; i < 10; ++i) {
    uint32_t val = i;
    auto *block = log.AllocateLogBlock(payload_size);
    alloc_size += payload_size + sizeof(noname::log::LogBlock);
    ASSERT_EQ(alloc_size, log.reserve_offset);

    log.PopulateLogBlock(block, reinterpret_cast<char*>(&val), payload_size);
    ASSERT_EQ(log.filled_offset, (payload_size+ sizeof(noname::log::LogBlock))*(i+1));
  }
  ASSERT_EQ(log.segments.size(), 5);
  system("rm -rf ./logtest");
}

TEST(LogTest, PersistedCreateNewSegments) {
  // Create a log with a small log and segment size
  // Testing if multiple segments are peresisted properly
  // This time check if the underlying file persisted correctly
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  auto payload_size = sizeof(uint32_t);

  noname::clog::Log log(dirname, 24, 12, false);
  // Keep inserting should flush twice to a segment
  // Ex. If there are 10 insertions than 5 segments should be created
  auto alloc_size = 0;
  auto segment_id = log.segment_id_counter;

  for (uint32_t i = 0; i < 10; ++i) {
    uint32_t val = i;
    auto *block = log.AllocateLogBlock(payload_size);
    alloc_size += payload_size + sizeof(noname::log::LogBlock);
    ASSERT_EQ(alloc_size, log.reserve_offset);

    log.PopulateLogBlock(block, reinterpret_cast<char*>(&val), payload_size);
    ASSERT_EQ(log.filled_offset, (payload_size+ sizeof(noname::log::LogBlock))*(i+1));
  }
  ASSERT_EQ(log.segments.size(), 5);
  log.Flush();

  // Create the file string and check if there are two elements per segment
  for (int i = 0; i < 5; i++) {
    // Open the file for the 3rd test
    std::string filename("./logtest/wal-0000000");
    std::stringstream sstream;
    sstream << std::hex << segment_id -1 + i;
    std::string result = sstream.str();
    filename+= result;

    auto fd = open(filename.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);

    // Ensure the size and file are able to be opened and check for data
    auto fsize = lseek(fd, 0, SEEK_END);
    ASSERT_NE(fsize, 0);

    for (int j = 0; j < 2; j++) {
      uint32_t val =- 1;
      auto log_size = sizeof(noname::log::LogBlock);
      auto offset = j * (log_size + payload_size) + log_size;
      auto ret = pread(fd, &val, payload_size, offset);
      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i*2 +j);
    }
    close(fd);
  }
  system("rm -rf ./logtest");
}


TEST(LogTest, PopulateInOrder) {
  // Test if the block populates in order after allocation
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  auto payload_size = sizeof(uint32_t);

  noname::clog::Log log(dirname, 16384, 1024, false);
  // Keep inserting should flush twice to a segment
  // Ex. If there are 10 insertions than 5 segments should be created
  auto alloc_size = 0;
  auto segment_id = log.segment_id_counter;
  std::vector<noname::log::LogBlock *> blocks;

  for (uint32_t i = 0; i < 10; ++i) {
    auto *block = log.AllocateLogBlock(payload_size);
    alloc_size += payload_size + sizeof(noname::log::LogBlock);
    ASSERT_EQ(alloc_size, log.reserve_offset);
    blocks.push_back(block);
  }

  for (uint32_t i = 0; i < 10; ++i) {
    uint32_t val = i;

    log.PopulateLogBlock(blocks[i], reinterpret_cast<char*>(&val), payload_size);
    ASSERT_EQ(log.filled_offset, (payload_size+ sizeof(noname::log::LogBlock))*(i+1));
  }

  log.Flush();

  // Check persisted correctly
  std::string filename("./logtest/wal-0000000");
  std::stringstream sstream;
  sstream << std::hex << segment_id - 1;
  std::string result = sstream.str();
  filename+= result;
  auto fd = open(filename.c_str(), O_RDWR);
  ASSERT_NE(fd, -1);

  // Ensure the size and file are able to be opened and check for data
  auto fsize = lseek(fd, 0, SEEK_END);
  ASSERT_NE(fsize, 0);

  for (int i =0; i < 10; i++) {
      uint32_t val =- 1;
      auto log_size = sizeof(noname::log::LogBlock);
      auto offset = i * (log_size + payload_size) + log_size;
      auto ret = pread(fd, &val, payload_size, offset);
      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i);
  }
  close(fd);
  system("rm -rf ./logtest");
}

void populate_delay(noname::clog::Log* l, noname::log::LogBlock * blocks) {
  // Function for thread
  // Wait 5 seconds and then populate the first block
  sleep(5);
  uint32_t val = 0;
  uint32_t payload_size = sizeof(uint32_t);
  l->PopulateLogBlock(blocks, reinterpret_cast<char*>(&val), payload_size);
}


TEST(LogTest, PopulateOutOrder) {
  // Check if populate is done in order when on populate is delayed
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");

  auto payload_size = sizeof(uint32_t);
  noname::clog::Log log(dirname, 16384, 1024, false);
  auto alloc_size = 0;
  auto segment_id = log.segment_id_counter;
  std::vector<noname::log::LogBlock *> *blocks = new std::vector<noname::log::LogBlock *>(10);

  for (uint32_t i = 0; i < 10; ++i) {
    auto *block = log.AllocateLogBlock(payload_size);
    alloc_size += payload_size + sizeof(noname::log::LogBlock);
    ASSERT_EQ(alloc_size, log.reserve_offset);
    (*blocks)[i] = block;
  }

  // Delay the first block populate
  std::thread t1 (populate_delay, &log, (*blocks)[0]);

  // Populates for blocks 1-9 should have to wait 5 seconds for the first to complete
  for (uint32_t i = 1; i < 10; ++i) {
    uint32_t val = i;
    log.PopulateLogBlock((*blocks)[i], reinterpret_cast<char*>(&val), payload_size);
    ASSERT_EQ(log.filled_offset, (payload_size+ sizeof(noname::log::LogBlock))*(i+1));
  }

  t1.join();
  log.Flush();

  std::string filename("./logtest/wal-0000000");
  std::stringstream sstream;
  sstream << std::hex << segment_id - 1;
  std::string result = sstream.str();
  filename+= result;
  auto fd = open(filename.c_str(), O_RDWR);
  ASSERT_NE(fd, -1);

  auto fsize = lseek(fd, 0, SEEK_END);
  ASSERT_NE(fsize, 0);

  for (int i = 0; i < 10; i++) {
      uint32_t val =- 1;
      auto log_size = sizeof(noname::log::LogBlock);
      auto offset = i * (log_size + payload_size) + log_size;
      auto ret = pread(fd, &val, payload_size, offset);
      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i);
  }

  close(fd);
  system("rm -rf ./logtest");
}

TEST(LogTest, PersistenceVariableSize) {
  // Create a log with small log buffer same as before
  // Alternate between putting a uint64 and uint32 with varying sizes
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  uint64_t segment_id;
  int payload_size;
  {
    noname::clog::Log log(dirname, 16384, 1024, false);
    segment_id = log.segment_id_counter;
    // Keep inserting - should flush four times to one segment
    auto alloc_size = 0;
    for (uint32_t i = 0; i < 4; ++i) {
      if (i % 2 == 0) {
        payload_size = sizeof(uint64_t);
        uint64_t val = i+1;
        auto *block = log.AllocateLogBlock(payload_size);
        alloc_size += payload_size + sizeof(noname::log::LogBlock);
        ASSERT_EQ(alloc_size, log.reserve_offset);
        ASSERT_EQ(log.segments.size(), 1);

        log.PopulateLogBlock(block, reinterpret_cast<char *>(&val), payload_size);
        ASSERT_EQ(log.filled_offset, alloc_size);

      } else {
        payload_size = sizeof(uint32_t);
        uint32_t val = i+1;
        auto *block = log.AllocateLogBlock(payload_size);
        alloc_size += payload_size + sizeof(noname::log::LogBlock);
        ASSERT_EQ(alloc_size, log.reserve_offset);
        ASSERT_EQ(log.segments.size(), 1);

        log.PopulateLogBlock(block, reinterpret_cast<char *>(&val), payload_size);
        ASSERT_EQ(log.filled_offset, alloc_size);
      }
    }
    log.Flush();
  }

  // File name might be different
  std::string filename("./logtest/wal-0000000");
  std::stringstream sstream;
  sstream << std::hex << segment_id - 1;
  std::string result = sstream.str();
  filename+= result;
  auto fd = open(filename.c_str(), O_RDWR);
  ASSERT_NE(fd, -1);

  // Ensure the size and file are able to be opened and check for data
  auto fsize = lseek(fd, 0, SEEK_END);
  ASSERT_NE(fsize, 0);

  auto filled = 0;
  auto log_size = sizeof(noname::log::LogBlock);
  for (uint32_t i = 0; i < 4; ++i) {
    if (i % 2 == 0) {
      payload_size = sizeof(uint64_t);
      uint64_t val = -1;
      auto ret = pread(fd, &val, payload_size, filled + log_size);

      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i+1);

      filled += (log_size + payload_size);

    } else {
      payload_size = sizeof(uint32_t);
      uint32_t val = -1;
      auto ret = pread(fd, &val, payload_size, filled + log_size);

      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i+1);

      filled += (log_size + payload_size);
    }
  }

  close(fd);
  system("rm -rf ./logtest");
}

void mt_Out_of_order(noname::clog::Log* l,
                     std::vector<noname::log::LogBlock *> *blocks,
                     uint32_t id, int work_amount, uint32_t interval) {
  // Takes the log and blocks (offsets from AllocatLogBlock) and populates every ith element where i
  // is the amount of threads. Ex if the id is 1 and there are 5 threads this thread will populate
  // 1, 6, 11, 16 .....
  uint32_t val = id + 1;
  for (int i = 0; i < work_amount; i++) {
    uint32_t payload_size = sizeof(uint32_t);
    l->PopulateLogBlock((*blocks)[val-1], reinterpret_cast<char*>(&val), payload_size);
    val += interval;
  }
}

TEST(LogTest, MTPopulateOutOfOrder) {
  // Allocate threads and populate out of order
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");

  noname::clog::Log log(dirname, 16384, 1024, false);

  // Change these variables for driver
  auto thread_amount = 5;
  auto log_amount = 20;
  std::vector<noname::log::LogBlock *> *blocks =
    new std::vector<noname::log::LogBlock *>(log_amount);

  auto alloc_size = 0;
  uint32_t payload_size = sizeof(uint32_t);

  for (uint32_t i = 0; i < log_amount; ++i) {
    auto *block = log.AllocateLogBlock(payload_size);
    alloc_size += payload_size + sizeof(noname::log::LogBlock);
    ASSERT_EQ(alloc_size, log.reserve_offset);
    (*blocks)[i] = block;
  }

  std::vector<std::thread *> threads(log_amount);
  for (int i = 0; i < thread_amount; i++) {
    threads[i] = new std::thread(mt_Out_of_order,
                                 &log, blocks, i,
                                 log_amount / thread_amount,
                                 thread_amount);
  }

  for (int i = 0; i < thread_amount; i++) {
    threads[i]->join();
  }


  log.Flush();

  // File name might be different
  auto segment_id = log.segment_id_counter;
  std::string filename("./logtest/wal-000000");
  std::stringstream sstream;
  sstream << std::hex << segment_id - 1;
  std::string result = sstream.str();
  filename+= result;
  auto fd = open(filename.c_str(), O_RDWR);
  ASSERT_NE(fd, -1);

  auto fsize = lseek(fd, 0, SEEK_END);
  ASSERT_NE(fsize, 0);

  for (int i = 0; i < log_amount; i++) {
      uint32_t val =- 1;
      auto log_size = sizeof(noname::log::LogBlock);
      auto offset = i * (log_size + payload_size) + log_size;
      auto ret = pread(fd, &val, payload_size, offset);
      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i + 1);
  }

  close(fd);

  system("rm -rf ./logtest");
}


std::atomic<uint64_t> created_segment_offset = 0;

void mt_log(noname::clog::Log *l,
            uint32_t id,
            int work_amount,
            uint32_t interval,
            std::vector<uint32_t>* a) {
  // Takes the log and blocks (offsets from AllocatLogBlock) and populates every ith element where i
  // is the amount of threads. Ex if the id is 1 and there are 5 threads this thread will populate
  // 1, 6, 11, 16 .....
  uint32_t val = id;
  uint32_t payload_size = sizeof(uint32_t);

  for (int i = 0; i < work_amount; i++) {
    auto *block = l->AllocateLogBlock(payload_size);
    auto idx = created_segment_offset.fetch_add(1);
    l->PopulateLogBlock(block, reinterpret_cast<char*>(&val), payload_size);
    (*a)[idx] = val;
    val += interval;
  }
}

TEST(LogTest, MTCreatedNewSegments) {
  // Create a log with a small log and segment size
  // Testing if multiple segments are peresisted properly multithreaded
  // Checks if segments are created correctly as well
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");
  auto payload_size = sizeof(uint32_t);
  auto log_amount = 10;
  auto thread_amount = 5;

  noname::clog::Log log(dirname, 24, 12, false);
  // Keep inserting should flush twice to a segment
  // Ex. If there are 10 insertions than 5 segments should be created
  auto alloc_size = 0;
  auto segment_id = log.segment_id_counter;
  // Threads will allocate in random order need to keep track of results
  std::vector<uint32_t>* answer = new std::vector<uint32_t>(log_amount);

  std::vector<std::thread *> threads(log_amount);
  for (int i = 0; i < thread_amount; i++) {
    threads[i] =  new std::thread(mt_log, &log, i,
                                  log_amount / thread_amount,
                                  thread_amount, answer);
  }

  for (int i = 0; i < thread_amount; i++) {
    threads[i]->join();
  }

  ASSERT_EQ(log.segments.size(), (log_amount / 2));
  log.Flush();

  // Create the file string and check if there are two elements per segment
  for (int i = 0; i < 5; i++) {
    // Open the file for the 3rd test
    std::string filename("./logtest/wal-000000");
    std::stringstream sstream;
    sstream << std::hex << segment_id -1 + i;
    std::string result = sstream.str();
    filename += result;

    auto fd = open(filename.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);

    // Ensure the size and file are able to be opened and check for data
    auto fsize = lseek(fd, 0, SEEK_END);
    ASSERT_NE(fsize, 0);

    for (int j = 0; j < 2; j++) {
      uint32_t val =- 1;
      auto log_size = sizeof(noname::log::LogBlock);
      auto offset = j * (log_size + payload_size) + log_size;
      auto ret = pread(fd, &val, payload_size, offset);
      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, (*answer)[i*2 +j]);
    }
    close(fd);
  }
  system("rm -rf ./logtest");
}


uint64_t atomic_log_amount = 20;
std::atomic<uint64_t> allocate_offset = 0;
std::atomic<uint64_t> populate_offset = 0;

void mt_atomic_allocate(noname::clog::Log* l, std::vector<noname::log::LogBlock *> *blocks) {
  // Function for thread
  uint64_t i = 0;
  auto alloc_size = 0;
  while (true) {
    i = allocate_offset.fetch_add(1);
    if (i > atomic_log_amount) {
      break;
    }
    uint32_t payload_size = sizeof(uint32_t);
    auto *block = l->AllocateLogBlock(payload_size);
    (*blocks)[i] = block;
  }
}

void mt_atomic_populate(noname::clog::Log* l, std::vector<noname::log::LogBlock *> *blocks) {
  // Populate using an atomic variable
  uint64_t i = 0;
  while (true) {
    i = populate_offset.fetch_add(1);
    if (i > atomic_log_amount) {
      break;
    }
    i += 1;
    uint32_t payload_size = sizeof(uint32_t);
    l->PopulateLogBlock((*blocks)[i - 1], reinterpret_cast<char*>(&i), payload_size);
  }
}


TEST(LogTest, MTPopulateDriver) {
  // Simple driver like program using atomic variables
  system("mkdir -p ./logtest && rm -rf ./logtest/*");
  std::string dirname("./logtest");

  noname::clog::Log log(dirname, 16384, 1024, false);

  // Change these variables for driver
  auto thread_amount = 5;
  std::vector<noname::log::LogBlock *>* blocks =
    new std::vector<noname::log::LogBlock *>(atomic_log_amount);
  std::vector<std::thread *> allocate_threads(thread_amount);

  for (uint32_t i = 0; i < thread_amount; ++i) {
    allocate_threads[i] =  new std::thread(mt_atomic_allocate, &log, blocks);
  }

  for (int i = 0; i < thread_amount; i++) {
    allocate_threads[i]->join();
  }

  std::vector<std::thread *> populate_threads(thread_amount);
  for (int i = 0; i < thread_amount; i++) {
    populate_threads[i] =  new std::thread(mt_atomic_populate, &log, blocks);
  }

  for (int i = 0; i < thread_amount; i++) {
    populate_threads[i]->join();
  }

  log.Flush();
  // File name might be different
  auto segment_id = log.segment_id_counter;
  std::string filename("./logtest/wal-000000");
  std::stringstream sstream;
  sstream << std::hex << segment_id - 1;
  std::string result = sstream.str();
  filename+= result;
  auto fd = open(filename.c_str(), O_RDWR);
  ASSERT_NE(fd, -1);

  auto fsize = lseek(fd, 0, SEEK_END);
  ASSERT_NE(fsize, 0);

  uint32_t payload_size = sizeof(uint32_t);
  for (int i = 0; i < atomic_log_amount; i++) {
      uint32_t val =- 1;
      auto log_size = sizeof(noname::log::LogBlock);
      auto offset = i * (log_size + payload_size) + log_size;
      auto ret = pread(fd, &val, payload_size, offset);
      ASSERT_EQ(ret, payload_size);
      ASSERT_EQ(val, i + 1);
  }

  close(fd);

  system("rm -rf ./logtest");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


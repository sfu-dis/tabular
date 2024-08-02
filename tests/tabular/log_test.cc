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

#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <limits>
#include <filesystem>

#include "tabular/log.h"
#include "tabular/config.h"

TEST(Log, Append) {
  constexpr uint64_t SIZE = 8;
  constexpr uint64_t LARGE_SIZE = 128;
  constexpr uint64_t INITIAL_SIZE = 64;

  noname::tabular::config_t config(false);
  noname::tabular::Log log(config, INITIAL_SIZE);

  uint8_t data[SIZE] = {0, 1, 2, 3, 4, 5, 6, 7};

  uint8_t out_data[SIZE];
  auto lsn = log.Append(data, SIZE);
  log.Get(lsn, out_data, SIZE);

  for (uint64_t i = 0; i < SIZE; i++) {
    ASSERT_EQ(out_data[i], data[i]);
  }

  uint8_t large_data[LARGE_SIZE] = {42};
  uint8_t large_out_data[LARGE_SIZE] = {0};

  auto large_lsn = log.Append(large_data, LARGE_SIZE);
  log.Get(large_lsn, large_out_data, LARGE_SIZE);

  for (uint64_t i = 0; i < LARGE_SIZE; i++) {
    ASSERT_EQ(large_out_data[i], large_data[i]);
  }
}

TEST(Log, Persistece) {
  constexpr uint64_t SIZE = 8;
  constexpr uint64_t INITIAL_SIZE = 64;
  const std::filesystem::path log_dir = "/tmp/noname-test";
  // create log directory
  std::error_code ec;
  std::filesystem::create_directories(log_dir, ec);
  assert(!ec);

  noname::tabular::config_t config(false, false,
                                   std::numeric_limits<uint32_t>::max(), log_dir.c_str());
  uint8_t data[SIZE] = {0, 1, 2, 3, 4, 5, 6, 7};

  {
    noname::tabular::Log log(config, INITIAL_SIZE);

    auto lsn = log.Append(data, SIZE);
    log.Flush();
  }

  auto log_file = log_dir / "noname-001.log";
  auto fd = open(log_file.c_str(), O_RDONLY);
  struct stat st;
  fstat(fd, &st);
  auto log_data = reinterpret_cast<uint8_t*>(mmap(nullptr, st.st_size,
                                                  PROT_READ, MAP_SHARED, fd, 0));
  for (auto i = 0; i < SIZE; i++) {
    ASSERT_EQ(log_data[i], data[i]);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

#include "asi/sync_storage.h"

TEST(SyncStorageTest, Create) {
  std::string filename("sync-storage-test");
  noname::asi::SyncStorage sync_storage(filename, false);
  ASSERT_FALSE(sync_storage.direct_io);
  ASSERT_GE(sync_storage.fd, 0);
}

TEST(SyncStorageTest, WriteAndRead) {
  std::string filename("sync-storage-test");
  noname::asi::SyncStorage sync_storage(filename, false);

  size_t size_in_bytes = 4096;
  char *support_array = reinterpret_cast<char *>(malloc(size_in_bytes));
  ASSERT_NE(support_array, nullptr);

  // set each byte to be '#' in support array
  memset(support_array, '#', size_in_bytes);

  // write support array to file, should be successful
  bool success = sync_storage.SyncWrite(
      reinterpret_cast<const char *>(support_array), size_in_bytes, 0);
  ASSERT_TRUE(success);

  // clear support array
  memset(support_array, 0, size_in_bytes);

  // read data from file to support array, should be successful
  success = sync_storage.SyncRead(support_array, size_in_bytes, 0);
  ASSERT_TRUE(success);

  // each byte read from file should be '#'
  for (uint32_t i = 0; i < size_in_bytes; i++) {
    ASSERT_EQ(support_array[i], '#');
  }

  free(support_array);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


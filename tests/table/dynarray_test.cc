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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "table/dynarray.h"

uint64_t capacity = size_t(1) << 32;

TEST(DynarrayTest, Create) {
  noname::table::DynamicArray<char> dynamic_array(false, capacity, 64);
  ASSERT_EQ(dynamic_array.capacity, capacity);
  ASSERT_NE(dynamic_array.data, nullptr);
}

TEST(DynarrayTest, Append) {
  noname::table::DynamicArray<char> dynamic_array(false, capacity, 64);
  char *entries = dynamic_array.data;
  // current size of dynamic_array should be a page size
  size_t page_size_in_bytes = dynamic_array.PageSize();

  // create a support_array to store data
  char *support_array = reinterpret_cast<char *>(malloc(2 * page_size_in_bytes));

  // set each byte to be '#' in support array
  memset(support_array, '#', page_size_in_bytes);

  // write data from support array to dynamic_array
  memcpy(entries, support_array, page_size_in_bytes);

  // each byte in dynamic_array should be '#'
  for (uint32_t i = 0; i < page_size_in_bytes; i++) {
    ASSERT_EQ(entries[i], '#');
  }

  // set each byte to be '@' in the rest part of support array
  memset(support_array + page_size_in_bytes, '@', page_size_in_bytes);

  // extend dynamic_array, now its size should be double page size
  dynamic_array.EnsureSize(2 * page_size_in_bytes);

  // write data from support array to the extended part of dynamic_array
  memcpy(entries + page_size_in_bytes, support_array + page_size_in_bytes, page_size_in_bytes);

  // now each byte in extended part of dynamic_array should be '@'
  for (uint32_t i = page_size_in_bytes; i < 2 * page_size_in_bytes; i++) {
    ASSERT_EQ(entries[i], '@');
  }

  free(support_array);
}

void AppendWithSegFault() {
  noname::table::DynamicArray<char>  dynamic_array(false, capacity, 64);
  char *entries = dynamic_array.data;
  // current size of dynamic_array should be a page size
  size_t page_size_in_bytes = dynamic_array.PageSize();

  // tend to write to an unallocated memory space, should be segfault
  memset(entries + page_size_in_bytes, '#', page_size_in_bytes);
}

TEST(DynarrayTest, AppendWithSegFault) {
  EXPECT_EXIT(AppendWithSegFault(), testing::KilledBySignal(SIGSEGV), "");
}

TEST(DynarrayTest, OperatorOverloadingInt32) {
  uint64_t kInts = 65536;
  uint64_t kSize = kInts * sizeof(int64_t);

  noname::table::DynamicArray<int32_t> dyn(false, capacity, kSize);
  char *entries = dyn.data;
  for (uint32_t i = 0; i < kInts; ++i) {
    dyn[i] = ~i;
  }

  // Verify
  for (uint32_t i = 0; i < kInts; ++i) {
    EXPECT_EQ(dyn[i], ~i);
    EXPECT_EQ((reinterpret_cast<int32_t *>(entries))[i], ~i);
  }
}

TEST(DynarrayTest, OperatorOverloadingInt64HugePages) {
  uint64_t kInts = 65536;
  uint64_t kSize = kInts * sizeof(int64_t);

  noname::table::DynamicArray<int64_t> dyn(true, capacity, kSize);
  char *entries = dyn.data;
  for (uint64_t i = 0; i < kInts; ++i) {
    dyn[i] = ~i;
  }

  // Verify
  for (uint64_t i = 0; i < kInts; ++i) {
    EXPECT_EQ(dyn[i], ~i);
    EXPECT_EQ((reinterpret_cast<int64_t *>(entries))[i], ~i);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


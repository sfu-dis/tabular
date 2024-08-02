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

#include "tabular/btree.h"

TEST(BTreeTest, SingleThreadInsertRead) {
  noname::table::config_t config(false,
                                 false,  // no hugepages
                                 100000);  // capacity
  noname::tabular::BTree<uint64_t, uint64_t> tree(config);

  constexpr int NUM_KEYS = 1024;
  bool ok;
  for (uint64_t k = 0; k < NUM_KEYS; k++) {
    ok = tree.insert(k, k);
    ASSERT_TRUE(ok);
  }

  for (uint64_t k = 0; k < NUM_KEYS; k++) {
    uint64_t result;
    ok = tree.lookup(k, result);
    ASSERT_TRUE(ok);
    ASSERT_EQ(result, k);
  }
}

TEST(BTreeTest, SingleThreadedScan) {
  noname::table::config_t config(false,
                                 false,    // no hugepages
                                 1000000,  // capacity
                                 500000);  // initial size

  constexpr int NUM_KEYS = 1024;
  noname::tabular::BTree<uint64_t, uint64_t> tree(config);
  bool ok;
  for (uint64_t k = 0; k < NUM_KEYS; k++) {
    ok = tree.insert(k, k);
    ASSERT_TRUE(ok);
  }

  auto result = new uint64_t[NUM_KEYS];
  auto count = tree.scan(0, NUM_KEYS, result);
  CHECK_EQ(count, NUM_KEYS);
  for (auto i = 0; i < NUM_KEYS; i++) {
    CHECK_EQ(result[i], i);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

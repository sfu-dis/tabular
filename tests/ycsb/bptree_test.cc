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

#include "third-party/bptree/tlx-leafds/container/btree_map.hpp"

TEST(BPTreeTest, SingleThreadInsertRead) {
  tlx::btree_map<uint64_t, uint64_t, std::less<uint64_t>,
                 tlx::btree_default_traits<uint64_t, uint64_t>,
                 std::allocator<uint64_t>, /*concurrent=*/true>
      tree;
  constexpr int NUM_KEYS = 1024;
  bool ok;
  for (uint64_t k = 1; k <= NUM_KEYS; k++) {
    auto [it, ok] = tree.insert({k, k});
    ASSERT_TRUE(ok);
  }

  for (uint64_t k = 1; k <= NUM_KEYS; k++) {
    auto val = tree.value(k);
    ASSERT_EQ(val, k);
  }

  // NOTE(ziyi) following code causes Illegal instruction (core dumped)
  for (uint64_t k = 1; k <= NUM_KEYS; k++) {
    tree.update({k, k + 1});
  }

  for (uint64_t k = 1; k <= NUM_KEYS; k++) {
    auto val = tree.value(k);
    ASSERT_EQ(val, k + 1);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

#include "table/ia_table.h"

noname::table::config_t config(false, false /* no hugepages */, 100000/* capacity */);

TEST(IndirectionArrayTableTest, Create) {
  noname::table::IndirectionArrayTable table(config);
  ASSERT_EQ(table.next_oid, 0);
  ASSERT_EQ(table.backing_array.capacity, config.ia_table_capacity);
  ASSERT_EQ(table.backing_array.use_1gb_hugepages, config.ia_table_dynarray_use_1gb_hugepages);
}

TEST(IndirectionArrayTableTest, PutGet) {
  noname::table::IndirectionArrayTable table(config);

  // Insert two objects
  auto obj1 = std::make_unique<noname::table::Object>();
  obj1.get()->id = 0xfeed;
  noname::table::OID oid1 = table.Put(obj1.get());
  ASSERT_EQ(oid1, 0);

  auto obj2 = std::make_unique<noname::table::Object>();
  obj2.get()->id = 0xbeef;
  noname::table::OID oid2 = table.Put(obj2.get());
  ASSERT_EQ(oid2, 1);

  // Read them back
  auto *object = table.Get(oid1);
  EXPECT_EQ(object->id, 0xfeed);

  object = table.Get(oid2);
  EXPECT_EQ(object->id, 0xbeef);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


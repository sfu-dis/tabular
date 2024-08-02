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

#include "stock_level.h"

namespace noname {
namespace benchmark {
namespace tpcc {

bool StockLevel::run() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint threshold = RandomNumber(r, 10, 20);
  const uint district_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // get pointers to schema information of each table
  thread_local auto *district_schema_info = db.TransactionGetSchemaInfo("district");
  thread_local auto *order_line_schema_info = db.TransactionGetSchemaInfo("order_line");
  thread_local auto *stock_schema_info = db.TransactionGetSchemaInfo("stock");
  // get d_next_o_id from district table
  auto *district_index = district_schema_info->index_infos[0].index;
  auto *district_table = district_schema_info->table_info.table;
  district_key dk {warehouse_id, district_id};
  table::Tuple *district_tuple = db.TransactionRead(district_index, (const char *)&dk,
                                                    district_index->key_size, district_table);
  uint32_t column_offset = district_schema_info->table_info.column_offsets[4];
  int d_next_o_id = *reinterpret_cast<int *>(district_tuple->GetConstValue(column_offset));
  // scan order_line table for a set of s_i_id
  auto *order_line_index = order_line_schema_info->index_infos[0].index;
  auto *order_line_table = order_line_schema_info->table_info.table;
  std::unordered_set<int> sids;
  for (int ol_o_id = d_next_o_id >= 20 ? d_next_o_id - 20 : 0; ol_o_id < d_next_o_id; ol_o_id++) {
    order_line_key olk_begin {warehouse_id, district_id, ol_o_id, 1};
    order_line_key olk_end {warehouse_id, district_id, ol_o_id, 15};

    Engine::ScanIterator si_order_line(order_line_table);
    db.TransactionScan(order_line_index, (const char *)&olk_begin, true, (const char *)&olk_end, true,
                        order_line_index->key_size, 15, false, si_order_line);
    table::Object *order_line_object;
    while ((order_line_object = si_order_line.Next()) != nullptr) {
      table::Tuple *order_line_tuple = reinterpret_cast<table::Tuple *>(order_line_object->data);
      column_offset = order_line_schema_info->table_info.column_offsets[4];
      int item_id = *reinterpret_cast<int *>(order_line_tuple->GetConstValue(column_offset));
      sids.insert(item_id);
    }
  }
  // query stock table and count # of itmes lower than threshold
  auto *stock_index = stock_schema_info->index_infos[0].index;
  auto *stock_table = stock_schema_info->table_info.table;
  int low_stock_count = 0;
  for (const auto &s_i_id : sids) {
    stock_key sk {warehouse_id, s_i_id};
    table::Tuple *stock_tuple = db.TransactionRead(stock_index, (const char *)&sk,
                                                    stock_index->key_size, stock_table);
    if (stock_tuple->GetOID() == table::kInvalidOID) {
      return false;
    }
    column_offset = stock_schema_info->table_info.column_offsets[2];
    int s_quantity = *reinterpret_cast<int *>(stock_tuple->GetConstValue(column_offset));
    if (s_quantity < threshold) {
      low_stock_count++;
    }
  }

  return true;
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname

/*
 * Copyright 2018-2024 Stichting DuckDB Foundation

 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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

#include "new_order.h"

namespace noname {
namespace benchmark {
namespace tpcc {

bool NewOrder::run() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint district_id = RandomNumber(r, 1, 10);
  const uint customer_id = GetCustomerId(r);
  const uint num_items = RandomNumber(r, 5, 15);
  uint item_ids[15], supplier_warehouse_ids[15], order_quantities[15];
  bool all_local = true;
  for (uint i = 0; i < num_items; i++) {
    item_ids[i] = GetItemId(r);
    if (g_disable_xpartition_txn || NumWarehouses() == 1 ||
        RandomNumber(r, 1, 100) > g_new_order_remote_item_pct) {
      supplier_warehouse_ids[i] = warehouse_id;
    } else {
      do {
        supplier_warehouse_ids[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplier_warehouse_ids[i] == warehouse_id);
      all_local = false;
    }
    order_quantities[i] = RandomNumber(r, 1, 10);
  }

  thread_local auto *customer_schema_info = db.TransactionGetSchemaInfo("customer");
  thread_local auto *warehouse_schema_info = db.TransactionGetSchemaInfo("warehouse");
  thread_local auto *district_schema_info = db.TransactionGetSchemaInfo("district");
  thread_local auto *oorder_schema_info = db.TransactionGetSchemaInfo("oorder");
  thread_local auto *new_order_schema_info = db.TransactionGetSchemaInfo("new_order");
  thread_local auto *order_line_schema_info = db.TransactionGetSchemaInfo("order_line");
  thread_local auto *stock_schema_info = db.TransactionGetSchemaInfo("stock");
  thread_local auto *item_schema_info = db.TransactionGetSchemaInfo("item");

  // Get customer
  auto *customer_index = customer_schema_info->index_infos[0].index;
  auto *customer_table = customer_schema_info->table_info.table;
  customer_key ck{warehouse_id, district_id, customer_id};
  auto *customer_tuple = db.TransactionRead(customer_index, (const char *)&ck,
                                            customer_index->key_size, customer_table);
  if (!customer_tuple) {
    return false;
  }

  // Get warehouse
  auto *warehouse_index = warehouse_schema_info->index_infos[0].index;
  auto *warehouse_table = warehouse_schema_info->table_info.table;
  warehouse_key wk{warehouse_id};
  auto *warehouse_tuple = db.TransactionRead(warehouse_index, (const char *)&wk,
                                             warehouse_index->key_size, warehouse_table);
  if (!warehouse_tuple) {
    return false;
  }

  // Get district
  auto *district_index = district_schema_info->index_infos[0].index;
  auto *district_table = district_schema_info->table_info.table;
  district_key dk{warehouse_id, district_id};
  auto *district_tuple = db.TransactionRead(district_index, (const char *)&dk,
                                            district_index->key_size, district_table);
  if (!district_tuple) {
    return false;
  }
  uint32_t off = district_schema_info->table_info.column_offsets[4];
  
  int d_next_o_id = g_new_order_fast_id_gen ?
     FastNewOrderIdGen(warehouse_id, district_id) :
     *reinterpret_cast<int *>(district_tuple->GetConstValue(off));

  // Update district
  uint32_t new_district_obj_size =
    sizeof(table::Object) + sizeof(table::Tuple) + district_tuple->GetSize();
  auto *new_district_obj = table::Object::Make(new_district_obj_size);
  table::Tuple *new_district_tuple = reinterpret_cast<table::Tuple *>(new_district_obj->data);
  memcpy(new_district_tuple, district_tuple, sizeof(table::Tuple) + district_tuple->GetSize());
  CHECK(!memcmp(district_tuple->tuple_data, new_district_tuple->tuple_data, district_tuple->GetSize()));
  *reinterpret_cast<int *>(new_district_tuple->GetConstValue(off)) = d_next_o_id + 1;
  auto success = db.TransactionUpdate(district_table, new_district_tuple->GetOID(), new_district_obj);
  if (!success) {
    return false;
  }

  // Insert oorder
  auto *oorder_index = oorder_schema_info->index_infos[0].index;
  auto *oorder_table = oorder_schema_info->table_info.table;
  oorder_key ok{warehouse_id, district_id, d_next_o_id};
  oorder_value ov{warehouse_id, district_id, d_next_o_id, customer_id, 0, num_items,
                  all_local, duckdb::Timestamp::GetCurrentTimestamp().value};
  success = db.TransactionInsert(oorder_table, (const char *)&ov, sizeof(ov),
                                 oorder_index, (const char *)&ok, oorder_index->key_size);
  if (!success) {
    return false;
  }

  // Insert new_order
  auto *new_order_index = new_order_schema_info->index_infos[0].index;
  auto *new_order_table = new_order_schema_info->table_info.table;
  new_order_key nok{warehouse_id, district_id, d_next_o_id};
  success = db.TransactionInsert(new_order_table, (const char *)&nok, sizeof(nok),
                                 new_order_index, (const char *)&nok, new_order_index->key_size);
  if (!success) {
    return false;
  }

  auto *item_index = item_schema_info->index_infos[0].index;
  auto *item_table = item_schema_info->table_info.table;
  auto *stock_index = stock_schema_info->index_infos[0].index;
  auto *stock_table = stock_schema_info->table_info.table;
  auto *order_line_index = order_line_schema_info->index_infos[0].index;
  auto *order_line_table = order_line_schema_info->table_info.table;
  for (uint ol_number = 1; ol_number <= num_items; ol_number++) {
    const uint ol_supply_w_id = supplier_warehouse_ids[ol_number - 1];
    const uint ol_i_id = item_ids[ol_number - 1];
    const uint ol_quantity = order_quantities[ol_number - 1];

    // Get item
    item_key ik{ol_i_id};
    auto *item_tuple = db.TransactionRead(item_index, (const char *)&ik,
                                          item_index->key_size, item_table);
    if (!item_tuple) {
      return false;
    }
    off = item_schema_info->table_info.column_offsets[2];

    float i_price = *reinterpret_cast<float *>(item_tuple->GetConstValue(off));
    float ol_amount = ol_quantity * i_price;

    // Get stock
    stock_key sk{ol_supply_w_id, ol_i_id};
    auto *stock_tuple = db.TransactionRead(stock_index, (const char *)&sk,
                                           stock_index->key_size, stock_table);
    if (!stock_tuple) {
      return false;
    }
    off = stock_schema_info->table_info.column_offsets[2];
    int s_quantity = *reinterpret_cast<int *>(stock_tuple->GetConstValue(off));

    // Insert order_line
    char dist_info[24];
    order_line_key olk{warehouse_id, district_id, d_next_o_id, ol_number};
    order_line_value olv{warehouse_id, district_id, d_next_o_id, ol_number, ol_i_id,
                         0, ol_supply_w_id, ol_quantity, ol_amount, 44, 24, dist_info};
    success = db.TransactionInsert(order_line_table, (const char *)&olv, sizeof(olv),
                                   order_line_index, (const char *)&olk, order_line_index->key_size);
    if (!success) {
      return false;
    }

    // Update stock
    int s_remote_cnt_increment = ol_supply_w_id == warehouse_id ? 0 : 1;

    uint32_t new_stock_obj_size =
      sizeof(table::Object) + sizeof(table::Tuple) + stock_tuple->GetSize();
    auto *new_stock_obj = table::Object::Make(new_stock_obj_size);
    table::Tuple *new_stock_tuple = reinterpret_cast<table::Tuple *>(new_stock_obj->data);
    memcpy(new_stock_tuple, stock_tuple, sizeof(table::Tuple) + stock_tuple->GetSize());
    off = stock_schema_info->table_info.column_offsets[2];
    *reinterpret_cast<int *>(new_stock_tuple->GetConstValue(off)) = s_quantity;
    off = stock_schema_info->table_info.column_offsets[3];
    *reinterpret_cast<float *>(new_stock_tuple->GetConstValue(off)) += ol_quantity;
    off = stock_schema_info->table_info.column_offsets[4];
    *reinterpret_cast<int *>(new_stock_tuple->GetConstValue(off)) += 1;
    off = stock_schema_info->table_info.column_offsets[5];
    *reinterpret_cast<int *>(new_stock_tuple->GetConstValue(off)) += s_remote_cnt_increment;
    success = db.TransactionUpdate(stock_table, new_stock_tuple->GetOID(), new_stock_obj);
    if (!success) {
      return false;
    }
  }

  return true;
}

const std::string NewOrder::GetDistInfo(int d_id, sql::QueryResult &qr) {
  switch (d_id) {
  case 1:
    return std::string(qr.results[0].at("s_dist_01"), 24);
  case 2:
    return std::string(qr.results[0].at("s_dist_02"), 24);
  case 3:
    return std::string(qr.results[0].at("s_dist_03"), 24);
  case 4:
    return std::string(qr.results[0].at("s_dist_04"), 24);
  case 5:
    return std::string(qr.results[0].at("s_dist_05"), 24);
  case 6:
    return std::string(qr.results[0].at("s_dist_06"), 24);
  case 7:
    return std::string(qr.results[0].at("s_dist_07"), 24);
  case 8:
    return std::string(qr.results[0].at("s_dist_08"), 24);
  case 9:
    return std::string(qr.results[0].at("s_dist_09"), 24);
  case 10:
    return std::string(qr.results[0].at("s_dist_10"), 24);
  default:
    return std::string();
  }
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


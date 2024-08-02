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

#include "order_status.h"


namespace noname {
namespace benchmark {
namespace tpcc {

bool OrderStatus::run() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint y = RandomNumber(r, 1, 100);
  const uint district_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  uint c_id;
  table::Tuple *customer_tuple;

  // get pointers to schema information of each table
  thread_local auto *customer_schema_info = db.TransactionGetSchemaInfo("customer");
  thread_local auto *oorder_schema_info = db.TransactionGetSchemaInfo("oorder");
  thread_local auto *order_line_schema_info = db.TransactionGetSchemaInfo("order_line");

  // get c_id, c_first, c_middle, c_last, c_balance
  if (y > 60) {
    // use warehouse_id, district_id, c_id as key
    c_id = RandomNumber(r, 1, 3000);
    auto *customer_index = customer_schema_info->index_infos[0].index;
    auto *customer_table = customer_schema_info->table_info.table;
    customer_key ck{warehouse_id, district_id, c_id};
    customer_tuple = db.TransactionRead(customer_index, (const char *)&ck, 
                                        customer_index->key_size, customer_table);
    if (!customer_tuple) {
      return false;
    }
    uint32_t column_offset = customer_schema_info->table_info.column_offsets[5]; //get offset to varchar
    uint32_t size = customer_tuple->GetVarValueSize(column_offset);
    char c_last[size];
    memcpy(&c_last[0], (const char *) customer_tuple->GetVarValuePayload(column_offset), size);
  } else {
    // use c_last 
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 1, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);
    
    static const std::string zeroes(16, 0);
    static const std::string ones(16, (char)255);

    customer_lastname_key c_k_0{warehouse_id, district_id, (const char *)lastname_buf, zeroes.c_str()};
    customer_lastname_key c_k_1{warehouse_id, district_id, (const char *)lastname_buf, ones.c_str()};

    //scan customer table for all matches
    auto *customer_index = customer_schema_info->index_infos[1].index; // the second indexInfo
    auto *customer_table = customer_schema_info->table_info.table;

    Engine::ScanIterator si_customer(customer_table);
    db.TransactionScan(customer_index, (const char *)&c_k_0, true, (const char *)&c_k_1, true,
                       customer_index->key_size, -1, false, si_customer);
    CHECK(si_customer.total > 0);
    table::Object *customer_object;
    auto middle = (si_customer.total % 2 == 0) ? (si_customer.total / 2) - 1 : si_customer.total / 2;
    customer_object = si_customer.Get(middle);
    if (!customer_object) {
      return false;
    }
    customer_tuple = reinterpret_cast<table::Tuple *>(customer_object->data);
    table::Tuple *customer_tuple = reinterpret_cast<table::Tuple *>(customer_object->data);
    // get c_id
    uint32_t column_offset = customer_schema_info->table_info.column_offsets[2];
    c_id = *reinterpret_cast<int *>(customer_tuple->GetConstValue(column_offset));
  }

  {
    //c_first
    uint32_t column_offset = customer_schema_info->table_info.column_offsets[6]; //get offset to varchar
    uint32_t size = customer_tuple->GetVarValueSize(column_offset);
    char c_first[size];
    memcpy(&c_first[0], (const char *) customer_tuple->GetVarValuePayload(column_offset), size);
    //c_middle
    char c_middle[2];
    column_offset = customer_schema_info->table_info.column_offsets[19];
    memcpy(&c_middle[0], (const char *) customer_tuple->GetVarValuePayload(column_offset), 2);
    //c_balance
    column_offset = customer_schema_info->table_info.column_offsets[8];
    float c_balance = *reinterpret_cast<float *>(customer_tuple->GetConstValue(column_offset));
  }

  // get largest O_ID
  // there may be no rows matching c_id 
  int o_id;
  int items = 0;
  char *oorder_results;

  auto *oorder_index = oorder_schema_info->index_infos[1].index;
  auto *oorder_table = oorder_schema_info->table_info.table;

  oorder_customer_id_key oorder_key_begin{warehouse_id, district_id, c_id, 0};
  oorder_customer_id_key oorder_key_end{warehouse_id, district_id, c_id,
                                        std::numeric_limits<int32_t>::max()};

  Engine::ScanIterator si_oorder(oorder_table);
  if (g_order_status_scan_hack) {
    db.TransactionScan(oorder_index, (const char *)&oorder_key_begin, true,
                       (const char *)&oorder_key_end, true,
                       oorder_index->key_size, 15, false, si_oorder);
    CHECK(si_oorder.total > 0);
  } else {
    db.TransactionScan(oorder_index, (const char *)&oorder_key_end, true,
                       (const char *)&oorder_key_begin, true,
                       oorder_index->key_size, 1, true, si_oorder);
    CHECK(si_oorder.total == 1);
  }
  table::Object *oorder_object;
  table::Tuple *oorder_tuple;
  if ((oorder_object = si_oorder.Next()) != nullptr) {
    oorder_tuple = reinterpret_cast<table::Tuple *>(oorder_object->data);
  } else {
    return false;
  }
  uint32_t column_offset = customer_schema_info->table_info.column_offsets[2];
  o_id = *reinterpret_cast<int *>(oorder_tuple->GetConstValue(column_offset));

  // get all order_line rows matching o_id, w_id, d_id
  auto *order_line_index = order_line_schema_info->index_infos[0].index;
  auto *order_line_table = order_line_schema_info->table_info.table;
  char *order_line_matches;

  order_line_key olk_begin{warehouse_id, district_id, o_id, 0};
  order_line_key olk_end{warehouse_id, district_id, o_id, std::numeric_limits<int32_t>::max()};

  Engine::ScanIterator si_order_line(order_line_table);
  db.TransactionScan(order_line_index, (const char *)&olk_begin, true, (const char *)&olk_end, true,
                     order_line_index->key_size, -1, false, si_order_line);
  CHECK(si_order_line.total >= 5 && si_order_line.total <= 15);

  table::Object *order_line_object;
  table::Tuple *order_line_tuple;
  if ((order_line_object = si_order_line.Next()) != nullptr) {
    order_line_tuple = reinterpret_cast<table::Tuple *>(order_line_object->data);
  }

  return true;
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname

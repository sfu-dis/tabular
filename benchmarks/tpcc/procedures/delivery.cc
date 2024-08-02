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

#include "delivery.h"


namespace noname {
namespace benchmark {
namespace tpcc {

bool Delivery::run() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint o_carrier_id = RandomNumber(r, 1, 10);
  const uint district_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  thread_local auto *new_order_schema_info = db.TransactionGetSchemaInfo("new_order");
  thread_local auto *oorder_schema_info = db.TransactionGetSchemaInfo("oorder");
  thread_local auto *order_line_schema_info = db.TransactionGetSchemaInfo("order_line");
  thread_local auto *customer_schema_info = db.TransactionGetSchemaInfo("customer");

  auto *new_order_index = new_order_schema_info->index_infos[0].index;
  auto *new_order_table = new_order_schema_info->table_info.table;

  auto *oorder_index = oorder_schema_info->index_infos[0].index;
  auto *oorder_table = oorder_schema_info->table_info.table;

  auto *order_line_index = order_line_schema_info->index_infos[0].index;
  auto *order_line_table = order_line_schema_info->table_info.table;

  auto *customer_index = customer_schema_info->index_infos[0].index;
  auto *customer_table = customer_schema_info->table_info.table;

  //for each district, get lowest order id
  for (uint district_id = 1; district_id <= NumDistrictsPerWarehouse(); district_id++) {
    new_order_key nok_begin{warehouse_id, district_id, last_no_o_ids[district_id - 1]};
    new_order_key nok_end{warehouse_id, district_id, std::numeric_limits<int32_t>::max()};

    Engine::ScanIterator si_new_order(new_order_table);
    db.TransactionScan(new_order_index, (const char *)&nok_begin, true, (const char *)&nok_end,
                       false, new_order_index->key_size, 1, false, si_new_order);
    CHECK(si_new_order.total <= 1);

    table::Object *new_order_object;
    table::Tuple *new_order_tuple;
    //get first valid tuple
    if ((new_order_object = si_new_order.Next()) != nullptr) {
      new_order_tuple = reinterpret_cast<table::Tuple *>(new_order_object->data);
    } else {
      continue;
    }

    uint32_t column_offset = new_order_schema_info->table_info.column_offsets[2];
    int o_id = *reinterpret_cast<int *>(new_order_tuple->GetConstValue(column_offset));
    last_no_o_ids[district_id - 1] = o_id + 1;

    //delete row in new_order
    bool success = db.TransactionDelete(new_order_table, new_order_object);
    if (!success) {
      return false;
    }

    //get c_id from oorder
    oorder_key ok{warehouse_id, district_id, o_id};
    table::Tuple *oorder_tuple = db.TransactionRead(oorder_index, (const char *)&ok,
                                                    oorder_index->key_size, oorder_table);
    if (!oorder_tuple) {
      return false;
    }
    column_offset = oorder_schema_info->table_info.column_offsets[3];
    int c_id = *reinterpret_cast<int *>(oorder_tuple->GetConstValue(column_offset));
    //update O_CARRIER_ID
    uint32_t new_oorder_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + oorder_tuple->GetSize();
    auto *new_oorder_obj = table::Object::Make(new_oorder_obj_size);
    table::Tuple *new_oorder_tuple = reinterpret_cast<table::Tuple *>(new_oorder_obj->data);
    memcpy(new_oorder_tuple, oorder_tuple, sizeof(table::Tuple) + oorder_tuple->GetSize());
    column_offset = oorder_schema_info->table_info.column_offsets[4];
    *reinterpret_cast<int *>(new_oorder_tuple->GetConstValue(column_offset)) = o_carrier_id;
    success =  db.TransactionUpdate(oorder_table, new_oorder_tuple->GetOID(), new_oorder_obj);
    if (!success) {
      return false;
    }

    //update each order_line and sum ol_amount
    float total_ol_amount = 0.0;
    char *order_line_results;

    order_line_key olk_begin{warehouse_id, district_id, o_id, 0};
    order_line_key olk_end{warehouse_id, district_id, o_id, std::numeric_limits<int32_t>::max()};

    Engine::ScanIterator si_order_line(order_line_table);
    db.TransactionScan(order_line_index, (const char *)&olk_begin, true, (const char *)&olk_end,
                       true, order_line_index->key_size, 15, false, si_order_line);
    
    table::Object *order_line_object;
    while ((order_line_object = si_order_line.Next()) != nullptr) {
      table::Tuple *order_line_tuple = reinterpret_cast<table::Tuple *>(order_line_object->data);
      column_offset = order_line_schema_info->table_info.column_offsets[6];
      total_ol_amount += *reinterpret_cast<float *>(order_line_tuple->GetConstValue(column_offset));

      //update timestamp
      uint32_t new_order_line_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + order_line_tuple->GetSize();
      auto *new_order_line_obj = table::Object::Make(new_order_line_obj_size);
      table::Tuple *new_order_line_tuple = reinterpret_cast<table::Tuple *>(new_order_line_obj->data);
      memcpy(new_order_line_tuple, order_line_tuple, sizeof(table::Tuple) + order_line_tuple->GetSize());
      column_offset = order_line_schema_info->table_info.column_offsets[5];
      *reinterpret_cast<int64_t *>(new_order_line_tuple->GetConstValue(column_offset)) = 
                                                                        duckdb::Timestamp::GetCurrentTimestamp().value;
      success = db.TransactionUpdate(order_line_table, new_order_line_tuple->GetOID(), new_order_line_obj);
      if (!success) {
        return false;
      }
    }

    //update customer c_balance and c_delivery_cnt
    customer_key ck{warehouse_id, district_id, c_id};
    table::Tuple *customer_tuple = db.TransactionRead(customer_index, (const char *)&ck,
                                                      customer_index->key_size, customer_table);
    if (!customer_tuple) {
      return false;
    }

    uint32_t new_customer_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + customer_tuple->GetSize();
    auto *new_customer_obj = table::Object::Make(new_customer_obj_size);
    table::Tuple *new_customer_tuple = reinterpret_cast<table::Tuple *>(new_customer_obj->data);
    memcpy(new_customer_tuple, customer_tuple, sizeof(table::Tuple) + customer_tuple->GetSize());
    column_offset = customer_schema_info->table_info.column_offsets[8];
    *reinterpret_cast<float *>(new_customer_tuple->GetConstValue(column_offset)) += total_ol_amount;
    column_offset = customer_schema_info->table_info.column_offsets[11];
    *reinterpret_cast<int *>(new_customer_tuple->GetConstValue(column_offset)) += 1;

    success = db.TransactionUpdate(customer_table, new_customer_tuple->GetOID(), new_customer_obj);
    if (!success) {
      return false;
    }
  }

  return true;
}




}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname

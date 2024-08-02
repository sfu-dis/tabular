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

#include "payment.h"

namespace noname {
namespace benchmark {
namespace tpcc {

bool Payment::run() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint district_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customer_district_id, customer_warehouse_id;
  if (g_disable_xpartition_txn || NumWarehouses() == 1 || RandomNumber(r, 1, 100) <= 85) {
    customer_district_id = district_id;
    customer_warehouse_id = warehouse_id;
  } else {
    customer_district_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customer_warehouse_id = RandomNumber(r, 1, NumWarehouses());
    } while (customer_warehouse_id == warehouse_id);
  }
  const float payment_amount = static_cast<float>(RandomNumber(r, 100, 500000) / 100.0);

  // get pointers to schema information of each table
  thread_local auto *warehouse_schema_info = db.TransactionGetSchemaInfo("warehouse");
  thread_local auto *district_schema_info = db.TransactionGetSchemaInfo("district");
  thread_local auto *customer_schema_info = db.TransactionGetSchemaInfo("customer");
  thread_local auto *history_schema_info = db.TransactionGetSchemaInfo("history");
  //get warehouse information, and update ytd balance
  auto *warehouse_index = warehouse_schema_info->index_infos[0].index;
  auto *warehouse_table = warehouse_schema_info->table_info.table;
  warehouse_key wk{warehouse_id};
  table::Tuple *warehouse_tuple = db.TransactionRead(warehouse_index, (const char *)&wk, 
                                                    warehouse_index->key_size, warehouse_table);
  if (!warehouse_tuple) {
    return false;
  }
  //get warehouse information
  //warehouse name
  uint32_t warehouse_offset = warehouse_schema_info->table_info.column_offsets[3];
  int warehouse_column_size = warehouse_tuple->GetVarValueSize(warehouse_offset);
  char w_name[warehouse_column_size];
  memcpy(w_name, (const char *) warehouse_tuple->GetVarValuePayload(warehouse_offset), warehouse_column_size);
  int w_name_length = warehouse_column_size;
  //warehouse street 1
  warehouse_offset = warehouse_schema_info->table_info.column_offsets[4];
  warehouse_column_size = warehouse_tuple->GetVarValueSize(warehouse_offset);
  char w_street_1[warehouse_column_size];
  memcpy(w_street_1, (const char *) warehouse_tuple->GetVarValuePayload(warehouse_offset), warehouse_column_size);
  //warehouse street 2
  warehouse_offset = warehouse_schema_info->table_info.column_offsets[5];
  warehouse_column_size = warehouse_tuple->GetVarValueSize(warehouse_offset);
  char w_street_2[warehouse_column_size];
  memcpy(w_street_2, (const char *) warehouse_tuple->GetVarValuePayload(warehouse_offset), warehouse_column_size);
  //warehouse city
  warehouse_offset = warehouse_schema_info->table_info.column_offsets[6];
  warehouse_column_size = warehouse_tuple->GetVarValueSize(warehouse_offset);
  char w_city[warehouse_column_size];
  memcpy(w_city, (const char *) warehouse_tuple->GetVarValuePayload(warehouse_offset), warehouse_column_size);
  //warehouse state
  warehouse_offset = warehouse_schema_info->table_info.column_offsets[7];
  warehouse_column_size = warehouse_tuple->GetVarValueSize(warehouse_offset);
  char w_state[warehouse_column_size];
  memcpy(w_state, (const char *) warehouse_tuple->GetVarValuePayload(warehouse_offset), warehouse_column_size);
  //warehouse zip
  warehouse_offset = warehouse_schema_info->table_info.column_offsets[8];
  warehouse_column_size = warehouse_tuple->GetVarValueSize(warehouse_offset);
  char w_zip[warehouse_column_size];
  memcpy(w_zip, (const char *) warehouse_tuple->GetVarValuePayload(warehouse_offset), warehouse_column_size);
  //update warehouse ytd
  uint32_t new_warehouse_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + warehouse_tuple->GetSize();
  auto *new_warehouse_obj = table::Object::Make(new_warehouse_obj_size);
  table::Tuple *new_warehouse_tuple = reinterpret_cast<table::Tuple *>(new_warehouse_obj->data);
  memcpy(new_warehouse_tuple, warehouse_tuple, sizeof(table::Tuple) + warehouse_tuple->GetSize());
  warehouse_offset = warehouse_schema_info->table_info.column_offsets[1];
  *reinterpret_cast<float *>(new_warehouse_tuple->GetConstValue(warehouse_offset)) += payment_amount;
  bool success = db.TransactionUpdate(warehouse_table, warehouse_tuple->GetOID(), new_warehouse_obj);
  if (!success) {
    return false;
  }

  //get district information, and update ytd balance
  auto *district_index = district_schema_info->index_infos[0].index;
  auto *district_table = district_schema_info->table_info.table;

  district_key dk{warehouse_id, district_id};
  table::Tuple *district_tuple = db.TransactionRead(district_index, (const char *)&dk, 
                                                    district_index->key_size, district_table);
  if (!district_tuple) {
    return false;
  }
  //district name
  uint32_t district_offset = district_schema_info->table_info.column_offsets[5];
  int district_column_size = district_tuple->GetVarValueSize(district_offset);
  char d_name[district_column_size];
  memcpy(d_name, (const char *)district_tuple->GetVarValuePayload(district_offset), district_column_size);
  int d_name_length = district_column_size;
  //district street 1
  district_offset = district_schema_info->table_info.column_offsets[6];
  district_column_size = district_tuple->GetVarValueSize(district_offset);
  char d_street_1[district_column_size];
  memcpy(d_street_1, (const char *)district_tuple->GetVarValuePayload(district_offset), district_column_size);
  //district street 2
  district_offset = district_schema_info->table_info.column_offsets[7];
  district_column_size = district_tuple->GetVarValueSize(district_offset);
  char d_street_2[district_column_size];
  memcpy(d_street_2, (const char *)district_tuple->GetVarValuePayload(district_offset), district_column_size);
  //district city
  district_offset = district_schema_info->table_info.column_offsets[8];
  district_column_size = district_tuple->GetVarValueSize(district_offset);
  char d_city[district_column_size];
  memcpy(d_city, (const char *)district_tuple->GetVarValuePayload(district_offset), district_column_size);
  //district state
  district_offset = district_schema_info->table_info.column_offsets[9];
  district_column_size = district_tuple->GetVarValueSize(district_offset);
  char d_state[district_column_size];
  memcpy(d_state, (const char *)district_tuple->GetVarValuePayload(district_offset), district_column_size);
  //district zip
  district_offset = district_schema_info->table_info.column_offsets[10];
  district_column_size = district_tuple->GetVarValueSize(district_offset);
  char d_zip[district_column_size];
  memcpy(d_zip, (const char *)district_tuple->GetVarValuePayload(district_offset), district_column_size);

  //update district ytd
  uint32_t new_district_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + district_tuple->GetSize();
  auto *new_district_obj = table::Object::Make(new_district_obj_size);
  table::Tuple *new_district_tuple = reinterpret_cast<table::Tuple *>(new_district_obj->data);
  memcpy(new_district_tuple, district_tuple, sizeof(table::Tuple) + district_tuple->GetSize());
  district_offset = district_schema_info->table_info.column_offsets[2];
  *reinterpret_cast<float *>(new_district_tuple->GetConstValue(district_offset)) += payment_amount;
  success = db.TransactionUpdate(district_table, district_tuple->GetOID(), new_district_obj);
  if (!success) {
    return false;
  }

  //get customer row
  auto *customer_table = customer_schema_info->table_info.table;
  table::Tuple *customer_tuple;
  if (RandomNumber(r, 1, 100) <= 60) {
    //use c_last
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 1, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeroes(16, 0);
    static const std::string ones(16, (char)255);

    customer_lastname_key c_k_0{customer_warehouse_id, customer_district_id, (const char *)lastname_buf, zeroes.c_str()};
    customer_lastname_key c_k_1{customer_warehouse_id, customer_district_id, (const char *)lastname_buf, ones.c_str()};

    //scan customer table for all matches
    auto *customer_index = customer_schema_info->index_infos[1].index; // the second indexInfo
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
  } else {
    //use c_id
    int customer_id = GetCustomerId(r);

    auto *customer_index = customer_schema_info->index_infos[0].index;
    customer_key ck{customer_warehouse_id, customer_district_id, customer_id};
    customer_tuple = db.TransactionRead(customer_index, (const char *)&ck, 
                                              customer_index->key_size, customer_table);
    if (!customer_tuple) {
      return false;
    }
  }
  //get c_id
  uint32_t customer_offset = customer_schema_info->table_info.column_offsets[2];
  int c_id = *reinterpret_cast<int *>(customer_tuple->GetConstValue(customer_offset));
  //get c_first
  customer_offset = customer_schema_info->table_info.column_offsets[6];
  uint32_t customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_first[customer_column_size];
  memcpy(c_first, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_middle
  customer_offset = customer_schema_info->table_info.column_offsets[19];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_middle[customer_column_size];
  memcpy(c_middle, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_street_1
  customer_offset = customer_schema_info->table_info.column_offsets[12];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_street_1[customer_column_size];
  memcpy(c_street_1, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_street_2
  customer_offset = customer_schema_info->table_info.column_offsets[13];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_street_2[customer_column_size];
  memcpy(c_street_2, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_city
  customer_offset = customer_schema_info->table_info.column_offsets[14];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_city[customer_column_size];
  memcpy(c_city, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_state
  customer_offset = customer_schema_info->table_info.column_offsets[15];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_state[customer_column_size];
  memcpy(c_state, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_zip
  customer_offset = customer_schema_info->table_info.column_offsets[16];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_zip[customer_column_size];
  memcpy(c_zip, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_phone
  customer_offset = customer_schema_info->table_info.column_offsets[17];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_phone[customer_column_size];
  memcpy(c_phone, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_since 
  customer_offset = customer_schema_info->table_info.column_offsets[18];
  duckdb::Timestamp c_since = *reinterpret_cast<duckdb::Timestamp *>(customer_tuple->GetConstValue(customer_offset));
  //get c_credit
  customer_offset = customer_schema_info->table_info.column_offsets[4];
  customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
  char c_credit[customer_column_size];
  memcpy(c_credit, (const char *)customer_tuple->GetVarValuePayload(customer_offset), customer_column_size);
  //get c_credit_lim
  customer_offset = customer_schema_info->table_info.column_offsets[7];
  float c_credit_lim = *reinterpret_cast<float *>(customer_tuple->GetConstValue(customer_offset));
  //get c_discount
  customer_offset = customer_schema_info->table_info.column_offsets[3];
  float c_discount = *reinterpret_cast<float *>(customer_tuple->GetConstValue(customer_offset));
  //get c_balance
  customer_offset = customer_schema_info->table_info.column_offsets[8];
  float c_balance = *reinterpret_cast<float *>(customer_tuple->GetConstValue(customer_offset));

  //check if bad credit, update c_data and increase size
  uint32_t c_data_size_increase = 0;
  if (strcmp(c_credit, "BC") == 0) {
    std::string prepend_data;
    prepend_data.append(std::to_string(c_id));
    prepend_data.append(std::to_string(customer_district_id));
    prepend_data.append(std::to_string(customer_warehouse_id));
    prepend_data.append(std::to_string(district_id));
    prepend_data.append(std::to_string(warehouse_id));
    prepend_data.append(std::to_string(payment_amount));
    uint32_t prepend_length = static_cast<uint32_t>(prepend_data.length());

    //change c_data
    customer_offset = customer_schema_info->table_info.column_offsets[20];
    customer_column_size = customer_tuple->GetVarValueSize(customer_offset);
    char *c_data[customer_column_size];
    memcpy(c_data, (const char *)customer_tuple->GetVarValuePayload(customer_offset),
           customer_column_size);
    std::string c_data_string((const char *) c_data);
    prepend_data.append(c_data_string);

    if (customer_column_size + prepend_length > 500) {
      if (customer_column_size != 500) {
        c_data_size_increase = 500 - customer_column_size;
      }
    } else {
      c_data_size_increase = prepend_length;
    }

    //update customer row
    uint32_t new_customer_obj_size = sizeof(table::Object) + sizeof(table::Tuple) + 
                                      customer_tuple->GetSize() + c_data_size_increase;
    auto *new_customer_obj = table::Object::Make(new_customer_obj_size);
    table::Tuple *new_customer_tuple = reinterpret_cast<table::Tuple *>(new_customer_obj->data);
    memcpy(new_customer_tuple, customer_tuple, sizeof(table::Tuple) + customer_tuple->GetSize());
    size_t min_length = 500;
    if (min_length > prepend_data.length()) {
      min_length = prepend_data.length();
    }
    if (c_data_size_increase != 0) {
      *reinterpret_cast<uint32_t *>(
        new_customer_tuple->GetVarValuePayload(customer_offset) - sizeof(uint32_t)) = 
          static_cast<uint32_t>(min_length);
    }
    new_customer_tuple->SetSize(customer_tuple->GetSize() + c_data_size_increase);
    //TODO Jeffrey - copying is causing segfault
    //prepend_data.copy(new_customer_tuple->GetVarValuePayload(customer_offset), min_length);
    
    //decrease balance
    customer_offset = customer_schema_info->table_info.column_offsets[8];
    *reinterpret_cast<float *>(new_customer_tuple->GetConstValue(customer_offset)) -= payment_amount;
    //increase c_ytd_payment
    customer_offset = customer_schema_info->table_info.column_offsets[9];
    *reinterpret_cast<float *>(new_customer_tuple->GetConstValue(customer_offset)) += payment_amount;
    //increase c_payment_cnt
    customer_offset = customer_schema_info->table_info.column_offsets[10];
    *reinterpret_cast<int *>(new_customer_tuple->GetConstValue(customer_offset)) += 1;

    success = db.TransactionUpdate(customer_table, new_customer_tuple->GetOID(), new_customer_obj);
    if (!success) {
      return false;
    }
  } else {
    uint32_t new_customer_obj_size =
      sizeof(table::Object) + sizeof(table::Tuple) + customer_tuple->GetSize();
    auto *new_customer_obj = table::Object::Make(new_customer_obj_size);
    table::Tuple *new_customer_tuple = reinterpret_cast<table::Tuple *>(new_customer_obj->data);
    memcpy(new_customer_tuple, customer_tuple, sizeof(table::Tuple) + customer_tuple->GetSize());
    //decrease balance
    customer_offset = customer_schema_info->table_info.column_offsets[8];
    *reinterpret_cast<float *>(new_customer_tuple->GetConstValue(customer_offset)) -= payment_amount;
    //increase c_ytd_payment
    customer_offset = customer_schema_info->table_info.column_offsets[9];
    *reinterpret_cast<float *>(new_customer_tuple->GetConstValue(customer_offset)) += payment_amount;
    //increase c_payment_cnt
    customer_offset = customer_schema_info->table_info.column_offsets[10];
    *reinterpret_cast<int *>(new_customer_tuple->GetConstValue(customer_offset)) += 1;

    success = db.TransactionUpdate(customer_table, new_customer_tuple->GetOID(), new_customer_obj);
    if (!success) {
      return false;
    }
  }

  //insert row into history
  auto *history_table = history_schema_info->table_info.table;

  char h_data[w_name_length + 4 + d_name_length + 1];
  h_data[w_name_length + 4 + d_name_length] = '\0';
  memset(h_data, ' ', w_name_length + 4 + d_name_length);
  memcpy(h_data, w_name, w_name_length);
  memcpy(&h_data[w_name_length + 4], d_name, d_name_length);

  history_value hv {c_id, customer_district_id, customer_warehouse_id, warehouse_id, district_id,
                      duckdb::Timestamp::GetCurrentTimestamp().value, payment_amount, 36, 
                      w_name_length + 4 + d_name_length, h_data};
  //no index for history table
  success = db.TransactionInsert(history_table, (const char *)&hv, sizeof(hv),
                                nullptr, nullptr, 0);
  if (!success) {
    return false;
  }

  return true;
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


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

#include "tpcc_load.h"

#ifdef OMCS_OFFSET
#include "index/latches/OMCSOffset.h"
#endif


namespace noname {
namespace benchmark {
namespace tpcc {

thread_local sql::SQLQuery noname_query;

void TPCCLoad::CreateTables() {
  transaction::Transaction t;
  noname_query.transaction = &t;

  auto result = noname_query.Query(create_warehouse_sql);
  CHECK(result);

  result = noname_query.Query(create_item_sql);
  CHECK(result);

  result = noname_query.Query(create_district_sql);
  CHECK(result);

  result = noname_query.Query(create_stock_sql);
  CHECK(result);

  result = noname_query.Query(create_customer_sql);
  CHECK(result);

  result = noname_query.Query(create_history_sql);
  CHECK(result);

  result = noname_query.Query(create_oorder_sql);
  CHECK(result);

  result = noname_query.Query(create_new_order_sql);
  CHECK(result);

  result = noname_query.Query(create_order_line_sql);
  CHECK(result);

  t.PreCommit();
  t.PostCommit();
}

void TPCCLoad::CreateIndexes() {
  transaction::Transaction t;
  noname_query.transaction = &t;

  auto result = noname_query.Query(create_warehouse_index_sql);
  CHECK(result);

  result = noname_query.Query(create_item_index_sql);
  CHECK(result);

  result = noname_query.Query(create_district_index_sql);
  CHECK(result);

  result = noname_query.Query(create_stock_index_sql);
  CHECK(result);

  result = noname_query.Query(create_customer_index_sql);
  CHECK(result);

  result = noname_query.Query(create_customer_name_index_sql);
  CHECK(result);

  result = noname_query.Query(create_oorder_index_sql);
  CHECK(result);

  result = noname_query.Query(create_oorder_customer_id_index_sql);
  CHECK(result);

  result = noname_query.Query(create_new_order_index_sql);
  CHECK(result);

  result = noname_query.Query(create_order_line_index_sql);
  CHECK(result);

  t.PreCommit();
  t.PostCommit();
}

void TPCCLoad::Run() {

#ifdef OMCS_OFFSET
  offset::init_qnodes();
  offset::reset_tls_qnodes();
#endif

  std::cout << "Loading..." << std::flush;
  catalog::Initialize();

  CreateTables();
  CreateIndexes();

  LoadItems();

  // Assume 2HT per core
  auto total_physical_threads_per_numa_node =
    std::thread::hardware_concurrency() / ((numa_max_node() + 1) * 2);

  // E.g., if there are 2 numa nodes, the total number of physical threads per numa node is 20,
  // and there are 10 worker threads, we will only use the threads on the first numa node to load,
  // so that worker threads would not touch remote memory.
  uint32_t total_loader_threads =
    std::ceil((double)nthreads / total_physical_threads_per_numa_node) *
    total_physical_threads_per_numa_node;

  uint32_t total_loader_threads_running = 0;
  for (uint32_t i = 1; i <= NumWarehouses(); ++i) {
  retry:
    thread::Thread *t = nullptr;
    if (total_loader_threads_running < total_loader_threads) {
      t = thread_pool->GetThread(true);
    }
    if (!t) {
      for (auto &loader : loaders) {
        if (loader->TryJoin()) {
          loader->Join();
          thread_pool->PutThread(loader);
          total_loader_threads_running--;
          goto retry;
        }
      }
      goto retry;
    }
    t->StartTask([this, i] { LoadAll(i); });
    loaders.push_back(t);
    total_loader_threads_running++;
  }

  for (auto &t : loaders) {
    t->Join();
    thread_pool->PutThread(t);
  }

  std::cout << "Done" << std::endl;
}

void TPCCLoad::LoadAll(uint w_id) {

#ifdef OMCS_OFFSET
  offset::reset_tls_qnodes();
#endif

  LoadWarehouse(w_id);
  LoadStocks(w_id);
  LoadDistricts(w_id);
  LoadCustomers(w_id);
  LoadHistory(w_id);
  LoadOorders(w_id);
  LoadNewOrders(w_id);
  LoadOrderLines(w_id);
}

void TPCCLoad::LoadWarehouse(uint w_id) {
  util::fast_random r(43253246);
  thread_local sql::PreparedStatement prepared_stmt_warehouse;
  thread_local bool is_prepared_stmt_warehouse = false;
  if (!is_prepared_stmt_warehouse) {
    noname_query.Prepare("INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_warehouse);
    is_prepared_stmt_warehouse = true;
  }

  transaction::Transaction t;
  noname_query.transaction = &t;

  noname_query.BindInt32(&prepared_stmt_warehouse, 1, w_id);
  noname_query.BindFloat(&prepared_stmt_warehouse, 2, 300000);
  noname_query.BindFloat(&prepared_stmt_warehouse, 3, (float)RandomNumber(r, 0, 2000) / 10000.0);
  noname_query.BindVarchar(&prepared_stmt_warehouse, 4, RandomStr(r, RandomNumber(r, 6, 10)).c_str());
  noname_query.BindVarchar(&prepared_stmt_warehouse, 5, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
  noname_query.BindVarchar(&prepared_stmt_warehouse, 6, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
  noname_query.BindVarchar(&prepared_stmt_warehouse, 7, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
  noname_query.BindVarchar(&prepared_stmt_warehouse, 8, RandomStr(r, 3).c_str());
  noname_query.BindVarchar(&prepared_stmt_warehouse, 9, "123456789");
  
  auto success = noname_query.ExecutePrepared(&prepared_stmt_warehouse);
  CHECK(success);

  t.PreCommit();
  t.PostCommit();
}

void TPCCLoad::LoadItems() {
  util::fast_random r(13985989);
  thread_local sql::PreparedStatement prepared_stmt_items;
  thread_local bool is_prepared_stmt_items = false;
  if (!is_prepared_stmt_items) {
    noname_query.Prepare("INSERT INTO item VALUES (?, ?, ?, ?, ?)", &prepared_stmt_items);
    is_prepared_stmt_items = true;
  }

  for (uint i = 1; i <= NumItems(); i++) {
    transaction::Transaction t;
    noname_query.transaction = &t;

    noname_query.BindInt32(&prepared_stmt_items, 1, i);
    noname_query.BindVarchar(&prepared_stmt_items, 2, RandomStr(r, RandomNumber(r, 14, 24)).c_str());
    noname_query.BindFloat(&prepared_stmt_items, 3, (float)RandomNumber(r, 100, 10000) / 100.0);
    const int len = RandomNumber(r, 26, 50);
    if (RandomNumber(r, 1, 100) > 10) {
      const std::string i_data = RandomStr(r, len);
      noname_query.BindVarchar(&prepared_stmt_items, 4, i_data.c_str());
    } else {
      const int startOriginal = RandomNumber(r, 2, (len - 8));
      const std::string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" +
          RandomStr(r, len - startOriginal - 7);
      noname_query.BindVarchar(&prepared_stmt_items, 4, i_data.c_str());
    }
    noname_query.BindInt32(&prepared_stmt_items, 5, RandomNumber(r, 1, 10000));

    auto success = noname_query.ExecutePrepared(&prepared_stmt_items);
    CHECK(success);

    t.PreCommit();
    t.PostCommit();
  }
}

void TPCCLoad::LoadStocks(uint w_id) {
  util::fast_random r(5342816);
  thread_local sql::PreparedStatement prepared_stmt_stocks;
  thread_local bool is_prepared_stmt_stocks = false;
  if (!is_prepared_stmt_stocks) {
    noname_query.Prepare("INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_stocks);
    is_prepared_stmt_stocks = true;
  }

  for (uint i = 1; i <= NumItems(); i++) {
    transaction::Transaction t;
    noname_query.transaction = &t;

    noname_query.BindInt32(&prepared_stmt_stocks, 1, w_id);
    noname_query.BindInt32(&prepared_stmt_stocks, 2, i);
    noname_query.BindInt32(&prepared_stmt_stocks, 3, RandomNumber(r, 10, 100));
    noname_query.BindFloat(&prepared_stmt_stocks, 4, 0);
    noname_query.BindInt32(&prepared_stmt_stocks, 5, 0);
    noname_query.BindInt32(&prepared_stmt_stocks, 6, 0);
    const int len = RandomNumber(r, 26, 50);
    if (RandomNumber(r, 1, 100) > 10) {
      const std::string s_data = RandomStr(r, len);
      noname_query.BindVarchar(&prepared_stmt_stocks, 7, s_data.c_str());
    } else {
      const int startOriginal = RandomNumber(r, 2, (len - 8));
      const std::string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" +
          RandomStr(r, len - startOriginal - 7);
      noname_query.BindVarchar(&prepared_stmt_stocks, 7, s_data.c_str());
    }
    noname_query.BindVarchar(&prepared_stmt_stocks, 8, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 9, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 10, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 11, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 12, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 13, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 14, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 15, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 16, RandomStr(r, 24).c_str());
    noname_query.BindVarchar(&prepared_stmt_stocks, 17, RandomStr(r, 24).c_str());

    auto success = noname_query.ExecutePrepared(&prepared_stmt_stocks);
    CHECK(success);

    t.PreCommit();
    t.PostCommit();
  }
}

void TPCCLoad::LoadDistricts(uint w_id) {
  util::fast_random r(96324245);
  thread_local sql::PreparedStatement prepared_stmt_districts;
  thread_local bool is_prepared_stmt_districts = false;
  if (!is_prepared_stmt_districts) {
    noname_query.Prepare("INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_districts);
    is_prepared_stmt_districts = true;
  }

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    transaction::Transaction t;
    noname_query.transaction = &t;

    noname_query.BindInt32(&prepared_stmt_districts, 1, w_id);
    noname_query.BindInt32(&prepared_stmt_districts, 2, d);
    noname_query.BindFloat(&prepared_stmt_districts, 3, 30000);
    noname_query.BindFloat(&prepared_stmt_districts, 4, (float)(RandomNumber(r, 0, 2000) / 10000.0));
    noname_query.BindInt32(&prepared_stmt_districts, 5, 3001);
    noname_query.BindVarchar(&prepared_stmt_districts, 6, RandomStr(r, RandomNumber(r, 6, 10)).c_str());
    noname_query.BindVarchar(&prepared_stmt_districts, 7, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
    noname_query.BindVarchar(&prepared_stmt_districts, 8, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
    noname_query.BindVarchar(&prepared_stmt_districts, 9, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
    noname_query.BindVarchar(&prepared_stmt_districts, 10, RandomStr(r, 3).c_str());
    noname_query.BindVarchar(&prepared_stmt_districts, 11, "123456789");

    auto success = noname_query.ExecutePrepared(&prepared_stmt_districts);
    CHECK(success);

    t.PreCommit();
    t.PostCommit();
  }
}

void TPCCLoad::LoadCustomers(uint w_id) {
  util::fast_random r(81458689);
  thread_local sql::PreparedStatement prepared_stmt_customers;
  thread_local bool is_prepared_stmt_customers = false;
  if (!is_prepared_stmt_customers) {
    noname_query.Prepare("INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_customers);
    is_prepared_stmt_customers = true;
  }

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
      transaction::Transaction t;
      noname_query.transaction = &t;

      noname_query.BindInt32(&prepared_stmt_customers, 1, w_id);
      noname_query.BindInt32(&prepared_stmt_customers, 2, d);
      noname_query.BindInt32(&prepared_stmt_customers, 3, c);
      noname_query.BindFloat(&prepared_stmt_customers, 4, (float)(RandomNumber(r, 1, 5000) / 10000.0));

      if (RandomNumber(r, 1, 100) <= 10) {
        noname_query.BindVarchar(&prepared_stmt_customers, 5, "BC");
      } else {
        noname_query.BindVarchar(&prepared_stmt_customers, 5, "GC");
      }

      uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
      static_assert(sizeof(lastname_buf) == 16, "xx");
      memset(lastname_buf, 1, sizeof(lastname_buf));
      std::string lastname;
      if (c <= 1000) {
        lastname = GetCustomerLastName(c - 1);
        //noname_query.BindVarchar(&prepared_stmt_customers, 6, GetCustomerLastName(c - 1).c_str());
      } else {
        lastname = GetNonUniformCustomerLastNameLoad(r);
        //noname_query.BindVarchar(&prepared_stmt_customers, 6, GetNonUniformCustomerLastNameLoad(r).c_str());
      }
      memcpy(lastname_buf, lastname.c_str(), lastname.size());
      noname_query.BindVarchar(&prepared_stmt_customers, 6, (const char *)lastname_buf);
      noname_query.BindVarchar(&prepared_stmt_customers, 7, RandomStr(r, 17).c_str());
      noname_query.BindFloat(&prepared_stmt_customers, 8, 50000);
      noname_query.BindFloat(&prepared_stmt_customers, 9, -10);
      noname_query.BindFloat(&prepared_stmt_customers, 10, 10);
      noname_query.BindInt32(&prepared_stmt_customers, 11, 1);
      noname_query.BindInt32(&prepared_stmt_customers, 12, 0);
      noname_query.BindVarchar(&prepared_stmt_customers, 13, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
      noname_query.BindVarchar(&prepared_stmt_customers, 14, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
      noname_query.BindVarchar(&prepared_stmt_customers, 15, RandomStr(r, RandomNumber(r, 10, 20)).c_str());
      noname_query.BindVarchar(&prepared_stmt_customers, 16, RandomStr(r, 3).c_str());
      noname_query.BindVarchar(&prepared_stmt_customers, 17, (RandomNStr(r, 4) + "1111").c_str());
      noname_query.BindVarchar(&prepared_stmt_customers, 18, RandomNStr(r, 16).c_str());
      noname_query.BindTimestamp(&prepared_stmt_customers, 19, duckdb::Timestamp::GetCurrentTimestamp());
      noname_query.BindVarchar(&prepared_stmt_customers, 20, "OE");
      noname_query.BindVarchar(&prepared_stmt_customers, 21, RandomStr(r, RandomNumber(r, 300, 500)).c_str());

      auto success = noname_query.ExecutePrepared(&prepared_stmt_customers);
      CHECK(success);

      t.PreCommit();
      t.PostCommit();
    }
  }
}

void TPCCLoad::LoadHistory(uint w_id) {
  util::fast_random r(2456721145);
  thread_local sql::PreparedStatement prepared_stmt_history;
  thread_local bool is_prepared_stmt_history = false;
  if (!is_prepared_stmt_history) {
    noname_query.Prepare("INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_history);
    is_prepared_stmt_history = true;
  }

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
      transaction::Transaction t;
      noname_query.transaction = &t;

      noname_query.BindInt32(&prepared_stmt_history, 1, c);
      noname_query.BindInt32(&prepared_stmt_history, 2, d);
      noname_query.BindInt32(&prepared_stmt_history, 3, w_id);
      noname_query.BindInt32(&prepared_stmt_history, 4, w_id);
      noname_query.BindInt32(&prepared_stmt_history, 5, d);
      noname_query.BindTimestamp(&prepared_stmt_history, 6, duckdb::Timestamp::GetCurrentTimestamp());
      noname_query.BindFloat(&prepared_stmt_history, 7, 10);
      noname_query.BindVarchar(&prepared_stmt_history, 8, RandomStr(r, RandomNumber(r, 10, 24)).c_str());

      auto success = noname_query.ExecutePrepared(&prepared_stmt_history);
      CHECK(success);

      t.PreCommit();
      t.PostCommit();
    }
  }
}

void TPCCLoad::LoadOorders(uint w_id) {
  util::fast_random r(612542457);
  thread_local sql::PreparedStatement prepared_stmt_oorders;
  thread_local bool is_prepared_stmt_oorders = false;
  if (!is_prepared_stmt_oorders) {
    noname_query.Prepare("INSERT INTO oorder VALUES (?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_oorders);
    is_prepared_stmt_oorders = true;
  }

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    std::set<uint> c_ids_s;
    std::vector<uint> c_ids;
    while (c_ids.size() != NumCustomersPerDistrict()) {
      const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
      if (c_ids_s.count(x)) {
        continue;
      }
      c_ids_s.insert(x);
      c_ids.emplace_back(x);
    }
    for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
      transaction::Transaction t;
      noname_query.transaction = &t;

      noname_query.BindInt32(&prepared_stmt_oorders, 1, w_id);
      noname_query.BindInt32(&prepared_stmt_oorders, 2, d);
      noname_query.BindInt32(&prepared_stmt_oorders, 3, c);
      noname_query.BindInt32(&prepared_stmt_oorders, 4, c_ids[c - 1]);
      if (c < 2101) {
        noname_query.BindInt32(&prepared_stmt_oorders, 5, RandomNumber(r, 1, 10));
      } else {
        noname_query.BindInt32(&prepared_stmt_oorders, 5, 0);
      }
      noname_query.BindInt32(&prepared_stmt_oorders, 6, RandomNumber(r, 5, 15));
      noname_query.BindInt32(&prepared_stmt_oorders, 7, 1);
      noname_query.BindTimestamp(&prepared_stmt_oorders, 8, duckdb::Timestamp::GetCurrentTimestamp());

      auto success = noname_query.ExecutePrepared(&prepared_stmt_oorders);
      CHECK(success);

      t.PreCommit();
      t.PostCommit();
    }
  }
}

void TPCCLoad::LoadNewOrders(uint w_id) {
  util::fast_random r(356765457);
  thread_local sql::PreparedStatement prepared_stmt_new_orders;
  thread_local bool is_prepared_stmt_new_orders = false;
  if (!is_prepared_stmt_new_orders) {
    noname_query.Prepare("INSERT INTO new_order VALUES (?, ?, ?)", &prepared_stmt_new_orders);
    is_prepared_stmt_new_orders = true;
  }

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
      if (c >= 2101) {
        transaction::Transaction t;
        noname_query.transaction = &t;

        noname_query.BindInt32(&prepared_stmt_new_orders, 1, w_id);
        noname_query.BindInt32(&prepared_stmt_new_orders, 2, d);
        noname_query.BindInt32(&prepared_stmt_new_orders, 3, c);

        auto success = noname_query.ExecutePrepared(&prepared_stmt_new_orders);
        CHECK(success);

        t.PreCommit();
        t.PostCommit();
      }
    }
  }
}

void TPCCLoad::LoadOrderLines(uint w_id) {
  util::fast_random r(7235782);
  thread_local sql::PreparedStatement prepared_stmt_order_lines;
  thread_local bool is_prepared_stmt_order_lines = false;
  if (!is_prepared_stmt_order_lines) {
    noname_query.Prepare("INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", &prepared_stmt_order_lines);
    is_prepared_stmt_order_lines = true;
  }

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
      uint count = RandomNumber(r, 5, 15);
      for (uint l = 1; l <= count; l++) {
        transaction::Transaction t;
        noname_query.transaction = &t;

        noname_query.BindInt32(&prepared_stmt_order_lines, 1, w_id);
        noname_query.BindInt32(&prepared_stmt_order_lines, 2, d);
        noname_query.BindInt32(&prepared_stmt_order_lines, 3, c);
        noname_query.BindInt32(&prepared_stmt_order_lines, 4, l);
        noname_query.BindInt32(&prepared_stmt_order_lines, 5, RandomNumber(r, 1, 100000));
        if (c < 2101) {
          noname_query.BindTimestamp(&prepared_stmt_order_lines, 6, duckdb::Timestamp::GetCurrentTimestamp());
          noname_query.BindFloat(&prepared_stmt_order_lines, 7, 0);
        } else {
          noname_query.BindTimestamp(&prepared_stmt_order_lines, 6, duckdb::timestamp_t(0));
          noname_query.BindFloat(&prepared_stmt_order_lines, 7, (float)(RandomNumber(r, 1, 999999) / 100.0));
        }
        noname_query.BindInt32(&prepared_stmt_order_lines, 8, w_id);
        noname_query.BindFloat(&prepared_stmt_order_lines, 9, 5);
        noname_query.BindVarchar(&prepared_stmt_order_lines, 10, RandomStr(r, 24).c_str());

        auto success = noname_query.ExecutePrepared(&prepared_stmt_order_lines);
        CHECK(success);

	t.PreCommit();
        t.PostCommit();
      }
    }
  }
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


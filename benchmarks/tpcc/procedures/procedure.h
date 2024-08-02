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

#pragma once

#include "benchmarks/tpcc/tpcc_ddl.h"
#include "benchmarks/tpcc/tpcc_util.h"
#include "engine/noname/noname.h"
#include "sql/query.h"
#include "table/tuple.h"

namespace noname {
namespace benchmark {
namespace tpcc {

struct Procedure {
  Procedure(Engine &db, sql::SQLQuery &noname_query, uint32_t home_warehouse_id)
    : db(db), noname_query(noname_query), home_warehouse_id(home_warehouse_id) {
      memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
    }
  virtual ~Procedure() {}

  virtual bool run() = 0;

  Engine &db;
  sql::SQLQuery &noname_query;
  uint32_t home_warehouse_id;
  int last_no_o_ids[10];
};

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


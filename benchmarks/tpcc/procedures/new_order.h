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

#include "procedure.h"

namespace noname {
namespace benchmark {
namespace tpcc {

struct NewOrder : Procedure {
  NewOrder(Engine &db, sql::SQLQuery &noname_query, uint32_t home_warehouse_id)
    : Procedure(db, noname_query, home_warehouse_id) {
  }

  bool run() override;
  const std::string GetDistInfo(int d_id, sql::QueryResult &qr);

  util::fast_random r{235285898 + home_warehouse_id};
};

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname


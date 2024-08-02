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

#include "sql/duckdb_query.h"
#include "transaction/string_arena.h"
#include "transaction/transaction-normal.h"

namespace noname {
namespace sql {

struct ExecutionContext {
  ExecutionContext(transaction::Transaction *transaction, transaction::StringArena *arena)
      : transaction(transaction), arena(arena), whether_abort(false) {}
  ~ExecutionContext() {}

  transaction::Transaction *transaction;
  transaction::StringArena *arena;
  bool whether_abort;
};

}  // namespace sql
}  // namespace nonamee


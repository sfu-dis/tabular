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

#pragma once

#include "sql/duckdb_internal.h"
#include "sql/execution/execution_context.h"
#include "sql/execution/executor_type.h"
#include "sql/query_result.h"

namespace noname {
namespace sql {

struct PullExecutor {
  PullExecutor() : executor_type(ExecutorType::INVALID) {}
  PullExecutor(ExecutionContext *execution_context, ExecutorType executor_type)
    : execution_context(execution_context), executor_type(executor_type) {}
  PullExecutor(ExecutionContext *execution_context,
               std::unique_ptr<PullExecutor> child_executor,
               ExecutorType executor_type)
    : execution_context(execution_context),
      child_executor(std::move(child_executor)),
      executor_type(executor_type) {}
  PullExecutor(ExecutionContext *execution_context,
               std::unique_ptr<PullExecutor> child_executor,
               std::vector<std::unique_ptr<duckdb::Expression>> *projection_expressions,
               ExecutorType executor_type)
    : execution_context(execution_context),
      child_executor(std::move(child_executor)),
      projection_expressions(projection_expressions),
      executor_type(executor_type) {}
  PullExecutor(ExecutionContext *execution_context,
               std::unique_ptr<PullExecutor> left_child_executor,
               std::unique_ptr<PullExecutor> right_child_executor,
               ExecutorType executor_type)
    : execution_context(execution_context),
      child_executor(std::move(left_child_executor)),
      right_child_executor(std::move(right_child_executor)),
      executor_type(executor_type) {}
  virtual ~PullExecutor() {}

  virtual void Init() = 0;
  virtual bool Next(void *record) = 0;

  inline bool IsUpdateOp() { return is_update_op; }
  inline bool IsDeleteOp() { return is_delete_op; }

  inline void SetUpdateOp(bool is_update_op_) {
    is_update_op = is_update_op_;
  }

  inline void SetDeleteOp(bool is_delete_op_) {
    is_delete_op = is_delete_op_;
  }

  ExecutionContext *execution_context;
  // main child, also as left child in JoinExecutor
  std::unique_ptr<PullExecutor> child_executor;
  // right child in JoinExecutor
  std::unique_ptr<PullExecutor> right_child_executor;
  std::vector<std::unique_ptr<duckdb::Expression>> *projection_expressions = nullptr;
  bool is_update_op = false;
  bool is_delete_op = false;
  ExecutorType executor_type;
};

}  // namespace sql
}  // namespace noname


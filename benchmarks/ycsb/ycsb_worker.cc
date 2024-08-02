/*
 * Copyright (C) 2023 Data-Intensive Systems Lab, Simon Fraser University.
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

#include <vector>

#include "ycsb_worker.h"

namespace noname {
namespace benchmark {
namespace ycsb {

YCSBWorkerLocalData::YCSBWorkerLocalData()
    : num_aborts(0), num_insert_aborts(0), num_read_aborts(0),
      num_scan_aborts(0), num_update_aborts(0) {}

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

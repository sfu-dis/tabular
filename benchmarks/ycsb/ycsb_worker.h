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

#pragma once

#include "foedus/uniform_random.hpp"
#include "foedus/zipfian_random.hpp"
#include "util.h"

namespace noname {
namespace benchmark {
namespace ycsb {

struct YCSBWorkerLocalData {
  YCSBWorkerLocalData();

  foedus::assorted::ZipfianRandom zipfian_random_;
  foedus::assorted::UniformRandom uniform_random_;
  util::fast_random fast_random_;

  uint64_t num_aborts;
  uint64_t num_read_aborts;
  uint64_t num_update_aborts;
  uint64_t num_insert_aborts;
  uint64_t num_scan_aborts;
};

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

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

#include <cstdint>
#include <functional>

namespace noname {
namespace benchmark {
namespace ycsb {


/**
 * @brief The workload percentage for a YCSB benchmark
 * The sum of percent numbers should be 100
 */
struct WorkloadPercentage {
  WorkloadPercentage();
  WorkloadPercentage(char description, int16_t insert_percent,
                     int16_t read_percent, int16_t update_percent,
                     int16_t scan_percent, int16_t rmw_percent);

  int16_t insert_percent() const { return insert_percent_; }
  int16_t read_percent() const { return read_percent_ - insert_percent_; }
  int16_t update_percent() const { return update_percent_ - read_percent_; }
  int16_t scan_percent() const { return scan_percent_ - update_percent_; }
  int16_t rmw_percent() const { return rmw_percent_ - scan_percent_; }

  char description_;
  // Cumulative percentage of i/r/u/s/rmw. From insert...rmw the percentages
  // accumulates, e.g., i=5, r=12 => we'll have 12-5=7% of reads in total.
  int16_t insert_percent_;
  int16_t read_percent_;
  int16_t update_percent_;
  int16_t scan_percent_;
  int16_t rmw_percent_;
};

}  // namespace ycsb
}  // namespace benchmark
}  // namespace noname

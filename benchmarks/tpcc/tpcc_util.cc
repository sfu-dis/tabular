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

#include "tpcc_util.h"

namespace noname {
namespace benchmark {
namespace tpcc {

uint64_t scale_factor = 1;
int g_uniform_item_dist = 1;
int g_wh_temperature = 0;
int g_new_order_remote_item_pct = 1;
int g_disable_xpartition_txn = 0;
bool g_order_status_scan_hack = true;
int g_new_order_fast_id_gen = 1;


util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids = nullptr;

// how much % of time a worker should use a random home wh
// 0 - always use home wh
// 0.5 - 50% of time use random wh
// 1 - always use a random wh
double g_wh_spread = 0;

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname

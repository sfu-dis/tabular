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

#include <filesystem>
#include <vector>

#include "table/inline_table.h"
#include "log/dlog/log.h"

namespace noname {
namespace tabular {

struct TableGroup {
  TableGroup(table::config_t config, bool is_persistent,
             const std::filesystem::path &logging_directory = "",
             size_t num_of_workers = 0);

  ~TableGroup();

  table::InlineTable *GetTable(table::FID fid, bool is_persistent);

  table::config_t table_config;
  std::vector<table::InlineTable *> tables;
  dlog::Logger *logger;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopped;
  static void EpochDaemon(TableGroup *group);
  void StartEpochDaemon();
  void StopEpochDaemon();
};

}  // namespace tabular
}  // namespace noname

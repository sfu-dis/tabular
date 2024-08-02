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

#include <filesystem>
#include <thread>

#include "log/dlog/log.h"

#include "table_group.h"

namespace noname {
namespace tabular {

TableGroup::TableGroup(table::config_t config, bool is_persistent,
                       const std::filesystem::path &logging_directory,
                       size_t num_of_workers)
    : logger(nullptr), table_config(config), epoch(0), stopped(false) {
  if (is_persistent) {
    logger = new dlog::Logger(logging_directory, num_of_workers);
    // TODO(hutx) recover tables from logging_directory
  }
}

table::InlineTable *TableGroup::GetTable(table::FID fid, bool is_persistent) {
  CHECK(fid <= tables.size());
  if (fid == tables.size()) {
    auto table = new table::InlineTable(table_config, is_persistent);

    table->fid = tables.size();
    tables.push_back(table);
  }
  return tables[fid];
}

TableGroup::~TableGroup() {
  for (auto t : tables) {
    delete t;
  }
}

void TableGroup::EpochDaemon(TableGroup *group) {
  while (!group->stopped.load(std::memory_order::acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    // Advance current epoch
    group->epoch.fetch_add(1, std::memory_order::acq_rel);
  }
}

void TableGroup::StartEpochDaemon() {
  std::thread t(TableGroup::EpochDaemon, this);
  t.detach();
}

void TableGroup::StopEpochDaemon() {
  stopped.store(true, std::memory_order::release);
}

}  // namespace tabular
}  // namespace noname

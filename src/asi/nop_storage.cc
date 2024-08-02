/*
 * Copyright (C) 2022 Data-Intensive Systems Lab, Simon Fraser University. 
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

#include "nop_storage.h"

namespace noname {
namespace asi {

NOPStorage::NOPStorage() : last_size(0) {}

NOPStorage::~NOPStorage() {}

bool NOPStorage::SyncRead(char *out_src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool NOPStorage::SyncWrite(const char *src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool NOPStorage::AsyncRead(const char *out_src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool NOPStorage::AsyncWrite(const char *src, const uint64_t size, uint64_t offset) {
  return true;
}

uint64_t NOPStorage::PollAsyncWrite() {
  return last_size;
}

bool NOPStorage::PeekAsyncWrite(uint64_t *out_size) {
  return true;
}

}  // namespace asi
}  // namespace noname

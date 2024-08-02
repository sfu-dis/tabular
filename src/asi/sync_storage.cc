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

// Implements an synchronous storage interface inherited from ASI.

#include "sync_storage.h"

namespace noname {
namespace asi {

SyncStorage::SyncStorage(std::string &filename, bool dio)
    : filename(filename), direct_io(dio) {
  int flags = dio ? O_DIRECT : 0;

  // TODO(tzwang): support opening existing segment (no O_TRUNC)
  flags |= (O_RDWR | O_CREAT | O_TRUNC);
  fd = open(filename.c_str(), flags, 0644);
  LOG_IF(FATAL, fd < 0);
}

bool SyncStorage::SyncRead(char *out_src, const uint64_t size, uint64_t offset) {
  auto nread = pread(fd, out_src, size, offset);
  if (nread != size) {
    return false;
  }
  return true;
}

bool SyncStorage::SyncWrite(const char *src, const uint64_t size, uint64_t offset) {
  if (size != pwrite(fd, src, size, offset)) {
    return false;
  }

  if (fsync(fd)) {
    return false;
  }

  return true;
}

bool SyncStorage::AsyncRead(const char *out_src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool SyncStorage::AsyncWrite(const char *src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

uint64_t SyncStorage::PollAsyncWrite() {
  LOG(FATAL) << "Not supported";
  return 0;
}

bool SyncStorage::PeekAsyncWrite(uint64_t *out_size) {
  LOG(FATAL) << "Not supported";
  return false;
}

}  // namespace asi
}  // namespace noname


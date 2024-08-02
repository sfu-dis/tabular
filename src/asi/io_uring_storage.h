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

// Implements an io_uring storage interface inherited from ASI.

#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <liburing.h>
#include <string>
#include "asi.h"

namespace noname {
namespace asi {

struct IOUringStorage : ASI {
  // Path/filename of a segment
  std::string filename;

  // File descriptor for a segment
  int fd;

  // Whether to use O_DIRECT
  bool direct_io;

  // io_uring structure
  struct io_uring ring;

  IOUringStorage(const std::string &filename, bool dio);
  ~IOUringStorage() { close(fd); }

  bool SyncRead(char *out_src, const uint64_t size, uint64_t offset) override;
  bool SyncWrite(const char *src, const uint64_t size, uint64_t offset) override;
  bool AsyncRead(const char *out_src, const uint64_t size, uint64_t offset) override;
  bool AsyncWrite(const char *src, const uint64_t size, uint64_t offset) override;
  uint64_t PollAsyncWrite() override;
  bool PeekAsyncWrite(uint64_t *out_size) override;
};

}  // namespace asi
}  // namespace noname

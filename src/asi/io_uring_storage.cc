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

#include "io_uring_storage.h"

namespace noname {
namespace asi {

IOUringStorage::IOUringStorage(const std::string &filename, bool dio)
    : filename(filename), direct_io(dio) {
  int flags = dio ? O_DIRECT : 0;

  // TODO(tzwang): support opening existing segment (no O_TRUNC)
  flags |= (O_RDWR | O_CREAT | O_TRUNC);
  fd = open(filename.c_str(), flags, 0644);
  LOG_IF(FATAL, fd < 0);

  // Initialize io_uring
  int ret = io_uring_queue_init(1024, &ring, 0);
  LOG_IF(FATAL, ret != 0) << "Error setting up io_uring: " << strerror(ret);
}

bool IOUringStorage::SyncRead(char *out_src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool IOUringStorage::SyncWrite(const char *src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool IOUringStorage::AsyncRead(const char *out_src, const uint64_t size, uint64_t offset) {
  LOG(FATAL) << "Not supported";
  return false;
}

bool IOUringStorage::AsyncWrite(const char *src, const uint64_t size, uint64_t offset) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  LOG_IF(FATAL, !sqe);

  io_uring_prep_write(sqe, fd, src, size, offset);

  // Encode data size which is useful upon completion (to add to durable_lsn)
  // Must be set after io_uring_prep_write (which sets user_data to 0)
  sqe->user_data = size;

  int nsubmitted = io_uring_submit(&ring);
  LOG_IF(FATAL, nsubmitted != 1);

  return true;
}

uint64_t IOUringStorage::PollAsyncWrite() {
  struct io_uring_cqe *cqe = nullptr;
  int ret = io_uring_wait_cqe(&ring, &cqe);
  LOG_IF(FATAL, ret < 0) << "Error waiting for completion: " << strerror(-ret);
  LOG_IF(FATAL, cqe->res < 0) << "Error in async operation: " << strerror(-cqe->res);
  uint64_t size = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);
  return size;
}

bool IOUringStorage::PeekAsyncWrite(uint64_t *out_size) {
  struct io_uring_cqe* cqe;
  int ret = io_uring_peek_cqe(&ring, &cqe);

  if (ret < 0) {
    if (ret == -EAGAIN) {
      // Nothing yet - caller should retry later
      return false;
    } else {
      LOG(FATAL) << strerror(ret);
    }
  }

  uint64_t size = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);
  *out_size = size;

  return true;
}

}  // namespace asi
}  // namespace noname

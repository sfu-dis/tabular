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

// Implements an abstract storage interface (ASI).

#pragma once

namespace noname {
namespace asi {

struct ASI {
  // Constructors
  ASI() {}

  // Destructor
  virtual ~ASI() {};

  // Synchronously read data from a file.
  // @out_src: pointer to destination data
  // @size: number of bytes to read
  // @offset: offset in the file
  // Returns whether the request succeeded.
  virtual bool SyncRead(char *out_src, const uint64_t size, uint64_t offset) = 0;
  
  // Synchronously write data to a file.
  // @src: pointer to source data
  // @size: number of bytes to write
  // @offset: offset in the file
  // Returns whether the request succeeded.
  virtual bool SyncWrite(const char *src, const uint64_t size, uint64_t offset) = 0;

  // Asynchronously read data from a file.
  // @out_src: pointer to destination data
  // @size: number of bytes to read
  // @offset: offset in the file
  // Returns whether the request succeeded.
  virtual bool AsyncRead(const char *out_src, const uint64_t size, uint64_t offset) = 0;

  // Asynchronously write data to a file.
  // @src: pointer to source data
  // @size: number of bytes to write
  // @offset: offset in the file
  // Returns whether the request succeeded.
  virtual bool AsyncWrite(const char *src, const uint64_t size, uint64_t offset) = 0;

  // Poll for completion of the previous async-write request. Blocks until the request is completed.
  // Returns the number of bytes written.
  virtual uint64_t PollAsyncWrite() = 0;

  // Query the current completion status of the previous async-write request.
  // @out_size: number of bytes written
  // Returns true if the I/O has concluded (not necessarly successful).
  // TODO(hutx): for now only single in-progress async request is supported.
  //  To enable multiple in-progress async requests, we need to also track the request and 
  //  let the caller to specify which request is being peek'ed.
  virtual bool PeekAsyncWrite(uint64_t *out_size) = 0;
};

}  // namespace asi
}  // namespace noname


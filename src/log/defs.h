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

namespace noname {
namespace log {

// Log sequence number (LSN) that uniquely identifies a log record
struct LSN {
  static const uint64_t kInvalidId = -1;
  static const uint64_t kInvalidOffset = -1;

  uint64_t segment_id;
  uint64_t segment_offset;

  LSN() : segment_id(kInvalidId), segment_offset(kInvalidOffset) {}
  LSN(uint64_t seg, uint64_t off) : segment_id(seg), segment_offset(off) {}
  ~LSN() {}

  LSN &operator=(const LSN &other) {
    segment_id = other.segment_id;
    segment_offset = other.segment_offset;
    return *this;
  }

  bool operator==(const LSN &other) {
    return (segment_id == other.segment_id) && (segment_offset == other.segment_offset);
  }

  bool operator>(const LSN &other) {
    return (segment_id > other.segment_id) ||
           (segment_id == other.segment_id && segment_offset > other.segment_offset);
  }

  bool operator<(const LSN &other) {
    return (segment_id < other.segment_id) ||
           (segment_id == other.segment_id && segment_offset < other.segment_offset);
  }
};

}  // namespace log
}  // namespace noname

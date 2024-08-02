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

template <size_t L>
struct GenericTPCCKey {
  char keys[L];
  static_assert(sizeof(char)==1);
  GenericTPCCKey() {
    std::memset(this->keys, 0, L);
  }

  // used because the member function of BTree index 
  //  take parameters by lvalue reference, may need to 
  //   change that later.
  GenericTPCCKey(const GenericTPCCKey &other) {
    std::memcpy(this->keys, other.keys, L);
  }
  
  GenericTPCCKey(GenericTPCCKey &&other) {
    CHECK(false);	  
    std::memcpy(this->keys, other.keys, L);
  }

/*
  int cmp(const GenericTPCCKey &lhs, const GenericTPCCKey &rhs) {
    for (int i = 0; i < L; ++i) {
      if (lhs.keys[i] > rhs.keys[i]) {
        return 1;
      }	else if (lhs.keys[i] < rhs.keys[i]) {
        return -1;
      }
    }
    return 0;
  }
*/
  bool operator==(const GenericTPCCKey &other) const {
    return std::memcmp(this->keys, other.keys, L) == 0;
  }
  bool operator!=(const GenericTPCCKey &other) const {
    return std::memcmp(this->keys, other.keys, L) != 0;
  }
  bool operator<(const GenericTPCCKey &other) const {
     return std::memcmp(this->keys, other.keys, L) < 0;
  }
  bool operator>(const GenericTPCCKey &other) const {
     return std::memcmp(this->keys, other.keys, L) > 0;
  }
  bool operator<=(const GenericTPCCKey &other) const {
     return std::memcmp(this->keys, other.keys, L) <= 0;
  }
  bool operator>=(const GenericTPCCKey &other) const {
     return std::memcmp(this->keys, other.keys, L) >= 0;
  }

  GenericTPCCKey& operator=(const GenericTPCCKey &other) {
    if ( this != &other ) {
      std::memcpy(this->keys, other.keys, L);
    }
    return *this;
  }
 
  GenericTPCCKey& operator=(GenericTPCCKey &&other) {
    CHECK(false);	  
    if ( this != &other ) {
      std::memcpy(this->keys, other.keys, L);
    }
    return *this;
  }
};

template struct GenericTPCCKey<4>;
template struct GenericTPCCKey<8>;
template struct GenericTPCCKey<12>;
template struct GenericTPCCKey<16>;
// template struct GenericTPCCKey<32>;
template struct GenericTPCCKey<40>;


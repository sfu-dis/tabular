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

#include <glog/logging.h>

#include <cstring>
#include <vector>
#include <atomic>

#include "util.h"

namespace noname {
namespace benchmark {
namespace tpcc {

extern uint64_t scale_factor;
extern int g_uniform_item_dist;
extern int g_wh_temperature;
extern double g_wh_spread;
extern int g_new_order_remote_item_pct;
extern int g_disable_xpartition_txn;
extern bool g_order_status_scan_hack;
extern int g_new_order_fast_id_gen;


static inline size_t NumWarehouses() { return (size_t)scale_factor; }

static constexpr inline size_t NumItems() { return 100000; }

static constexpr inline size_t NumDistrictsPerWarehouse() { return 10; }

static constexpr inline size_t NumCustomersPerDistrict() { return 3000; }

// 80/20 access: 80% of all accesses touch 20% of WHs (randmonly
// choose one from hot_whs), while the 20% of accesses touch the
// remaining 80% of WHs.
static std::vector<uint> hot_whs;
static std::vector<uint> cold_whs;

extern util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids;

static inline unsigned pick_wh(util::fast_random &r, uint home_wh) {
  if (g_wh_temperature) {  // do it 80/20 way
    uint w = 0;
    if (r.next_uniform() >= 0.2) {  // 80% access
      w = hot_whs[r.next() % hot_whs.size()];
    } else {
      w = cold_whs[r.next() % cold_whs.size()];
    }
    LOG_IF(FATAL, w < 1 || w > NumWarehouses());
    return w;
  } else {
    // wh_spread = 0: always use home wh
    // wh_spread = 1: always use random wh
    if (g_wh_spread == 0 || r.next_uniform() >= g_wh_spread) {
      return home_wh;
    }
    return r.next() % NumWarehouses() + 1;
  }
}

// following oltpbench, we really generate strings of len - 1...
static inline std::string RandomStr(util::fast_random &r, uint len) {
  // this is a property of the oltpbench implementation...
  if (!len) return "";

  uint i = 0;
  std::string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = (char)r.next_char();
    // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
    // is a less restrictive filter than isalnum()
    if (!isalnum(c)) {
      continue;
    }
    buf[i++] = c;
  }
  return buf;
}

// RandomNStr() actually produces a std::string of length len
static inline std::string RandomNStr(util::fast_random &r, uint len) {
  const char base = '0';
  std::string buf(len, 0);
  for (uint i = 0; i < len; i++) {
    buf[i] = (char)(base + (r.next() % 10));
  }
  return buf;
}

static inline int RandomNumber(util::fast_random &r, int min, int max) {
  return (int)(r.next_uniform() * (max - min + 1) + min);
}

static inline int NonUniformRandom(util::fast_random &r, int A, int C, int min, int max) {
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) %
          (max - min + 1)) + min;
}

static inline int GetItemId(util::fast_random &r) {
  return g_uniform_item_dist ? RandomNumber(r, 1, NumItems())
                             : NonUniformRandom(r, 8191, 7911, 1, NumItems());
}

static inline int GetCustomerId(util::fast_random &r) {
  return NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict());
}

static std::string NameTokens[] = {
  "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
};

// all tokens are at most 5 chars long
static const size_t CustomerLastNameMaxSize = 5 * 3;

static inline size_t GetCustomerLastName(uint8_t *buf, int num) {
  const std::string &s0 = NameTokens[num / 100];
  const std::string &s1 = NameTokens[(num / 10) % 10];
  const std::string &s2 = NameTokens[num % 10];
  uint8_t *const begin = buf;
  const size_t s0_sz = s0.size();
  const size_t s1_sz = s1.size();
  const size_t s2_sz = s2.size();
  memcpy(buf, s0.data(), s0_sz);
  buf += s0_sz;
  memcpy(buf, s1.data(), s1_sz);
  buf += s1_sz;
  memcpy(buf, s2.data(), s2_sz);
  buf += s2_sz;
  return buf - begin;
}

static inline std::string GetCustomerLastName(int num) {
  std::string ret;
  ret.resize(CustomerLastNameMaxSize);
  ret.resize(GetCustomerLastName((uint8_t *)&ret[0], num));
  return ret;
}

static inline std::string GetNonUniformCustomerLastNameLoad(util::fast_random &r) {
  return GetCustomerLastName(NonUniformRandom(r, 255, 157, 0, 999));
}

static inline std::string GetNonUniformCustomerLastNameRun(util::fast_random &r) {
  return GetCustomerLastName(NonUniformRandom(r, 255, 223, 0, 999));
}

static inline size_t GetNonUniformCustomerLastNameRun(uint8_t *buf, util::fast_random &r) {
  return GetCustomerLastName(buf, NonUniformRandom(r, 255, 223, 0, 999));
}

static inline size_t GetNonUniformCustomerLastNameRun(char *buf, util::fast_random &r) {
  return GetNonUniformCustomerLastNameRun((uint8_t *)buf, r);
}


static inline std::atomic<uint64_t> &NewOrderIdHolder(unsigned warehouse,
                                                 unsigned district) {
  CHECK(warehouse >= 1 && warehouse <= NumWarehouses());
  CHECK(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
      (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t FastNewOrderIdGen(unsigned warehouse,
                                         unsigned district) {
  return NewOrderIdHolder(warehouse, district)
      .fetch_add(1, std::memory_order_acq_rel);
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace noname

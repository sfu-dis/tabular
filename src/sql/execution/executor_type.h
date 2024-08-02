/*
 * YASE: Yet Another Storage Engine
 *
 * CMPT 454 Database Systems II
 *
 * Copyright (C) School of Computing Science, Simon Fraser University
 *
 * Not for distribution without prior approval.
 */

#pragma once

namespace noname {
namespace sql {

enum class ExecutorType : uint8_t {
  INVALID = 0,
  SCAN = 1,
  FILTER = 2,
  PROJECTION = 3,
  JOIN = 4,
  AGGREGATION = 5,
  UPDATE = 6,
  DELETE = 7,
  ORDER = 8,
  LIMIT = 9,
  ALTER = 10,
  CREATE_INDEX = 11
};

}  // namespace sql
}  // namespace noname


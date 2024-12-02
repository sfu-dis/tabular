// TODO(ziyi) add bptree when it supports updates
#include <glog/logging.h>
#include <gtest/gtest.h>

// #include "benchmarks/ycsb/art_wrapper.h"
#include "benchmarks/ycsb/masstree_wrapper.h"
#include "benchmarks/ycsb/bptree_wrapper.h"

template <typename T>
class YCSBIndexWrapperTest : public testing::Test {};

using YCSBIndexWrapperTypes = ::testing::Types<
    noname::benchmark::ycsb::MasstreeWrapper<uint64_t, uint64_t>,
    noname::benchmark::ycsb::BPTreeWrapper<uint64_t, uint64_t>
    // TODO(ziyi) art index wrapper tests need its values to be an indirection
    // to a record with the original key installed
    /* , noname::benchmark::ycsb::ARTWrapper<uint64_t, uint64_t> */>;

TYPED_TEST_SUITE(YCSBIndexWrapperTest, YCSBIndexWrapperTypes);

TYPED_TEST(YCSBIndexWrapperTest, SingleThreadedInsertReadUpdate) {
  // constexpr size_t NUM_KEYS = 1024;
  constexpr size_t NUM_KEYS = 1024;

  auto index = TypeParam();
  bool ok;
  for (size_t i = 1; i <= NUM_KEYS; i++) {
    ok = index.insert(i, i);
    CHECK(ok);
  }

  for (size_t i = 1; i <= NUM_KEYS; i++) {
    uint64_t value = 0;
    ok = index.lookup(i, value);
    CHECK(ok);
    CHECK_EQ(value, i);
  }

  for (size_t i = 1; i <= NUM_KEYS; i++) {
    ok = index.update(i, i + 1);
    CHECK(ok);
  }

  for (size_t i = 1; i <= NUM_KEYS; i++) {
    uint64_t value = 0;
    ok = index.lookup(i, value);
    CHECK(ok);
    CHECK_EQ(value, i + 1);
  }
}

int main(int argc, char **argv) {
  ::google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

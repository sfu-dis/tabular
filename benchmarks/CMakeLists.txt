add_library(perf perf.cc)
target_link_libraries(perf thread config)
add_subdirectory(ycsb)
add_subdirectory(tpcc)

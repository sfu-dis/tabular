set(TPCC_SRC tpcc_bench.cc tpcc_load.cc tpcc_util.cc)

add_subdirectory(procedures)

macro(add_tpcc)
  cmake_parse_arguments(TPCC "" "NAME" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_executable(${TPCC_NAME} ${TPCC_SRC})
  target_link_libraries(${TPCC_NAME} gtest glog gflags perf)
  # target_link_libraries(${TPCC_NAME} noname query_benchmark tpcc_procedure)
  target_link_libraries(${TPCC_NAME} noname tpcc_procedure)
  target_link_libraries(${TPCC_NAME} tabular inline_table transaction ia_table dynarray config dlog object)
  target_link_libraries(${TPCC_NAME} ${TPCC_LIBRARIES})
  target_link_libraries(${TPCC_NAME} ${JEMALLOC_LIBRARIES})
  target_compile_definitions(${TPCC_NAME} PUBLIC ${TPCC_DEFINITIONS})
endmacro()

add_tpcc(
        NAME tpcc_masstree_bench
	LIBRARIES query_benchmark_masstree
)

add_tpcc(
        NAME tpcc_naive_tabular_btree_bench
	LIBRARIES query_benchmark_naive_tabular_btree
)


add_tpcc(

        NAME tpcc_tabular_btree_bench
	LIBRARIES query_benchmark_tabular_btree
)

add_tpcc(

        NAME tpcc_materialized_tabular_btree_bench
	LIBRARIES query_benchmark_materialized_tabular_btree
)

add_tpcc(
        NAME tpcc_olc_btree_bench
	LIBRARIES query_benchmark_olc_btree
)


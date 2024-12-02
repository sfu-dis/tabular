add_executable(index_wrapper_test index_wrapper_test.cc)
set_target_properties(index_wrapper_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/ycsb)
target_compile_definitions(index_wrapper_test PUBLIC OMCS_LOCK)
target_link_libraries(index_wrapper_test ycsb_masstree artolc_upgrade)
target_include_directories(index_wrapper_test PUBLIC
                            "${CMAKE_SOURCE_DIR}/third-party/bptree/ParallelTools"
                            "${CMAKE_SOURCE_DIR}/third-party/bptree/tlx-leafds"
                            ${masstree_SOURCE_DIR})
add_test(NAME index_wrapper_test COMMAND index_wrapper_test)

add_executable(bptree_test bptree_test.cc)
set_target_properties(bptree_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/ycsb)
target_include_directories(bptree_test PUBLIC "${CMAKE_SOURCE_DIR}/third-party/bptree/ParallelTools" "${CMAKE_SOURCE_DIR}/third-party/bptree/tlx-leafds")
if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  target_compile_definitions(bptree_test PUBLIC DEBUG=1)
else()
  target_compile_definitions(bptree_test PUBLIC DEBUG=0)
endif ()
add_test(NAME bptree_test COMMAND bptree_test)

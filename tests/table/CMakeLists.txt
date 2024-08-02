add_executable(dynarray_test dynarray_test.cc)
set_target_properties(dynarray_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/table)
target_link_libraries(dynarray_test dynarray config)
add_test(NAME dynarray_test COMMAND dynarray_test)

add_executable(ia_table_test ia_table_test.cc)
set_target_properties(ia_table_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/table)
target_link_libraries(ia_table_test ia_table object dynarray config)
add_test(NAME ia_table_test COMMAND ia_table_test)

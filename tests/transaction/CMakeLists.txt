add_executable(transaction_test transaction_test.cc)
set_target_properties(transaction_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/transaction)
target_link_libraries(transaction_test transaction object ia_table dynarray config)
add_test(NAME transaction_test COMMAND transaction_test)

add_executable(occ_test occ_test.cc)
set_target_properties(occ_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/transaction)
target_link_libraries(occ_test occ object ia_table dynarray config dlog tabular)
add_test(NAME occ_test COMMAND occ_test)

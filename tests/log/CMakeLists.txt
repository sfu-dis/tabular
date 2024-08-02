add_executable(segment_tests segment_tests.cc)
set_target_properties(segment_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
target_link_libraries(segment_tests dlog config uring)
add_test(NAME segment_tests COMMAND segment_tests)

add_executable(dlog_tests dlog_tests.cc)
set_target_properties(dlog_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
target_link_libraries(dlog_tests dlog config uring)
add_test(NAME dlog_tests COMMAND dlog_tests)

add_executable(clog_tests clog_tests.cc)
set_target_properties(clog_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
target_link_libraries(clog_tests clog config uring)
add_test(NAME clog_tests COMMAND clog_tests)



set_property(GLOBAL APPEND PROPERTY DLOG_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/../segment.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/log.cc
)
get_property(DLOG_SRC GLOBAL PROPERTY DLOG_SRC)
add_library(dlog SHARED ${DLOG_SRC})
target_link_libraries(dlog io_uring_storage glog)

set_property(GLOBAL APPEND PROPERTY DLOG_NOP_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/../segment.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/log.cc
)
get_property(DLOG_NOP_SRC GLOBAL PROPERTY DLOG_NOP_SRC)
add_library(dlog_nop SHARED ${DLOG_NOP_SRC})
target_link_libraries(dlog_nop nop_storage glog)
target_compile_definitions(dlog_nop PUBLIC LOG_NOP)

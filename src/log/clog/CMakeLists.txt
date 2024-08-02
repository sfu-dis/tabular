set_property(GLOBAL APPEND PROPERTY CLOG_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/../segment.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/log.cc
)
get_property(CLOG_SRC GLOBAL PROPERTY CLOG_SRC)
add_library(clog SHARED ${CLOG_SRC})
target_link_libraries(clog io_uring_storage glog)


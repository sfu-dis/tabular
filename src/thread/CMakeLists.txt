set_property(GLOBAL APPEND PROPERTY UTIL_THREAD_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/thread.cc
)
get_property(UTIL_THREAD_SRC GLOBAL PROPERTY UTIL_THREAD_SRC)
add_library(thread ${UTIL_THREAD_SRC})
target_link_libraries(thread glog config)

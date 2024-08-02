set_property(GLOBAL APPEND PROPERTY IO_URING_STORAGE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/io_uring_storage.cc
)
get_property(IO_URING_STORAGE_SRC GLOBAL PROPERTY IO_URING_STORAGE_SRC)
add_library(io_uring_storage SHARED ${IO_URING_STORAGE_SRC})
target_link_libraries(io_uring_storage glog)

set_property(GLOBAL APPEND PROPERTY NOP_STORAGE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/nop_storage.cc
)
get_property(NOP_STORAGE_SRC GLOBAL PROPERTY NOP_STORAGE_SRC)
add_library(nop_storage SHARED ${NOP_STORAGE_SRC})
target_link_libraries(nop_storage glog)

set_property(GLOBAL APPEND PROPERTY SYNC_STORAGE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/io_uring_storage.cc
)
get_property(SYNC_STORAGE_SRC GLOBAL PROPERTY SYNC_STORAGE_SRC)
add_library(sync_storage sync_storage.cc)
target_link_libraries(sync_storage glog)

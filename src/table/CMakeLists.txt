set_property(GLOBAL APPEND PROPERTY TABLE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cc
)
get_property(TABLE_SRC GLOBAL PROPERTY TABLE_SRC)
add_library(dynarray ${TABLE_SRC})
target_link_libraries(dynarray glog)

set_property(GLOBAL APPEND PROPERTY IA_TABLE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/ia_table.cc
)
get_property(IA_TABLE_SRC GLOBAL PROPERTY IA_TABLE_SRC)
add_library(ia_table ${IA_TABLE_SRC})
target_link_libraries(ia_table dynarray glog)
if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC jemalloc)
  pkg_search_module(JEMALLOC REQUIRED jemalloc)
  include_directories(${JEMALLOC_INCLUDE_DIRS})
  target_link_libraries(ia_table ${JEMALLOC_LIBRARIES})
endif ()

set_property(GLOBAL APPEND PROPERTY INLINE_TABLE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/inline_table.cc
)
get_property(INLINE_TABLE_SRC GLOBAL PROPERTY INLINE_TABLE_SRC)
add_library(inline_table ${INLINE_TABLE_SRC})
target_link_libraries(inline_table dynarray glog)

 set_property(GLOBAL APPEND PROPERTY OBJECT_SRC
   ${CMAKE_CURRENT_SOURCE_DIR}/object.cc
 )
 get_property(OBJECT_SRC GLOBAL PROPERTY OBJECT_SRC)
 add_library(object ${OBJECT_SRC})
 target_link_libraries(object dynarray glog)

set_property(GLOBAL APPEND PROPERTY TUPLE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/tuple.cc
)
get_property(TUPLE_SRC GLOBAL PROPERTY TUPLE_SRC)
add_library(tuple ${TUPLE_SRC})
target_link_libraries(tuple glog)

set_property(GLOBAL APPEND PROPERTY TABULAR_SRC
${CMAKE_CURRENT_SOURCE_DIR}/table_group.cc
)

get_property(TABULAR_SRC GLOBAL PROPERTY TABULAR_SRC)
add_library(tabular ${TABULAR_SRC})
target_link_libraries(tabular dynarray glog)

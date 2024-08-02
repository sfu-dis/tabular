set_property(GLOBAL APPEND PROPERTY NONAME_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/noname.cc
)
get_property(NONAME_SRC GLOBAL PROPERTY NONAME_SRC)
add_library(noname ${NONAME_SRC})
target_link_libraries(noname transaction-normal tuple)


set_property(GLOBAL APPEND PROPERTY TRANSACTION_NORMAL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/transaction-normal.cc
)
get_property(TRANSACTION_NORMAL_SRC GLOBAL PROPERTY TRANSACTION_NORMAL_SRC)
add_library(transaction-normal ${TRANSACTION_NORMAL_SRC})
target_link_libraries(transaction-normal dlog ia_table dynarray glog)

set_property(GLOBAL APPEND PROPERTY TRANSACTION_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/transaction.cc
)
get_property(TRANSACTION_SRC GLOBAL PROPERTY TRANSACTION_SRC)
add_library(transaction ${TRANSACTION_SRC})
target_link_libraries(transaction tabular ia_table dynarray glog)

set_property(GLOBAL APPEND PROPERTY OCC_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/occ.cc
)
get_property(OCC_SRC GLOBAL PROPERTY OCC_SRC)
add_library(occ ${OCC_SRC})
target_link_libraries(occ inline_table dynarray glog)

include_directories(${masstree_SOURCE_DIR}/)

# Masstree
add_library(
  masstree_wrapper
  SHARED
  masstree_wrapper.cc
  ${masstree_SOURCE_DIR}/compiler.cc
  ${masstree_SOURCE_DIR}/straccum.cc
  ${masstree_SOURCE_DIR}/str.cc
  ${masstree_SOURCE_DIR}/string.cc
)
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  target_compile_options(masstree_wrapper PRIVATE -fsanitize=undefined)
  target_link_options(masstree_wrapper PRIVATE -fsanitize=undefined)
endif ()

# Inline BTree
add_library(
  inline_btree_wrapper
  SHARED
  inline_btree_wrapper.cc
)

target_compile_definitions(
  inline_btree_wrapper
  PUBLIC
  BTREE_INLINE_TABULAR
)

target_link_libraries(inline_btree_wrapper tabular inline_table 
  transaction ia_table dynarray config dlog occ
)

# Materialized Inline BTree
add_library(
  materialized_tabular_btree_wrapper
  SHARED
  materialized_tabular_btree_wrapper.cc
)

target_compile_definitions(
  materialized_tabular_btree_wrapper
  PUBLIC
  BTREE_MATERIALIZED_INLINE_TABULAR
)

target_link_libraries(materialized_tabular_btree_wrapper tabular inline_table 
  transaction ia_table dynarray config dlog occ
)


# Naive Inline BTree
add_library(
  naive_btree_wrapper
  SHARED
  naive_btree_wrapper.cc
)

target_compile_definitions(
  naive_btree_wrapper
  PUBLIC
  BTREE_NAIVE_TABULAR
)

target_link_libraries(naive_btree_wrapper tabular inline_table 
  transaction ia_table dynarray config dlog occ
)



# NATIVE OLC BTree
add_library(
  olc_btree_wrapper
  SHARED
  olc_btree_wrapper.cc
)

target_compile_definitions(
	olc_btree_wrapper
	PUBLIC
	BTREE_NATIVE_OLC 
)

# TODO(mohamed) : keep only necessaty dependencies
target_link_libraries(olc_btree_wrapper inline_table 
  transaction ia_table dynarray config dlog
)



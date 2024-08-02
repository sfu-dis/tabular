include_directories(${masstree_SOURCE_DIR}/)

add_library(inline_btree_catalog catalog.cc)
target_compile_definitions(
  inline_btree_catalog 
  PUBLIC 
  BTREE_INDEX
  BTREE_INLINE_TABULAR
)

add_library(olc_btree_catalog catalog.cc)
target_compile_definitions(
  olc_btree_catalog 
  PUBLIC 
  BTREE_INDEX
  BTREE_NATIVE_OLC  
)

add_library(naive_tabular_btree_catalog catalog.cc)
target_compile_definitions(
  naive_tabular_btree_catalog 
  PUBLIC 
  BTREE_INDEX
  BTREE_NAIVE_TABULAR
)

add_library(materialized_tabular_btree_catalog catalog.cc)
target_compile_definitions(
  materialized_tabular_btree_catalog 
  PUBLIC 
  BTREE_INDEX
  BTREE_MATERIALIZED_TABULAR
)



add_library(masstree_catalog catalog.cc)
target_compile_definitions(masstree_catalog PUBLIC MASSTREE_INDEX)


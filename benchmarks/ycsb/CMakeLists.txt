set(YCSB_SRC ycsb_bench.cc ycsb_loader.cc ycsb_worker.cc)

macro(add_ycsb)
  cmake_parse_arguments(YCSB "" "NAME" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_executable(${YCSB_NAME} ${YCSB_SRC})
  target_link_libraries(${YCSB_NAME} gtest glog perf)
  target_link_libraries(${YCSB_NAME} tabular inline_table object ia_table dynarray config)
  target_link_libraries(${YCSB_NAME} ${YCSB_LIBRARIES})
  target_link_libraries(${YCSB_NAME} ${JEMALLOC_LIBRARIES})
  target_include_directories(${YCSB_NAME} PUBLIC foedus)
  target_compile_definitions(${YCSB_NAME} PUBLIC ${YCSB_DEFINITIONS})
endmacro()

list(APPEND node_sizes 256 512 1024 2048 4096 8192 16384)
list(APPEND node_size_suffixes "_256" "_512" "_1K" "_2K" "_4K" "_8K" "_16K")

foreach(node_size node_size_suffix IN ZIP_LISTS node_sizes node_size_suffixes)
  add_ycsb(
    NAME ycsb_btree_optiql_bench${node_size_suffix}
    DEFINITIONS BTREE_OPTIQL OMCS_LOCK OMCS_OP_READ OMCS_OFFSET OMCS_OFFSET_NUMA_QNODE
                BTREE_PAGE_SIZE=${node_size}
  )

  add_ycsb(
    NAME ycsb_btree_olc_bench${node_size_suffix}
    DEFINITIONS BTREE_OLC OMCS_LOCK
                BTREE_PAGE_SIZE=${node_size}
  )

  add_ycsb(
    NAME ycsb_btree_lc_stdrw_bench${node_size_suffix}
    DEFINITIONS BTREE_LC_STDRW RWLOCK STDRW_LOCK
                BTREE_PAGE_SIZE=${node_size}
  )

  add_ycsb(
    NAME ycsb_btree_lc_tbbrw_bench${node_size_suffix}
    DEFINITIONS BTREE_LC_TBBRW RWLOCK TBBRW_LOCK
                BTREE_PAGE_SIZE=${node_size}
    LIBRARIES TBB::tbb
  )

  add_ycsb(
    NAME ycsb_btree_tabular_bench${node_size_suffix}
    DEFINITIONS BTREE_TABULAR
                BTREE_PAGE_SIZE=${node_size}
    LIBRARIES transaction
  )

  add_ycsb(
    NAME ycsb_btree_inline_tabular_bench${node_size_suffix}
    DEFINITIONS BTREE_INLINE_TABULAR
                BTREE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog
  )

  add_ycsb(
    NAME ycsb_btree_inline_tabular_materialized_bench${node_size_suffix}
    DEFINITIONS BTREE_INLINE_TABULAR MATERIALIZED_READ MATERIALIZED_UPDATE MATERIALIZED_INSERT
                BTREE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog
  )

  add_ycsb(
    NAME ycsb_btree_inline_tabular_log_nop_bench${node_size_suffix}
    DEFINITIONS BTREE_INLINE_TABULAR
                BTREE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog_nop
  )

  add_ycsb(
    NAME ycsb_btree_inline_tabular_materialized_log_nop_bench${node_size_suffix}
    DEFINITIONS BTREE_INLINE_TABULAR MATERIALIZED_READ MATERIALIZED_UPDATE MATERIALIZED_INSERT
                BTREE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog_nop
  )

  add_ycsb(
    NAME ycsb_hashtable_olc_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_OPTIMISTIC_LC
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog
  )

  add_ycsb(
    NAME ycsb_hashtable_lc_stdrw_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_LC_STDRW RWLOCK STDRW_LOCK
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog
  )

  add_ycsb(
    NAME ycsb_hashtable_lc_tbbrw_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_LC_TBBRW RWLOCK TBBRW_LOCK
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog TBB::tbb
  )

  add_ycsb(
    NAME ycsb_hashtable_mvcc_tabular_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_MVCC_TABULAR
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES transaction dlog
  )

  add_ycsb(
    NAME ycsb_hashtable_inline_tabular_materialized_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_INLINE_TABULAR HASHTABLE_MATERIALIZED_READ HASHTABLE_MATERIALIZED_UPDATE
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog
  )

  add_ycsb(
    NAME ycsb_hashtable_inline_tabular_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_INLINE_TABULAR
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog
  )

  add_ycsb(
    NAME ycsb_hashtable_inline_tabular_materialized_log_nop_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_INLINE_TABULAR HASHTABLE_MATERIALIZED_READ HASHTABLE_MATERIALIZED_UPDATE 
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog_nop
  )

  add_ycsb(
    NAME ycsb_hashtable_inline_tabular_log_nop_bench${node_size_suffix}
    DEFINITIONS HASHTABLE_INLINE_TABULAR
                HASHTABLE_PAGE_SIZE=${node_size}
    LIBRARIES occ dlog_nop
  )

endforeach()

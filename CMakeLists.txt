cmake_minimum_required(VERSION 3.8)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

project(noname)

enable_testing()

set(CMAKE_CXX_CPPLINT "cpplint;--quiet;--linelength=100;--filter=-build/c++11,-build/include_subdir,-whitespace/comments;--exclude=${CMAKE_SOURCE_DIR}/src/config/config.cc")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -flto=auto")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto=auto -fuse-linker-plugin")

# Add a custom build mode: https://gitlab.kitware.com/cmake/community/-/wikis/FAQ#how-can-i-extend-the-build-modes-with-a-custom-made-one-
set(CMAKE_CXX_FLAGS_RWDI "-O2 -g -DNDEBUG -fno-omit-frame-pointer -fno-inline")
set(CMAKE_CXX_FLAGS_RO2 "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RO1 "-O1 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RO0 "-O0 -DNDEBUG")

# -funroll-completely-grow-size is also one of the options enabled in -O3. But it is an internal undocumented flag which cannot be enabled through command-line arguments: https://github.com/gcc-mirror/gcc/blob/886f256ce3be4aa85f30af88558f0dfcb8003300/gcc/common.opt#L3188C29-L3188C29
set(O3_OVER_O2_FLAGS "-fgcse-after-reload -fipa-cp-clone -floop-interchange -floop-unroll-and-jam -fpeel-loops -fpredictive-commoning -fsplit-loops -fsplit-paths -ftree-loop-distribution -ftree-partial-pre -funswitch-loops -fvect-cost-model=dynamic -fversion-loops-for-strides")
set(CMAKE_CXX_FLAGS_REL0  "-O2 ${O3_OVER_O2_FLAGS} -DNDEBUG")

# OPTIMAL_FLAGS has all flags from O3_OVER_O2_FLAGS except -fpeel-loops
set(OPTIMAL_FLAGS "-fgcse-after-reload -fipa-cp-clone -floop-interchange -floop-unroll-and-jam -fpredictive-commoning -fsplit-loops -fsplit-paths -ftree-loop-distribution -ftree-partial-pre -funswitch-loops -fvect-cost-model=dynamic -fversion-loops-for-strides")
set(CMAKE_CXX_FLAGS_OPTIMAL  "-O2 ${OPTIMAL_FLAGS} -DNDEBUG")


set(CMAKE_CXX_FLAGS_REL1  "-O2 -fgcse-after-reload -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL2  "-O2 -fipa-cp-clone -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL3  "-O2 -floop-interchange -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL4  "-O2 -floop-unroll-and-jam -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL5  "-O2 -fpeel-loops -DNDEBUG")

set(CMAKE_CXX_FLAGS_REL6  "-O2 -fpredictive-commoning -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL7  "-O2 -fsplit-loops -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL8  "-O2 -fsplit-paths -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL9  "-O2 -ftree-loop-distribution -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL10 "-O2 -ftree-partial-pre -DNDEBUG")

set(CMAKE_CXX_FLAGS_REL11 "-O2 -funswitch-loops -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL12 "-O2 -fvect-cost-model=dynamic -DNDEBUG")
set(CMAKE_CXX_FLAGS_REL13 "-O2 -fversion-loops-for-strides -DNDEBUG")

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  message("-- Debug mode")
else()
  message("-- ${CMAKE_BUILD_TYPE} mode, using jemalloc")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC jemalloc)
  pkg_search_module(JEMALLOC REQUIRED jemalloc)
  include_directories(${JEMALLOC_INCLUDE_DIRS})
endif ()

find_package(TBB REQUIRED)

set(LINK_FLAGS "-lnuma -lpthread -luring -pthread -lrt -lglog -lgtest")
link_libraries(${LINK_FLAGS})

# Store shared libraries under /lib
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Assume 64-byte cache line
add_definitions(-DCACHELINE_SIZE=64)
# glog headers require this definition
add_definitions(-DGLOG_USE_GLOG_EXPORT)

include(FetchContent)

# DuckDB
FetchContent_Declare(
  duckdb
  GIT_REPOSITORY https://github.com/duckdb/duckdb.git
  GIT_TAG 58816cf7dfe6c47690bd9a1d117b3cfc8eb9cd25
)
FetchContent_GetProperties(duckdb)
if (NOT duckdb_POPULATED)
  message("-- Downloading DuckDB ...")
  FetchContent_Populate(duckdb)
  message("-- Finish")
  include_directories(${duckdb_SOURCE_DIR}/src/include/)
  if(NOT EXISTS ${duckdb_SOURCE_DIR}/src/amalgamation/)
    execute_process(COMMAND "python3" ${duckdb_SOURCE_DIR}/scripts/amalgamation.py --extended --splits=4 WORKING_DIRECTORY ${duckdb_SOURCE_DIR})
    execute_process(COMMAND "cp" ${CMAKE_CURRENT_SOURCE_DIR}/third-party/duckdb/CMakeLists.txt ${duckdb_SOURCE_DIR}/src/amalgamation/)
  endif ()
  add_subdirectory(${duckdb_SOURCE_DIR}/src/amalgamation/)
endif ()

# Masstree
FetchContent_Declare(
  masstree
  GIT_REPOSITORY https://github.com/kohler/masstree-beta.git
  GIT_TAG 11198427a1170654ca646dd20d96c8f349bca2bd
)
FetchContent_GetProperties(masstree)
if (NOT masstree_POPULATED)
  FetchContent_Populate(masstree)
endif ()

# Include pre-generated Masstree config
if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/third-party/masstree/config-debug.h")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include ${CMAKE_CURRENT_SOURCE_DIR}/third-party/masstree/config-release.h")
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third-party)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
link_directories(${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

add_subdirectory(src/asi)
add_subdirectory(src/catalog)
add_subdirectory(src/log/clog)
add_subdirectory(src/log/dlog)
add_subdirectory(src/config)
add_subdirectory(src/engine)
add_subdirectory(src/index/wrappers)
add_subdirectory(src/sql)
add_subdirectory(src/table)
add_subdirectory(src/thread)
add_subdirectory(src/transaction)
add_subdirectory(src/tabular)
add_subdirectory(tests)
add_subdirectory(benchmarks)

#get_property(LIB_SRC GLOBAL PROPERTY ALL_SRC)

# Engine library
#add_library(noname SHARED ${LIB_SRC})
#set_target_properties(noname PROPERTIES COMPILE_FLAGS "")

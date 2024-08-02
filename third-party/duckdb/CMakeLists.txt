# This file will be copied to ${duckdb_SOURCE_DIR}/src/amalgamation/,
# so that we can get a duckdb library there

# Original duckdb has these warnings when compiling,
# add these options to handle.
add_compile_options(-Wno-maybe-uninitialized)
add_compile_options(-Wno-unused-function)
add_compile_options(-Wno-stringop-overflow)
add_compile_options(-Wno-uninitialized)

add_library(
  duckdb
  duckdb-1.cpp
  duckdb-2.cpp
  duckdb-3.cpp
  duckdb-4.cpp
  duckdb-fastpforlib.cpp
  duckdb-fmt.cpp
  duckdb-fsst.cpp
  duckdb-hyperloglog.cpp
  duckdb-libpg_query.cpp
  duckdb-mbedtls.cpp
  duckdb-miniz.cpp
  duckdb-re2.cpp
  duckdb-utf8proc.cpp
)

# Errors exist when using C++20, so use C++11 to compile duckdb
set_property(TARGET duckdb PROPERTY CXX_STANDARD 11)

# Now we have a duckdb target, just reuse it, no need to compile again
# add_library(duckdb IMPORTED GLOBAL)
# set_target_properties(duckdb PROPERTIES IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/lib/libduckdb.a")

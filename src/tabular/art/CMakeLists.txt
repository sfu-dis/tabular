macro(add_libart)
  cmake_parse_arguments(LIBART "" "NAME" "DEFINITIONS;LIBRARIES" ${ARGN})

  add_library(${LIBART_NAME} ${ART_TABULAR_SRC})
  target_link_libraries(${LIBART_NAME} glog ${LIBART_LIBRARIES})
  target_compile_definitions(${LIBART_NAME} PUBLIC ${LIBART_DEFINITIONS})
endmacro()

set(ART_TABULAR_SRC Tree.cpp N.cpp N4.cpp N16.cpp N48.cpp N256.cpp)

add_libart(
  NAME art_tabular
  DEFINITIONS OMCS_LOCK ART_OLC_UPGRADE
  LIBRARIES tabular
)

add_libart(
  NAME art_tabular_materialized
  DEFINITIONS OMCS_LOCK ART_OLC_UPGRADE
              MATERIALIZED_READ MATERIALIZED_UPDATE
  LIBRARIES tabular
)

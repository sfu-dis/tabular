cc_library(
    name = "flat_hash_map",
    hdrs = ["flat_hash_map.hpp"],
)
cc_library(
    name = "parallel",
    hdrs = ["parallel.h"],
    deps = [
        "@parlaylib//parlay:parallel",
    ],
)
cc_library(
    name = "sort",
    hdrs = ["sort.hpp"],
    deps = [
        "@parlaylib//parlay:primitives",
        "parallel"
    ],
)

cc_library(
    name = "reducer",
    hdrs = ["reducer.h"],
    deps = [
        "parallel",
        "sort"
    ],
)

cc_library(
    name = "concurrent_hash_map",
    hdrs = ["concurrent_hash_map.hpp"],
    deps = [
        "parallel",
        "reducer"
    ],
)


package(
    default_visibility = ["//visibility:public"],
)

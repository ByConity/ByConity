if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/parallel-hashmap/parallel_hashmap/phmap.h")
    message (FATAL_ERROR "submodule contrib/parallel-hashmap is missing. to fix try run: \n git submodule update --init --recursive")
endif ()

set(PARALLEL_HASHMAP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/parallel-hashmap/")
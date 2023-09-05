include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_common_storage_election .)

add_library(clickhouse_common_storage_election ${clickhouse_common_storage_election_headers} ${clickhouse_common_storage_election_sources})

target_link_libraries (clickhouse_common_storage_election PUBLIC clickhouse_common_io common PRIVATE string_utils)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
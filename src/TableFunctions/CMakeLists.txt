include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_table_functions .)

list(REMOVE_ITEM clickhouse_table_functions_sources ITableFunction.cpp TableFunctionFactory.cpp)
list(REMOVE_ITEM clickhouse_table_functions_headers ITableFunction.h TableFunctionFactory.h)

add_library(clickhouse_table_functions ${clickhouse_table_functions_sources})
target_link_libraries(clickhouse_table_functions PRIVATE clickhouse_parsers clickhouse_storages_system dbms)

if (USE_HIVE)
    target_link_libraries(clickhouse_table_functions PRIVATE ${HIVE_METASTORE_LIBRARY} ${THRIFT_LIBRARY})
    target_include_directories (clickhouse_table_functions PRIVATE ${HIVE_METASTORE_INCLUDE_DIR})
endif()
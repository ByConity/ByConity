file(GLOB_RECURSE EXTERNAL_CATALOG_SRCS "*.cpp" "*.h")


add_library(external_catalog_lib ${EXTERNAL_CATALOG_SRCS} )
target_include_directories(external_catalog_lib SYSTEM BEFORE
    PRIVATE ${BYTEKV_INCLUDE_DIR}
    PUBLIC  ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include
    PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/foundationdb/include
    PUBLIC  ${HIVE_METASTORE_INCLUDE_DIR})

target_link_libraries(external_catalog_lib PUBLIC catalog_lib  clickhouse_common_io dbms ${HIVE_METASTORE_LIBRARY} ${THRIFT_LIBRARY})

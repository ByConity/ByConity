file(GLOB_RECURSE EXTERNAL_CATALOG_SRCS "*.cpp" "*.h")


add_library(external_catalog_lib ${EXTERNAL_CATALOG_SRCS} )
target_include_directories(external_catalog_lib SYSTEM BEFORE
    PRIVATE ${BYTEKV_INCLUDE_DIR}
    PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include ${HIVE_METASTORE_INCLUDE_DIR})

target_link_libraries(external_catalog_lib PUBLIC catalog_lib fdb_c clickhouse_common_io)

if(FoundationDB-Client_FOUND)
  target_include_directories(external_catalog_lib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
endif()
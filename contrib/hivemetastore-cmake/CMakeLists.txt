set (SRCS
    ${ClickHouse_SOURCE_DIR}/contrib/hivemetastore/hive_metastore_constants.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hivemetastore/hive_metastore_types.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hivemetastore/ThriftHiveMetastore.cpp
)

add_library(hivemetastore ${SRCS})
target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
target_include_directories(hivemetastore SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/hivemetastore)

# include(${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/ConfigureChecks.cmake ClickHouse_SOURCE_DIR)
# configure_file("${ClickHouse_SOURCE_DIR}/contrib/thrift/build/cmake/config.h.in" "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src/thrift/config.h")
# target_include_directories(hivemetastore PUBLIC ../hivemetastore)
# target_include_directories(hivemetastore PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src)
# target_link_libraries (hivemetastore PRIVATE boost::headers_only)
# target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
# target_link_libraries(hivemetastore PUBLIC consistent-hashing)
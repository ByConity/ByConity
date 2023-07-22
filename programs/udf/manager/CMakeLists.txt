add_subdirectory(resources)
add_executable(udf-manager Init.cpp Manager.cpp)

target_include_directories(udf-manager PRIVATE ${Protobuf_INCLUDE_DIR} ${ClickHouse_SOURCE_DIR}/src ${ClickHouse_SOURCE_DIR}/base)
target_link_libraries(udf-manager PRIVATE ${JEMALLOC_LIBRARIES} ${ZLIB_LIBRARIES} OpenSSL::SSL brpc-static clickhouse_protos_lib Poco::Util fmt udfresource boost::headers_only)
add_dependencies(udf-manager udfresource)

install(TARGETS udf-manager RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT udf)

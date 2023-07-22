add_library(srvstub UDFImpl.cpp UDFServer.cpp)
target_include_directories(srvstub PRIVATE ${Protobuf_INCLUDE_DIR} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${ClickHouse_SOURCE_DIR}/base ${ClickHouse_SOURCE_DIR}/src)
target_link_libraries(srvstub PRIVATE ${ZLIB_LIBRARIES} ${JEMALLOC_LIBRARIES} OpenSSL::SSL cityhash clickhouse_protos_lib brpc-static _boost_headers_only pcg_random fmt PUBLIC roaring Poco::Util)

set (jni_proto_sources common/src/main/proto/hudi.proto)

PROTOBUF_GENERATE_CPP(
    jni_data_proto_sources
    jni_data_proto_headers
    ${jni_proto_sources})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
set (CMAKE_CXX_CLANG_TIDY "")

add_library(jni_data_protos ${jni_data_proto_headers} ${jni_data_proto_sources})
target_include_directories(jni_data_protos SYSTEM PUBLIC ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (jni_data_protos PRIVATE ${Protobuf_LIBRARY})

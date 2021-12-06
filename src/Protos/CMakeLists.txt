protobuf_generate_cpp(dbms_proto_srcs dbms_proto_headers registry.proto)

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(clickhouse_protos_lib ${dbms_proto_srcs} ${dbms_proto_headers})

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_protos_lib PRIVATE "-w")

target_link_libraries (clickhouse_protos_lib PRIVATE ${Protobuf_LIBRARY})
target_include_directories(clickhouse_protos_lib SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/..")
target_include_directories (clickhouse_protos_lib SYSTEM BEFORE PRIVATE ${Protobuf_INCLUDE_DIR})
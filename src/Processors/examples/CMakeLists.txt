add_executable(read_orc read_orc.cpp ${ORC_SRCS})
target_link_libraries(read_orc  
${ARROW_LIBRARY}
clickhouse_functions
clickhouse_aggregate_functions
clickhouse_parsers
clickhouse_storages_system
dbms
clickhouse_common_zookeeper
string_utils
$<TARGET_OBJECTS:protobuf::libprotobuf> )
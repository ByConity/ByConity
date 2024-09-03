set(CLICKHOUSE_DUMPER_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Dumper.cpp ${CMAKE_CURRENT_SOURCE_DIR}/DumpHelper.cpp)
set(CLICKHOUSE_DUMPER_LINK PRIVATE clickhouse_functions clickhouse_table_functions clickhouse_aggregate_functions PUBLIC daemon)
set(CLICKHOUSE_DUMPER_INCLUDE SYSTEM PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

clickhouse_program_add(dumper)

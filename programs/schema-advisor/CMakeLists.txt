set(CLICKHOUSE_SCHEMA_ADVISOR_SOURCES 
    SchemaAdvisor.cpp 
    CodecAdvisor.cpp 
    TypeAdvisor.cpp 
    IndexAdvisor.cpp
    PrewhereAdvisor.cpp  
    SampleColumnReader.cpp
    Statistics.cpp
    CompressedStatisticsCollectBuffer.cpp
    ColumnUsageExtractor.cpp
    MockGlobalContext.cpp
    MockEnvironment.cpp
)

set(CLICKHOUSE_SCHEMA_ADVISOR_LINK 
    PRIVATE
        boost::program_options
        clickhouse_functions
	clickhouse_aggregate_functions
        clickhouse_parsers
        dbms
	clickhouse_storages_system
)

if (CLICKHOUSE_SPLIT_BINARY)
	list(APPEND CLICKHOUSE_SCHEMA_ADVISOR_LINK $<TARGET_OBJECTS:protobuf::libprotobuf>)
endif()

clickhouse_program_add(schema-advisor)

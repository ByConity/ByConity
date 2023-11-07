add_library(
        libmetrics2
        metric_client.cpp
        metric_client.h
        metric_collector.cpp
        metric_collector.h
        metric_helper.cpp
        metric_helper.h
        metrics.cpp
        metrics.h
        )

target_include_directories(libmetrics2 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} PRIVATE ${clickhouse_common_io_headers})
target_link_libraries(libmetrics2 PRIVATE clickhouse_common_io common ch_contrib::msgpack)

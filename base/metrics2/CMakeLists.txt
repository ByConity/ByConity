if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include")
        message(FATAL_ERROR "submodule contrib/msgpack-c is missing. to fix try run: \n git submodule update --init --recursive")
endif()

message(STATUS "Using third msgpack libs: ${ClickHouse_SOURCE_DIR}/contrib/msgpack-c")

add_definitions(
        -Wno-old-style-cast 
        -Wno-reserved-id-macro
        -Wno-zero-as-null-pointer-constant
        -Wno-covered-switch-default 
        -Wno-extra-semi-stmt 
        -Wno-missing-noreturn 
        -Wno-shadow-field-in-constructor 
        -Wno-cast-align)

include_directories(${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include)
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
target_include_directories(libmetrics2 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${clickhouse_common_io_headers})
target_link_libraries(libmetrics2 clickhouse_common_io common msgpackc-static)
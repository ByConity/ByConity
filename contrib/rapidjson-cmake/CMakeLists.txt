set(RAPIDJSON_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rapidjson/include")

add_library(rapidjson INTERFACE)
target_include_directories(rapidjson BEFORE INTERFACE "${RAPIDJSON_INCLUDE_DIR}")

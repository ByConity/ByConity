include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set (LINK_RESOURCE_LIB INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:source_files> -Wl,${NO_WHOLE_ARCHIVE}")

clickhouse_embed_binaries(
    TARGET source_files
    RESOURCES iudf.py overload.py
)

add_library(udfresource ${rsrc} populate.cpp)

target_link_libraries(udfresource PRIVATE ${LINK_RESOURCE_LIB})
target_include_directories(udfresource INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
add_dependencies(udfresource source_files)

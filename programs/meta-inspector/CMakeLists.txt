set(CLICKHOUSE_META_INSPECTOR_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/MetastoreInspector.cpp
)
set(CLICKHOUSE_META_INSPECTOR_LINK
    PRIVATE
        catalog_lib
    PUBLIC
        common
)

clickhouse_program_add(meta-inspector)
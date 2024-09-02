set(CLICKHOUSE_STORAGE_TOOLS_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/StorageTools.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/PartInspector.cpp)
        # ${CMAKE_CURRENT_SOURCE_DIR}/S3StorageChecker.cpp)
set(CLICKHOUSE_STORAGE_TOOLS_LINK PRIVATE clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY} ${ROCKSDB_LIBRARY})

clickhouse_program_add(storage-tools)

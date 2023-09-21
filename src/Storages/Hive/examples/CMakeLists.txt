if (ENABLE_HIVE)
    add_executable(hive_metastore_client hive_metastore_client.cpp)
    target_link_libraries (hive_metastore_client PUBLIC ${HIVE_METASTORE_LIBRARY} ${THRIFT_LIBRARY} boost::program_options)
    target_include_directories (hive_metastore_client PUBLIC ${HIVE_METASTORE_INCLUDE_DIR})

    add_executable(glue_client glue_client.cpp)
    target_link_libraries(glue_client  PUBLIC boost::program_options clickhouse_common_io ${HIVE_METASTORE_LIBRARY}   ${AWS_GLUE_LIBRARY})
    target_include_directories(glue_client SYSTEM PUBLIC ${HIVE_METASTORE_INCLUDE_DIR} ${AWS_GLUE_INCLUDE_DIR})
endif()

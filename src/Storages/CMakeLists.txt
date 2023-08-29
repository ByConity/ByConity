add_subdirectory(MergeTree)
add_subdirectory(System)
add_subdirectory(Hive)

if(ENABLE_EXAMPLES)
    add_subdirectory(examples)
    add_subdirectory(HDFS/examples)
endif()

# tools for verifying hdfs.
add_subdirectory(HDFS/hdfs_tool)
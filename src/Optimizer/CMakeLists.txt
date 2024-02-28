if (ENABLE_TESTS)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
    add_subdirectory(tests)
endif ()

set (CONFIG_BUILD "${CMAKE_CURRENT_BINARY_DIR}/VersionInfo.generated.cpp")
get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)
get_property (BUILD_INCLUDE_DIRECTORIES DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)

configure_file (VersionInfo.generated.cpp.in ${CONFIG_BUILD})
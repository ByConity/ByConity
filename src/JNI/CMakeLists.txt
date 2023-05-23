include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_jni_lib .)
add_library(clickhouse_jni_lib ${clickhouse_jni_lib_sources})
message(STATUS "sources=${clickhouse_jni_lib_sources}, headers=${clickhouse_jni_lib_headers}, ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(clickhouse_jni_lib SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Build with maven
find_program(MVN_EXE mvn)
if (MVN_EXE)
    message(STATUS "Maven executable found: ${MVN_EXE}")
else()
    message(FATAL_ERROR "Maven executeable not found")
endif()

add_custom_target(mvn_build
    COMMAND ${MVN_EXE} -DskipTests clean package
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/jni
    COMMENT "Building with Maven"
)

install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/jni/distribution/target/jni-extension_1.0-SNAPSHOT-jar-with-dependencies.jar
        DESTINATION ${CMAKE_INSTALL_PREFIX}/jar)

# add_dependencies(clickhouse_jni_lib mvn_build)

# protos
add_subdirectory(jni)
target_link_libraries(clickhouse_jni_lib PUBLIC jni_data_protos)

# jni library
target_link_libraries(clickhouse_jni_lib PUBLIC ${JNI_LIBRARIES})
target_include_directories(clickhouse_jni_lib SYSTEM BEFORE PUBLIC ${JNI_INCLUDE_DIRS})

# nanoarrow
set_source_files_properties(JNIArrowStream.cpp PROPERTIES COMPILE_FLAGS "-w") # disable warning from nanoarrow
target_link_libraries(clickhouse_jni_lib PRIVATE nanoarrow)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

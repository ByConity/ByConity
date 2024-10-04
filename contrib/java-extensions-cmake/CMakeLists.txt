set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/java-extensions")

file(GLOB JNI_SOURCES "${LIBRARY_DIR}/cpp/src/*.cpp")
file(GLOB JNI_HEADERS "${LIBRARY_DIR}/cpp/include/jni/*.h")

add_library(clickhouse_jni_lib ${JNI_SOURCES})
target_include_directories(clickhouse_jni_lib SYSTEM PUBLIC "${LIBRARY_DIR}/cpp/include")

# JVM
target_link_libraries(clickhouse_jni_lib PUBLIC jvm)

# Nanoarrow
# disable warning from nanoarrow
set_source_files_properties(JNIArrowStream.cpp PROPERTIES COMPILE_FLAGS "-w")
target_link_libraries(clickhouse_jni_lib PRIVATE nanoarrow)

# Protos
set (jni_proto_sources
    "${LIBRARY_DIR}/java/common/src/main/proto/hudi.proto"
    "${LIBRARY_DIR}/java/common/src/main/proto/paimon.proto"
    )

PROTOBUF_GENERATE_CPP(
    jni_data_proto_sources
    jni_data_proto_headers
    ${jni_proto_sources})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
set (CMAKE_CXX_CLANG_TIDY "")

add_library(jni_data_protos ${jni_data_proto_headers} ${jni_data_proto_sources})
target_include_directories(jni_data_protos SYSTEM PUBLIC ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (jni_data_protos PRIVATE ${Protobuf_LIBRARY})
target_link_libraries(clickhouse_jni_lib PUBLIC jni_data_protos)

# Build java project
option (ENABLE_BUILD_JAVA_EXTENSIONS "Enable build java-extensions" OFF)

if (NOT ENABLE_BUILD_JAVA_EXTENSIONS)
    return()
endif()

find_program(MAVEN_EXECUTABLE mvn)
find_program(JAVA_EXECUTABLE java)

if(NOT MAVEN_EXECUTABLE)
    message(FATAL_ERROR "Maven not found. Please install Maven.")
endif()
if(NOT JAVA_EXECUTABLE)
    message(FATAL_ERROR "Java not found. Please install Java 8.")
endif()
execute_process(
    COMMAND ${JAVA_EXECUTABLE} -version
    OUTPUT_VARIABLE JAVA_VERSION_OUTPUT
    ERROR_VARIABLE JAVA_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE
)
string(REGEX MATCH "version \"([0-9]+)\\.([0-9]+)" JAVA_VERSION_MATCH "${JAVA_VERSION_OUTPUT}")
if(JAVA_VERSION_MATCH)
    string(REGEX REPLACE ".*version \"([0-9]+)\\.([0-9]+).*" "\\1" JAVA_VERSION_MAJOR "${JAVA_VERSION_OUTPUT}")
    string(REGEX REPLACE ".*version \"([0-9]+)\\.([0-9]+).*" "\\2" JAVA_VERSION_MINOR "${JAVA_VERSION_OUTPUT}")
    if(JAVA_VERSION_MAJOR EQUAL 1 AND JAVA_VERSION_MINOR EQUAL 8)
        message(STATUS "Found Java version: ${JAVA_VERSION_MAJOR}.${JAVA_VERSION_MINOR}")
    else()
        message(FATAL_ERROR "Java version 1.8 is required(Neither hither nor lower). Found version: ${JAVA_VERSION_MAJOR}.${JAVA_VERSION_MINOR}")
    endif()
else()
    message(FATAL_ERROR "Failed to determine Java version.")
endif()

set(JAVA_PROJECT_DIR ${ClickHouse_SOURCE_DIR}/contrib/java-extensions/java)
set(JAVA_OUTPUT_DIR ${ClickHouse_BINARY_DIR}/lib/jar)

set(HUDI_JAR_PATH ${JAVA_PROJECT_DIR}/hudi/target/hudi-reader-jar-with-dependencies.jar)
set(LAS_JAR_PATH ${JAVA_PROJECT_DIR}/las/target/las-reader-jar-with-dependencies.jar)
set(PAIMON_JAR_PATH ${JAVA_PROJECT_DIR}/paimon/target/paimon-reader-jar-with-dependencies.jar)
set( ALL_JAR_PATHS
    ${HUDI_JAR_PATH}
    ${LAS_JAR_PATH}
    ${PAIMON_JAR_PATH}
)

file(GLOB_RECURSE JAVA_COMMON_SOURCES ${JAVA_PROJECT_DIR}/common/src/main/*)
file(GLOB_RECURSE JAVA_HUDI_SOURCES ${JAVA_PROJECT_DIR}/hudi/src/main/*)
file(GLOB_RECURSE JAVA_LAS_SOURCES ${JAVA_PROJECT_DIR}/las/src/main/*)
file(GLOB_RECURSE JAVA_PAIMON_SOURCES ${JAVA_PROJECT_DIR}/paimon/src/main/*)

add_custom_command(
    OUTPUT ${ALL_JAR_PATHS}
    COMMAND ${MAVEN_EXECUTABLE} clean package -DskipTests $ENV{BUILD_JAVA_EXTENSION_EXTRA_OPTS} -T 4 2>&1 | tee /dev/tty
    WORKING_DIRECTORY ${JAVA_PROJECT_DIR}
    DEPENDS ${JAVA_COMMON_SOURCES} ${JAVA_HUDI_SOURCES} ${JAVA_LAS_SOURCES} ${JAVA_PAIMON_SOURCES}
    COMMENT "Building Java project with Maven"
    VERBATIM
)

add_custom_target(build_java
    DEPENDS ${ALL_JAR_PATHS}
    VERBATIM
)

add_custom_command(TARGET build_java POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${JAVA_OUTPUT_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${HUDI_JAR_PATH} ${JAVA_OUTPUT_DIR}/hudi-reader-jar-with-dependencies.jar
    COMMAND ${CMAKE_COMMAND} -E copy ${LAS_JAR_PATH} ${JAVA_OUTPUT_DIR}/las-reader-jar-with-dependencies.jar
    COMMAND ${CMAKE_COMMAND} -E copy ${PAIMON_JAR_PATH} ${JAVA_OUTPUT_DIR}/paimon-reader-jar-with-dependencies.jar
    COMMENT "Copying build artifacts to output directory"
)

add_dependencies(clickhouse_jni_lib build_java)

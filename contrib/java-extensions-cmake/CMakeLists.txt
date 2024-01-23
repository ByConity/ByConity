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
set (jni_proto_sources "${LIBRARY_DIR}/java/common/src/main/proto/hudi.proto")

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

if (JAVA_BUILD)
    # Build with maven
    find_program(MVN_EXE mvn)
    if (MVN_EXE)
        message(STATUS "Maven executable found: ${MVN_EXE}")
    else()
        message(FATAL_ERROR "Maven executeable not found")
    endif()

    add_custom_target(mvn_build
        COMMAND ${MVN_EXE} -DskipTests clean package
        WORKING_DIRECTORY ${LIBRARY_DIR}/java
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/lib/jar
        COMMAND mv ${LIBRARY_DIR}/java/las/target/las-reader-jar-with-dependencies.jar ${CMAKE_BINARY_DIR}/lib/jar
        COMMENT "Building with Maven"
    )
    add_dependencies(clickhouse_jni_lib mvn_build)
endif ()
    # Download from remote repository
#     set(JAR_URL "https://maven.byted.org/repository/releases/org/byconity/las/1.0.0.2/las-1.0.0.2-jar-with-dependencies.jar")
#     set(JAR_FILE "${CMAKE_BINARY_DIR}/lib/jar/las-reader-jar-with-dependencies.jar")

#     add_custom_target(fetch_jar
#         COMMAND mkdir -p ${CMAKE_BINARY_DIR}/lib/jar
#         COMMAND curl -o "${JAR_FILE}" "${JAR_URL}"
#         COMMENT "Download Jar"
#     )
#     add_dependencies(clickhouse_jni_lib fetch_jar)
# endif()

# install
# install (
#     DIRECTORY "${CMAKE_BINARY_DIR}/lib/jar"
#     DESTINATION ${CMAKE_INSTALL_LIBDIR}
#     COMPONENT clickhouse
#     FILES_MATCHING
#     PATTERN "*.jar"
# )

option (ENABLE_JAVA_EXTENSIONS "Enable java-extensions" OFF)

if (NOT ENABLE_JAVA_EXTENSIONS)
    return()
endif()

find_package(JNI)
if(JNI_FOUND)
    # include_directories(${JNI_INCLUDE_DIRS})
    # target_link_libraries(my_target ${JNI_LIBRARIES})
    set (USE_JNI 1)
endif()
message (STATUS "Using jni=${USE_JNI}: ${JNI_INCLUDE_DIRS} : ${JNI_LIBRARIES}")

# optional nanoarrow library
if (EXISTS "${ClickHouse_SOURCE_DIR}/contrib/arrow-nanoarrow/CMakeLists.txt")
    cmake_minimum_required(VERSION 3.11)
    include(FetchContent)
    set(NANOARROW_NAMESPACE "NanoArrow")
    FetchContent_Declare(
        nanoarrow_content
        SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/arrow-nanoarrow)
    FetchContent_MakeAvailable(nanoarrow_content)
    set (USE_NANOARROW 1)
else()
    message(WARNING "submodule contrib/arrow-nanoarrow is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif()
message (STATUS "Using nanoarrow=${USE_NANOARROW}")

# depends on protobuf
if (USE_JNI AND USE_NANOARROW AND USE_PROTOBUF)
    set (USE_JAVA_EXTENSIONS 1)
endif ()
message (STATUS "Using java extensions=${USE_JAVA_EXTENSIONS}")

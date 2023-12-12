option (ENABLE_CPP_JIEBA "Enable cpp jieba" ON)

if (ENABLE_CPP_JIEBA)

    set(CPP_JIEBA_ROOT "${ClickHouse_SOURCE_DIR}/contrib/cppjieba")

    if(NOT EXISTS "${CPP_JIEBA_ROOT}/CMakeLists.txt")
        message(WARNING "submodule contrib/cppjieba is missing. to fix try run: \n git submodule update --init --recursive")
    endif()

    set(USE_CPP_JIEBA 1)

    set(CPP_JIEBA_INCLUDE_DIR "${CPP_JIEBA_ROOT}/include")
    set(CPP_JIEBA_LIMONP_INCLUDE_DIR "${CPP_JIEBA_ROOT}/deps/limonp/include")
    set(CPP_JIEBA_LIBRARY cppjieba)

endif()

message(STATUS "Using cppjieba=${USE_CPP_JIEBA} : ${CPP_JIEBA_INCLUDE_DIR} : ${CPP_JIEBA_LIBRARY}")
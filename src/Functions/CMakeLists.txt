configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

add_subdirectory(divide)

include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp IFunctionMySql.cpp FunctionFactory.cpp FunctionHelpers.cpp extractTimeZoneFromFunctionArguments.cpp FunctionsLogical.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h IFunctionMySql.h FunctionFactory.h FunctionHelpers.h extractTimeZoneFromFunctionArguments.h FunctionsLogical.h)

add_library(clickhouse_functions_obj OBJECT ${clickhouse_functions_sources})

list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_obj>)

list (APPEND PUBLIC_LIBS
        ch_contrib::wyhash
        ${BASE64_LIBRARY}
        ${CITYHASH_LIBRARIES}
        ${FARMHASH_LIBRARIES}
        ${FASTOPS_LIBRARY}
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        consistent-hashing
        dbms
        metrohash
        murmurhash
        biginteger
        ch_contrib::hashidsxx
        ch_contrib::morton_nd
)

list (APPEND PRIVATE_LIBS
        ${ZLIB_LIBRARIES}
        boost::filesystem
        divide_impl
        ch_contrib::xxHash
)

if (TARGET ch_rust::blake3)
    list (APPEND PUBLIC_LIBS
        ch_rust::blake3
    )
endif()

if (OPENSSL_CRYPTO_LIBRARY)
    list (APPEND PUBLIC_LIBS ${OPENSSL_CRYPTO_LIBRARY})
endif()

target_include_directories(clickhouse_functions_obj SYSTEM PRIVATE ${SPARSEHASH_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    set (STRIP_DSF_DEFAULT ON)
else()
    set (STRIP_DSF_DEFAULT OFF)
endif()


#  Provides faster linking and lower binary size.
#  Tradeoff is the inability to debug some source files with e.g. gdb
#  (empty stack frames and no local variables)."
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS "Do not generate debugger info for ClickHouse functions" ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    message(WARNING "Not generating debugger info for ClickHouse functions")
    target_compile_options(clickhouse_functions_obj PRIVATE "-g0")
else()
    message(STATUS "Generating debugger info for ClickHouse functions")
endif()

if (USE_ICU)
    list (APPEND PRIVATE_LIBS ${ICU_LIBRARIES})
    target_include_directories(clickhouse_functions_obj SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_FASTOPS)
    target_include_directories (clickhouse_functions_obj SYSTEM PRIVATE ${FASTOPS_INCLUDE_DIR})
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (USE_EMBEDDED_COMPILER)
    list (APPEND PRIVATE_LIBS ${REQUIRED_LLVM_LIBRARIES})
    target_include_directories(clickhouse_functions_obj SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()

if(USE_BASE64)
    target_include_directories(clickhouse_functions_obj SYSTEM PRIVATE ${BASE64_INCLUDE_DIR})
endif()

if (USE_H3)
    list (APPEND PRIVATE_LIBS  ${H3_LIBRARY})
    target_include_directories(clickhouse_functions_obj SYSTEM PRIVATE ${H3_INCLUDE_DIR})
endif()

list (APPEND PRIVATE_LIBS hyperscan)

if(USE_SIMDJSON)
    list (APPEND PRIVATE_LIBS simdjson)
endif()

if(USE_RAPIDJSON)
    target_include_directories(clickhouse_functions_obj SYSTEM PRIVATE ${RAPIDJSON_INCLUDE_DIR})
endif()

add_subdirectory(GatherUtils)
list (APPEND PRIVATE_LIBS clickhouse_functions_gatherutils)

add_subdirectory(URL)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_url>)

add_subdirectory(array)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_array>)

if (ENABLE_GEOS)
    add_subdirectory(GIS)
    list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_gis>)
endif()

add_subdirectory(UniqTheta)
list (APPEND OBJECT_LIBS $<TARGET_OBJECTS:clickhouse_functions_uniqtheta>)

add_subdirectory(JSONPath)
list (APPEND PRIVATE_LIBS clickhouse_functions_jsonpath)

if (USE_STATS)
    list (APPEND PRIVATE_LIBS stats)
endif()

if (ENABLE_NLP)
    list (APPEND PRIVATE_LIBS ch_contrib::cld2)
endif()

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

target_link_libraries(clickhouse_functions_obj PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})

if (USE_STATIC_LIBRARIES OR NOT SPLIT_SHARED_LIBRARIES)
    # Used to forward the linking information to the final binaries such as clickhouse / unit_tests_dbms,
    # since such information are lost after we convert to OBJECT target
    add_library(clickhouse_functions INTERFACE)
    target_link_libraries(clickhouse_functions INTERFACE ${OBJECT_LIBS} ${PUBLIC_LIBS} ${PRIVATE_LIBS})
else()
    add_library(clickhouse_functions SHARED ${OBJECT_LIBS})
    target_link_libraries(clickhouse_functions PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})
endif ()

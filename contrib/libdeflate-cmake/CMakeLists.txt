set(LIBDEFLATE_ROOT_DIR ${ClickHouse_SOURCE_DIR}/contrib/libdeflate)

set(LIB_SOURCES
    "${LIBDEFLATE_ROOT_DIR}/common_defs.h"
    "${LIBDEFLATE_ROOT_DIR}/libdeflate.h"
    "${LIBDEFLATE_ROOT_DIR}/lib/arm/cpu_features.c"
    "${LIBDEFLATE_ROOT_DIR}/lib/arm/cpu_features.h"
    "${LIBDEFLATE_ROOT_DIR}/lib/cpu_features_common.h"
    "${LIBDEFLATE_ROOT_DIR}/lib/deflate_constants.h"
    "${LIBDEFLATE_ROOT_DIR}/lib/lib_common.h"
    "${LIBDEFLATE_ROOT_DIR}/lib/utils.c"
    "${LIBDEFLATE_ROOT_DIR}/lib/x86/cpu_features.c"
    "${LIBDEFLATE_ROOT_DIR}/lib/x86/cpu_features.h"
)
option(LIBDEFLATE_COMPRESSION_SUPPORT "Support compression" ON)
option(LIBDEFLATE_DECOMPRESSION_SUPPORT "Support decompression" ON)
option(LIBDEFLATE_ZLIB_SUPPORT "Support the zlib format" ON)
option(LIBDEFLATE_GZIP_SUPPORT "Support the gzip format" ON)
option(LIBDEFLATE_BUILD_STATIC_LIB "Build the static library" ON)

if(LIBDEFLATE_COMPRESSION_SUPPORT)
    list(APPEND LIB_SOURCES
         "${LIBDEFLATE_ROOT_DIR}/lib/arm/matchfinder_impl.h"
         "${LIBDEFLATE_ROOT_DIR}/lib/bt_matchfinder.h"
         "${LIBDEFLATE_ROOT_DIR}/lib/deflate_compress.c"
         "${LIBDEFLATE_ROOT_DIR}/lib/deflate_compress.h"
         "${LIBDEFLATE_ROOT_DIR}/lib/hc_matchfinder.h"
         "${LIBDEFLATE_ROOT_DIR}/lib/ht_matchfinder.h"
         "${LIBDEFLATE_ROOT_DIR}/lib/matchfinder_common.h"
         "${LIBDEFLATE_ROOT_DIR}/lib/x86/matchfinder_impl.h"
    )
endif()

if(LIBDEFLATE_DECOMPRESSION_SUPPORT)
    list(APPEND LIB_SOURCES
         ${LIBDEFLATE_ROOT_DIR}/lib/decompress_template.h
         ${LIBDEFLATE_ROOT_DIR}/lib/deflate_decompress.c
         ${LIBDEFLATE_ROOT_DIR}/lib/x86/decompress_impl.h
    )
endif()
if(LIBDEFLATE_ZLIB_SUPPORT)
    list(APPEND LIB_SOURCES
         ${LIBDEFLATE_ROOT_DIR}/lib/adler32.c
         ${LIBDEFLATE_ROOT_DIR}/lib/adler32_vec_template.h
         ${LIBDEFLATE_ROOT_DIR}/lib/arm/adler32_impl.h
         ${LIBDEFLATE_ROOT_DIR}/lib/x86/adler32_impl.h
         ${LIBDEFLATE_ROOT_DIR}/lib/zlib_constants.h
    )
    if(LIBDEFLATE_COMPRESSION_SUPPORT)
        list(APPEND LIB_SOURCES ${LIBDEFLATE_ROOT_DIR}/lib/zlib_compress.c)
    endif()
    if(LIBDEFLATE_DECOMPRESSION_SUPPORT)
        list(APPEND LIB_SOURCES ${LIBDEFLATE_ROOT_DIR}/lib/zlib_decompress.c)
    endif()
endif()
if(LIBDEFLATE_GZIP_SUPPORT)
    list(APPEND LIB_SOURCES
         ${LIBDEFLATE_ROOT_DIR}/lib/arm/crc32_impl.h
         ${LIBDEFLATE_ROOT_DIR}/lib/arm/crc32_pmull_helpers.h
         ${LIBDEFLATE_ROOT_DIR}/lib/arm/crc32_pmull_wide.h
         ${LIBDEFLATE_ROOT_DIR}/lib/crc32.c
         ${LIBDEFLATE_ROOT_DIR}/lib/crc32_multipliers.h
         ${LIBDEFLATE_ROOT_DIR}/lib/crc32_tables.h
         ${LIBDEFLATE_ROOT_DIR}/lib/gzip_constants.h
         ${LIBDEFLATE_ROOT_DIR}/lib/x86/crc32_impl.h
         ${LIBDEFLATE_ROOT_DIR}/lib/x86/crc32_pclmul_template.h
    )
    if(LIBDEFLATE_COMPRESSION_SUPPORT)
        list(APPEND LIB_SOURCES ${LIBDEFLATE_ROOT_DIR}/lib/gzip_compress.c)
    endif()
    if(LIBDEFLATE_DECOMPRESSION_SUPPORT)
        list(APPEND LIB_SOURCES ${LIBDEFLATE_ROOT_DIR}/lib/gzip_decompress.c)
    endif()
endif()

if(LIBDEFLATE_BUILD_STATIC_LIB)
    add_library(libdeflate_static STATIC ${LIB_SOURCES})

    # This alias allows third-party usage of the library with CMake to work the
    # same way with add_subdirectory() as with other ways.
    # add_library(libdeflate::libdeflate_static ALIAS libdeflate_static)

    if(WIN32 AND NOT MINGW)
        set(STATIC_LIB_NAME deflatestatic)
    else()
        set(STATIC_LIB_NAME deflate)
    endif()
    set_target_properties(libdeflate_static PROPERTIES
                          OUTPUT_NAME ${STATIC_LIB_NAME}
                          PUBLIC_HEADER libdeflate.h)
    target_include_directories(libdeflate_static PUBLIC ${LIB_INCLUDE_DIRS})
    target_compile_definitions(libdeflate_static PRIVATE ${LIB_COMPILE_DEFINITIONS})
    target_compile_options(libdeflate_static PRIVATE ${LIB_COMPILE_OPTIONS})
    list(APPEND LIB_TARGETS libdeflate_static)
endif()

if(TARGET libdeflate_static)
    message(STATUS "cmake found libdeflate_static")
endif()




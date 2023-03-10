option(USE_SNAPPY "Enable snappy library" ${ENABLE_LIBRARIES})

if(NOT USE_SNAPPY)
    if (USE_INTERNAL_SNAPPY_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal snappy library with USE_SNAPPY=OFF")
    endif()
    return()
endif()

option (USE_INTERNAL_SNAPPY_LIBRARY "Set to FALSE to use system snappy library instead of bundled" ${NOT_UNBUNDLED})

if(NOT USE_INTERNAL_SNAPPY_LIBRARY)
    find_library(SNAPPY_LIBRARY snappy)
    if (NOT SNAPPY_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system snappy library")
    endif()
else ()
    # FIXME (UNIQUE KEY): Let snappy "find snappy-stubs-public.h"
    set(SNAPPY_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/snappy" "${ClickHouse_BINARY_DIR}/contrib/snappy" CACHE INTERNAL "")
    set(SNAPPY_LIBRARY snappy)
endif()

message(STATUS "Using snappy=${USE_SNAPPY}: ${SNAPPY_INCLUDE_DIR} : ${SNAPPY_LIBRARY}")

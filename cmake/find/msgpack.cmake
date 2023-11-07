option (ENABLE_MSGPACK "Enable msgpack library" ${ENABLE_LIBRARIES})

if(NOT ENABLE_MSGPACK)
    if(USE_INTERNAL_MSGPACK_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal msgpack with ENABLE_MSGPACK=OFF")
    endif()
    return()
endif()

option (USE_INTERNAL_MSGPACK_LIBRARY "Set to FALSE to use system msgpack library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/msgpack-c/include/msgpack.hpp")
    message(WARNING "Submodule contrib/msgpack-c is missing. To fix try run: \n git submodule update --init --recursive")
    message(FATAL_ERROR "Cannot use internal msgpack")
endif()

set(USE_MSGPACK 1)

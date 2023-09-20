option(ENABLE_VE_TOS "Enable VE-TOS" ON)

if (NOT ENABLE_VE_TOS)
    set(USE_VE_TOS 0)
    return()
endif()

# using internal curl
if (NOT CURL_FOUND) 
    set(USE_VE_TOS 0)    
    message(${RECONFIGURE_MESSAGE_LEVEL} "Could not find curl for ve-tos")
    return()
endif ()

#using internal openssl
if (NOT OPENSSL_FOUND)
    set(USE_VE_TOS 0) 
    message(${RECONFIGURE_MESSAGE_LEVEL} "Could not find openSSL for ve-tos")
    return()
endif ()

# We don't know any linux distribution with package for it
option(USE_INTERNAL_VE_TOS_LIBRARY "Set to FALSE to use system ve-tos instead of bundled (experimental - set to OFF on your own risk)" ON) 

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/ve-tos-cpp-sdk/sdk/include/TosClient.h")
    if(USE_INTERNAL_VE_TOS_LIBRARY)
        message(WARNING "submodule contrib/ve-tos-cpp-sdk is missing. to fix try run: \n git submodule update --init --recursive")
	message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal VE-TOS library")
	set(USE_INTERNAL_VE_TOS_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_VE_TOS_LIBRARY 1)
endif()

if(VE_TOS_LIBRARY AND VE_TOS_INCLUDE_DIR)
    set(USE_VE_TOS 1)
elseif(NOT MISSING_INTERNAL_VE_TOS_LIBRARY)
    set(VE_TOS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/ve-tos-cpp-sdk/sdk/include"  "${ClickHouse_SOURCE_DIR}/contrib/ve-tos-cpp-sdk-cmake/include")
    set(VE_TOS_LIBRARY ve-tos-cpp-sdk-lib)
    set(USE_INTERNAL_VE_TOS_LIBRARY 1)
    set(USE_VE_TOS 1)
else()
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannout enable ve-tos")
endif()

message(STATUS "Using ve-tos=${USE_VE_TOS}: ${VE_TOS_INCLUDE_DIR} : ${VE_TOS_LIBRARY}")

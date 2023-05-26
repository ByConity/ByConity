if(NOT OS_FREEBSD)
    option(ENABLE_AWS_GLUE "Enable AWS_GLUE" ${ENABLE_LIBRARIES})
elseif(ENABLE_AWS_GLUE OR USE_INTERNAL_AWS_AWS_GLUE_LIBRARY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use AWS_GLUE on FreeBSD")
endif()


option(USE_INTERNAL_AWS_AWS_GLUE_LIBRARY "Set to FALSE to use system AWS_GLUE instead of bundled (experimental set to OFF on your own risk)"
       ON)

if (NOT USE_INTERNAL_POCO_LIBRARY AND USE_INTERNAL_AWS_AWS_GLUE_LIBRARY)
    message (FATAL_ERROR "Currently AWS_GLUE support can be built only with internal POCO library")
endif()

if (NOT USE_INTERNAL_AWS_AWS_GLUE_LIBRARY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Compilation with external AWS_GLUE library is not supported yet")
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-glue")
    message (WARNING "submodule contrib/aws is missing. to fix try run: \n git submodule update --init --recursive")
    if (USE_INTERNAL_AWS_AWS_GLUE_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal AWS_GLUE library")
    endif ()
    set (MISSING_AWS_AWS_GLUE 1)
endif ()

if (USE_INTERNAL_AWS_AWS_GLUE_LIBRARY AND NOT MISSING_AWS_AWS_GLUE)
    set(AWS_GLUE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-glue/include")
    set(AWS_GLUE_CORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core/include")
    set(AWS_GLUE_LIBRARY aws_glue)
    set(USE_INTERNAL_AWS_LIBRARY 1)
    set(USE_AWS_AWS_GLUE 1)
else()
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't enable AWS_GLUE")
    set(USE_INTERNAL_AWS_GLUE_LIBRARY 0)
    set(USE_AWS_GLUE 0)
endif ()

message (STATUS "Using aws_glue=${USE_AWS_GLUE}: ${AWS_GLUE_INCLUDE_DIR} : ${AWS_GLUE_LIBRARY}")

option (USE_UNWIND "Enable libunwind" ${ENABLE_LIBRARIES})

if (USE_UNWIND)
    add_subdirectory(contrib/libunwind-old-cmake)
    message (STATUS "Using libunwind-old for better jeprof backtrace performance")
endif ()

if (TARGET jemalloc AND ENABLE_JEMALLOC)
    set(USE_JEMALLOC 1)
endif()

if (TARGET ch_contrib::aws_s3)
    set(USE_AWS_S3 1)
endif()

if (TARGET ch_contrib::ulid)
    set(USE_ULID 1)
endif()

if (TARGET ch_rust::blake3)
    set(USE_BLAKE3 1)
endif()

if (TARGET hualloc AND ENABLE_HUALLOC)
    set(USE_HUALLOC 1)
endif()

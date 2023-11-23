if (TARGET jemalloc AND ENABLE_JEMALLOC)
    set(USE_JEMALLOC 1)
endif()

if (TARGET ch_contrib::aws_s3)
    set(USE_AWS_S3 1)
endif()

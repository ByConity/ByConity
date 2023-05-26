SET(AWS_S3_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3")
SET(AWS_GLUE_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-glue")

file(GLOB AWS_S3_SOURCES
    "${AWS_S3_LIBRARY_DIR}/source/*.cpp"
)

file(GLOB AWS_S3_MODEL_SOURCES
    "${AWS_S3_LIBRARY_DIR}/source/model/*.cpp"
)

file(GLOB S3_UNIFIED_SRC
    ${AWS_S3_SOURCES}
    ${AWS_S3_MODEL_SOURCES}
)

set(S3_INCLUDES
    "${AWS_S3_LIBRARY_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include/")
message(STATUS $S3_INCLUDES)

add_library(aws_s3 ${S3_UNIFIED_SRC})

target_include_directories(aws_s3 PUBLIC ${S3_INCLUDES})
target_link_libraries(aws_s3 PRIVATE aws_core)
SET(AWS_GLUE_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-glue")
file(GLOB AWS_GLUE_SOURCES
    "${AWS_GLUE_LIBRARY_DIR}/source/*.cpp"
)

file(GLOB AWS_GLUE_MODEL_SOURCES
    "${AWS_GLUE_LIBRARY_DIR}/source/model/*.cpp"
)

file(GLOB GLUE_UNIFIED_SRC
    ${AWS_GLUE_SOURCES}
    ${AWS_GLUE_MODEL_SOURCES}
)

set(GLUE_INCLUDES
    "${AWS_GLUE_LIBRARY_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include/"
)

add_library(aws_glue ${GLUE_UNIFIED_SRC})
target_include_directories(aws_glue PUBLIC ${GLUE_INCLUDES})
target_link_libraries(aws_glue PRIVATE aws_core)

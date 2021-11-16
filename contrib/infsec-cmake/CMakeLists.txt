set(INFSEC_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/infsec/client)

set(SRCS
${INFSEC_SOURCE_DIR}/InfSecClient.cpp
${INFSEC_SOURCE_DIR}/util.cpp)

add_library(infsec STATIC ${SRCS})
target_include_directories(infsec PRIVATE ${INFSEC_SOURCE_DIR})
target_include_directories(infsec PRIVATE ${RAPIDJSON_INCLUDE_DIR})

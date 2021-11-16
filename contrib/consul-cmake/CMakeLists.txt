set(CONSUL_ROOT_DIR ${CMAKE_SOURCE_DIR}/contrib/consul)

set(SRCS
    ${CONSUL_ROOT_DIR}/base64.cpp
    ${CONSUL_ROOT_DIR}/bridge.cpp
    ${CONSUL_ROOT_DIR}/discovery.cpp
    ${CONSUL_ROOT_DIR}/register.cpp
    ${CONSUL_ROOT_DIR}/requests.cpp
    ${CONSUL_ROOT_DIR}/safety.cpp
    )

add_library(consul ${SRCS})
target_include_directories(consul PUBLIC .)
target_include_directories(consul PRIVATE ${RAPIDJSON_INCLUDE_DIR})
target_include_directories(consul PRIVATE ${MINIZIP_INCLUDE_DIR})
target_link_libraries(consul PRIVATE ${MINIZIP_LIBRARIES})

# CURL
find_package(CURL REQUIRED)
target_include_directories(consul PRIVATE ${CURL_INCLUDE_DIRS})
# FIXME: The library found in ${CURL_LIBRARIES} is static. Use `curl` directly to link the shared one.
# target_link_libraries(rdkafka PRIVATE ${CURL_LIBRARIES})
target_link_libraries(consul PRIVATE curl)

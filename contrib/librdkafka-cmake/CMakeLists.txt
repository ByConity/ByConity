set(RDKAFKA_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/librdkafka/src)

set(SRCS
  ${RDKAFKA_SOURCE_DIR}/crc32c.c
  ${RDKAFKA_SOURCE_DIR}/rdaddr.c
  ${RDKAFKA_SOURCE_DIR}/rdavl.c
  ${RDKAFKA_SOURCE_DIR}/rdbuf.c
  ${RDKAFKA_SOURCE_DIR}/rdcrc32.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_assignor.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_background.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_broker.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_buf.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_cgrp.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_conf.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_event.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_feature.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_lz4.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_metadata.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_metadata_cache.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_msg.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_msgset_reader.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_msgset_writer.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_offset.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_op.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_partition.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_pattern.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_queue.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_range_assignor.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_request.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_roundrobin_assignor.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_sasl.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_plain.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_subscription.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_timer.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_topic.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_transport.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_interceptor.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_header.c
  ${RDKAFKA_SOURCE_DIR}/rdkafka_zstd.c
  ${RDKAFKA_SOURCE_DIR}/rdlist.c
  ${RDKAFKA_SOURCE_DIR}/rdlog.c
  ${RDKAFKA_SOURCE_DIR}/rdmurmur2.c
  ${RDKAFKA_SOURCE_DIR}/rdports.c
  ${RDKAFKA_SOURCE_DIR}/rdrand.c
  ${RDKAFKA_SOURCE_DIR}/rdregex.c
  ${RDKAFKA_SOURCE_DIR}/rdstring.c
  ${RDKAFKA_SOURCE_DIR}/rdunittest.c
  ${RDKAFKA_SOURCE_DIR}/rdvarint.c
  ${RDKAFKA_SOURCE_DIR}/snappy.c
  ${RDKAFKA_SOURCE_DIR}/tinycthread.c
  ${RDKAFKA_SOURCE_DIR}/xxhash.c
  ${RDKAFKA_SOURCE_DIR}/rdgz.c
)

## `librdkafka` is used for ByteDance and no SASL and SCRAM are supported.

if (USE_BYTEDANCE_RDKAFKA)
    ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/version.h
        COMMAND ${RDKAFKA_SOURCE_DIR}/gen_version.sh ${CMAKE_CURRENT_BINARY_DIR}/version.h)

    set(SRCS ${SRCS}
        ${CMAKE_CURRENT_BINARY_DIR}/version.h

        ${RDKAFKA_SOURCE_DIR}/consul_tools.cc
        ${RDKAFKA_SOURCE_DIR}/rdhdrhistogram.c
        ${RDKAFKA_SOURCE_DIR}/rdkafka_admin.c
        ${RDKAFKA_SOURCE_DIR}/rdkafka_aux.c
        ${RDKAFKA_SOURCE_DIR}/rdkafka_config_helper.c
        ${RDKAFKA_SOURCE_DIR}/rdkafka_consumer_info.c

        ${CMAKE_SOURCE_DIR}/contrib/librdkafka/cJSON/cJSON.c
        )
endif ()

add_library(rdkafka ${LINK_MODE} ${SRCS})
target_include_directories(rdkafka SYSTEM PUBLIC include)
target_include_directories(rdkafka SYSTEM PUBLIC ${RDKAFKA_SOURCE_DIR})         # Because weird logic with "include_next" is used.
target_include_directories(rdkafka SYSTEM PRIVATE ${ZSTD_INCLUDE_DIR}/common)   # Because wrong path to "zstd_errors.h" is used.
target_link_libraries(rdkafka PUBLIC ${ZLIB_LIBRARIES} ${ZSTD_LIBRARY} ${LZ4_LIBRARY})
if(OPENSSL_SSL_LIBRARY AND OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries(rdkafka PUBLIC ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
endif()

if (USE_BYTEDANCE_RDKAFKA)
    target_include_directories(rdkafka PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

    # infsec
    target_include_directories(rdkafka PRIVATE ${INFSEC_INCLUDE_DIR})
    target_link_libraries(rdkafka PRIVATE ${INFSEC_LIBRARIES})

    # consul
    target_include_directories(rdkafka PRIVATE ${CONSUL_INCLUDE_DIR})
    target_link_libraries(rdkafka PRIVATE ${CONSUL_LIBRARIES})

    # json
    target_include_directories(rdkafka PRIVATE ${CMAKE_SOURCE_DIR}/contrib/librdkafka)

    # CURL
    find_package(CURL REQUIRED)
    target_include_directories(rdkafka PRIVATE ${CURL_INCLUDE_DIRS})
    # FIXME: The library found in ${CURL_LIBRARIES} is static. Use `curl` directly to link the shared one.
    # target_link_libraries(rdkafka PRIVATE ${CURL_LIBRARIES})
    target_link_libraries(rdkafka PRIVATE curl)

    # test
    add_executable(test-librdkafka ${LINK_MODE} test-librdkafka.cc)
    target_link_libraries(test-librdkafka rdkafka pthread)
endif ()

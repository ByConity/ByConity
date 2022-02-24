set (CPPKMS_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/cppkms)

set (SRCS
    ${CPPKMS_LIBRARY_DIR}/src/aesgcm.cpp
    ${CPPKMS_LIBRARY_DIR}/src/base64.cpp
    ${CPPKMS_LIBRARY_DIR}/src/network.cpp
    ${CPPKMS_LIBRARY_DIR}/src/algo_hmac.cpp
    ${CPPKMS_LIBRARY_DIR}/src/AesCipher.cpp
    ${CPPKMS_LIBRARY_DIR}/src/KMSClient.cpp
)

add_library (cppkms ${SRCS})

target_include_directories (cppkms PUBLIC ${CPPKMS_LIBRARY_DIR}/include)
target_include_directories (cppkms PRIVATE ${INFSEC_INCLUDE_DIR} ${RAPIDJSON_INCLUDE_DIR} ${OPENSSL_INCLUDE_DIR} ${CURL_INCLUDE_DIR})

target_link_libraries (cppkms PRIVATE pthread infsec ${OPENSSL_LIBRARIES} ${CURL_LIBRARIES})

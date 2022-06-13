if(ENABLE_TESTS)
add_executable(tso_bytekv_test bytekv_test.cpp)
target_link_libraries(tso_bytekv_test PRIVATE tso_lib dbms)

add_executable(tso_client_test client_test.cpp)
target_link_libraries(tso_client_test PRIVATE tso_lib dbms)

add_custom_command(TARGET tso_client_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:tso_client_test> ${PROJECT_BINARY_DIR}/dbms/programs/tso_client_test)
endif()

set(CLICKHOUSE_TSO_SERVER_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tso_server.cpp)
set(CLICKHOUSE_TSO_SERVER_LINK PRIVATE tso_lib dbms)

cnch_program_add(tso_server)

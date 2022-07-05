set(CLICKHOUSE_DAEMONMANAGER_SOURCE
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonManager.cpp
        )

add_library(daemon_manager_lib ${CLICKHOUSE_DAEMONMANAGER_SOURCE})
target_include_directories(daemon_manager_lib SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)
target_link_libraries(daemon_manager_lib daemon dbms)
# add_subdirectory(tests)


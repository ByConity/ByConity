set(CLICKHOUSE_DAEMONMANAGER_SOURCE
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonManager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonFactory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonJob.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonJobTxnGC.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/BackgroudJobExecutor.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/BackgroundJob.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonManagerClient.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonManagerServiceImpl.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonHelper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/Metrics.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/registerDaemons.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/BGJobStatusInCatalog.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonJobServerBGThread.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonJobGlobalGC.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/TargetServerCalculator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/FixCatalogMetaDataTask.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonJobAutoStatistics.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/DaemonJobBackup.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/Metrics/MetricsData.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/Metrics/MetricsWriter.cpp
        )

add_library(daemon_manager_lib ${CLICKHOUSE_DAEMONMANAGER_SOURCE})
target_include_directories(daemon_manager_lib SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)
target_include_directories(daemon_manager_lib SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash/)
target_include_directories(daemon_manager_lib SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
target_include_directories(daemon_manager_lib SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/murmurhash/include)
target_link_libraries(daemon_manager_lib daemon dbms)
# add_subdirectory(tests)


/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/KafkaLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/MaterializedMySQLLog.h>
#include <Interpreters/UniqueTableLog.h>
#include <Storages/MaterializedView/ViewRefreshTaskLog.h>
#include "Interpreters/AutoStatsTaskLog.h"


namespace DB
{

class CnchQueryLog;
class CnchAutoStatsTaskLog;

// Query metrics definitions
constexpr auto CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME = "cnch_kafka_log";
constexpr auto CNCH_SYSTEM_LOG_QUERY_LOG_TABLE_NAME = "cnch_query_log";
constexpr auto CNCH_SYSTEM_LOG_MATERIALIZED_MYSQL_LOG_TABLE_NAME = "cnch_materialized_mysql_log";
constexpr auto CNCH_SYSTEM_LOG_UNIQUE_TABLE_LOG_TABLE_NAME = "cnch_unique_table_log";
constexpr auto CNCH_SYSTEM_LOG_VIEW_REFRESH_TASK_LOG_TABLE_NAME = "cnch_view_refresh_task_log";
constexpr auto CNCH_SYSTEM_LOG_AUTO_STATS_TASK_LOG_TABLE_NAME = "cnch_auto_stats_task_log";

/** Modified version of SystemLog that flushes data to a CnchMergeTree table.
  * Altering of schema is also possible for columns that are not part of primary/partition keys.
  * Reordering of columns are not supported.
  */
class CnchSystemLogs
{
public:
    CnchSystemLogs(ContextPtr global_context);
    ~CnchSystemLogs();

    std::shared_ptr<CloudKafkaLog> getKafkaLog() const
    {
        std::lock_guard<std::mutex> g(mutex);
        return cloud_kafka_log;
    }

    std::shared_ptr<CloudMaterializedMySQLLog> getMaterializedMySQLLog() const
    {
        std::lock_guard<std::mutex> g(mutex);
        return cloud_materialized_mysql_log;
    }

    std::shared_ptr<CloudUniqueTableLog> getUniqueTableLog() const
    {
        std::lock_guard<std::mutex> g(mutex);
        return cloud_unique_table_log;
    }

    std::shared_ptr<CnchQueryLog> getCnchQueryLog() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return cnch_query_log;
    }

    std::shared_ptr<ViewRefreshTaskLog> getViewRefreshTaskLog() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return cnch_view_refresh_task_log;
    }

    std::shared_ptr<CnchAutoStatsTaskLog> getCnchAutoStatsTaskLog() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return cnch_auto_stats_task_log;
    }

    void shutdown();

private:
    std::shared_ptr<CloudKafkaLog> cloud_kafka_log;
    std::shared_ptr<CloudMaterializedMySQLLog> cloud_materialized_mysql_log;
    std::shared_ptr<CloudUniqueTableLog> cloud_unique_table_log;
    std::shared_ptr<CnchQueryLog> cnch_query_log;
    std::shared_ptr<ViewRefreshTaskLog> cnch_view_refresh_task_log;
    std::shared_ptr<CnchAutoStatsTaskLog> cnch_auto_stats_task_log;

    int init_time_in_worker{};
    int init_time_in_server{};
    mutable std::mutex mutex;
    template<typename CloudLog>
    bool initInServerForSingleLog(ContextPtr & global_context,
        const String & db,
        const String & tb,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & config,
        std::shared_ptr<CloudLog> & cloud_log);
    template<typename CloudLog>
    bool initInWorkerForSingleLog(ContextPtr & global_context,
        const String & db,
        const String & tb,
        const String & config_prefix,
        const Poco::Util::AbstractConfiguration & config,
        std::shared_ptr<CloudLog> & cloud_log);

    bool initInServer(ContextPtr global_context);
    bool initInWorker(ContextPtr global_context);

    BackgroundSchedulePool::TaskHolder init_task;
    LoggerPtr log;

    std::vector<ISystemLog *> logs;
};

constexpr auto CNCH_KAFKA_LOG_CONFIG_PREFIX = "cnch_kafka_log";
constexpr auto CNCH_QUERY_LOG_CONFIG_PREFIX = "cnch_query_log";
constexpr auto CNCH_MATERIALIZED_MYSQL_LOG_CONFIG_PREFIX = "cnch_materialized_mysql_log";
constexpr auto CNCH_UNIQUE_TABLE_LOG_CONFIG_PREFIX = "cnch_unique_table_log";
constexpr auto CNCH_VIEW_REFRESH_TASK_PREFIX = "cnch_view_refresh_task_log";
constexpr auto CNCH_AUTO_STATS_TASK_LOG_CONFIG_PREFIX = "cnch_auto_stats_task_log";

/// Instead of typedef - to allow forward declaration.
class CloudKafkaLog : public CnchSystemLog<KafkaLogElement>
{
public:
    using CnchSystemLog<KafkaLogElement>::CnchSystemLog;
    void logException(const StorageID & storage_id, String msg, String consumer_id = "");
};

/// Instead of typedef - to allow forward declaration.
class CloudMaterializedMySQLLog : public CnchSystemLog<MaterializedMySQLLogElement>
{
public:
    using CnchSystemLog<MaterializedMySQLLogElement>::CnchSystemLog;
};

/// Instead of typedef - to allow forward declaration.
class CloudUniqueTableLog : public CnchSystemLog<UniqueTableLogElement>
{
public:
    using CnchSystemLog<UniqueTableLogElement>::CnchSystemLog;
};

} // end namespace

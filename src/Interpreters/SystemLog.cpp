/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Interpreters/SystemLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/PartMergeLog.h>
#include <Interpreters/RemoteReadLog.h>
#include <Interpreters/ServerPartLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/MutationLog.h>
#include <Interpreters/KafkaLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Interpreters/AutoStatsTaskLog.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr size_t DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS = 7500;
constexpr size_t DEFAULT_METRIC_LOG_COLLECT_INTERVAL_MILLISECONDS = 1000;

/// Creates a system log with MergeTree engine using parameters from config
template <typename TSystemLog>
std::shared_ptr<TSystemLog> createSystemLog(
    ContextPtr context,
    const String & default_database_name,
    const String & default_table_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    bool enable_by_default)
{
    if (!config.has(config_prefix) && !enable_by_default)
        return {};

    if (config.has(config_prefix) && !config.getBool(config_prefix + ".enable", true))
        return {};

    String database = config.getString(config_prefix + ".database", default_database_name);
    String table = config.getString(config_prefix + ".table", default_table_name);

    if (database != default_database_name)
    {
        /// System tables must be loaded before other tables, but loading order is undefined for all databases except `system`
        LOG_ERROR(getLogger("SystemLog"), "Custom database name for a system table specified in config."
            " Table `{}` will be created in `system` database instead of `{}`", table, database);
        database = default_database_name;
    }

    String engine;
    if (config.has(config_prefix + ".engine"))
    {
        if (config.has(config_prefix + ".partition_by"))
            throw Exception("If 'engine' is specified for system table, "
                "PARTITION BY parameters should be specified directly inside 'engine' and 'partition_by' setting doesn't make sense",
                ErrorCodes::BAD_ARGUMENTS);
        if (config.has(config_prefix + ".ttl"))
            throw Exception("If 'engine' is specified for system table, "
                            "TTL parameters should be specified directly inside 'engine' and 'ttl' setting doesn't make sense",
                            ErrorCodes::BAD_ARGUMENTS);
        engine = config.getString(config_prefix + ".engine");
    }
    else
    {
        String partition_by = config.getString(config_prefix + ".partition_by", "toYYYYMM(event_date)");
        engine = "ENGINE = MergeTree";
        if (!partition_by.empty())
            engine += " PARTITION BY (" + partition_by + ")";
        String ttl = config.getString(config_prefix + ".ttl", "");
        if (!ttl.empty())
            engine += " TTL " + ttl;
        engine += " ORDER BY (event_date, event_time)";
    }
    // Validate engine definition grammatically to prevent some configuration errors
    ParserStorage storage_parser(ParserSettings::valueOf(context->getSettingsRef()));
    parseQuery(storage_parser, engine.data(), engine.data() + engine.size(),
            "Storage to create table for " + config_prefix, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds",
                                                          DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);

    return std::make_shared<TSystemLog>(context, database, table, engine, flush_interval_milliseconds);
}

}


SystemLogs::SystemLogs(ContextPtr global_context, const Poco::Util::AbstractConfiguration & config)
{
    bool on_server = global_context->getServerType() == ServerType::cnch_server;
    bool on_worker = global_context->getServerType() == ServerType::cnch_worker;

    query_log = createSystemLog<QueryLog>(global_context, "system", "query_log", config, "query_log", true);
    query_thread_log = createSystemLog<QueryThreadLog>(global_context, "system", "query_thread_log", config, "query_thread_log", false);
    query_exchange_log = createSystemLog<QueryExchangeLog>(global_context, "system", "query_exchange_log", config, "query_exchange_log", true);
    part_log = createSystemLog<PartLog>(global_context, "system", "part_log", config, "part_log", on_worker);
    part_merge_log = createSystemLog<PartMergeLog>(global_context, "system", "part_merge_log", config, "part_merge_log", on_server);
    server_part_log = createSystemLog<ServerPartLog>(global_context, "system", "server_part_log", config, "server_part_log", on_server);
    trace_log = createSystemLog<TraceLog>(global_context, "system", "trace_log", config, "trace_log", false);
    crash_log = createSystemLog<CrashLog>(global_context, "system", "crash_log", config, "crash_log", false);
    text_log = createSystemLog<TextLog>(global_context, "system", "text_log", config, "text_log", false);
    metric_log = createSystemLog<MetricLog>(global_context, "system", "metric_log", config, "metric_log", false);
    asynchronous_metric_log = createSystemLog<AsynchronousMetricLog>(
        global_context, "system", "asynchronous_metric_log", config,
        "asynchronous_metric_log", false);
    opentelemetry_span_log = createSystemLog<OpenTelemetrySpanLog>(
        global_context, "system", "opentelemetry_span_log", config,
        "opentelemetry_span_log", false);
    mutation_log = createSystemLog<MutationLog>(global_context, "system", "mutation_log", config, "mutation_log", true);
    kafka_log = createSystemLog<KafkaLog>(global_context, "system", "kafka_log", config, "kafka_log", true);
    processors_profile_log = createSystemLog<ProcessorsProfileLog>(global_context, "system", "processors_profile_log", config, "processors_profile_log", false);
    remote_read_log = createSystemLog<RemoteReadLog>(global_context, "system", "remote_read_log", config, "remote_read_log", true);
    zookeeper_log = createSystemLog<ZooKeeperLog>(global_context, "system", "zookeeper_log", config, "zookeeper_log", false);
    auto_stats_task_log = createSystemLog<AutoStatsTaskLog>(global_context, "system", "auto_stats_task_log", config, "auto_stats_task_log", true);

    if (query_log)
        logs.emplace_back(query_log.get());
    if (query_thread_log)
        logs.emplace_back(query_thread_log.get());
    if (query_exchange_log)
        logs.emplace_back(query_exchange_log.get());

    if (part_log)
        logs.emplace_back(part_log.get());
    if (part_merge_log)
        logs.emplace_back(part_merge_log.get());
    if (server_part_log)
        logs.emplace_back(server_part_log.get());
    if (trace_log)
        logs.emplace_back(trace_log.get());
    if (crash_log)
        logs.emplace_back(crash_log.get());
    if (text_log)
        logs.emplace_back(text_log.get());
    if (metric_log)
        logs.emplace_back(metric_log.get());
    if (asynchronous_metric_log)
        logs.emplace_back(asynchronous_metric_log.get());
    if (opentelemetry_span_log)
        logs.emplace_back(opentelemetry_span_log.get());
    if (mutation_log)
        logs.emplace_back(mutation_log.get());
    if (kafka_log)
        logs.emplace_back(kafka_log.get());
    if (processors_profile_log)
        logs.emplace_back(processors_profile_log.get());
    if (remote_read_log)
        logs.emplace_back(remote_read_log.get());
    if (zookeeper_log)
        logs.emplace_back(zookeeper_log.get());
    if (auto_stats_task_log)
        logs.emplace_back(auto_stats_task_log.get());

    try
    {
        for (auto & log : logs)
            log->startup();
    }
    catch (...)
    {
        /// join threads
        shutdown();
        throw;
    }

    if (metric_log)
    {
        size_t collect_interval_milliseconds = config.getUInt64("metric_log.collect_interval_milliseconds",
                                                                DEFAULT_METRIC_LOG_COLLECT_INTERVAL_MILLISECONDS);
        metric_log->startCollectMetric(collect_interval_milliseconds);
    }

    if (crash_log)
    {
        CrashLog::initialize(crash_log);
    }
}


SystemLogs::~SystemLogs()
{
    shutdown();
}

void SystemLogs::shutdown()
{
    for (auto & log : logs)
        log->shutdown();
}

}

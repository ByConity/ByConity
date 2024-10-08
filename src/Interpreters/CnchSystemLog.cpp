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

#include <Interpreters/CnchSystemLog.h>
#include <Interpreters/CnchSystemLogHelper.h>
#include <Interpreters/KafkaLog.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/MaterializedMySQLLog.h>
#include <Interpreters/UniqueTableLog.h>
#include <algorithm>

namespace DB
{

constexpr size_t DEFAULT_CNCH_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS = 30000;
constexpr auto CNCH_SYSTEM_LOG_WORKER_TABLE_SUFFIX = "_local_insertion";

template <typename LogElement>
ASTPtr constructCreateTableQuery(const String & database_name,
    const String & table_name,
    const String & storage_def)
{
    auto create = std::make_shared<ASTCreateQuery>();
    create->database = database_name;
    create->table = table_name;

    auto ordinary_columns = LogElement::getNamesAndTypes();
    auto alias_columns = LogElement::getNamesAndAliases();
    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(ordinary_columns, alias_columns, ParserSettings::CLICKHOUSE));
    create->set(create->columns_list, new_columns_list);

    ParserStorage storage_parser;
    ASTPtr storage_ast = parseQuery(
        storage_parser, storage_def.data(), storage_def.data() + storage_def.size(),
        "Storage to create table for " + LogElement::name(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    create->set(create->storage, storage_ast);

    return create;
}

template<typename CloudLog>
std::shared_ptr<CloudLog> createCnchLog(
    ContextPtr & context,
    const String & database,
    const String & cnch_table,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    if (!config.has(config_prefix))
        return {};

    String table = cnch_table + CNCH_SYSTEM_LOG_WORKER_TABLE_SUFFIX;

    size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds", DEFAULT_CNCH_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);

    if (context->getServerType() == ServerType::cnch_server)
    {
        return std::make_shared<CloudLog>(context, database, cnch_table, cnch_table, flush_interval_milliseconds);
    }
    else if (context->getServerType() == ServerType::cnch_worker)
    {
        return std::make_shared<CloudLog>(context, database, table, cnch_table, flush_interval_milliseconds);
    }
    else
        return nullptr;
}

template <typename LogElement>
String prepareEngineClause(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String engine = "ENGINE = CnchMergeTree() ";
    if (std::is_same_v<LogElement, QueryLogElement>)
        engine += " ORDER BY (`query_id`, `event_time`) ";

    String partition_by = config.getString(config_prefix + ".partition_by", "toStartOfDay(event_time)");
    if (!partition_by.empty())
        engine += " PARTITION BY (" + partition_by + ")";

    /// be consistent with cnch1.4, in which ttl field just configures the duration, e.g., 31 DAY, instead of the full ttl expression
    String ttl = config.getString(config_prefix + ".ttl", "31 DAY");
    if (!ttl.empty())
        engine += " TTL toStartOfDay(event_time) + INTERVAL " + ttl;

    engine += " SETTINGS index_granularity = 8192, enable_addition_bg_task = 1";
    return engine;
}

template String prepareEngineClause<QueryLogElement>(const Poco::Util::AbstractConfiguration &, const String &);

template <>
String prepareEngineClause<ViewRefreshTaskLogElement>(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String engine = "ENGINE = CnchMergeTree() ";
    engine += " ORDER BY (database, view, query_id)";

    String partition_by = config.getString(config_prefix + ".partition_by", "event_date");
    if (!partition_by.empty())
        engine += " PARTITION BY (" + partition_by + ")";

    /// be consistent with cnch1.4, in which ttl field just configures the duration, e.g., 31 DAY, instead of the full ttl expression
    String ttl = config.getString(config_prefix + ".ttl", "31 DAY");
    if (!ttl.empty())
        engine += " TTL event_date + INTERVAL " + ttl;

    engine += " SETTINGS index_granularity = 8192";

    return engine;
}

template <>
String prepareEngineClause<KafkaLogElement>(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String engine = "ENGINE = CnchMergeTree() ";
    engine += " ORDER BY (event_date, event_time)";

    String partition_by = config.getString(config_prefix + ".partition_by", "event_date");
    if (!partition_by.empty())
        engine += " PARTITION BY (" + partition_by + ")";

    /// be consistent with cnch1.4, in which ttl field just configures the duration, e.g., 31 DAY, instead of the full ttl expression
    String ttl = config.getString(config_prefix + ".ttl", "31 DAY");
    if (!ttl.empty())
        engine += " TTL event_date + INTERVAL " + ttl;

    engine += " SETTINGS index_granularity = 8192";

    return engine;
}

template <>
String prepareEngineClause<AutoStatsTaskLogElement>(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String engine = "ENGINE = CnchMergeTree() ";
    engine += " ORDER BY (event_date, event_time)";

    String partition_by = config.getString(config_prefix + ".partition_by", "event_date");
    if (!partition_by.empty())
        engine += " PARTITION BY (" + partition_by + ")";

    /// be consistent with cnch1.4, in which ttl field just configures the duration, e.g., 31 DAY, instead of the full ttl expression
    String ttl = config.getString(config_prefix + ".ttl", "90 DAY");
    if (!ttl.empty())
        engine += " TTL event_date + INTERVAL " + ttl;

    engine += " SETTINGS index_granularity = 8192";

    return engine;
}

template <>
String prepareEngineClause<MaterializedMySQLLogElement>(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String engine = "ENGINE = CnchMergeTree() ";
    engine += " ORDER BY (event_date, event_time)";

    String partition_by = config.getString(config_prefix + ".partition_by", "event_date");
    if (!partition_by.empty())
        engine += " PARTITION BY (" + partition_by + ")";

    /// be consistent with cnch1.4, in which ttl field just configures the duration, e.g., 31 DAY, instead of the full ttl expression
    String ttl = config.getString(config_prefix + ".ttl", "31 DAY");
    if (!ttl.empty())
        engine += " TTL event_date + INTERVAL " + ttl;

    engine += " SETTINGS index_granularity = 8192";

    return engine;
}

template <>
String prepareEngineClause<UniqueTableLogElement>(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    String engine = "ENGINE = CnchMergeTree() ";
    engine += " ORDER BY (event_date, event_time)";

    String partition_by = config.getString(config_prefix + ".partition_by", "event_date");
    if (!partition_by.empty())
        engine += " PARTITION BY (" + partition_by + ")";

    /// be consistent with cnch1.4, in which ttl field just configures the duration, e.g., 31 DAY, instead of the full ttl expression
    String ttl = config.getString(config_prefix + ".ttl", "31 DAY");
    if (!ttl.empty())
        engine += " TTL event_date + INTERVAL " + ttl;

    engine += " SETTINGS index_granularity = 8192";

    return engine;
}

template <typename LogElement>
bool prepareDatabaseAndTable(
    const ContextPtr & context,
    const String & database_name,
    const String & table_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    LoggerPtr log)
{
    if (!config.has(config_prefix) ||
        (context->getServerType() != ServerType::cnch_server))
       return true;

    if (!createDatabaseInCatalog(context, database_name, log))
        return false;

    auto create = std::make_shared<ASTCreateQuery>();
    create->database = database_name;
    create->table = table_name;

    auto ordinary_columns = LogElement::getNamesAndTypes();
    auto alias_columns = LogElement::getNamesAndAliases();
    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(ordinary_columns, alias_columns, ParserSettings::CLICKHOUSE));
    create->set(create->columns_list, new_columns_list);

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
        engine = prepareEngineClause<LogElement>(config, config_prefix);
    }

    ASTPtr create_query = constructCreateTableQuery<LogElement>(database_name, table_name, engine);

    if (!prepareCnchTable(context, database_name, table_name, create_query, log))
        return false;

    ColumnsWithTypeAndName log_element_columns;
    for (const auto & name_and_type : ordinary_columns)
        log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

    Block expected_block(std::move(log_element_columns));

    if (!syncTableSchema(context, database_name, table_name, expected_block, log))
        return false;
    if (!createView(context, database_name, table_name, log))
        return false;
    return true;
}

/// Create QueryMetricLog object to provide flushing thread for `cnch_system.query_metrics`

CnchSystemLogs::CnchSystemLogs(ContextPtr global_context)
{
    log = getLogger("CnchSystemLogs");
    if (global_context->getServerType() == ServerType::cnch_server)
    {
        init_task = global_context->getSchedulePool().createTask(
            "CnchSystemLogsInitializer",
            [this, global_context] ()
            {
                ++init_time_in_server;
                LOG_DEBUG(log, "Initialise CNCH System log on try: {}", init_time_in_server);
                if (!initInServer(global_context))
                {
                    LOG_DEBUG(log, "Failed to initialise CnchSystemLog on try: {}", init_time_in_server);
                    if (init_time_in_server < 100)
                        init_task->scheduleAfter(10000);
                }
            });

        init_task->activate();
        init_task->scheduleAfter(10000);
    }

    else if (global_context->getServerType() == ServerType::cnch_worker)
    {
        init_task = global_context->getSchedulePool().createTask(
            "CnchSystemLogsInitializer",
            [this, global_context] ()
            {
                ++init_time_in_worker;
                LOG_DEBUG(log, "Initialised CNCH System log on try: {}", init_time_in_worker);
                if (!initInWorker(global_context))
                {
                    LOG_DEBUG(log, "Failed to initialise CnchSystemLog on try: {}", init_time_in_worker);
                    if (init_time_in_worker < 100)
                        init_task->scheduleAfter(10000);
                }
            }
        );

        init_task->activate();
        init_task->scheduleAfter(10000);
    }
}


template<typename CloudLog>
bool CnchSystemLogs::initInServerForSingleLog(ContextPtr & global_context,
    const String & db,
    const String & tb,
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & config,
    std::shared_ptr<CloudLog> & cloud_log)
{
    bool ret = false;

    {
        std::lock_guard<std::mutex> g(mutex);
        if (cloud_log)
            return true;
    }

    try
    {
        LOG_INFO(log, "Initializing CNCH System log on server for {}.{}", db, tb);
        if (!ret)
            ret = prepareDatabaseAndTable<typename CloudLog::LogElement>(global_context, db, tb, config, config_prefix, log);
        if (!ret)
            LOG_INFO(log, "Failed to prepareDatabaseAndTable for {}.{}", db, tb);

        if (ret)
        {
            std::shared_ptr<CloudLog> temp_log =
                createCnchLog<CloudLog>(global_context, db, tb, config, config_prefix);
            bool actual_init = true;
            if (temp_log)
            {
                std::lock_guard<std::mutex> g(mutex);
                cloud_log = std::move(temp_log);
                logs.emplace_back(cloud_log.get());
                cloud_log->startup();
            }
            else
                actual_init = false;

            if (actual_init)
                LOG_INFO(log, "Initializing CNCH System log on server for {}.{} successfully", db, tb);
            else
                LOG_INFO(log, "Skip initializing CNCH System log on server for {}.{}", db, tb);
        }
    }
    catch (...)
    {
        {
            std::lock_guard<std::mutex> g(mutex);
            cloud_log.reset();
        }
        ret = false;
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    return ret;
}


bool CnchSystemLogs::initInServer(ContextPtr global_context)
{
    const auto & config = global_context->getConfigRef();

    LOG_INFO(log, "Initializing CNCH System log on server");

    bool kafka_ret = true;
    bool materialized_mysql_ret = true;
    bool unique_table_ret = true;
    bool query_metrics_ret = true;
    bool query_worker_metrics_ret = true;
    bool cnch_query_log_ret = true;
    bool cnch_view_refresh_task_log_ret = true;
    bool cnch_auto_stats_task_log_ret = true;


    if (config.has(CNCH_KAFKA_LOG_CONFIG_PREFIX))
        kafka_ret = initInServerForSingleLog<CloudKafkaLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME,
            CNCH_KAFKA_LOG_CONFIG_PREFIX,
            config,
            cloud_kafka_log);

    if (config.has(CNCH_MATERIALIZED_MYSQL_LOG_CONFIG_PREFIX))
        materialized_mysql_ret = initInServerForSingleLog<CloudMaterializedMySQLLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_MATERIALIZED_MYSQL_LOG_TABLE_NAME,
            CNCH_MATERIALIZED_MYSQL_LOG_CONFIG_PREFIX,
            config,
            cloud_materialized_mysql_log);

    if (config.has(CNCH_UNIQUE_TABLE_LOG_CONFIG_PREFIX))
        unique_table_ret = initInServerForSingleLog<CloudUniqueTableLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_UNIQUE_TABLE_LOG_TABLE_NAME,
            CNCH_UNIQUE_TABLE_LOG_CONFIG_PREFIX,
            config,
            cloud_unique_table_log);

    if (config.has(CNCH_QUERY_LOG_CONFIG_PREFIX))
        cnch_query_log_ret = initInServerForSingleLog<CnchQueryLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_QUERY_LOG_TABLE_NAME,
            CNCH_QUERY_LOG_CONFIG_PREFIX,
            config,
            cnch_query_log);

    if (config.has(CNCH_VIEW_REFRESH_TASK_PREFIX))
        cnch_view_refresh_task_log_ret = initInServerForSingleLog<ViewRefreshTaskLog>(
            global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_VIEW_REFRESH_TASK_LOG_TABLE_NAME,
            CNCH_VIEW_REFRESH_TASK_PREFIX,
            config,
            cnch_view_refresh_task_log);

    if (config.has(CNCH_AUTO_STATS_TASK_LOG_CONFIG_PREFIX))
        cnch_auto_stats_task_log_ret = initInServerForSingleLog<CnchAutoStatsTaskLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_AUTO_STATS_TASK_LOG_TABLE_NAME,
            CNCH_AUTO_STATS_TASK_LOG_CONFIG_PREFIX,
            config,
            cnch_auto_stats_task_log);

    return (kafka_ret && materialized_mysql_ret && unique_table_ret && query_metrics_ret && query_worker_metrics_ret
    && cnch_query_log_ret && cnch_view_refresh_task_log_ret && cnch_auto_stats_task_log_ret);
}

template<typename CloudLog>
bool CnchSystemLogs::initInWorkerForSingleLog(ContextPtr & global_context,
    const String & db,
    const String & tb,
    const String & config_prefix,
    const Poco::Util::AbstractConfiguration & config,
    std::shared_ptr<CloudLog> & cloud_log)
{
    bool ret = false;
    try
    {
        if (!config.has(config_prefix))
            return true;

        std::shared_ptr<CloudLog> temp_log = createCnchLog<CloudLog>(global_context,
            db, tb, config, config_prefix);

        {
            std::lock_guard<std::mutex> g(mutex);
            cloud_log = std::move(temp_log);
            logs.emplace_back(cloud_log.get());
            cloud_log->startup();
        }

        LOG_INFO(log, "Initializing CNCH System log on worker for {}.{} successfully", db, tb);
        ret = true;
    }
    catch (...)
    {
        {
            std::lock_guard<std::mutex> g(mutex);
            cloud_log.reset();
        }
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    return ret;
}

bool CnchSystemLogs::initInWorker(ContextPtr global_context)
{
    const auto & config = global_context->getConfigRef();

    LOG_INFO(log, "Initializing CNCH System log on worker");

    bool kafka_ret = true;
    bool materialized_mysql_ret = true;
    bool unique_table_ret = true;

    if (config.has(CNCH_KAFKA_LOG_CONFIG_PREFIX))
        kafka_ret = initInWorkerForSingleLog<CloudKafkaLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME,
            CNCH_KAFKA_LOG_CONFIG_PREFIX,
            config,
            cloud_kafka_log);

    if (config.has(CNCH_MATERIALIZED_MYSQL_LOG_CONFIG_PREFIX))
        materialized_mysql_ret = initInWorkerForSingleLog<CloudMaterializedMySQLLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_MATERIALIZED_MYSQL_LOG_TABLE_NAME,
            CNCH_MATERIALIZED_MYSQL_LOG_CONFIG_PREFIX,
            config,
            cloud_materialized_mysql_log);

    if (config.has(CNCH_UNIQUE_TABLE_LOG_CONFIG_PREFIX))
        unique_table_ret = initInWorkerForSingleLog<CloudUniqueTableLog>(global_context,
            CNCH_SYSTEM_LOG_DB_NAME,
            CNCH_SYSTEM_LOG_UNIQUE_TABLE_LOG_TABLE_NAME,
            CNCH_UNIQUE_TABLE_LOG_CONFIG_PREFIX,
            config,
            cloud_unique_table_log);

    return (kafka_ret && materialized_mysql_ret && unique_table_ret);
}

CnchSystemLogs::~CnchSystemLogs()
{
    shutdown();
}

void CnchSystemLogs::shutdown()
{
    if (init_task)
        init_task->deactivate();

    std::lock_guard<std::mutex> g(mutex);
    for (auto & l : logs)
    {
        if (l)
            l->shutdown();
    }
}

void CloudKafkaLog::logException(const StorageID & storage_id, String msg, String consumer_id)
{
    try
    {
        KafkaLogElement elem;
        elem.event_type = KafkaLogElement::EXCEPTION;
        elem.event_time = time(nullptr);
        elem.database = storage_id.database_name;
        elem.table = storage_id.table_name;
        elem.cnch_database = storage_id.database_name;
        elem.cnch_table = storage_id.table_name;
        elem.consumer = std::move(consumer_id);
        elem.has_error = true;
        elem.last_exception = std::move(msg);
        add(elem);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

} /// end namespace

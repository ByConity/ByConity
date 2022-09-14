#include <Interpreters/CnchSystemLog.h>
#include <Interpreters/CnchSystemLogHelper.h>
#include <Interpreters/CnchQueryMetrics/QueryMetricLog.h>
#include <Interpreters/CnchQueryMetrics/QueryWorkerMetricLog.h>
#include <Parsers/ASTSetQuery.h>

#include <algorithm>

namespace DB
{

constexpr size_t DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS = 7500;

/// FIXME: @Dao
// std::shared_ptr<CloudKafkaLog> createCloudKafkaLog(
//     Context & context,
//     const String & database,
//     const String & cnch_table,
//     const Poco::Util::AbstractConfiguration & config,
//     const String & config_prefix)
// {
//     if (!config.has(config_prefix))
//         return {};

//     String table = cnch_table + CNCH_SYSTEM_LOG_WORKER_TABLE_SUFFIX;

//     String partition_by = config.getString(config_prefix + ".partition_by", "event_date");
//     String query_columns = KafkaLogElement::createBlock().dumpForQuery();

//     String create_query = "CREATE TABLE " + database + "." + table + query_columns +
//         " ENGINE = CloudMergeTree(" + database + ", "+ cnch_table +
//         " ) ORDER BY (event_date, event_time) \
//         PARTITION BY (" + partition_by + ")";

//     size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds", DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);
//     size_t flush_max_row_count = config.getUInt64(config_prefix + ".flush_max_row_count", DEFAULT_CNCH_LOG_FLUSH_MAX_ROW_COUNT);

//     if (context.getServerType() == ServerType::cnch_server)
//     {
//         return std::make_shared<CloudKafkaLog>(context, database, cnch_table, cnch_table, "", flush_interval_milliseconds, flush_max_row_count);
//     }
//     else if (context.getServerType() == ServerType::cnch_worker)
//     {
//         return std::make_shared<CloudKafkaLog>(context, database, table, cnch_table, create_query, flush_interval_milliseconds, flush_max_row_count);
//     }
//     else
//         return nullptr;
// }

// bool prepareCnchKafkaLogDatabaseAndTables(
//     Context & context,
//     const String & database,
//     const String & table,
//     const Poco::Util::AbstractConfiguration & config,
//     const String & config_prefix)
// {
//     if (!config.has(config_prefix) ||
//         (context.getServerType() != ServerType::cnch_server))
//        return true;

//     const Block expected_block = KafkaLogElement::createBlock();
//     const String query_columns = expected_block.dumpForQuery();
//     const String ttl = config.getString(config_prefix + ".ttl", "31 DAY");
//     const String ttl_expr = ttl.empty() ? "" : "event_date + INTERVAL " + ttl;
//     const String full_ttl_expr = ttl_expr.empty() ? "" : " TTL " + ttl_expr;

//     if (!createDatabaseInCatalog(context, database, Logger::get("CnchSystemLogs")))
//         return false;

//     String create_query = "CREATE TABLE IF NOT EXISTS " + database + "." + table + " " + query_columns +
//         " ENGINE = CnchMergeTree() PARTITION BY (event_date) ORDER BY (event_date, event_time)\
//         " + full_ttl_expr + " SETTINGS index_granularity = 8192";

//     if (!prepareCnchTable(context, database, table, create_query, Logger::get("CnchSystemLogs")))
//         return false;

//     SettingsChanges changes;
//     changes.push_back(SettingChange("index_granularity", Field(UInt64(8192))));

//     if (!syncTableSchema(context, database, table, expected_block, ttl_expr, changes, Logger::get("CnchSystemLogs")))
//         return false;
//     if (!createView(context, database, table, Logger::get("CnchSystemLogs")))
//         return false;
//     return true;
// }

/// Create or update cnch_system.query_metrics and cnch_system.query_worker_metrics tables
bool prepareCnchQueryMetricsDatabaseAndTables(ContextPtr global_context)
{
    const auto & config = global_context->getConfigRef();

    /// For cnch_system.query_metrics
    {
        const String database = CNCH_SYSTEM_LOG_DB_NAME;
        const String table = CNCH_SYSTEM_LOG_QUERY_METRICS_TABLE_NAME;
        const String config_prefix = QUERY_METRICS_CONFIG_PREFIX;
        const String ttl_value = config.getString(config_prefix + ".ttl", "31 DAY");

        if (!createDatabaseInCatalog(global_context, database, Poco::Logger::get("CnchSystemLogs")))
            return false;

        ColumnsWithTypeAndName log_element_columns;
        auto log_element_names_and_types = QueryMetricElement::getNamesAndTypes();

        for (auto name_and_type : log_element_names_and_types)
            log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

        const Block expected_block(std::move(log_element_columns));
        const String query_columns = expected_block.dumpForQuery();
        const String order_by_expr = " ORDER BY (`event_time`, `query_id`)";
        const String partition_by_expr = " PARTITION BY toDate(event_time)";
        /// FIXME: Add after TTL is supported
        // const String ttl_expr = ttl_value.empty() ? "" : " TTL toDate(event_time) + INTERVAL " + ttl_value;
        const String ttl_expr;
        const String storage_policy = ", storage_policy = \'" + global_context->getDefaultCnchPolicyName() + "\'";
        auto enable_memory_buffer = config.getBool("enable_query_metrics_memory_buffer", false);
        String memory_buffer = enable_memory_buffer ? ", cnch_enable_memory_buffer = 1" : "";

        /// TODO: Enable unique key
        // auto enable_unique_key = config.getBool("enable_query_metrics_unique_key", false);
        // String memory_buffer = (enable_memory_buffer && !enable_unique_key)
            // ? ", cnch_enable_memory_buffer = 1" : "";
        // String unique_key = enable_unique_key ? " UNIQUE KEY (`query_id`, `state`)" : "";

        const String create_query = "CREATE TABLE " + database + "." + table + " " + query_columns +
            " ENGINE = CnchMergeTree()" + order_by_expr + partition_by_expr + ttl_expr +
            " SETTINGS index_granularity = 8192" + storage_policy + memory_buffer + ", enable_addition_bg_task = 1";

        if (!prepareCnchTable(global_context, database, table, create_query, Poco::Logger::get("CnchSystemLogs")))
            return false;

        SettingsChanges changes;
        changes.push_back(SettingChange("index_granularity", Field(UInt64(8192))));
        // Ensure memory buffer is disabled if disabled in config
        changes.push_back(SettingChange("cnch_enable_memory_buffer", enable_memory_buffer ? Field(UInt64(1)) : Field(UInt64(0))));

        /// FIXME: after ALTER is supported
        // if (!syncTableSchema(global_context, database, table, expected_block, ttl_expr, changes, Poco::Logger::get("CnchSystemLog")))
        //     return false;
        if (!createView(global_context, database, table, Poco::Logger::get("CnchSystemLogs")))
            return false;
    }

    /// For cnch_system.query_worker_metrics
    {
        const String database = CNCH_SYSTEM_LOG_DB_NAME;
        const String table = CNCH_SYSTEM_LOG_QUERY_WORKER_METRICS_TABLE_NAME;
        const String config_prefix = QUERY_WORKER_METRICS_CONFIG_PREFIX;
        const String ttl_value = config.getString(config_prefix + ".ttl", "31 DAY");

        if (!createDatabaseInCatalog(global_context, database, Poco::Logger::get("CnchSystemLogs")))
            return false;

        ColumnsWithTypeAndName log_element_columns;
        auto log_element_names_and_types = QueryWorkerMetricElement::getNamesAndTypes();

        for (auto name_and_type : log_element_names_and_types)
            log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

        const Block expected_block(std::move(log_element_columns));
        const String query_columns = expected_block.dumpForQuery();
        const String order_by_expr = " ORDER BY (`event_time`, `initial_query_id`, `current_query_id`)";
        const String partition_by_expr = " PARTITION BY toDate(event_time)";
        /// FIXME: Add after TTL is supported
        // const String ttl_expr = ttl_value.empty() ? "" : " TTL toDate(event_time) + INTERVAL " + ttl_value;
        const String ttl_expr;
        const String storage_policy = ", storage_policy = \'" + global_context->getDefaultCnchPolicyName() + "\'";

        const String create_query = "CREATE TABLE " + database + "." + table + " " + query_columns +
            " ENGINE = CnchMergeTree()" + order_by_expr + partition_by_expr + ttl_expr +
            " SETTINGS index_granularity = 8192" + storage_policy + ", enable_addition_bg_task = 1";

        if (!prepareCnchTable(global_context, database, table, create_query, Poco::Logger::get("CnchSystemLogs")))
            return false;

        SettingsChanges changes;
        changes.push_back(SettingChange("index_granularity", Field(UInt64(8192))));

        /// FIXME: after ALTER is supported
        // if (!syncTableSchema(global_context, database, table, expected_block, ttl_expr, changes, Poco::Logger::get("CnchSystemLogs")))
        //     return false;
        if (!createView(global_context, database, table, Poco::Logger::get("CnchSystemLogs")))
            return false;
    }
    return true;
}

/// Create QueryMetricLog object to provide flushing thread for `cnch_system.query_metrics`
std::shared_ptr<QueryMetricLog> createQueryMetricLog(
    ContextPtr global_context,
    const Poco::Util::AbstractConfiguration & config)
{
    const String config_prefix = QUERY_METRICS_CONFIG_PREFIX;
    const String database = CNCH_SYSTEM_LOG_DB_NAME;
    const String table = CNCH_SYSTEM_LOG_QUERY_METRICS_TABLE_NAME;
    size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds",
        DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);

    auto enable_memory_buffer = config.getBool("enable_query_metrics_memory_buffer", false);
    if (enable_memory_buffer)
        flush_interval_milliseconds = std::min<size_t>(flush_interval_milliseconds, 500); // Max limit of 500ms

    return std::make_shared<QueryMetricLog>(global_context, database, table, table, "", flush_interval_milliseconds, 0);
}

/// Create QueryWorkerMetricLog to provide flushing thread for `cnch_system.query_worker_metrics`
std::shared_ptr<QueryWorkerMetricLog> createQueryWorkerMetricLog(
    ContextPtr global_context,
    const Poco::Util::AbstractConfiguration & config)
{
    // The query_worker_metrics config must be identical in cnch-server and cnch-worker
    const String config_prefix = QUERY_WORKER_METRICS_CONFIG_PREFIX;
    const String database = CNCH_SYSTEM_LOG_DB_NAME;
    const String table = CNCH_SYSTEM_LOG_QUERY_WORKER_METRICS_TABLE_NAME;
    size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds",
        DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);

    return std::make_shared<QueryWorkerMetricLog>(global_context, database, table, table, "", flush_interval_milliseconds, 0);
}

CnchSystemLogs::CnchSystemLogs(ContextPtr global_context)
{
    log = &Poco::Logger::get("CnchSystemLogs");
    if (global_context->getServerType() == ServerType::cnch_server)
    {
        init_task = global_context->getSchedulePool().createTask(
            "CnchSystemLogsInitializer",
            [this, global_context] ()
            {
                ++init_time_in_server;
                LOG_DEBUG(log, "Initialise CNCH System log on try: {}", init_time_in_server);
                if (!init_in_server(global_context))
                {
                    LOG_DEBUG(log, "Failed to initialise CnchSystemLog on try: {}", init_time_in_server);
                    if (init_time_in_server < 100)
                        init_task->scheduleAfter(10000);
                }
            });

        init_task->activate();
        init_task->scheduleAfter(10000);
    }
    /// FIXME: @Dao
    // else if (global_context->getServerType() == ServerType::cnch_worker)
    // {
    //     init_task = global_context->getSchedulePool().createTask(
    //         "CnchSystemLogsInitializer",
    //         [this, global_context] ()
    //         {
    //             ++init_time_in_worker;
    //             LOG_DEBUG(log, "Initialised CNCH System log on try: {}", init_time_in_worker);
    //             if (!init_in_worker(global_context))
    //             {
    //                 LOG_DEBUG(log, "Failed to initialise CnchSystemLog on try: {}", init_time_in_worker);
    //                 if (init_time_in_worker < 100)
    //                     init_task->scheduleAfter(10000);
    //             }
    //         }
    //     );

    //     init_task->activate();
    //     init_task->scheduleAfter(10000);
    // }
}

bool CnchSystemLogs::init_in_server(ContextPtr global_context)
{
    bool ret = true;
    try
    {
        bool kafka_ret = true;
        bool query_metric_ret = true;
        const auto & config = global_context->getConfigRef();
        LOG_INFO(log, "Initializing CNCH System log on server");

        /// FIXME: @Dao
        // if (!prepareCnchKafkaLogDatabaseAndTables(global_context, CNCH_SYSTEM_LOG_DB_NAME, CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME, config, CNCH_KAFKA_LOG_CONFIG_PREFIX))
        // {
        //     LOG_INFO(log, "Failed to prepareCnchKafkaLogDatabaseAndTables");
        //     kafka_ret = false;
        // }

        // if (kafka_ret &&
        //     config.has(CNCH_KAFKA_LOG_CONFIG_PREFIX) &&
        //     !std::atomic_load_explicit(&server_cloud_kafka_log, std::memory_order_seq_cst))
        // {
        //     std::shared_ptr<CloudKafkaLog> temp_server_cloud_kafka_log =
        //         createCloudKafkaLog(
        //             global_context,
        //             CNCH_SYSTEM_LOG_DB_NAME,
        //             CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME,
        //             config,
        //             CNCH_KAFKA_LOG_CONFIG_PREFIX
        //         );

        //     std::atomic_store_explicit(&server_cloud_kafka_log,
        //         temp_server_cloud_kafka_log,
        //         std::memory_order_seq_cst);
        //     LOG_INFO(log, "Initializing cnch kafka log succesfully");
        // }

        if (global_context->getSettingsRef().enable_query_level_profiling)
        {
            // Create database and tables
            if (!prepareCnchQueryMetricsDatabaseAndTables(global_context))
            {
                LOG_INFO(log, "Failed to prepareCnchQueryMetricsDatabaseAndTables");
                query_metric_ret = false;
            }
            if (query_metric_ret)
            {
                if (!std::atomic_load_explicit(&query_metrics, std::memory_order_seq_cst))
                {
                    auto temp_query_metrics = createQueryMetricLog(global_context, config);
                    std::atomic_store_explicit(&query_metrics,
                                                temp_query_metrics,
                                                std::memory_order_seq_cst);
                    LOG_INFO(&Poco::Logger::get("CnchSystemLog"), "Created query metrics log");
                }
                if (!std::atomic_load_explicit(&query_worker_metrics, std::memory_order_seq_cst))
                {
                    auto temp_query_worker_metrics = createQueryWorkerMetricLog(global_context, config);
                    std::atomic_store_explicit(&query_worker_metrics,
                        temp_query_worker_metrics,
                        std::memory_order_seq_cst);
                    LOG_INFO(&Poco::Logger::get("CnchSystemLog"), "Created query worker metrics log");
                }
            }
        }
        ret = kafka_ret && query_metric_ret;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        ret = false;
    }
    return ret;
}

/// FIXME: @Dao
// bool CnchSystemLogs::init_in_worker(ContextPtr global_context)
// {
//     bool ret = false;
//     try
//     {
//         auto & config = global_context->getConfigRef();
//         LOG_INFO(log, "Initializing CNCH System log on worker");
//         if (config.has(CNCH_KAFKA_LOG_CONFIG_PREFIX) &&
//             !std::atomic_load_explicit(&worker_cloud_kafka_log, std::memory_order_seq_cst))
//         {
//             std::shared_ptr<CloudKafkaLog> temp_worker_cloud_kafka_log
//                 = createCloudKafkaLog(
//                     global_context,
//                     CNCH_SYSTEM_LOG_DB_NAME,
//                     CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME,
//                     config,
//                     CNCH_KAFKA_LOG_CONFIG_PREFIX
//                   );

//             if (temp_worker_cloud_kafka_log)
//                 std::atomic_store_explicit(&worker_cloud_kafka_log,
//                     temp_worker_cloud_kafka_log,
//                     std::memory_order_seq_cst);
//             LOG_INFO(log, "Initializing cnch kafka log succesfully");
//         }
//         ret = true;
//     }
//     catch (...)
//     {
//         tryLogCurrentException(log, __PRETTY_FUNCTION__);
//         ret = false;
//     }

//     return ret;
// }

CnchSystemLogs::~CnchSystemLogs()
{
    if (init_task)
        init_task->deactivate();
    /// FIXME: @Dao
    // if (worker_cloud_kafka_log)
    //     worker_cloud_kafka_log->shutdown();
    // if (server_cloud_kafka_log)
    //     server_cloud_kafka_log->shutdown();
    if (query_metrics)
        query_metrics->shutdown();
    if (query_worker_metrics)
        query_worker_metrics->shutdown();
}

/// FIXME: @Dao
// void CloudKafkaLog::logException(const StorageID & storage_id, String msg, String consumer_id)
// {
//     try
//     {
//         KafkaLogElement elem;
//         elem.event_type = KafkaLogElement::EXCEPTION;
//         elem.event_time = time(nullptr);
//         elem.database = storage_id.database_name;
//         elem.table = storage_id.table_name;
//         elem.cnch_database = storage_id.database_name;
//         elem.cnch_table = storage_id.table_name;
//         elem.consumer = std::move(consumer_id);
//         elem.has_error = true;
//         elem.last_exception = std::move(msg);
//         add(elem);
//     }
//     catch (...)
//     {
//         tryLogCurrentException(log, __PRETTY_FUNCTION__);
//     }
// }

} /// end namespace

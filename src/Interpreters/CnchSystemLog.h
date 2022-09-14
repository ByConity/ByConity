#pragma once

#include <Parsers/ParserDropQuery.h>
#include <Interpreters/KafkaLog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Transaction/CnchWorkerTransaction.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchCreateQueryHelper.h>
#include <Catalog/Catalog.h>
#include <Common/serverLocality.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>


namespace DB
{

class QueryMetricLog;
class QueryWorkerMetricLog;

namespace ErrorCodes
{
extern const int SYSTEM_ERROR;
extern const int UNKNOWN_TABLE;
extern const int LOGICAL_ERROR;
}

constexpr size_t DEFAULT_CNCH_LOG_FLUSH_MAX_ROW_COUNT = 10000; // one record has size ~200 byte => total size 2Mb

// Query metrics definitions
constexpr auto CNCH_SYSTEM_LOG_DB_NAME = "cnch_system";
constexpr auto CNCH_SYSTEM_LOG_QUERY_METRICS_TABLE_NAME = "query_metrics";
constexpr auto CNCH_SYSTEM_LOG_QUERY_WORKER_METRICS_TABLE_NAME = "query_worker_metrics";
constexpr auto CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME = "cnch_kafka_log";

static inline bool isQueryMetricsTable(const String & database, const String & table)
{
    return (database == CNCH_SYSTEM_LOG_DB_NAME || database == "system") &&
            (table == CNCH_SYSTEM_LOG_QUERY_METRICS_TABLE_NAME ||
            table == CNCH_SYSTEM_LOG_QUERY_WORKER_METRICS_TABLE_NAME);
}


/** Modified version of SystemLog that flushes data to a CnchMergeTree table.
  * Supports flushing through
  * 1. Periodic flushing
  * 2. Buffer size
  *
  * Altering of schema is also possible for columns that are not part of primary/partition keys.
  * Reordering of columns and prepending new columns are not supported.
  */
struct CnchSystemLogs
{
    CnchSystemLogs(ContextPtr global_context);
    ~CnchSystemLogs();

    /// FIXME: @Dao
    // std::shared_ptr<CloudKafkaLog> server_cloud_kafka_log;
    // std::shared_ptr<CloudKafkaLog> worker_cloud_kafka_log;
    std::shared_ptr<QueryMetricLog> query_metrics;                /// Used to log query metrics.
    std::shared_ptr<QueryWorkerMetricLog> query_worker_metrics;   /// Used to log query worker metrics.
    int init_time_in_worker{};
    int init_time_in_server{};
    bool init_in_worker(ContextPtr global_context);
    bool init_in_server(ContextPtr global_context);
    BackgroundSchedulePool::TaskHolder init_task;
    Poco::Logger * log;
};

constexpr auto CNCH_SYSTEM_LOG_WORKER_TABLE_SUFFIX = "_local_insertion";
constexpr auto QUERY_METRICS_CONFIG_PREFIX = "query_metrics";
constexpr auto QUERY_WORKER_METRICS_CONFIG_PREFIX = "query_worker_metrics";
constexpr auto CNCH_KAFKA_LOG_CONFIG_PREFIX = "cnch_kafka_log";

template<typename LogElement>
class CnchSystemLog : WithContext, private boost::noncopyable
{
public:
    CnchSystemLog(const ContextPtr & global_context_,
        const String & database_name_,
        const String & table_name_,
        const String & cnch_table_name_,
        const String & create_query_,
        size_t flush_interval_milliseconds_,
        size_t flush_max_row_count_);


    void flushImpl(bool quiet);

    virtual bool needFlush(size_t elapsed_time_ms, size_t record_cnt);

    virtual ~CnchSystemLog();

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */
    void add(const LogElement & element)
    {
        if (this->is_shutdown)
            return;

        /// Without try we could block here in case of queue overflow.
        if (!queue.tryPush({false, element}))
            LOG_ERROR(this->log, "{} queue is full", LogElement::name());
    }

    /// Flush data in the buffer to disk
    void flush()
    {
        if (!this->is_shutdown)
            flushImpl(false);
    }

    String getDatabaseName() const
    {
        return this->database_name;
    }

    String getTableName() const
    {
        return this->table_name;
    }

    StoragePtr prepareTable(std::function<void()> self_shutdown, ContextMutablePtr query_context);

    void writeToCnchTable(Block & block, StoragePtr & table, ContextMutablePtr query_context);

    /// Stop the background flush thread before destructor. No more data will be written.
    void shutdown();

protected:
    const String database_name;
    const String table_name;
    UUID uuid;
    const String cnch_table_name;
    const String create_query;
    Poco::Logger * log;

    std::atomic<bool> is_shutdown{false};

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      */
    bool is_prepared = false;
    const size_t flush_interval_milliseconds;
    const size_t flush_max_row_count_setting;
    size_t flush_max_row_count;

    using QueueItem = std::pair<bool, LogElement>;        /// First element is shutdown flag for thread.

    /// Queue is bounded. But its size is quite large to not block in all normal cases.
    ConcurrentBoundedQueue<QueueItem> queue {DBMS_SYSTEM_LOG_QUEUE_SIZE};

    /** Data that was pulled from queue. Data is accumulated here before enough time passed.
      * It's possible to implement double-buffering, but we assume that insertion into table is faster
      *  than accumulation of large amount of log records (for example, for query log - processing of large amount of queries).
      */
    std::vector<LogElement> data;
    std::mutex data_mutex;

    /** In this thread, data is pulled from 'queue' and stored in 'data', and then written into table.
      */
    ThreadFromGlobalPool saving_thread;

    void threadFunction();
};

template <typename LogElement>
CnchSystemLog<LogElement>::CnchSystemLog(const ContextPtr & global_context_,
    const String & database_name_,
    const String & table_name_,
    const String & cnch_table_name_,
    const String & create_query_,
    size_t flush_interval_milliseconds_,
    size_t flush_max_row_count_)
    : WithContext(global_context_)
    , database_name(std::move(database_name_))
    , table_name(std::move(table_name_))
    , cnch_table_name(std::move(cnch_table_name_))
    , create_query(std::move(create_query_))
    , log(&Poco::Logger::get("CnchSystemLog (" + database_name + "." + table_name + ")"))
    , flush_interval_milliseconds(flush_interval_milliseconds_)
    , flush_max_row_count_setting(flush_max_row_count_)
    , flush_max_row_count(flush_max_row_count_)
{
    auto storage = getContext()->getCnchCatalog()->getTable(*getContext(), database_name, cnch_table_name, TxnTimestamp::maxTS());
    if (!storage)
        throw Exception("Failed to get table " + database_name + "." + cnch_table_name + " from catalog!", ErrorCodes::LOGICAL_ERROR);
    uuid = storage->getStorageUUID();
    data.reserve(DBMS_SYSTEM_LOG_QUEUE_SIZE);
    saving_thread = ThreadFromGlobalPool([this] { threadFunction(); });
}

template <typename LogElement>
void CnchSystemLog<LogElement>::writeToCnchTable(Block & block, StoragePtr & table, ContextMutablePtr query_context)
{
    std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
    insert->table_id.database_name = database_name;
    insert->table_id.table_name = table_name;
    ASTPtr query_ptr(insert.release());

    auto host_with_port = getContext()->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(uuid), false);
    auto host_address = host_with_port.host;
    auto host_port = host_with_port.rpc_port;
    auto server_client = query_context->getCnchServerClient(host_address, host_port);
    if (!server_client)
    {
        throw Exception("Failed to get ServerClient", ErrorCodes::SYSTEM_ERROR);
    }

    TransactionCnchPtr cnch_txn;
    if (query_context->getServerType() == ServerType::cnch_server
        && isLocalServer(host_with_port.getRPCAddress(), std::to_string(getContext()->getRPCPort())))
    {
        cnch_txn = query_context->getCnchTransactionCoordinator().createTransaction();
        query_context->setCurrentTransaction(cnch_txn);
    }
    else
    {
        LOG_DEBUG(log, "Using table host server for committing: {}", server_client->getRPCAddress());
        cnch_txn = std::make_shared<CnchWorkerTransaction>(query_context, server_client);
        query_context->setCurrentTransaction(cnch_txn);
    }

    {
        SCOPE_EXIT({
            try
            {
                if (dynamic_pointer_cast<CnchServerTransaction>(cnch_txn))
                    query_context->getCnchTransactionCoordinator().finishTransaction(cnch_txn);
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        });

        auto & client_info = query_context->getClientInfo();
        // Obtain ServerClient host port & set them in ClientInfo
        auto host_ports = server_client->getHostWithPorts();
        auto current_addr_port = client_info.current_address.port();

        // Poco::Net::SocketAddress can't parse ipv6 host with [] for example [::1], so always pass by host_port string created by createHostPortString
        client_info.current_address =
            Poco::Net::SocketAddress(createHostPortString(host_ports.host, current_addr_port));
        client_info.rpc_port = host_ports.rpc_port;

        BlockOutputStreamPtr stream = table->write(query_ptr, table->getInMemoryMetadataPtr(), query_context);

        ///  It is possible for writes to fail
        stream->writePrefix();
        stream->write(block);
        stream->writeSuffix();

        if (dynamic_pointer_cast<CnchWorkerTransaction>(cnch_txn))
        {
            cnch_txn->commitV2();
        }
    }
}

template <typename LogElement>
StoragePtr CnchSystemLog<LogElement>::prepareTable(std::function<void()> self_shutdown, ContextMutablePtr query_context)
{
    bool table_not_found = false;
    StoragePtr table;

    String description = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name);

    if (getContext()->getServerType() == ServerType::cnch_worker)
    {
        StorageID table_id(database_name, table_name);
        if (!DatabaseCatalog::instance().isDatabaseExist(database_name))
        {
            String query = "CREATE DATABASE " + database_name;
            ParserCreateQuery parser;
            ASTPtr ast = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            if (!ast)
            {
                LOG_WARNING(log, "Failed to parse query: {}", query);
                if (self_shutdown)
                    self_shutdown();
                return nullptr;
            }
            InterpreterCreateQuery interpreter(ast, query_context);
            interpreter.execute();
        }

        StoragePtr server_storage{nullptr};
        try
        {
            server_storage = getContext()->getCnchCatalog()->getTable(*query_context, database_name, cnch_table_name, TxnTimestamp::maxTS());
        }
        catch (Exception & e)
        {
            /// do not need to log exception if table not found.
            if (e.code() == ErrorCodes::UNKNOWN_TABLE)
            {
                table_not_found = true;
            }
            else
                tryLogDebugCurrentException(__PRETTY_FUNCTION__);
        }

        if (!server_storage)
        {
            if (table_not_found)
            {
                LOG_DEBUG(log, "Table in server {}.{} no longer exists, shutting down log", database_name, cnch_table_name);
                if (self_shutdown)
                    self_shutdown();
            }
            else
            {
                LOG_DEBUG(log, "Table in server {}.{} was not found due to an unexpected exception.", database_name, cnch_table_name);
                /// it could be temporary disconnect
                table.reset();
            }
        }
        else
        {
            UUID server_uuid = server_storage->getStorageUUID();
            /// Drop the table if UUID doesn't match
            if (uuid != server_uuid)
            {
                table.reset();
                String drop_table_query = "DROP TABLE IF EXISTS " + database_name + "." + table_name;
                ParserDropQuery drop_parser;

                ASTPtr ast = parseQuery(drop_parser, drop_table_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                if (!ast)
                {
                    LOG_WARNING(log, "Failed to parse query: {}", drop_table_query);
                    if (self_shutdown)
                        self_shutdown();
                    return nullptr;
                }
                InterpreterDropQuery interpreter(ast, query_context);
                interpreter.execute();
            }
            else
            {
                table = DatabaseCatalog::instance().tryGetTable(table_id, query_context);
            }
        }

        if (table)
        {
            if (!is_prepared)
                LOG_DEBUG(log, "Will use existing table {}.{}", database_name, table_name);
        }
        else
        {
            LOG_DEBUG(log, "create new table: {}", description);

            ParserCreateQuery parser;
            ASTPtr ast = parseQuery(parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            if (!ast)
            {
                LOG_WARNING(log, "Failed to parse query: {}", create_query);
                if (self_shutdown)
                    self_shutdown();
                return nullptr;
            }
            auto & create = ast->as<ASTCreateQuery &>();
            create.uuid = uuid;

            InterpreterCreateQuery interpreter(ast, query_context);
            interpreter.execute();

            table = DatabaseCatalog::instance().tryGetTable(table_id, query_context);
        }
    }
    else
    {
        StorageID table_id(database_name, cnch_table_name);
        try
        {
            table = DatabaseCatalog::instance().getTable(table_id, query_context);
        }
        catch (Exception & e)
        {
            table.reset();
            /// do not need to log exception if table not found.
            if (e.code() == ErrorCodes::UNKNOWN_TABLE)
                table_not_found = true;
            else
                tryLogDebugCurrentException(__PRETTY_FUNCTION__);

        }

        if (!table)
        {
            if (table_not_found)
            {
                LOG_DEBUG(log, "Table {} no longer exists, shutting down log", description);
                if (self_shutdown)
                    self_shutdown();
            }
            else
            {
                LOG_DEBUG(log, "Table {} was not found due to an unexpected exception.", description);
                return nullptr;
            }
        }
        else
        {
            ColumnsWithTypeAndName log_element_columns;
            auto log_element_names_and_types = LogElement::getNamesAndTypes();

            for (auto name_and_type : log_element_names_and_types)
                log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

            const Block expected(std::move(log_element_columns));
            const Block actual = table->getInMemoryMetadataPtr()->getSampleBlockNonMaterialized();

            if (!blocksHaveEqualStructure(actual, expected))
            {
                LOG_DEBUG(log, "Existing table {} has changed, shutting down log", description);

                table.reset();
                if (self_shutdown)
                    self_shutdown();
                return nullptr;
            }
            else if (!is_prepared)
                LOG_DEBUG(log, "Will use existing table {} for {}", description, LogElement::name());
        }
    }

    is_prepared = true;
    return table;
}


template <typename LogElement>
void CnchSystemLog<LogElement>::flushImpl(bool quiet)
{
    String description = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name);
    std::unique_lock lock(data_mutex);

    try
    {
        if (quiet && data.empty())
            return;

        auto self_shutdown = [this]() {
            bool old_val = false;
            if (!this->is_shutdown.compare_exchange_strong(old_val, true))
                return;
            if (!queue.push({true, {}}))
                LOG_WARNING(log, "queue has already been finished");
        };

        /// Use temporary context in case the txn affects the original context
        auto query_context = Context::createCopy(getContext());
        query_context->makeSessionContext();
        query_context->makeQueryContext();

        /// We check for existence of the table in worker and create it as needed at every flush.
        /// BTW, flush method is called from single thread.
        auto table = this->prepareTable(self_shutdown, query_context);

        if (!table)
        {
            LOG_ERROR(log, "Can't find table {} for {}, treat as temporary error!", description, LogElement::name());

            throw Exception("Failed to get table " + description
                + " for " + LogElement::name(), ErrorCodes::SYSTEM_ERROR);
        }

        ColumnsWithTypeAndName log_element_columns;
        auto log_element_names_and_types = LogElement::getNamesAndTypes();

        for (auto name_and_type : log_element_names_and_types)
            log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

        Block block(std::move(log_element_columns));

        MutableColumns columns = block.mutateColumns();
        for (const auto & elem : data)
            elem.appendToBlock(columns);

        block.setColumns(std::move(columns));

        this->writeToCnchTable(block, table, query_context);

        ///  Clear data after successful insertion and reset flush_max_row_count.
        data.clear();
        flush_max_row_count = flush_max_row_count_setting;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        /// In case of exception, only clean accumulated data when it is too big to avoid locking.
        if ((data.size() > (DBMS_SYSTEM_LOG_QUEUE_SIZE - (flush_max_row_count_setting * 2))) ||
            (data.size() > (DBMS_SYSTEM_LOG_QUEUE_SIZE - DEFAULT_CNCH_LOG_FLUSH_MAX_ROW_COUNT)))
        {
            LOG_INFO(log, "clean accumulated data to avoid locking, loss {} rows", std::to_string(data.size()));
            data.clear();
            flush_max_row_count = flush_max_row_count_setting;
        }
        else
        {
            if (data.size() > flush_max_row_count)
                flush_max_row_count += flush_max_row_count_setting;
        }
    }
}

template <typename LogElement>
void CnchSystemLog<LogElement>::shutdown()
{
    bool old_val = false;
    if (this->is_shutdown.compare_exchange_strong(old_val, true))
    {
        /// Tell thread to shutdown.
        if (!queue.push({true, {}}))
            LOG_WARNING(log, "queue has already been finished");
    }

    if (saving_thread.joinable())
        saving_thread.join();
}

template <typename LogElement>
void CnchSystemLog<LogElement>::threadFunction()
{
    String thread_name = "CnchSysLogFlush";
    setThreadName(thread_name.c_str());

    Stopwatch time_after_last_write;
    bool first = true;
    size_t buffer_size = 0;

    while (true)
    {
        try
        {
            if (first)
            {
                time_after_last_write.restart();
                first = false;
            }

            QueueItem element;
            bool has_element = false;

            bool is_empty;
            {
                std::unique_lock lock(data_mutex);
                is_empty = data.empty();
            }

            /// data.size() is increased only in this function
            /// TODO: get rid of data and queue duality

            if (is_empty)
            {
                has_element = queue.pop(element);
            }
            else
            {
                size_t milliseconds_elapsed = time_after_last_write.elapsed() / 1000000;
                if (milliseconds_elapsed < flush_interval_milliseconds)
                    has_element = queue.tryPop(element, flush_interval_milliseconds - milliseconds_elapsed);
            }

            if (has_element)
            {
                if (element.first)
                {
                    /// Shutdown.
                    /// NOTE: flush data to CloudMergeTree engine even when it is already in shutdown state.
                    flush();
                    break;
                }
                else
                {
                    std::unique_lock lock(data_mutex);
                    data.push_back(element.second);
                    buffer_size = data.size();
                }
            }

            size_t milliseconds_elapsed = time_after_last_write.elapsed() / 1000000;
            if (needFlush(milliseconds_elapsed, buffer_size))
            {
                /// Write data to a table.
                flushImpl(true);
                time_after_last_write.restart();
            }
        }
        catch (...)
        {
            /// In case of exception we lost accumulated data - to avoid locking.
            data.clear();
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

template <typename LogElement>
bool CnchSystemLog<LogElement>::needFlush(size_t elapsed_time_ms, size_t record_cnt)
{
    return ((elapsed_time_ms >= flush_interval_milliseconds) ||
            (record_cnt > flush_max_row_count));
}

template <typename LogElement>
CnchSystemLog<LogElement>::~CnchSystemLog()
{
    shutdown();
}

/// Instead of typedef - to allow forward declaration.
class CloudKafkaLog : public CnchSystemLog<KafkaLogElement>
{
public:
    using CnchSystemLog<KafkaLogElement>::CnchSystemLog;
    void logException(const StorageID & storage_id, String msg, String consumer_id = "");
};

} // end namespace

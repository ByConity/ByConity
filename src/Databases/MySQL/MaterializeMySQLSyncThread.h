#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <mutex>
#    include <Common/Logger.h>
#    include <Core/MySQL/MySQLClient.h>
#    include <DataStreams/BlockIO.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/IDatabase.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Databases/MySQL/MaterializeMySQLSettings.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <mysqlxx/Pool.h>
#    include <mysqlxx/PoolWithFailover.h>


namespace DB
{

/** MySQL table structure and data synchronization thread
 *
 *  When catch exception, it always exits immediately.
 *  In this case, you need to execute DETACH DATABASE and ATTACH DATABASE after manual processing
 *
 *  The whole work of the thread includes synchronous full data and real-time pull incremental data
 *
 *  synchronous full data:
 *      We will synchronize the full data when the database is first create or not found binlog file in MySQL after restart.
 *
 *  real-time pull incremental data:
 *      We will pull the binlog event of MySQL to parse and execute when the full data synchronization is completed.
 */
class MaterializeMySQLSyncThread : WithContext
{
public:
    ~MaterializeMySQLSyncThread();

    MaterializeMySQLSyncThread(
        ContextPtr context,
        const String & database_name_,
        const String & mysql_database_name_,
        const String & thread_key_,
        mysqlxx::Pool && pool_,
        MySQLClient && client_,
        MaterializeMySQLSettings * settings_,
        const String & assigned_table);

    void stopSynchronization();

    void startSynchronization();

    void assertMySQLAvailable();

    static bool isMySQLSyncThread();

    void setBinLogInfo(const MySQLBinLogInfo & binlog) { binlog_info = binlog; }

private:
    LoggerPtr log;

    String database_name;
    String mysql_database_name;

    String assigned_materialized_table;
    String thread_key;

    mutable mysqlxx::Pool pool;
    mutable MySQLClient client;
    MaterializeMySQLSettings * settings;
    String query_prefix;
    MySQLBinLogInfo binlog_info;

    Int64 already_skip_errors = 0;

    BackgroundSchedulePool::TaskHolder heartbeat_task;
    time_t last_heartbeat_time {0};
    UInt64 exception_occur_times{0};

    struct Buffers
    {
        String database;
        String table_suffix;

        /// thresholds
        size_t max_block_rows = 0;
        size_t max_block_bytes = 0;
        size_t total_blocks_rows = 0;
        size_t total_blocks_bytes = 0;

        using BufferAndUniqueColumns = std::pair<Block, std::vector<size_t>>;
        using BufferAndUniqueColumnsPtr = std::shared_ptr<BufferAndUniqueColumns>;
        std::unordered_map<String, BufferAndUniqueColumnsPtr> data;

        Buffers(const String & database_, const String & table_suffix_) : database(database_), table_suffix(table_suffix_) {}

        void prepareCommit(ContextMutablePtr query_context, const String & table_name, const MySQLBinLogInfo & binlog);

        void commit(ContextPtr context, const MySQLReplication::Position & position);

        void add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes);

        bool checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const;

        BufferAndUniqueColumnsPtr getTableDataBuffer(const String & table, ContextPtr context);

        String printBuffersInfo();
    };

    void synchronization();

    bool isCancelled() { return sync_quit.load(std::memory_order_relaxed); }

    bool prepareSynchronized(MaterializeMetadata & metadata);

    void flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata);

    void onEvent(Buffers & buffers, const MySQLReplication::BinlogEventPtr & event, MaterializeMetadata & metadata);

    void commitPosition(const MySQLReplication::Position & position);
    void tryCommitPosition(const MySQLReplication::Position & position);

    std::atomic<bool> sync_quit{false};
    std::unique_ptr<ThreadFromGlobalPool> background_thread_pool;
    void executeDDLAtomic(const QueryEvent & query_event);

    void setSynchronizationThreadException(const std::exception_ptr & exception);
    void recordException(bool create_log = true, const String & resync_table = "");

    void heartbeat();
    void reportSyncFailed();
    void stopSelf();
};

}

#endif

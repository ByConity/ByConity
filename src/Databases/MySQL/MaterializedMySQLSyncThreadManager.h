#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>
#include <CloudServices/ICnchBGThread.h>
#include <CloudServices/CnchWorkerClientPools.h>

namespace DB
{

class MaterializedMySQLSyncThreadManager : public ICnchBGThread
{
public:
    MaterializedMySQLSyncThreadManager(ContextPtr context_, const StorageID & storage_id_);
    ~MaterializedMySQLSyncThreadManager() override;

    void drop() override;

    void executeDDLQuery(const String & query, const String & sync_thread_key, const MySQLBinLogInfo & binlog);
    void executeDDLQueryImpl(const MySQLDDLQueryParams & ddl_params, const String & sync_thread_key, const MySQLBinLogInfo & binlog);
    void executeDDLQuerySuffix(const String & sync_thread_key, const MySQLBinLogInfo & binlog, const MySQLDDLQueryParams & ddl_params);
    void handleHeartbeatOfSyncThread(const String & sync_thread_key);
    void handleSyncFailedThread(const String & sync_thread_key);
    void manualResyncTable(const String & table_name);

    MySQLSyncStatusInfo getSyncStatusInfo();

protected:
    void runImpl() override;

    bool iterate();

private:
    /// Used to ensure the lifecycle of database pointer
    DatabasePtr database_ptr = nullptr;
    DatabaseCnchMaterializedMySQL * materialized_mysql_ptr = nullptr;

    /// Create a struct to avoid trying to copy MySQLClient, which is noncopyable
    /// Only initialize once based on assumption that definition of MaterializedMySQL is not changeable
    struct MySQLComponent
    {
        mutable mysqlxx::Pool pool;
        mutable MySQLClient client;

        MySQLComponent(mysqlxx::Pool && pool_, MySQLClient && client_)
            : pool(std::move(pool_)), client(std::move(client_)) {}

        ~MySQLComponent()
        {
            try
            {
                client.disconnect();
            } catch (...)
            {
                tryLogCurrentException(__func__);
            }
        }
    };
    std::shared_ptr<MySQLComponent> mysql_component, mysql_readonly_replica_component;

    NameSet materialized_tables_list;
    bool initialized{false};
    struct SyncThreadScheduleInfo
    {
        SyncThreadScheduleInfo() = default;
        SyncThreadScheduleInfo(const SyncThreadScheduleInfo & other)
            : assigned_materialized_table(other.assigned_materialized_table)
            , create_queries(other.create_queries)
            , worker_client(other.worker_client)
            , binlog_info(other.binlog_info)
            , sync_thread_key(other.sync_thread_key)
        {}

        String assigned_materialized_table;
        Strings create_queries;
        CnchWorkerClientPtr worker_client;
        MySQLBinLogInfo binlog_info;
        /// Used to identify each sync thread;
        /// Also used as the suffix of local table on Cnch Worker side to identify cloud-table
        String sync_thread_key;
        bool is_running{false};

        mutable std::mutex mutex;
    };
    using SyncThreadScheduleInfoPtr = std::shared_ptr<SyncThreadScheduleInfo>;
    std::vector<SyncThreadScheduleInfoPtr> threads_info;
    /**
    * It should be noted that in order to prevent deadlocks, when resync lock and status lock need to be acquired at the same time,
    * locks should be acquired in the order of resync lock->status lock
    */
    std::mutex status_info_mutex;
    NameSet sync_failed_tables;
    NameSet skipped_unsupported_tables;

    CnchWorkerClientPoolPtr worker_pool;

    /// Sync info
    std::atomic<MaterializedMySQLSyncType> sync_type{MaterializedMySQLSyncType::PreparingSync};
    /// Record last exception for debug
    String last_exception;
    std::mutex last_exception_mutex;

    /** Resync table **/
    mutable std::mutex resync_mutex;
    NameSet resync_tables;
    std::atomic<bool> resync_table_quit{false};
    BackgroundSchedulePool::TaskHolder resync_table_task;

    void clearData() override;

    void initialize();
    void updateResyncTables();
    void initializeMaterializedMySQL(std::shared_ptr<Protos::MaterializedMySQLManagerMetadata> & manager_metadata);
    void createReadonlyReplicaClient(const String & replica_info, const String & mysql_database_name);

    void checkStatusOfSyncThread(SyncThreadScheduleInfoPtr & info);
    void scheduleSyncThreadToWorker(SyncThreadScheduleInfoPtr & info);
    CnchWorkerClientPtr selectWorker();

    void stopSyncThreads();
    void stopSyncThreadOnWorker(SyncThreadScheduleInfoPtr & info);

    Protos::MaterializedMySQLBinlogMetadata createBinlogMetadata(const MaterializeMetadata & metadata, const String & table_name = "");
    Protos::MaterializedMySQLBinlogMetadata createBinlogMetadata(const MySQLBinLogInfo & binlog, const String & table_name = "");
    void getBinlogMetadataFromCatalog(MySQLBinLogInfo & binlog, const String & table_name = "");

    /// Get mysql latest position info. If there has exception, return empty position.
    Position getMasterPosition() const;

    /// Each time when the manager starts, get materialized_tables_list according to include_tables, exclude_tables
    void updateSyncTableList();
    /// Check whether this table should be synced base on the settings "include_tables" and "exclude_tables"
    /// ignore_skip: whether to apply skip_sync_failed_tables and skipped_unsupported_tables
    bool shouldSyncTable(const String & table_name, bool ignore_skip = false, bool need_lock = true);

    void setSyncType(MaterializedMySQLSyncType new_type);
    void recordException(bool create_log = true, const String & resync_table = "");

    bool isResyncTableCancelled() { return resync_table_quit.load(std::memory_order_relaxed); }
    void runResyncTableTask();
    void doResyncTable(const String & table_name);
};

}
#endif

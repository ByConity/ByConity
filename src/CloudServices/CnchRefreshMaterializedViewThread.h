#pragma once
#include <CloudServices/ICnchBGThread.h>

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Interpreters/VirtualWarehouseHandle.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/StorageMaterializedView.h>
#include <Transaction/ICnchTransaction.h>
#include <WorkerTasks/ManipulationList.h>

#include <condition_variable>

namespace DB
{

class CnchWorkerClient;
using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
using CnchWorkerClientPool = RpcClientPool<CnchWorkerClient>;
using CnchWorkerClientPoolPtr = std::shared_ptr<CnchWorkerClientPool>;

class CnchRefreshMaterializedViewThread;

struct MvRefreshTaskRecord
{
    explicit MvRefreshTaskRecord(CnchRefreshMaterializedViewThread & p) : parent(p) {}
    ~MvRefreshTaskRecord();

    CnchRefreshMaterializedViewThread & parent;
    ManipulationType type;
    String task_id;
    TransactionCnchPtr transaction;
    AsyncRefreshParamPtr refresh_param;

    // local execute threads
    ThreadFromGlobalPool local_execute_thread;

    CnchWorkerClientPtr worker;
    size_t lost_count{0};
};

enum class MvPartitionRefreshStatus
{
    WAIT_PREPARE = 0,
    PREPARE,
    PENDING,
    RUNNING,
    END
};

struct MvRefreshPartitionRecord
{
    MvRefreshPartitionRecord(): status(MvPartitionRefreshStatus::PREPARE), start_ts(0) {}
    ~MvRefreshPartitionRecord() = default;

    String task_id;
    MvPartitionRefreshStatus status;
    UInt64 start_ts;
};

class CnchRefreshMaterializedViewThread : public ICnchBGThread
{
    using TaskRecord = MvRefreshTaskRecord;
    using TaskRecordPtr = std::unique_ptr<TaskRecord>;
    using MvRefreshPartitionRecordPtr = std::shared_ptr<MvRefreshPartitionRecord>;

    friend struct MvRefreshTaskRecord;

public:
    CnchRefreshMaterializedViewThread(ContextPtr context, const StorageID & id);
    ~CnchRefreshMaterializedViewThread() override;
    
    bool constructAndScheduleRefreshTasks(StoragePtr & istorage, StorageMaterializedView & storage);
    void handleRefreshTaskOnFinish(String task_id, Int64 txn_id);

private:
    using TasksRecordsMap = std::unordered_map<String, TaskRecordPtr>;
    using MvRefreshRecordsMap = std::unordered_map<String, MvRefreshPartitionRecordPtr>;

    void preStart() override;
    void clearData() override;

    void runHeartbeatTask();
    void runImpl() override;
    bool startRefreshTask(StoragePtr & istorage, StorageMaterializedView & storage);
    String submitTaskRecord(StoragePtr istorage, StorageMaterializedView & storage, TaskRecordPtr record);
    String executeTaskLocal(
        StoragePtr istorage,
        StorageMaterializedView & storage,
        TaskRecordPtr record,
        ContextMutablePtr query_context);

    StorageMaterializedView & checkAndGetMaterializedViewTable(StoragePtr & storage);
    UInt64 checkAndGetRefreshInterval(StorageMaterializedView & storage);

    CnchWorkerClientPtr getWorker(ManipulationType type);
    void removeTaskImpl(const String & task_id, std::lock_guard<std::mutex> &);

    std::mutex task_records_mutex;
    TasksRecordsMap task_records;

    std::mutex partition_stats_mutex;
    MvRefreshRecordsMap partition_stats;

    /// Separate quota for refresh tasks
    std::atomic<int> running_tasks{0};
    std::mutex worker_pool_mutex;
    String vw_name;
    VirtualWarehouseHandle vm_handle;
    std::atomic_bool shutdown_called{false};
};


}

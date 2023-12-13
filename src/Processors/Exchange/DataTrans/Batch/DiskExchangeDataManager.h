#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Protos/registry.pb.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <boost/core/noncopyable.hpp>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>
#include <common/types.h>

namespace DB
{

struct DiskExchangeDataManagerOptions
{
    /// relative path in disk to store exchange data
    String path;
    String storage_policy;
    /// disk exchange data manager's volume name
    String volume;
    /// every gc_interval_seconds seconds, disk exchange manager will check and delete all files not needed by current tasks
    size_t gc_interval_seconds;
    /// files will expire after this
    size_t file_expire_seconds;
    /// random interval before start gc task
    size_t start_gc_random_wait_seconds = 300;
    String toString() const;
};

class DiskPartitionWriter;
using DiskPartitionWriterPtr = std::shared_ptr<DiskPartitionWriter>;
class DiskExchangeDataManager;
using DiskExchangeDataManagerPtr = std::shared_ptr<DiskExchangeDataManager>;
class DiskExchangeDataManager final : public WithContext, boost::noncopyable
{
public:
    static DiskExchangeDataManagerPtr createDiskExchangeDataManager(
        const ContextWeakMutablePtr & global_context, const ContextPtr & curr_context, const DiskExchangeDataManagerOptions & options);
    DiskExchangeDataManager(
        const ContextWeakMutablePtr & context_,
        DiskPtr disk_,
        const DiskExchangeDataManagerOptions & options_,
        ServiceDiscoveryClientPtr service_discovery_client_,
        const String & psm_name_);
    ~DiskExchangeDataManager();

    /// Submit read exchange data task, the task will be run in global thread pool
    void submitReadTask(const String & query_id, const ExchangeDataKeyPtr & key, Processors processors);
    /// Submit write exchange data task, the task will be run in global thread pool
    void submitWriteTask(DiskPartitionWriterPtr writer, ThreadGroupStatusPtr thread_group);
    /// create processors, this executor will read exchange data, and send them through brpc
    Processors createProcessors(BroadcastSenderProxyPtr sender, Block header, ContextPtr query_context) const;
    /// cancel all exchange data tasks in query_id, exchange_id.
    void cancel(UInt64 query_unique_id, UInt64 exchange_id);
    void submitCleanupTask(UInt64 query_unique_id);
    void cleanup(UInt64 query_unique_id); /// TODO @lianxuechao make cleanup async for brpc
    PipelineExecutorPtr getExecutor(const ExchangeDataKeyPtr & key);
    /// write file name, formatted as "root_path/<query_unique_id>/exchange_<exchange_id>_<partition_id>.data.tmp"
    String getTemporaryFileName(const ExchangeDataKey & key) const;
    /// commit file name, formatted as "root_path/<query_unique_id>/exchange_<exchange_id>_<partition_id>.data"
    String getFileName(const ExchangeDataKey & key) const;
    DiskPtr getDisk() const
    {
        return disk;
    }
    /// create a file buffer for DiskPartitionWriter
    std::unique_ptr<WriteBufferFromFileBase> createFileBufferForWrite(const ExchangeDataKeyPtr & key);
    void createWriteTaskDirectory(UInt64 query_unique_id, const String & query_id, const String & coordinator_addr);
    void shutdown();
    void setFileExpireSeconds(size_t file_expire_seconds_)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        this->file_expire_seconds = file_expire_seconds_;
    }

private:
    Protos::AliveQueryInfo readQueryInfo(UInt64 query_unique_id) const;
    /// one round of gc
    void gc();
    /// will start a gc bg thread, which runs gc every gc_interval_seconds
    void runGC();
    struct ReadTask
    {
        ReadTask(const String query_id_, ExchangeDataKeyPtr key_, Processors processors_)
            : query_id(query_id_), key(key_), processors(std::move(processors_))
        {
            executor = std::make_shared<PipelineExecutor>(processors);
        }
        String query_id;
        ExchangeDataKeyPtr key;
        Processors processors;
        PipelineExecutorPtr executor;
    };
    using ReadTaskPtr = std::shared_ptr<ReadTask>;
    /// finish senders of a specific task, so that downstream wont wait until timeout
    static void finishSenders(const ReadTaskPtr & task, BroadcastStatusCode code, String message);

    Poco::Logger * logger;
    bthread::Mutex mutex;
    std::map<ExchangeDataKeyPtr, ReadTaskPtr, ExchangeDataKeyPtrLess> read_tasks;
    std::map<ExchangeDataKeyPtr, DiskPartitionWriterPtr, ExchangeDataKeyPtrLess> write_tasks;
    /// this controls the life time for disk exchange shuffule files, only deletes from it when cleanup() is called
    /// insert <query unique id, query id> into alive_queries before creating the corresponding directory
    std::map<UInt64, Protos::AliveQueryInfo> alive_queries;
    size_t start_gc_random_wait_seconds;
    DiskPtr disk;
    std::filesystem::path path;
    std::atomic_bool is_shutdown{false};
    size_t gc_interval_seconds;
    size_t file_expire_seconds;
    bthread::ConditionVariable shutdown_cv;
    BackgroundSchedulePool::TaskHolder gc_task;
    bthread::ConditionVariable all_task_done_cv;
    /// send rpc to all servers
    ServiceDiscoveryClientPtr service_discovery_client;
    String psm_name;
    /// only used to make sure disk write directory creation is atomic for multiple plansegment with the same query unique id
    /// we used another mutex to avoid any I/O with bthread::Mutex.
    std::mutex disk_mutex;
};

} // namespace DB

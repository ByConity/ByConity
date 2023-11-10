#pragma once

#include <atomic>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/IProcessor.h>
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
};

class DiskPartitionWriter;
using DiskPartitionWriterPtr = std::shared_ptr<DiskPartitionWriter>;
class DiskExchangeDataManager;
using DiskExchangeDataManagerPtr = std::shared_ptr<DiskExchangeDataManager>;
class DiskExchangeDataManager : public WithContext
{
public:
    static DiskExchangeDataManagerPtr createDiskExchangeDataManager(
        const ContextWeakMutablePtr & global_context, const ContextPtr & curr_context, const DiskExchangeDataManagerOptions & options);
    DiskExchangeDataManager(const ContextWeakMutablePtr & context_, DiskPtr disk_, const DiskExchangeDataManagerOptions & options_);

    /// Submit read exchange data task, the task will be run in global thread pool
    void submitReadTask(const String & query_id, const ExchangeDataKeyPtr & key, Processors processors);
    /// Submit write exchange data task, the task will be run in global thread pool
    void submitWriteTask(DiskPartitionWriterPtr writer, ThreadGroupStatusPtr thread_group);
    /// create processors, this executor will read exchange data, and send them through brpc
    Processors createProcessors(BroadcastSenderProxyPtr sender, Block header, ContextPtr query_context) const;
    /// cancel all exchange data tasks in query_id, exchange_id.
    void cancel(uint64_t query_unique_id, uint64_t exchange_id);
    void cleanup(uint64_t query_unique_id);
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
    /// create the directory for write task, the path is formatted as "root_path/<query_unique_id>"
    void createWriteTaskDirectory(uint64_t query_unique_id);

private:
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
    // schedule task is used here to avoid brpc being blocked
    BackgroundSchedulePool::TaskHolder schedule_task;
    bthread::Mutex mutex;
    std::map<ExchangeDataKeyPtr, ReadTaskPtr, ExchangeDataKeyPtrLess> tasks;
    DiskPtr disk;
    std::filesystem::path path;
};

} // namespace DB

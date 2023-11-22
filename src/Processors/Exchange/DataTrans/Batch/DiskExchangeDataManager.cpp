#include <algorithm>
#include <atomic>
#include <compare>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string_view>
#include <utility>
#include <stdio.h>
#include <Core/Field.h>
#include <Disks/DiskType.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/Batch/Reader/DiskExchangeDataSource.h>
#include <Processors/Exchange/DataTrans/Batch/Writer/DiskPartitionWriter.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/Pipe.h>
#include <QueryPlan/QueryPlan.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include "common/types.h"
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <common/defines.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
    extern const int CANNOT_UNLINK;
}

DiskExchangeDataManagerPtr DiskExchangeDataManager::createDiskExchangeDataManager(
    const ContextWeakMutablePtr & global_context, const ContextPtr & curr_context, const DiskExchangeDataManagerOptions & options)
{
    if (options.path.empty())
        throw Exception("relative path configuration for bsp mode should not be empty", ErrorCodes::LOGICAL_ERROR);
    VolumePtr volume = curr_context->getStoragePolicy(options.storage_policy)->getVolumeByName(options.volume, true);
    chassert(volume);
    /// for now, we only support single disk deployment for bsp mode.
    DiskPtr disk = volume->getDefaultDisk();
    chassert(disk);
    if (volume->getDisks().size() != 1)
        LOG_INFO(
            &Poco::Logger::get("DiskExchangeDataManager"),
            "bsp mode now only supports single disk, will use default disk:{} of volume:{}",
            disk->getName(),
            volume->getName());
    disk->createDirectories(options.path);
    /// for now we only support local disk, for new type of disks to be used, below requests must be satisfied
    /// 1. New disk type must allow *ATOMIC* create file operation.
    /// 2. New disk type must allow write, read and delete operations.
    if (disk->getType() != DiskType::Type::Local)
        throw Exception("disk " + disk->getName() + " for bsp mode should be local", ErrorCodes::LOGICAL_ERROR);
    return std::make_shared<DiskExchangeDataManager>(global_context, std::move(disk), options);
}

DiskExchangeDataManager::DiskExchangeDataManager(
    const ContextWeakMutablePtr & context_, DiskPtr disk_, const DiskExchangeDataManagerOptions & options_)
    : WithContext(context_), logger(&Poco::Logger::get("DiskExchangeDataManager")), disk(std::move(disk_)), path(options_.path)
{
}

void DiskExchangeDataManager::submitReadTask(const String & query_id, const ExchangeDataKeyPtr & key, Processors processors)
{
    ReadTaskPtr task;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto & value = tasks[key];
        if (value)
            throw Exception(
                fmt::format("Submit exchange data task failed duplicate key:{}", *key), ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
        value = std::make_shared<ReadTask>(query_id, key, std::move(processors));
        task = value;
    }

    try
    {
        ThreadFromGlobalPool thread([&, task_cp = task]() mutable {
            BroadcastStatusCode code = BroadcastStatusCode::ALL_SENDERS_DONE;
            auto msg = fmt::format("finish senders for query:{} key:{}", task_cp->query_id, *task_cp->key);
            try
            {
                LOG_TRACE(logger, "query:{} key:{} read task starts execution", task_cp->query_id, *task_cp->key);
                task_cp->executor->execute(2); // TODO thread number @lianxuechao
                LOG_TRACE(logger, "query:{} key:{} read task execution done", task_cp->query_id, *task_cp->key);
            }
            catch (...)
            {
                code = BroadcastStatusCode::SEND_UNKNOWN_ERROR;
                msg = fmt::format("query:{} key:{} read task execution exception", task_cp->query_id, *task_cp->key);
                tryLogCurrentException(logger, msg);
            }

            finishSenders(task_cp, code, msg);
            std::unique_lock<bthread::Mutex> lock(mutex);
            tasks.erase(task_cp->key);
        });
        thread.detach();
    }
    catch (...)
    {
        {
            std::unique_lock<bthread::Mutex> lock(mutex);
            tasks.erase(task->key);
        }
        auto error_msg = fmt::format("query:{} key:{} read task schedule exception", task->query_id, *task->key);
        tryLogCurrentException(logger, error_msg);
        finishSenders(task, BroadcastStatusCode::SEND_UNKNOWN_ERROR, error_msg);
    }
}

void DiskExchangeDataManager::submitWriteTask(DiskPartitionWriterPtr writer, ThreadGroupStatusPtr thread_group)
{
    ThreadFromGlobalPool thread([&, writer_cp = writer, thread_group_cp = thread_group]() mutable {
        if (thread_group_cp)
            CurrentThread::attachTo(thread_group_cp);
        try
        {
            LOG_TRACE(logger, "key:{} write task starts execution", *writer_cp->getKey());
            writer_cp->runWriteTask();
            writer_cp->setWriteTaskDone();
            LOG_TRACE(logger, "key:{} write task execution done", *writer_cp->getKey());
        }
        catch (...)
        {
            writer_cp->cancel();
            tryLogCurrentException(logger, fmt::format("key:{} write task execution exception", *writer_cp->getKey()));
        }
    });
    thread.detach();
}

void DiskExchangeDataManager::cancel(uint64_t query_unique_id, uint64_t exchange_id)
{
    auto from = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, 0);
    auto to = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, std::numeric_limits<uint64_t>::max());
    chassert(*from < *to);
    // 1. remove tasks
    std::vector<ReadTaskPtr> cancel_tasks;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto begin = tasks.lower_bound(from); // first key >= from
        auto end = tasks.upper_bound(to); // firsy key > to
        if (begin != end)
        {
            std::transform(begin, end, std::back_inserter(cancel_tasks), [](auto & iter) { return iter.second; });
            tasks.erase(begin, end);
        }
    }
    // 2. cancel all executors
    for (auto & task : cancel_tasks)
    {
        auto & executor = task->executor;
        executor->cancel();
        LOG_TRACE(logger, fmt::format("query:{} key:{} cancel task", task->query_id, *task->key));
    }
}

void DiskExchangeDataManager::cleanup(uint64_t query_unique_id)
{
    auto file_path = path / std::to_string(query_unique_id);
    disk->removeRecursive(file_path);
}

PipelineExecutorPtr DiskExchangeDataManager::getExecutor(const ExchangeDataKeyPtr & key)
{
    std::unique_lock<bthread::Mutex> lock(mutex);
    if (tasks.find(key) != tasks.end())
    {
        return tasks[key]->executor;
    }
    return nullptr;
}

void DiskExchangeDataManager::finishSenders(const ReadTaskPtr & task, BroadcastStatusCode code, String message)
{
    for (auto & processor : task->processors)
    {
        if (auto * sink = dynamic_cast<BroadcastExchangeSink *>(processor.get()))
        {
            for (auto & sender : sink->getSenders())
            {
                sender->finish(code, message);
            }
        }
    }
}

String DiskExchangeDataManager::getTemporaryFileName(const ExchangeDataKey & key) const
{
    auto file_path = path / std::to_string(key.query_unique_id);
    file_path = file_path / fmt::format("exchange_{}_{}.data.tmp", key.exchange_id, key.parallel_index);
    return file_path;
}

String DiskExchangeDataManager::getFileName(const ExchangeDataKey & key) const
{
    auto file_path = path / std::to_string(key.query_unique_id);
    file_path = file_path / fmt::format("exchange_{}_{}.data", key.exchange_id, key.parallel_index);
    return file_path;
}

std::unique_ptr<WriteBufferFromFileBase> DiskExchangeDataManager::createFileBufferForWrite(const ExchangeDataKeyPtr & key)
{
    auto file_path = getTemporaryFileName(*key);
    auto file_buf = disk->writeFile(file_path);
    return file_buf;
}

void DiskExchangeDataManager::createWriteTaskDirectory(uint64_t query_unique_id)
{
    disk->createDirectory(path / std::to_string(query_unique_id));
}

Processors DiskExchangeDataManager::createProcessors(BroadcastSenderProxyPtr sender, Block header, ContextPtr query_context) const
{
    auto key = sender->getDataKey();
    auto file_name = getFileName(*key);
    auto buf = disk->readFile(file_name);
    auto source = std::make_shared<DiskExchangeDataSource>(header, std::move(buf));
    String name = BroadcastExchangeSink::generateName(key->exchange_id);
    ExchangeOptions exchange_options{
        .exchange_timeout_ts = query_context->getQueryExpirationTimeStamp(),
        .force_use_buffer = query_context->getSettingsRef().exchange_force_use_buffer};
    auto sink = std::make_shared<BroadcastExchangeSink>(
        std::move(header), std::vector<BroadcastSenderPtr>{std::move(sender)}, exchange_options, std::move(name));
    connect(source->getOutputs().front(), sink->getInputs().front());
    return {std::move(source), std::move(sink)};
}

}

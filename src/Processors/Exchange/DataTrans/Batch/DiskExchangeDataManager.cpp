#include <algorithm>
#include <atomic>
#include <chrono>
#include <compare>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <Core/Field.h>
#include <Disks/DiskType.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DistributedStages/ExchangeDataTracker.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/Batch/Reader/DiskExchangeDataSource.h>
#include <Processors/Exchange/DataTrans/Batch/Writer/DiskPartitionWriter.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxy.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcExchangeReceiverRegistryService.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/Pipe.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Protos/registry.pb.h>
#include <QueryPlan/QueryPlan.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <asm-generic/errno-base.h>
#include <boost/filesystem/directory.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <brpc/controller.h>
#include <bthread/mutex.h>
#include <fmt/format.h>
#include <incubator-brpc/src/brpc/controller.h>
#include <sys/types.h>
#include <Poco/Exception.h>
#include <Poco/Logger.h>
#include <Common/Brpc/BrpcAsyncResultHolder.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/RpcClientPool.h>
#include <Common/ThreadStatus.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <common/sleep.h>
#include <common/types.h>


namespace
{
const size_t HEARTBEAT_BRPC_TIMEOUT_MS = 10000;
}

namespace CurrentMetrics
{
extern const Metric BackgroundBspGCSchedulePoolTask;
extern const Metric BackgroundBspCleanupSchedulePoolTask;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_UNLINK;
    extern const int FILE_DOESNT_EXIST;
    extern const int BSP_EXCHANGE_DATA_DISK_LIMIT_EXCEEDED;
}

namespace
{
    const size_t disk_partition_wait_done_timeout_ms = 20000;
}

String DiskExchangeDataManagerOptions::toString() const
{
    return fmt::format(
        "DiskExchangeDataManagerOptions[path:{}, storage_policy:{}, volume:{}, gc_interval_seconds:{}, file_expire_seconds:{}]",
        this->path,
        this->storage_policy,
        this->volume,
        this->gc_interval_seconds,
        this->file_expire_seconds);
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
            getLogger("DiskExchangeDataManager"),
            "bsp mode now only supports single disk, will use default disk:{} of volume:{}",
            disk->getName(),
            volume->getName());
    disk->createDirectories(options.path);
    /// for now we only support local disk, for new type of disks to be used, below requests must be satisfied
    /// 1. New disk type must allow *ATOMIC* create file operation.
    /// 2. New disk type must allow write, read and delete operations.
    if (disk->getType() != DiskType::Type::Local)
        throw Exception("disk " + disk->getName() + " for bsp mode should be local", ErrorCodes::LOGICAL_ERROR);
    String psm_name = curr_context->getCnchServerClientPool().getServiceName();
    auto sd_client = curr_context->getServiceDiscoveryClient();
    return std::make_shared<DiskExchangeDataManager>(global_context, std::move(disk), options, std::move(sd_client), psm_name);
}

DiskExchangeDataManager::DiskExchangeDataManager(
    const ContextWeakMutablePtr & context_,
    DiskPtr disk_,
    const DiskExchangeDataManagerOptions & options_,
    ServiceDiscoveryClientPtr service_discovery_client_,
    const String & psm_name_)
    : WithContext(context_)
    , logger(getLogger("DiskExchangeDataManager"))
    , start_gc_random_wait_seconds(options_.start_gc_random_wait_seconds)
    , disk(std::move(disk_))
    , path(options_.path)
    , gc_interval_seconds(options_.gc_interval_seconds)
    , file_expire_seconds(options_.file_expire_seconds)
    , service_discovery_client(std::move(service_discovery_client_))
    , psm_name(psm_name_)
    , cleanup_thread_pool(options_.cleanup_thread_pool_size, options_.cleanup_thread_pool_size / 10, options_.cleanup_thread_pool_size * 2)
    , max_disk_bytes(options_.max_disk_bytes)
{
    ssize_t current_size = getFileSizeRecursively(path);
    if (current_size > max_disk_bytes)
        throw Exception(
            ErrorCodes::BSP_EXCHANGE_DATA_DISK_LIMIT_EXCEEDED,
            "current file size(which is {}) already exceeded max_disk_bytes(which is {})",
            current_size,
            max_disk_bytes);
    global_disk_written_bytes.store(current_size, std::memory_order_relaxed);
    gc_task
        = context_.lock()
              ->getExtraSchedulePool(
                  SchedulePool::Type::BspGC, SettingFieldUInt64(1), CurrentMetrics::BackgroundBspGCSchedulePoolTask, "BspGCSchedulePool")
              .createTask("bsp_gc", [&]() { this->runGC(); });
    gc_task->activateAndSchedule();
    LOG_INFO(logger, "created with options {}", options_.toString());
}

DiskExchangeDataManager::~DiskExchangeDataManager()
{
    if (!is_shutdown.load(std::memory_order_acquire))
        shutdown();
}

void DiskExchangeDataManager::submitReadTask(
    const String & query_id, const ExchangeDataKeyPtr & key, Processors processors, const String & addr)
{
    ReadTaskPtr task;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto & value = read_tasks[key];
        if (value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("Submit read exchange data task failed duplicate key:{}", *key));
        value = std::make_shared<ReadTask>(query_id, key, std::move(processors));
        task = value;
    }
    try
    {
        ThreadFromGlobalPool thread([&, task_cp = task, addr_cp = addr]() mutable {
            BroadcastStatusCode code = BroadcastStatusCode::ALL_SENDERS_DONE;
            auto msg = fmt::format("finish senders for query:{} key:{}", task_cp->query_id, *task_cp->key);
            try
            {
                LOG_TRACE(logger, "query:{} key:{} read task starts execution", task_cp->query_id, *task_cp->key);
                task_cp->executor->execute(2);
                LOG_TRACE(logger, "query:{} key:{} read task execution done", task_cp->query_id, *task_cp->key);
                if (task_cp->executor->isCancelled())
                    code = BroadcastStatusCode::SEND_CANCELLED;
                finishSenders(task_cp, code, msg);
            }
            catch (const Exception & e)
            {
                msg = fmt::format(
                    "query:{} key:{} read task execution exception {}",
                    task_cp->query_id,
                    *task_cp->key,
                    getCurrentExceptionMessage(false));
                tryLogCurrentException(logger, msg);
                if (!addr_cp.empty())
                    reportError(task_cp->query_id, addr_cp, e.code(), msg);
            }

            {
                std::unique_lock<bthread::Mutex> lock(mutex);
                read_tasks.erase(task_cp->key);
            }
            // release sender proxy before setDone
            task_cp->setDone();
            all_task_done_cv.notify_all();
        });
        thread.detach();
    }
    catch (const Exception & e)
    {
        auto error_msg = fmt::format("query:{} key:{} read task schedule exception", task->query_id, *task->key);
        tryLogCurrentException(logger, error_msg);
        if (!addr.empty())
            reportError(task->query_id, addr, e.code(), error_msg);
        {
            std::unique_lock<bthread::Mutex> lock(mutex);
            read_tasks.erase(task->key);
            task->setDone();
            all_task_done_cv.notify_all();
        }
    }
}

void DiskExchangeDataManager::submitWriteTask(
    UInt64 query_unique_id, PlanSegmentInstanceId instance_id, DiskPartitionWriterPtr writer, ThreadGroupStatusPtr thread_group)
{
    std::multimap<UInt64, ExchangeDataKeyPtr>::iterator iter;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        write_tasks.insert({writer->getKey(), writer});
        alive_queries[query_unique_id].segment_write_task_keys.insert({instance_id, writer->getKey()});
    }
    try
    {
        ThreadFromGlobalPool thread([&, writer_cp = writer, thread_group_cp = thread_group]() mutable {
            if (thread_group_cp)
                CurrentThread::attachTo(thread_group_cp);
            try
            {
                LOG_TRACE(logger, "key:{} write task starts execution", *writer_cp->getKey());
                writer_cp->runWriteTask();
                LOG_TRACE(logger, "key:{} write task execution done", *writer_cp->getKey());
            }
            catch (...)
            {
                auto msg = fmt::format("key:{} write task execution exception {}", *writer_cp->getKey(), getCurrentExceptionMessage(true));
                writer_cp->finish(BroadcastStatusCode::SEND_UNKNOWN_ERROR, msg);
                tryLogCurrentException(logger, msg);
            }
            {
                std::unique_lock<bthread::Mutex> lock(mutex);
                write_tasks.erase(writer_cp->getKey());
            }
            all_task_done_cv.notify_all();
        });
        thread.detach();
    }
    catch (...)
    {
        {
            std::unique_lock<bthread::Mutex> lock(mutex);
            write_tasks.erase(writer->getKey());
            all_task_done_cv.notify_all();
        }
        auto error_msg = fmt::format("key:{} write task schedule exception", *writer->getKey());
        tryLogCurrentException(logger, error_msg);
    }
}

void DiskExchangeDataManager::cancelReadTask(uint64_t query_unique_id, uint64_t exchange_id)
{
    auto from = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, 0);
    auto to = std::make_shared<ExchangeDataKey>(query_unique_id, exchange_id, std::numeric_limits<uint64_t>::max());
    // 1. remove tasks
    std::vector<ReadTaskPtr> cancel_tasks;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto begin = read_tasks.lower_bound(from); // first key >= from
        auto end = read_tasks.upper_bound(to); // firsy key > to
        if (begin != end)
            std::transform(begin, end, std::back_inserter(cancel_tasks), [](auto & iter) { return iter.second; });
    }
    // 2. cancel all executors
    for (auto & task : cancel_tasks)
    {
        auto & executor = task->executor;
        if (executor)
            executor->cancel();
        task->waitDone();
        LOG_TRACE(logger, fmt::format("query:{} key:{} cancel task", task->query_id, *task->key));
    }
    SCOPE_EXIT({
        for (auto & task : cancel_tasks)
        {
            std::unique_lock<bthread::Mutex> lock(mutex);
            read_tasks.erase(task->key);
        }
    });
}

void DiskExchangeDataManager::cancelReadTask(const ExchangeDataKeyPtr & key)
{
    // 1. remove tasks
    ReadTaskPtr cancel_task;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto iter = read_tasks.find(key);
        if (iter != read_tasks.end())
            cancel_task = iter->second;
    }
    // 2. cancel all executors
    if (cancel_task)
    {
        auto executor = cancel_task->executor;
        if (executor)
            executor->cancel();
        cancel_task->waitDone();
        LOG_TRACE(logger, fmt::format("query:{} key:{} cancel task", cancel_task->query_id, *cancel_task->key));
    }
    SCOPE_EXIT({
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (cancel_task)
            read_tasks.erase(cancel_task->key);
    });
}

bool DiskExchangeDataManager::cleanupPreviousSegmentInstance(UInt64 query_unique_id, PlanSegmentInstanceId instance_id)
{
    LOG_TRACE(
        logger,
        "cleanupPreviousSegmentInstance for query_unique_id:{} segment_id:{} parallel_id:{}",
        query_unique_id,
        instance_id.segment_id,
        instance_id.parallel_index);
    std::vector<DiskPartitionWriterPtr> cancel_tasks;
    std::vector<ssize_t> write_bytes;
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (alive_queries.find(query_unique_id) != alive_queries.end())
        {
            auto & alive_query = alive_queries[query_unique_id];
            auto iter = alive_query.segment_write_task_keys.find(instance_id);
            auto end = alive_query.segment_write_task_keys.end();
            if (iter != end)
            {
                auto w_iter = write_tasks.find(iter->second);
                if (w_iter != write_tasks.end())
                {
                    cancel_tasks.emplace_back(w_iter->second);
                    write_bytes.emplace_back(alive_query.segment_instance_write_bytes[w_iter->second->getKey()]);
                    alive_query.segment_instance_write_bytes[w_iter->second->getKey()] = 0;
                }
            }
        }
    }
    if (!cancelWriteTasks(cancel_tasks))
        return false;
    for (size_t i = 0; i < cancel_tasks.size(); i++)
    {
        auto write_byte = write_bytes[i];
        auto writer = cancel_tasks[i];
        if (!is_shutdown.load(std::memory_order_acquire))
        {
            disk->removeFileIfExists(getFileName(*writer->getKey()));
            disk->removeFileIfExists(getTemporaryFileName(*writer->getKey()));
            global_disk_written_bytes.fetch_sub(write_byte, std::memory_order_relaxed);
            LOG_INFO(logger, "try removing file for key:{}", *writer->getKey());
        }
    }
    return true;
}

bool DiskExchangeDataManager::cancelWriteTasks(const std::vector<DiskPartitionWriterPtr> & writers)
{
    bool wait_succ = true;
    for (const auto & writer : writers)
    {
        auto status = writer->finish(BroadcastStatusCode::SEND_CANCELLED, "cancelled by cancelWriteTasks");
        if (status.code != BroadcastStatusCode::SEND_CANCELLED)
            LOG_WARNING(logger, "key:{} finish status.code:{} status.message:{}", *writer->getKey(), status.code, status.message);
    }
    try
    {
        UInt64 timeout_ms = time_in_milliseconds(std::chrono::system_clock::now()) + disk_partition_wait_done_timeout_ms;
        for (const auto & writer : writers)
        {
            auto curr = time_in_milliseconds(std::chrono::system_clock::now());
            writer->waitDone(curr < timeout_ms ? timeout_ms - curr : 0); /// might throw
        }
    }
    catch (...)
    {
        wait_succ = false;
        tryLogCurrentException(logger);
    }
    return wait_succ;
}

void DiskExchangeDataManager::submitCleanupTask(UInt64 query_unique_id)
{
    try
    {
        cleanup_thread_pool.scheduleOrThrow([&, query_unique_id_cp = query_unique_id]() mutable {
            try
            {
                cleanup(query_unique_id_cp);
            }
            catch (...)
            {
                tryLogCurrentException(logger, __PRETTY_FUNCTION__);
            }
        });
    }
    catch (...)
    {
        auto error_msg = fmt::format("key:{} cleanup task schedule exception", query_unique_id);
        tryLogCurrentException(logger, error_msg);
    }
}

void DiskExchangeDataManager::cleanup(uint64_t query_unique_id)
{
    SCOPE_EXIT({
        std::unique_lock<bthread::Mutex> lock(mutex);
        cleanup_tasks.erase(query_unique_id);
        alive_queries.erase(query_unique_id);
        all_task_done_cv.notify_all();
    });
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        cleanup_tasks.insert(query_unique_id);
    }
    auto file_path = path / std::to_string(query_unique_id);
    /// cancel before removal
    std::vector<DiskPartitionWriterPtr> write_on_the_run;
    std::vector<ReadTaskPtr> read_on_the_run;
    auto from = std::make_shared<ExchangeDataKey>(query_unique_id, 0, 0);
    auto to
        = std::make_shared<ExchangeDataKey>(query_unique_id, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto wbegin = write_tasks.lower_bound(from); // first key >= from
        auto wend = write_tasks.upper_bound(to); // first key > to
        auto rbegin = read_tasks.lower_bound(from); // first key >= from
        auto rend = read_tasks.upper_bound(to); // first key > to
        if (wbegin != wend)
            std::transform(wbegin, wend, std::back_inserter(write_on_the_run), [](auto & iter) { return iter.second; });
        if (rbegin != rend)
            std::transform(rbegin, rend, std::back_inserter(read_on_the_run), [](auto & iter) { return iter.second; });
    }
    bool wait_succ = cancelWriteTasks(write_on_the_run);
    {
        /// wbegin and wend is recalculated, as other tasks might have modified the iterator, same for rbegin and rend below
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto wbegin = write_tasks.lower_bound(from);
        auto wend = write_tasks.upper_bound(to);
        if (wbegin != wend)
            write_tasks.erase(wbegin, wend);
    }
    for (auto & task : read_on_the_run)
    {
        task->executor->cancel();
    }
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto rbegin = read_tasks.lower_bound(from);
        auto rend = read_tasks.upper_bound(to);
        if (rbegin != rend)
            read_tasks.erase(rbegin, rend);
    }
    bool removed = false;
    if (wait_succ && !is_shutdown.load(std::memory_order_acquire) && disk->exists(file_path))
    {
        removed = true;
        removeWriteTaskDirectory(query_unique_id);
    }
    LOG_INFO(logger, "cleanup for query_unique_id:{} removed:{} file_path:{}", query_unique_id, removed, file_path.string());
}

PipelineExecutorPtr DiskExchangeDataManager::getExecutor(const ExchangeDataKeyPtr & key)
{
    std::unique_lock<bthread::Mutex> lock(mutex);
    if (read_tasks.find(key) != read_tasks.end())
    {
        return read_tasks[key]->executor;
    }
    return nullptr;
}

Protos::AliveQueryInfo DiskExchangeDataManager::readQueryInfo(UInt64 query_unique_id) const
{
    auto buf = disk->readFile(path / std::to_string(query_unique_id) / "query_info", {.local_fs_method = LocalFSReadMethod::read});
    std::string str;
    readStringBinary(str, *buf);
    Protos::AliveQueryInfo query_info;
    query_info.ParseFromString(str);
    return query_info;
}

void DiskExchangeDataManager::gc()
{
    std::vector<std::variant<String, UInt64>> delete_files;
    std::vector<Protos::AliveQueryInfo> not_alive_queries;
    std::vector<String> file_names;
    disk->listFiles(path, file_names);
    std::unordered_map<UInt64, AliveQueryInfo> req_contents;
    {
        std::lock_guard<bthread::Mutex> lock(mutex);
        /// all alive_queries in mem will be checked for aliveness
        for (const auto & alive_query : alive_queries)
            req_contents.insert({alive_query.first, alive_query.second});
    }
    auto now = std::chrono::system_clock::now();
    auto expire = std::chrono::seconds(file_expire_seconds);
    for (const auto & file_name : file_names)
    {
        UInt64 query_unique_id;
        try
        {
            query_unique_id = boost::lexical_cast<UInt64>(file_name);
            auto query_info = readQueryInfo(query_unique_id);
            // if file is too old, delete it, and no need to request server
            auto last_modified = std::chrono::system_clock::from_time_t(disk->getLastModified(path / file_name).epochTime());
            if (last_modified + expire < now)
            {
                not_alive_queries.push_back(std::move(query_info));
                LOG_INFO(
                    logger,
                    fmt::format(
                        "file:{} too old, expire:{} now:{}",
                        file_name,
                        duration_cast<std::chrono::seconds>(last_modified.time_since_epoch()).count(),
                        duration_cast<std::chrono::seconds>(now.time_since_epoch()).count()));
            }
            // if not found in alive_queries, request server for aliveness
            else if (req_contents.find(query_unique_id) == req_contents.end())
            {
                ssize_t file_size = getFileSizeRecursively(file_name);
                req_contents.insert({query_unique_id, AliveQueryInfo{std::move(query_info), file_size, {}}});
            }
        }
        catch (boost::bad_lexical_cast & /*exception*/)
        {
            /// invalid
            LOG_WARNING(logger, fmt::format("invalid file name:{}", file_name));
            delete_files.push_back(path / file_name);
        }
        /// cant find query_info file, either it is not created yet, or query_info is corrupt
        catch (ErrnoException & e)
        {
            if (e.getErrno() == ENOENT)
            {
                std::unique_lock<bthread::Mutex> lock(mutex);
                if (alive_queries.find(query_unique_id) == alive_queries.end())
                {
                    LOG_INFO(
                        logger,
                        fmt::format(
                            "query info not found, directory:{} and query_unique_id:{} is not in alive_queries",
                            file_name,
                            query_unique_id));
                    delete_files.push_back(path / file_name);
                }
            }
            else
            {
                tryLogCurrentException(logger, fmt::format("query info is corrupt, directory:{} and query_unique_id:{}", file_name, query_unique_id));
                delete_files.push_back(path / file_name);
            }
        }
        catch (...)
        {
            tryLogCurrentException(logger, fmt::format("query info other exception, directory:{} and query_unique_id:{}", file_name, query_unique_id));
            delete_files.push_back(path / file_name);
        }
    }

    auto endpoints = service_discovery_client->lookup(psm_name, ComponentType::SERVER);
    auto req = std::make_shared<Protos::ExchangeDataHeartbeatRequest>();
    for (const auto & content : req_contents)
    {
        auto * query_info = req->add_infos();
        query_info->set_query_unique_id(content.second.proto.query_unique_id());
        query_info->set_query_id(content.second.proto.query_id());
    }

    std::vector<BrpcAsyncResultHolder<Protos::ExchangeDataHeartbeatRequest, Protos::ExchangeDataHeartbeatResponse>> result_holders;
    for (const auto & ep : endpoints)
    {
        BrpcAsyncResultHolder<Protos::ExchangeDataHeartbeatRequest, Protos::ExchangeDataHeartbeatResponse> holder;
        holder.channel = RpcChannelPool::getInstance().getClient(ep.getRPCAddress(), BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        holder.cntl = std::make_unique<brpc::Controller>();
        holder.request = req;
        holder.response = std::make_unique<Protos::ExchangeDataHeartbeatResponse>();
        holder.cntl->set_timeout_ms(HEARTBEAT_BRPC_TIMEOUT_MS);
        Protos::RegistryService_Stub stub = Protos::RegistryService_Stub(&holder.channel->getChannel());
        stub.sendExchangeDataHeartbeat(holder.cntl.get(), holder.request.get(), holder.response.get(), brpc::DoNothing());
        result_holders.push_back(std::move(holder));
    }

    std::unordered_map<uint64_t, size_t> cnts;
    for (const auto & holder : result_holders)
    {
        try
        {
            brpc::Join(holder.cntl->call_id());
            if (holder.cntl->Failed())
                throw Exception(
                    ErrorCodes::BRPC_EXCEPTION,
                    fmt::format("wait for heart beat response failed, error text:{}", holder.cntl->ErrorText()));
            for (const auto & not_alive_query : holder.response->not_alive_queries())
            {
                auto & cnt = cnts[not_alive_query.query_unique_id()];
                cnt += 1;
                if (cnt == result_holders.size())
                {
                    Protos::AliveQueryInfo elm;
                    elm.set_query_unique_id(not_alive_query.query_unique_id());
                    elm.set_query_id(not_alive_query.query_id());
                    not_alive_queries.push_back(std::move(elm));
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        for (const auto & not_alive_query : not_alive_queries)
        {
            delete_files.push_back(not_alive_query.query_unique_id());
            LOG_INFO(
                logger,
                fmt::format(
                    "GC will remove query_id:{} query_unique_id:{}", not_alive_query.query_id(), not_alive_query.query_unique_id()));
            req_contents.erase(not_alive_query.query_unique_id());
        }
        /// add alive queries back
        for (auto & content : req_contents)
        {
            auto & p = alive_queries[content.first];
            p = std::move(content.second);
        }
    }
    /// delete all files need to delete
    for (const auto & delete_file : delete_files)
        removeWriteTaskDirectory(delete_file);
    /// finally remove from alive_queries
    std::unique_lock<bthread::Mutex> lock(mutex);
    for (const auto & not_alive_query : not_alive_queries)
    {
        alive_queries.erase(not_alive_query.query_unique_id());
    }
}

void DiskExchangeDataManager::runGC()
{
    static bool initialized = false;
    auto start = std::chrono::steady_clock::now();
    decltype(start) end;
    size_t takes = 0;
    if (!initialized)
    {
        std::uniform_int_distribution<> dist(0, start_gc_random_wait_seconds);
        double seconds = dist(thread_local_rng);
        /// randomly wait for 300 seconds, to avoid rpc storm when cluster restarts
        std::this_thread::sleep_for(std::chrono::seconds(static_cast<size_t>(seconds)));
        initialized = true;
    }
    try
    {
        LOG_INFO(logger, "start GC task...");
        this->gc();
        end = std::chrono::steady_clock::now();
        takes = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
        LOG_INFO(logger, "GC task successfully ended takes:{} seconds", takes);
    }
    catch (...)
    {
        end = std::chrono::steady_clock::now();
        takes = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
        tryLogCurrentException(logger, __PRETTY_FUNCTION__);
    }
    if (gc_interval_seconds > takes)
        gc_task->scheduleAfter(1000 * (gc_interval_seconds - takes));
    else
        gc_task->schedule();
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

void DiskExchangeDataManager::reportError(const String & query_id, const String & coordinator_addr, Int32 code, const String & message)
{
    try
    {
        std::shared_ptr<RpcClient> rpc_client
            = RpcChannelPool::getInstance().getClient(coordinator_addr, BrpcChannelPoolOptions::DEFAULT_CONFIG_KEY);
        brpc::Controller cntl;
        Protos::ReportPlanSegmentErrorRequest request;
        Protos::ReportPlanSegmentErrorResponse response;
        Protos::PlanSegmentManagerService_Stub stub(&rpc_client->getChannel());
        request.set_code(code);
        request.set_message(message);
        request.set_query_id(query_id);
        stub.reportPlanSegmentError(&cntl, &request, &response, nullptr);
        rpc_client->assertController(cntl);
    }
    catch (...)
    {
        tryLogCurrentException(logger, __PRETTY_FUNCTION__);
    }
}

String DiskExchangeDataManager::getTemporaryFileName(const ExchangeDataKey & key) const
{
    return getFileName(key).append(".tmp");
}

String DiskExchangeDataManager::getFileName(const ExchangeDataKey & key) const
{
    auto file_path = path / std::to_string(key.query_unique_id);
    file_path = file_path / fmt::format("exchange_{}_{}_{}.data", key.exchange_id, key.partition_id, key.parallel_index);
    return file_path;
}

// TODO(wangtao.vip): try to make return value a single file.
std::vector<std::unique_ptr<ReadBufferFromFileBase>> DiskExchangeDataManager::readFiles(const ExchangeDataKey & key) const
{
    std::vector<std::unique_ptr<ReadBufferFromFileBase>> ret;
    auto abs_file_path = getFileName(key);
    ret.push_back(disk->readFile(abs_file_path, {.local_fs_method = LocalFSReadMethod::read}));
    return ret;
}

std::unique_ptr<WriteBufferFromFileBase> DiskExchangeDataManager::createFileBufferForWrite(const ExchangeDataKeyPtr & key)
{
    auto file_path = getTemporaryFileName(*key);
    auto file_buf = disk->writeFile(file_path);
    return file_buf;
}

void DiskExchangeDataManager::createWriteTaskDirectory(UInt64 query_unique_id, const String & query_id, const String & coordinator_addr)
{
    checkEnoughSpace();
    Protos::AliveQueryInfo proto;
    proto.set_query_unique_id(query_unique_id);
    proto.set_query_id(query_id);
    proto.set_coordinator_address(coordinator_addr);
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto & p = alive_queries[query_unique_id];
        if (p.proto.query_id().empty())
            p.proto = proto;
    }
    std::unique_lock<std::mutex> lock(disk_mutex);
    if (!disk->exists(path / std::to_string(query_unique_id)))
    {
        /// the creation of dir and query_info file is not atomic for gc thread, take caution!
        disk->createDirectory(path / std::to_string(query_unique_id));
        auto buf = disk->writeFile(path / std::to_string(query_unique_id) / "query_info.tmp");
        auto str = proto.SerializeAsString();
        writeStringBinary(str, *buf);
        buf->next();
        buf->sync();
        buf->finalize();
        disk->moveFile(path / std::to_string(query_unique_id) / "query_info.tmp", path / std::to_string(query_unique_id) / "query_info");
        LOG_INFO(logger, fmt::format("Create write directory for query_id:{} query_unique_id: {}", query_id, query_unique_id));
    }
}

Processors DiskExchangeDataManager::createProcessors(BroadcastSenderProxyPtr sender, Block header, ContextPtr query_context)
{
    auto key = sender->getDataKey();
    auto source = std::make_shared<DiskExchangeDataSource>(header, key, query_context);
    String name = BroadcastExchangeSink::generateName(key->exchange_id);
    ExchangeOptions exchange_options = ExchangeUtils::getExchangeOptions(query_context);
    auto sink = std::make_shared<BroadcastExchangeSink>(
        std::move(header), std::vector<BroadcastSenderPtr>{std::move(sender)}, exchange_options, std::move(name));
    connect(source->getOutputs().front(), sink->getInputs().front());
    return {std::move(source), std::move(sink)};
}

void DiskExchangeDataManager::shutdown()
{
    bool expected = false;
    if (this->is_shutdown.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed))
    {
        gc_task->deactivate();
        try
        {
            LOG_TRACE(logger, "Waiting cleanup_thread_pool pool finishing");
            cleanup_thread_pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        std::unique_lock<bthread::Mutex> lock(mutex);
        for (const auto & p : read_tasks)
        {
            p.second->executor->cancel();
        }
        for (const auto & p : write_tasks)
        {
            p.second->finish(BroadcastStatusCode::SEND_CANCELLED, "cancelled when shutdown");
        }
        if (!all_task_done_cv.wait_for(
                lock, std::chrono::seconds(60), [&]() { return read_tasks.empty() && write_tasks.empty() && cleanup_tasks.empty(); }))
            LOG_ERROR(logger, "tasks still running");
    }
}

void DiskExchangeDataManager::updateWrittenBytes(UInt64 query_unique_id, ExchangeDataKeyPtr key, ssize_t disk_written_bytes)
{
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto it = alive_queries.find(query_unique_id);
        if (it != alive_queries.end())
        {
            it->second.disk_written_bytes += disk_written_bytes;
            it->second.segment_instance_write_bytes[key] += disk_written_bytes;
        }
        else
        {
            LOG_WARNING(
                logger,
                "cant find query when updateWrittenBytes query_unique_id:{} disk_written_bytes:{}",
                query_unique_id,
                disk_written_bytes);
            return;
        }
    }
    global_disk_written_bytes.fetch_add(disk_written_bytes, std::memory_order_relaxed);
}

void DiskExchangeDataManager::checkEnoughSpace()
{
    if (global_disk_written_bytes.load(std::memory_order_relaxed) > max_disk_bytes)
        throw Exception(
            ErrorCodes::BSP_EXCHANGE_DATA_DISK_LIMIT_EXCEEDED,
            "exchange data file exceeded max_disk_bytes, current size:{} max_disk_bytes:{}",
            global_disk_written_bytes.load(std::memory_order_relaxed),
            max_disk_bytes);
}

void DiskExchangeDataManager::removeWriteTaskDirectory(const std::variant<String, UInt64> & delete_item)
{
    String delete_file = std::holds_alternative<String>(delete_item) ? std::get<String>(delete_item)
                                                                     : (path / std::to_string(std::get<UInt64>(delete_item))).string();
    if (disk->exists(delete_file))
    {
        disk->removeRecursive(delete_file);
        if (std::holds_alternative<UInt64>(delete_item))
        {
            auto query_unique_id = std::get<UInt64>(delete_item);
            ssize_t disk_written_bytes = 0;
            {
                std::unique_lock<bthread::Mutex> lock(mutex);
                auto it = alive_queries.find(query_unique_id);
                if (it != alive_queries.end())
                    disk_written_bytes = alive_queries[query_unique_id].disk_written_bytes;
            }
            LOG_INFO(logger, "Removed disk exchange data files under directory {} disk_written_bytes:{}", delete_file, disk_written_bytes);
            global_disk_written_bytes.fetch_sub(disk_written_bytes, std::memory_order_relaxed);
        }
        else
        {
            auto file_size = getFileSizeRecursively(delete_file);
            LOG_INFO(logger, "Removed disk exchange data files under directory {} file_size:{}", delete_file, file_size);
            global_disk_written_bytes.fetch_sub(file_size, std::memory_order_relaxed);
        }
    }
    else
    {
        LOG_WARNING(logger, "cannot find file {} to delete", delete_file);
    }
}

ssize_t DiskExchangeDataManager::getFileSizeRecursively(const String & file_path)
{
    ssize_t file_size = 0;
    if (disk->isDirectory(file_path))
    {
        for (auto iter = disk->iterateDirectory(file_path); iter->isValid(); iter->next())
        {
            file_size += getFileSizeRecursively(iter->path());
        }
    }
    else if (disk->isFile(file_path))
    {
        file_size += disk->getFileSize(file_path);
    }
    return file_size;
}

void DiskExchangeDataManager::ReadTask::setDone()
{
    {
        std::unique_lock<bthread::Mutex> lk(done_mutex);
        done = true;
    }
    done_cv.notify_all();
}

void DiskExchangeDataManager::ReadTask::waitDone()
{
    std::unique_lock<bthread::Mutex> lock(done_mutex);
    if (!done_cv.wait_for(lock, std::chrono::milliseconds(disk_partition_wait_done_timeout_ms), [&]() { return done; }))
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, fmt::format("wait for DiskExchangeDataSource {} done timeout", *key));
    }
}
}

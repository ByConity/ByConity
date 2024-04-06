#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <Processors/Exchange/DataTrans/Batch/Writer/DiskPartitionWriter.h>
#include <Processors/Exchange/DataTrans/BroadcastSenderProxyRegistry.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/time.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>


namespace
{
constexpr size_t WRITE_TASK_INTERACTIVE_INTERVAL = 100;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

DiskPartitionWriter::DiskPartitionWriter(ContextPtr context_, DiskExchangeDataManagerPtr mgr_, Block header_, ExchangeDataKeyPtr key_)
    : IBroadcastSender(true)
    , context(std::move(context_))
    , mgr(std::move(mgr_))
    , disk(mgr->getDisk())
    , header(std::move(header_))
    , key(std::move(key_))
    , log(&Poco::Logger::get("DiskPartitionWriter"))
    , data_queue(std::make_shared<BoundedDataQueue<Chunk>>(context->getSettingsRef().exchange_remote_receiver_queue_size))
    , enable_disk_writer_metrics(context->getSettingsRef().log_query_exchange)
{
    auto query_expiration_ts = context->getQueryExpirationTimeStamp();
    query_expiration_ms = query_expiration_ts.tv_sec * 1000 + query_expiration_ts.tv_nsec / 1000000;
    LOG_TRACE(log, "constructed for key:{}", *key);
}

DiskPartitionWriter::~DiskPartitionWriter()
{
    try
    {
        auto query_exchange_log = context->getQueryExchangeLog();
        if (enable_disk_writer_metrics && query_exchange_log)
        {
            QueryExchangeLogElement element;
            element.initial_query_id = context->getInitialQueryId();
            element.exchange_id = key->exchange_id;
            element.partition_id = key->partition_id;
            element.parallel_index = key->parallel_index;
            element.event_time
                = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            element.send_time_ms = sender_metrics.send_time_ms.get_value();
            element.num_send_times = sender_metrics.num_send_times.get_value();
            element.send_rows = sender_metrics.send_rows.get_value();
            element.send_bytes = sender_metrics.send_bytes.get_value();
            element.send_uncompressed_bytes = sender_metrics.send_uncompressed_bytes.get_value();
            element.ser_time_ms = sender_metrics.ser_time_ms.get_value();
            element.send_retry = sender_metrics.send_retry.get_value();
            element.send_retry_ms = sender_metrics.send_retry_ms.get_value();
            element.overcrowded_retry = sender_metrics.overcrowded_retry.get_value();
            element.finish_code = sender_metrics.finish_code;
            element.is_modifier = sender_metrics.is_modifier;
            element.message = sender_metrics.message;
            element.type = "disk_partition_writer";
            element.disk_partition_writer_commit_ms = writer_metrics.commit_ms.get_value();
            element.disk_partition_writer_create_file_ms = writer_metrics.create_file_ms.get_value();
            element.disk_partition_writer_pop_ms = writer_metrics.pop_ms.get_value();
            element.disk_partition_writer_write_ms = writer_metrics.write_ms.get_value();
            element.disk_partition_writer_write_num = writer_metrics.write_num.get_value();
            query_exchange_log->add(element);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

BroadcastStatus DiskPartitionWriter::sendImpl(Chunk chunk)
{
    auto now = time_in_milliseconds(std::chrono::system_clock::now());
    size_t timeout = now <= query_expiration_ms ? query_expiration_ms - now : 0;
    bool succ = data_queue->tryPush(std::move(chunk), timeout);
    if (!succ)
        return finish(BroadcastStatusCode::SEND_TIMEOUT, "send data timeout");
    return BroadcastStatus(BroadcastStatusCode::RUNNING);
}

void DiskPartitionWriter::merge(IBroadcastSender &&)
{
    throw Exception("merge is not implemented for DiskPartitionWriter", ErrorCodes::NOT_IMPLEMENTED);
}

BroadcastStatus DiskPartitionWriter::finish(BroadcastStatusCode status_code, String message)
{
    /// make sure finish is called only once
    auto expected = static_cast<int>(BroadcastStatusCode::RUNNING);
    bool is_modifier = false;
    Stopwatch s;
    if (sender_metrics.finish_code.compare_exchange_strong(
            expected, static_cast<int>(status_code), std::memory_order_acq_rel, std::memory_order_relaxed))
    {
        finished.store(true, std::memory_order_release);
        is_modifier = true;
        SCOPE_EXIT({
            data_queue->close();
        });
        if (status_code == BroadcastStatusCode::ALL_SENDERS_DONE)
        {
            std::unique_lock<bthread::Mutex> lock(done_mutex);
            auto now = time_in_milliseconds(std::chrono::system_clock::now());
            size_t timeout = now <= query_expiration_ms ? query_expiration_ms - now : 0;
            if (!done_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&]() { return done || data_queue->closed(); }))
            {
                sender_metrics.finish_code.store(BroadcastStatusCode::SEND_TIMEOUT, std::memory_order_release);
                if (enable_disk_writer_metrics)
                    sender_metrics.message = fmt::format(
                    "wait flushing data to disk timeout for key:{} done:{} data_queue->closed():{}", *key, done, data_queue->closed());
                status_code = static_cast<BroadcastStatusCode>(sender_metrics.finish_code.load(std::memory_order_acquire));
                return BroadcastStatus(status_code, true, sender_metrics.message);
            }
            if (enable_disk_writer_metrics)
                s.start();
            /// commit file by renaming
            disk->replaceFile(mgr->getTemporaryFileName(*key), mgr->getFileName(*key));
            if (enable_disk_writer_metrics)
                writer_metrics.commit_ms << s.elapsedMilliseconds();
        }
        /// in other cases, we need to close the data_queue
        else
        {
            data_queue->close();
        }
        if (enable_disk_writer_metrics)
            sender_metrics.message = message;
        LOG_TRACE(log, "finished for key:{} status change to code:{} message:{}", *key, status_code, sender_metrics.message);
    }
    else
    {
        message = fmt::format(
            "name:{} already finished failed to change from {} to {}",
            getName(),
            static_cast<BroadcastStatusCode>(sender_metrics.finish_code.load(std::memory_order_acquire)),
            status_code);
    }
    status_code = static_cast<BroadcastStatusCode>(sender_metrics.finish_code.load(std::memory_order_acquire));
    return BroadcastStatus(status_code, is_modifier, message);
}

void DiskPartitionWriter::runWriteTask()
{
    Stopwatch s;
    if (enable_disk_writer_metrics)
        s.start();
    auto buf = mgr->createFileBufferForWrite(key);
    auto stream
        = std::make_unique<NativeChunkOutputStream>(*buf, header);
    if (enable_disk_writer_metrics)
        writer_metrics.create_file_ms << s.elapsedMilliseconds();

    /// only breaks when
    /// 1. data_queue is closed.
    /// 2. finish is called, so no more data will be pushed to queue, and data_queue is empty.
    if (enable_disk_writer_metrics)
        s.restart();
    while (!data_queue->closed() && !(finished.load(std::memory_order_acquire) && data_queue->empty()))
    {
        Chunk chunk;
        if (data_queue->tryPop(chunk, WRITE_TASK_INTERACTIVE_INTERVAL))
        {
            if (enable_disk_writer_metrics)
            {
                writer_metrics.pop_ms << s.elapsedMilliseconds();
                s.restart();
            }
            stream->write(std::move(chunk));
            if (enable_disk_writer_metrics)
            {
                writer_metrics.write_ms << s.elapsedMilliseconds();
                writer_metrics.write_num << 1;
                s.restart();
            }
        }
        /// pop failed, save pop time if needed
        else if (enable_disk_writer_metrics)
        {
            writer_metrics.pop_ms << s.elapsedMilliseconds();
            s.restart();
        }
    }

    buf->sync();

    SCOPE_EXIT({
        {
            std::unique_lock<bthread::Mutex> lock(done_mutex);
            done = true;
        }
        done_cv.notify_all();
        mgr->updateWrittenBytes(key->query_unique_id, key, buf->count());
    });
}

} // namespace DB

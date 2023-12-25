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
    : IBroadcastSender(context_->getSettingsRef().log_query_exchange)
    , context(std::move(context_))
    , mgr(std::move(mgr_))
    , disk(mgr->getDisk())
    , header(std::move(header_))
    , key(std::move(key_))
    , log(&Poco::Logger::get("DiskPartitionWriter"))
    , data_queue(std::make_shared<BoundedDataQueue<Chunk>>(context->getSettingsRef().exchange_remote_receiver_queue_size))
    , timeout(context->getSettingsRef().exchange_timeout_ms)
    , low_cardinality_allow_in_native_format(context->getSettings().low_cardinality_allow_in_native_format)
{
    LOG_TRACE(log, "constructed for key:{}", *key);
}

DiskPartitionWriter::~DiskPartitionWriter()
{
    try
    {
        auto query_exchange_log = context->getQueryExchangeLog();
        if (enable_sender_metrics && query_exchange_log)
        {
            QueryExchangeLogElement element;
            element.initial_query_id = context->getInitialQueryId();
            element.exchange_id = std::to_string(key->exchange_id);
            element.partition_id = std::to_string(key->parallel_index);
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
    bool succ = data_queue->tryPush(std::move(chunk), timeout);
    if (!succ)
        return BroadcastStatus(BroadcastStatusCode::SEND_TIMEOUT);
    return BroadcastStatus(BroadcastStatusCode::RUNNING);
}

void DiskPartitionWriter::merge(IBroadcastSender &&)
{
    throw Exception("merge is not implemented for DiskPartitionWriter", ErrorCodes::NOT_IMPLEMENTED);
}

BroadcastStatus DiskPartitionWriter::finish(BroadcastStatusCode status_code, String message_)
{
    /// make sure finish is called only once
    bool expected = false;
    Stopwatch s;
    if (finished.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed))
    {
        SCOPE_EXIT({
            data_queue->close();
            buf = nullptr; /// this operation should close the file
        });
        std::unique_lock<bthread::Mutex> lock(done_mutex);
        if (!done_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&]() { return done || data_queue->closed(); }))
            throw Exception(fmt::format("wait flushing data to disk timeout for key:{}", *key), ErrorCodes::TIMEOUT_EXCEEDED);
        if (data_queue->closed())
            throw Exception(fmt::format("data queue closed for key:{}", *key), ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
        chassert(buf); /// buf must be created at this point
        if (enable_sender_metrics)
            s.start();
        buf->sync();
        /// commit file by renaming
        disk->replaceFile(mgr->getTemporaryFileName(*key), mgr->getFileName(*key));
        if (enable_sender_metrics)
            writer_metrics.commit_ms << s.elapsedMilliseconds();
        LOG_TRACE(log, "finished for key:{}", *key);
    }
    return BroadcastStatus(status_code, true, message_);
}

void DiskPartitionWriter::runWriteTask()
{
    Stopwatch s;
    if (enable_sender_metrics)
        s.start();
    buf = mgr->createFileBufferForWrite(key);
    auto stream
        = std::make_unique<NativeChunkOutputStream>(*buf, DBMS_TCP_PROTOCOL_VERSION, header, !low_cardinality_allow_in_native_format);
    if (enable_sender_metrics)
        writer_metrics.create_file_ms << s.elapsedMilliseconds();

    /// only breaks when
    /// 1. data_queue is closed.
    /// 2. finish is called, so no more data will be pushed to queue, and data_queue is empty.
    if (enable_sender_metrics)
        s.restart();
    while (!data_queue->closed() && !(finished.load(std::memory_order_acquire) && data_queue->empty()))
    {
        Chunk chunk;
        if (data_queue->tryPop(chunk, WRITE_TASK_INTERACTIVE_INTERVAL))
        {
            if (enable_sender_metrics)
            {
                writer_metrics.pop_ms << s.elapsedMilliseconds();
                s.restart();
            }
            stream->write(std::move(chunk));
            if (enable_sender_metrics)
            {
                writer_metrics.write_ms << s.elapsedMilliseconds();
                writer_metrics.write_num << 1;
                s.restart();
            }
        }
        /// pop failed, save pop time if needed
        else if (enable_sender_metrics)
        {
            writer_metrics.pop_ms << s.elapsedMilliseconds();
            s.restart();
        }
    }
    {
        std::unique_lock<bthread::Mutex> lock(done_mutex);
        done = true;
    }
    done_cv.notify_all();
}

void DiskPartitionWriter::cancel()
{
    LOG_DEBUG(log, "cancelled for key:{}", *key);
    data_queue->close();
}

String DiskPartitionWriter::getFileName() const
{
    chassert(buf);
    return buf->getFileName();
}
} // namespace DB

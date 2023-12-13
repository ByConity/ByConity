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

DiskPartitionWriter::DiskPartitionWriter(
    const ContextPtr & context, DiskExchangeDataManagerPtr mgr_, Block header_, ExchangeDataKeyPtr key_)
    : mgr(std::move(mgr_))
    , disk(mgr->getDisk())
    , header(std::move(header_))
    , key(std::move(key_))
    , log(&Poco::Logger::get("DiskPartitionWriter"))
    , data_queue(std::make_shared<BoundedDataQueue<Chunk>>(context->getSettingsRef().exchange_remote_receiver_queue_size))
    , timeout(context->getSettingsRef().exchange_timeout_ms)
    , low_cardinality_allow_in_native_format(context->getSettings().low_cardinality_allow_in_native_format)
{
    enable_sender_metrics = true;
    LOG_TRACE(log, "constructed for key:{}", *key);
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
        buf->sync();
        /// commit file by renaming
        disk->replaceFile(mgr->getTemporaryFileName(*key), mgr->getFileName(*key));
        LOG_TRACE(log, "finished for key:{}", *key);
    }
    return BroadcastStatus(status_code, true, message_);
}

void DiskPartitionWriter::runWriteTask()
{
    buf = mgr->createFileBufferForWrite(key);
    auto stream
        = std::make_unique<NativeChunkOutputStream>(*buf, DBMS_TCP_PROTOCOL_VERSION, header, !low_cardinality_allow_in_native_format);

    /// only breaks when
    /// 1. data_queue is closed.
    /// 2. finish is called, so no more data will be pushed to queue, and data_queue is empty.
    while (!data_queue->closed() && !(finished.load(std::memory_order_acquire) && data_queue->empty()))
    {
        Chunk chunk;
        if (data_queue->tryPop(chunk, WRITE_TASK_INTERACTIVE_INTERVAL))
        {
            stream->write(std::move(chunk));
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

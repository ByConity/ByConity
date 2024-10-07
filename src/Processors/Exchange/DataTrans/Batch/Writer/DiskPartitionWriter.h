#pragma once
#include <Common/Logger.h>
#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>

namespace DB
{

/// DiskPartitionWriter is responsible for writing data of a single partition to disk
class DiskPartitionWriter : public IBroadcastSender
{
public:
    DiskPartitionWriter(ContextPtr context, const DiskExchangeDataManagerPtr & mgr_, Block header_, ExchangeDataKeyPtr key_);
    ~DiskPartitionWriter() override;
    /// send data to queue
    BroadcastStatus sendImpl(Chunk chunk) override;
    /// run write task
    void runWriteTask();
    void merge(IBroadcastSender && sender) override;
    String getName() const override
    {
        return "DiskPartitionWriter";
    }
    BroadcastSenderType getType() override
    {
        return BroadcastSenderType::Disk;
    }
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    inline ExchangeDataKeyPtr getKey() const
    {
        return key;
    }
    void waitDone(size_t timeout_ms)
    {
        std::unique_lock<bthread::Mutex> lock(done_mutex);
        if (!done_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]() { return done; }))
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, fmt::format("wait for {} done timeout", *key));
        }
    }

private:
    struct DiskPartitionWriterMetrics
    {
        bvar::Adder<size_t> create_file_ms{};
        bvar::Adder<size_t> pop_ms{};
        bvar::Adder<size_t> write_ms{};
        bvar::Adder<size_t> write_num{};
        bvar::Adder<size_t> commit_ms{};
        bvar::Adder<size_t> wait_done_ms{};
        bvar::Adder<size_t> sync_ms{};
    };
    DiskPartitionWriterMetrics writer_metrics;
    ContextPtr context;
    std::weak_ptr<DiskExchangeDataManager> mgr;
    DiskPtr disk;
    Block header;
    ExchangeDataKeyPtr key;
    LoggerPtr log;
    /// data_queue is used here to ensure thread-safety(by background write task) when multiple write/finish are called from different threads
    /// TODO @lianxuechao optimize for single-thread case
    std::shared_ptr<BoundedDataQueue<Chunk>> data_queue;
    std::atomic_bool finished{false};
    bthread::Mutex done_mutex;
    bthread::ConditionVariable done_cv;
    bool done = false;
    bool enable_disk_writer_metrics;
    size_t query_expiration_ms;
};

using DiskPartitionWriterPtr = std::shared_ptr<DiskPartitionWriter>;
} // namespace DB

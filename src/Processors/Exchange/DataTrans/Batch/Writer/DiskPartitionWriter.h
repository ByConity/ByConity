#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/DataTrans/NativeChunkOutputStream.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include "IO/WriteBufferFromFileBase.h"

namespace DB
{

/// DiskPartitionWriter is responsible for writing data of a single partition to disk
class DiskPartitionWriter : public IBroadcastSender
{
public:
    DiskPartitionWriter(const ContextPtr & context, DiskExchangeDataManagerPtr mgr_, Block header_, ExchangeDataKeyPtr key_);
    ~DiskPartitionWriter() override = default;
    /// send data to queue
    BroadcastStatus sendImpl(Chunk chunk) override;
    /// run write task
    void runWriteTask();
    /// cancel write task
    void cancel();
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
    String getFileName() const;
    inline ExchangeDataKeyPtr getKey() const
    {
        return key;
    }

private:
    DiskExchangeDataManagerPtr mgr;
    DiskPtr disk;
    Block header;
    ExchangeDataKeyPtr key;
    std::unique_ptr<WriteBufferFromFileBase> buf;
    Poco::Logger * log;
    /// data_queue is used here to ensure thread-safety(by background write task) when multiple write/finish are called from different threads
    /// TODO @lianxuechao optimize for single-thread case
    std::shared_ptr<BoundedDataQueue<Chunk>> data_queue;
    size_t timeout;
    std::atomic_bool finished{false};
    bthread::Mutex done_mutex;
    bthread::ConditionVariable done_cv;
    bool done = false;
    bool low_cardinality_allow_in_native_format;
};

using DiskPartitionWriterPtr = std::shared_ptr<DiskPartitionWriter>;
} // namespace DB

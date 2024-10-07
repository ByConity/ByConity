#pragma once

#include <Common/Logger.h>
#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <brpc/stream.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include "common/logger_useful.h"
#include "common/types.h"
#include <Common/Throttler.h>
#include "Storages/MergeTree/MarkRange.h"
#include "Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h"
#include <Disks/DiskType.h>

namespace DB
{
using DataQueuePtr = std::shared_ptr<BoundedDataQueue<butil::IOBuf>>;

struct DistributedDataClientOption
{
    UInt64 max_request_rate = 0;
    UInt64 connection_timeout_ms = 5000;
    UInt64 read_timeout_ms = 10000;
    UInt64 max_retry_times = 3;
    UInt64 retry_sleep_ms = 100;
    UInt64 max_queue_count = 100000;
};

struct WriteFile
{
    String key{};
    String path{};
    UInt64 offset{0};
    UInt64 length{0};
};

class DataStreamReader
{
public:
    explicit DataStreamReader(const String remote_addr_, const String local_key_name_, const DistributedDataClientOption & options_)
        : remote_addr(remote_addr_)
        , local_key_name(local_key_name_)
        , queue(std::make_shared<BoundedDataQueue<butil::IOBuf>>(options_.max_queue_count))
    {
    }

    void pushReceiveQueue(butil::IOBuf packet, UInt64 timeout_ms);
    void readReceiveQueue(BufferBase::Buffer & buffer /*output*/, UInt64 timeout_ms);

    String readFileInfoMessage() {
        if (local_key_name == remote_file_path)
            return fmt::format("Stream file path: {}/{}, file size: {}", remote_addr, remote_file_path, remote_file_size);
        return fmt::format("Stream file local key {}, remote file: {}/{}, remote size: {}", local_key_name, remote_addr, remote_file_path, remote_file_size);
    }

    String remote_addr;
    String local_key_name;
    String remote_file_path;
    UInt64 remote_file_size;
    DataQueuePtr queue;
    LoggerPtr log = getLogger("StreamClientHandler");
};

class StreamClientHandler : public brpc::StreamInputHandler
{
public:
    explicit StreamClientHandler(std::shared_ptr<DataStreamReader> file_reader_, const DistributedDataClientOption & option_ = {})
        : file_reader(file_reader_), option(option_)
    {
    }

    virtual int on_received_messages(brpc::StreamId id, butil::IOBuf * const messages[], size_t size) override;

    virtual void on_idle_timeout(brpc::StreamId id) override
    {
        LOG_TRACE(
            log,
            "Client Stream({}) has no data transmission for a while: {}",
            id,
            file_reader->readFileInfoMessage());
    }
    virtual void on_closed(brpc::StreamId id) override
    {
        LOG_TRACE(log, "Client Stream({}) is closed: {}", id, file_reader->readFileInfoMessage());
    }
    void on_finished(brpc::StreamId id, int32_t) override
    {
        LOG_TRACE(log, "Client Stream({}) is finished: {}", id, file_reader->readFileInfoMessage());
    }

private:
    std::shared_ptr<DataStreamReader> file_reader;
    DistributedDataClientOption option;

    LoggerPtr log = getLogger("StreamClientHandler");
};

class DistributedDataClient
{
public:
    explicit DistributedDataClient(const String & remote_addr_, const String local_key_name_ = {}, const DistributedDataClientOption & option_ = {})
        : remote_addr(remote_addr_)
        , local_key_name(local_key_name_)
        , file_reader(std::make_shared<DataStreamReader>(remote_addr_, local_key_name_, option_))
        , option(option_)
        , read_rate_throttler(option.max_request_rate == 0 ? std::nullopt : std::optional<Throttler>(option.max_request_rate))
    {
    }

    ~DistributedDataClient() { close(); }

    bool createReadStream();
    bool read(UInt64 offset, UInt64 length, BufferBase::Buffer &);
    bool write(const String & disk_name, const std::vector<WriteFile> & files) const;
    bool close() const;

    String remote_addr;
    String local_key_name;
    String remote_file_path;
    UInt64 remote_file_size{0};

    std::shared_ptr<DataStreamReader> file_reader;

    DistributedDataClientOption option;
    brpc::StreamId stream_id{brpc::INVALID_STREAM_ID};

    std::optional<Throttler> read_rate_throttler;

    LoggerPtr log = getLogger("DistributedDataClient");
};

}

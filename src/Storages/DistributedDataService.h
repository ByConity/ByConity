#pragma once

#include <Common/Logger.h>
#include <cstddef>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Protos/distributed_file.pb.h>
#include <brpc/stream.h>
#include <fmt/core.h>
#include <google/protobuf/service.h>
#include "common/logger_useful.h"
#include "common/types.h"
#include <Common/Brpc/BrpcServiceDefines.h>

namespace DB
{

class DistributedDataService : public Protos::FileStreamService
{
public:
    explicit DistributedDataService(ContextMutablePtr & context_);
    explicit DistributedDataService(int max_buf_size_ = 1024 * 1024, int thread_pool_size_ = 16, int max_retry_count = 3);

    void acceptConnection(
        ::google::protobuf::RpcController * controller,
        const ::DB::Protos::ConnectRequest * request,
        ::DB::Protos::ConnectResponse * response,
        ::google::protobuf::Closure * closeure) override;

    virtual String getFileFullPath(const String & key) { return key; }

    static std::unique_ptr<ThreadPool> thread_pool;

protected:
    int max_buf_size{0};
    int max_retry_count{3};
    DisksMap disks;

    LoggerPtr log = getLogger("DistributedDataService");
};

REGISTER_SERVICE_IMPL(DistributedDataService);

class StreamServiceHandler : public brpc::StreamInputHandler
{
public:
    explicit StreamServiceHandler(const String & request_key_, const String & file_path_, const UInt64 file_size_, const int max_retry_count_)
        : request_key(request_key_), file_path(file_path_), file_size(file_size_), max_retry_count(max_retry_count_)
    {
    }

    virtual int on_received_messages(brpc::StreamId id, butil::IOBuf * const messages[], size_t size) override;

    virtual void on_idle_timeout(brpc::StreamId id) override
    {
        LOG_TRACE(log, "Service Stream = {} has no data transmission for a while: {}", id, readFileInfoMessage());
    }
    virtual void on_closed(brpc::StreamId id) override { LOG_TRACE(log, "Service Stream = {} is closed: {}", id, readFileInfoMessage()); }
    void on_finished(brpc::StreamId id, int32_t) override { LOG_TRACE(log, "Service Stream = {} is finished: {}", id, readFileInfoMessage()); }

    String readFileInfoMessage()
    {
        if (request_key == file_path)
            return fmt::format("Stream file path: {}, file size: {}", file_path, file_size);
        return fmt::format("Stream file request key {}, file path: {}, file size: {}", request_key, file_path, file_size);
    }

private:
    String request_key;
    String file_path;
    UInt64 file_size;
    int max_retry_count;

    LoggerPtr log = getLogger("StreamServiceHandler");
};

}

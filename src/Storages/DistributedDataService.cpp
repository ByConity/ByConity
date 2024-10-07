#include <cstddef>
#include <memory>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <Processors/Exchange/DataTrans/Brpc/WriteBufferFromBrpcBuf.h>
#include <Storages/DistributedDataService.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <brpc/stream_impl.h>
#include <fmt/core.h>
#include <sys/types.h>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include "Common/Stopwatch.h"
#include "Common/ThreadPool.h"
#include "common/logger_useful.h"
#include "common/types.h"
#include "Interpreters/Context.h"
#include "Storages/DistributedDataCommon.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::unique_ptr<ThreadPool> DistributedDataService::thread_pool;

DistributedDataService::DistributedDataService(ContextMutablePtr & context_)
    : max_buf_size(context_->getSettingsRef().exchange_stream_max_buf_size)
    , max_retry_count(context_->getSettingsRef().distributed_data_service_max_retry_count)
    , disks(context_->getDisksMap())
{
    DistributedDataService::thread_pool = std::make_unique<ThreadPool>(
        context_->getSettingsRef().distributed_data_service_pool_size,
        context_->getSettingsRef().distributed_data_service_pool_size,
        context_->getSettingsRef().distributed_data_service_pool_size * 100);
}

DistributedDataService::DistributedDataService(int max_buf_size_, int thread_pool_size_, int max_retry_count_)
    : max_buf_size(max_buf_size_), max_retry_count(max_retry_count_)
{
    DistributedDataService::thread_pool = std::make_unique<ThreadPool>(thread_pool_size_, thread_pool_size_, thread_pool_size_ * 100);
}

void DistributedDataService::acceptConnection(
    ::google::protobuf::RpcController * controller,
    const ::DB::Protos::ConnectRequest * request,
    ::DB::Protos::ConnectResponse * response,
    ::google::protobuf::Closure * closeure)
{
    Stopwatch watch;
    brpc::StreamId sender_stream_id = brpc::INVALID_STREAM_ID;
    brpc::Controller * cntl = static_cast<brpc::Controller *>(controller);

    /// this done_guard guarantee to call done->Run() in any situation
    brpc::ClosureGuard done_guard(closeure);
    brpc::StreamOptions stream_options;
    std::shared_ptr<StreamServiceHandler> file_service_handler;

    try
    {
        String file_path = getFileFullPath(request->key());
        UInt64 file_size = 0;
        if (!file_path.empty())
        {
            struct stat obj_stat;
            if (stat(file_path.c_str(), &obj_stat) == 0)
            {
               file_size = obj_stat.st_size;
            }
        }

        file_service_handler = std::make_shared<StreamServiceHandler>(request->key(), file_path, file_size, max_retry_count);

        stream_options.max_buf_size = max_buf_size;
        stream_options.handler = file_service_handler;
        if (brpc::StreamAccept(&sender_stream_id, *cntl, &stream_options) != 0)
        {
            sender_stream_id = brpc::INVALID_STREAM_ID;
            String error_msg = fmt::format("Fail to accept stream for file_path: {}, request_key: {}", file_path, request->key());
            LOG_ERROR(log, error_msg);
            cntl->SetFailed(error_msg);
            return;
        }

        response->set_file_full_path(file_path);
        response->set_file_size(file_size);
        LOG_TRACE(
            log,
            "Receive create stream_id: {} take {}ms: {}",
            sender_stream_id,
            watch.elapsedMilliseconds(),
            file_service_handler->readFileInfoMessage());
        return;
    }
    catch (Exception & e)
    {
        String error_msg = fmt::format(
            "Failed accept remote client connetion: {}, {}",
            file_service_handler ? file_service_handler->readFileInfoMessage() : request->key(),
            e.message());
        LOG_ERROR(log, error_msg);
        return;
    }
}

int StreamServiceHandler::on_received_messages(brpc::StreamId id, butil::IOBuf * const messages[], [[maybe_unused]] size_t size)
{
    for (size_t i = 0; i < size; i++)
    {
        auto read_buffer = std::make_unique<ReadBufferFromBrpcBuf>(*messages[i]);
        UInt64 offset = 0;
        UInt64 length = 0;
        readVarUInt(offset, *read_buffer);
        readVarUInt(length, *read_buffer);

        if (static_cast<UInt64>(offset) >= file_size)
            throw Exception(
                fmt::format("Can't read invalid offset {} since file: {}", offset, readFileInfoMessage()), ErrorCodes::LOGICAL_ERROR);

        if (length <= 0)
            throw Exception(
                fmt::format("Can't read invalid offset {} length {} since file: {}", offset, length, readFileInfoMessage()),
                ErrorCodes::LOGICAL_ERROR);

        DistributedDataService::thread_pool->scheduleOrThrowOnError(
            [retry = max_retry_count, message = readFileInfoMessage(), i, id, offset, length, file = file_path, size = file_size] {
                Stopwatch watch;

                ReadBufferFromFile in(file);
                WriteBufferFromBrpcBuf out;
                in.seek(offset, SEEK_SET);
                copyData(in, out, std::min(static_cast<UInt64>(size) - offset, length));
                out.finish();

                bool success = brpcWriteWithRetry(id, out.getFinishedBuf(), retry, message);
                LOG_TRACE(
                    getLogger("StreamServiceHandler"),
                    "Response(success = {}) read request[{}] stream_id: {}, "
                    "offset: {}, length: {}({}), total take {}ms from first request id: {}",
                    success,
                    i,
                    id,
                    offset,
                    length,
                    size - offset,
                    watch.elapsedMilliseconds(),
                    message);
            });
    }

    return 0;
}
} // namespace DB

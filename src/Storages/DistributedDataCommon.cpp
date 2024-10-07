#include <Storages/DistributedDataCommon.h>
#include <bthread/bthread.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include "Common/ErrorCodes.h"
#include "common/logger_useful.h"

const auto STREAM_WAIT_TIMEOUT_MS = 1000;

namespace DB
{
bool brpcWriteWithRetry(brpc::StreamId id, const butil::IOBuf & buf, int retry_count, const String & message)
{
    while (retry_count-- > 0)
    {
        int rect_code = brpc::StreamWrite(id, buf);
        if (rect_code == 0)
            return true;
        else if (rect_code == EAGAIN)
        {
            bthread_usleep(50 * 1000);
            timespec timeout = butil::milliseconds_from_now(STREAM_WAIT_TIMEOUT_MS);
            int wait_res_code = brpc::StreamWait(id, &timeout);
            if (wait_res_code == EINVAL)
            {
                // TODO: retain stream object before finish code is read.
                // Ingore error when writing to the closed stream, because this stream is closed by remote peer before read any finish code.
                LOG_INFO(getLogger("DistributedDataCommon"), "Stream-{}, file info {} is closed", id, message);
                return false;
            }

            LOG_TRACE(
                getLogger("DistributedDataCommon"),
                "Stream write buffer full wait for {} ms,  remaining retry count-{}, stream_id-{}, {} wait res code: {} size:{} ", // todo(jiashuo): support distributed file stealing
                STREAM_WAIT_TIMEOUT_MS,
                retry_count,
                id,
                message,
                wait_res_code,
                buf.size());
        }
        else if (rect_code == EINVAL)
        {
            // Ingore error when writing to the closed stream, because this stream is closed by remote peer before read any finish code.
            LOG_INFO(getLogger("DistributedDataCommon"), "Stream-{} with {} is closed", id, message);
            return false;
        }
        else if (rect_code == 1011) //EOVERCROWDED   | 1011 | The server is overcrowded
        {
            bthread_usleep(1000 * 1000);
            LOG_WARNING(
                getLogger("DistributedDataCommon"),
                "Stream-{} write buffer error rect_code: EOVERCROWDED, server is overcrowded: {}",
                id,
                message);
        }

        // stream finished
        else if (rect_code == -1)
        {
            int stream_finished_code = 0;
            auto rc = brpc::StreamFinishedCode(id, stream_finished_code);
            // Stream is closed by remote peer and we can get finish code now
            if (rc == EINVAL)
                return false;
            LOG_INFO(
                getLogger("DistributedDataCommon"),
                "Stream-{} write receive finish request, finish code: -1: {}",
                id,
                rect_code,
                message);
            return false;
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Stream-{} write buffer occurred error, the rect_code that we can not handle: {}, message: {}", rect_code, message),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    return false;
}
}

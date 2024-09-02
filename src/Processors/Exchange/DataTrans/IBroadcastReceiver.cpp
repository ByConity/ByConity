#include <memory>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DeserializeBufTransform.h>
#include <butil/iobuf.h>

namespace DB
{
void IBroadcastReceiver::addToMetricsMaybe(size_t recv_time_ms, size_t dser_time_ms, size_t recv_counts, const Chunk & chunk)
{
    if (enable_receiver_metrics)
    {
        receiver_metrics.dser_time_ms << dser_time_ms;
        receiver_metrics.recv_counts << recv_counts;
        receiver_metrics.recv_time_ms << recv_time_ms;
        auto info = chunk.getChunkInfo() ? std::dynamic_pointer_cast<const DeserializeBufTransform::IOBufChunkInfo>(chunk.getChunkInfo())
                                         : nullptr;
        if (info)
        {
            receiver_metrics.recv_bytes << info->io_buf.size();
        }
        else
        {
            receiver_metrics.recv_uncompressed_bytes << chunk.bytes();
            receiver_metrics.recv_rows << chunk.getNumRows();
        }
    }
}
void IBroadcastReceiver::addToMetricsMaybe(size_t recv_time_ms, size_t dser_time_ms, size_t recv_counts, const butil::IOBuf & io_buf)
{
    if (enable_receiver_metrics)
    {
        receiver_metrics.dser_time_ms << dser_time_ms;
        receiver_metrics.recv_counts << recv_counts;
        receiver_metrics.recv_time_ms << recv_time_ms;
        receiver_metrics.recv_bytes << io_buf.size();
    }
}
}

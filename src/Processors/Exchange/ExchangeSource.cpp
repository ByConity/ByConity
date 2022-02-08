#include <algorithm>
#include <atomic>
#include <optional>
#include <variant>

#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/SegmentScheduler.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

ExchangeSource::ExchangeSource(Block header_, BroadcastReceiverPtr receiver_, ExchangeOptions options_)
    : SourceWithProgress(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , throw_on_other_segment_error(false)
    , logger(&Poco::Logger::get("ExchangeSource"))
{
}

ExchangeSource::ExchangeSource(Block header_, BroadcastReceiverPtr receiver_, ExchangeOptions options_, bool throw_on_other_segment_error_)
    : SourceWithProgress(std::move(header_), false)
    , receiver(std::move(receiver_))
    , options(options_)
    , throw_on_other_segment_error(throw_on_other_segment_error_)
    , logger(&Poco::Logger::get("ExchangeSource"))
{
}

ExchangeSource::~ExchangeSource() = default;

String ExchangeSource::getName() const
{
    return "ExchangeSource: " + receiver->getName();
}

IProcessor::Status ExchangeSource::prepare()
{
    const auto & status = SourceWithProgress::prepare();
    if (status == Status::Finished)
    {
        if (inited.load(std::memory_order_relaxed))
            receiver->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "Output port finished");
    }
    return status;
}

std::optional<Chunk> ExchangeSource::tryGenerate()
{
    if (!inited.load(std::memory_order_relaxed))
    {
        receiver->registerToSenders(std::min(std::max(options.exhcange_timeout_ms / 3, 1000u), 5000u));
        inited.store(true, std::memory_order_relaxed);
    }
    if (was_query_canceled || was_receiver_finished)
        return std::nullopt;

    RecvDataPacket packet = receiver->recv(options.exhcange_timeout_ms);

    if (std::holds_alternative<Chunk>(packet))
    {
        Chunk chunk = std::move(std::get<Chunk>(packet));
        LOG_TRACE(logger, "{} receive chunk with rows: {}", getName(), chunk.getNumRows());
        return std::make_optional(std::move(chunk));
    }
    const auto & status = std::get<BroadcastStatus>(packet);
    was_receiver_finished = true;

    if (status.code > BroadcastStatusCode::RECV_REACH_LIMIT)
    {
        if (throw_on_other_segment_error || status.is_modifer)
        {
            auto query_id = CurrentThread::getQueryId().toString();
            String exception = CurrentThread::get().getQueryContext()->getSegmentScheduler()->getException(query_id, 100);
            throw Exception(
                getName() + " fail to receive data: " + status.message + " code: " + std::to_string(status.code)
                    + " exception: " + exception,
                ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
        }
    }

    return std::nullopt;
}

void ExchangeSource::onCancel()
{
    LOG_TRACE(logger, "ExchangeSource {} onCancel", getName());
    was_query_canceled = true;
    if(inited.load(std::memory_order_relaxed))
        receiver->finish(BroadcastStatusCode::RECV_CANCELLED, "Cancelled by pipeline");
}

}

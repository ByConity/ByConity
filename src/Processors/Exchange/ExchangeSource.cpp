#include <algorithm>
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
    , logger(&Poco::Logger::get("ExchangeSource"))
{
}

ExchangeSource::~ExchangeSource() = default;

IProcessor::Status ExchangeSource::prepare()
{
    const auto & status = SourceWithProgress::prepare();
    if (status == Status::Finished)
    {
        receiver->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "Output port finished");
    }
    return status;
}

std::optional<Chunk> ExchangeSource::tryGenerate()
{
    if (!inited)
    {
        receiver->registerToSenders(std::max(options.exhcange_timeout_ms / 3, 1000u));
        inited = true;
    }
    if (was_query_canceled || was_receiver_finished)
        return std::nullopt;

    RecvDataPacket packet = receiver->recv(options.exhcange_timeout_ms / 2);

    if (std::holds_alternative<Chunk>(packet))
    {
        Chunk chunk = std::move(std::get<Chunk>(packet));
        LOG_TRACE(logger, "Receive chunk with rows: {}", chunk.getNumRows());
        return std::make_optional(std::move(chunk));
    }
    const auto & status = std::get<BroadcastStatus>(packet);
    was_receiver_finished = true;

    if (status.is_modifer && status.code > 0)
    {
        throw Exception(
            "Fail to receive data: " + status.message + " code: " + std::to_string(status.code), ErrorCodes::EXCHANGE_DATA_TRANS_EXCEPTION);
    }

    return std::nullopt;
}

void ExchangeSource::onCancel()
{
    LOG_TRACE(logger, "ExchangeSource onCancel");
    was_query_canceled = true;
    receiver->finish(BroadcastStatusCode::RECV_CANCELLED, "Cancelled by pipeline");
}

void ExchangeSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_receiver_finished = true;
        receiver->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "Output port finished");
    }
}

}

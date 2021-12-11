#include <optional>
#include <variant>

#include <DataStreams/RemoteQueryExecutor.h>
#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

ExchangeSource::ExchangeSource(Block header_, BroadcastReceiverPtr receiver_ptr_)
    : SourceWithProgress(std::move(header_), false), receive_ptr(receiver_ptr_)
{
}

ExchangeSource::~ExchangeSource() = default;

ISource::Status ExchangeSource::prepare()
{
    const auto & status = SourceWithProgress::prepare();
    if (status == Status::Finished)
    {
        receive_ptr->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "Output port finished");
    }
    return status;
}

std::optional<Chunk> ExchangeSource::tryGenerate()
{
    if (was_query_canceled || was_receiver_finished)
        return std::nullopt;

    RecvDataPacket packet = receive_ptr->recv(0);

    if (std::holds_alternative<Chunk>(packet))
    {
        return std::make_optional(std::move(std::get<Chunk>(packet)));
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
    was_query_canceled = true;
    receive_ptr->finish(BroadcastStatusCode::RECV_CANCELLED, "Cancelled by pipeline");
}

void ExchangeSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_receiver_finished = true;
        receive_ptr->finish(BroadcastStatusCode::RECV_REACH_LIMIT, "Output port finished");
    }
}

}

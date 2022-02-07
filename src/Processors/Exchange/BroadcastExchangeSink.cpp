#include <Common/Exception.h>
#include <common/logger_useful.h>

#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
BroadcastExchangeSink::BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_)
    : IExchangeSink(std::move(header_)), senders(std::move(senders_)), logger(&Poco::Logger::get("BroadcastExchangeSink"))

{
}

BroadcastExchangeSink::~BroadcastExchangeSink() = default;


void BroadcastExchangeSink::consume(Chunk chunk)
{
    for (size_t i = 0; i < senders.size() - 1; ++i)
    {
        auto status = ExchangeUtils::sendAndCheckReturnStatus(*senders[i], chunk.clone());
        if (status.code != BroadcastStatusCode::RUNNING)
            finish();
    }
    auto status = ExchangeUtils::sendAndCheckReturnStatus(*senders.back(), std::move(chunk));
    if (status.code != BroadcastStatusCode::RUNNING)
        finish();
}

void BroadcastExchangeSink::onFinish()
{
    LOG_TRACE(logger, "BroadcastExchangeSink finish");
}

void BroadcastExchangeSink::onCancel()
{
    LOG_TRACE(logger, "BroadcastExchangeSink cancel");

    for (auto & sender : senders)
    {
        sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
    }
}

}

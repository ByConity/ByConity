#include <Common/Exception.h>
#include <common/logger_useful.h>

#include <Processors/Exchange/BroadcastExchangeSink.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeHelpers.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
BroadcastExchangeSink::BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_)
    : ISink(std::move(header_)), senders(std::move(senders_)), logger(&Poco::Logger::get("BroadcastExchangeSink"))

{
}

BroadcastExchangeSink::~BroadcastExchangeSink() = default;


void BroadcastExchangeSink::consume(Chunk chunk)
{
    for (auto & sender: senders)
    {
        sendAndCheckReturnStatus(*sender, chunk.clone());
    }
}

void BroadcastExchangeSink::onFinish()
{
    LOG_TRACE(logger, "BroadcastExchangeSink finish");
    was_finished = true;
}

void BroadcastExchangeSink::onCancel()
{
    LOG_TRACE(logger, "BroadcastExchangeSink cancel");
    if (!was_finished)
    {
        for(auto & sender: senders){
            sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
        }
    }
}

}

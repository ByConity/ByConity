#include <memory>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/LoadBalancedExchangeSink.h>
#include <Processors/ISink.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <common/types.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <Processors/Exchange/ExchangeHelpers.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int EXCHANGE_DATA_TRANS_EXCEPTION;
}

class RoundRobinSelector : public LoadBalancedExchangeSink::LoadBalanceSelector
{
public:
    explicit RoundRobinSelector(size_t partition_num_) : LoadBalanceSelector(partition_num_) { }
    virtual size_t selectNext() override { return count++ % partition_num; }

private:
    UInt32 count = rand(); // NOLINT
};

LoadBalancedExchangeSink::LoadBalancedExchangeSink(Block header_, BroadcastSenderPtrs senders_)
    : ISink(std::move(header_))
    , senders(std::move(senders_))
    , partition_selector(std::make_unique<RoundRobinSelector>(senders.size()))
    , logger(&Poco::Logger::get("LoadBalancedExchangeSink"))
{
}

LoadBalancedExchangeSink::~LoadBalancedExchangeSink() = default;


void LoadBalancedExchangeSink::consume(Chunk chunk)
{
    sendAndCheckReturnStatus(*senders[partition_selector->selectNext()], std::move(chunk));
}

void LoadBalancedExchangeSink::onFinish()
{
    LOG_TRACE(logger, "LoadBalancedExchangeSink finish");
    was_finished = true;
}

void LoadBalancedExchangeSink::onCancel()
{
    LOG_TRACE(logger, "LoadBalancedExchangeSink cancel");
    if (!was_finished)
    {
        for (const BroadcastSenderPtr & sender : senders)
            sender->finish(BroadcastStatusCode::SEND_CANCELLED, "Cancelled by pipeline");
    }
}

}

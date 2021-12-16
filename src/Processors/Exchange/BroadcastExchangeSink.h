#pragma once
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>
#include <Poco/Logger.h>

namespace DB
{

/// Sink which broadcast data to ExchangeSource.
class BroadcastExchangeSink : public ISink
{
public:
    explicit BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_);
    virtual ~BroadcastExchangeSink() override;
    String getName() const override { return "BroadcastExchangeSink"; }
    
protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;
    virtual void onCancel() override;

private:
    BroadcastSenderPtrs senders;
    Poco::Logger * logger;
    std::atomic<bool> was_finished = false;
};

}

#pragma once
#include <atomic>
#include <Processors/Exchange/BufferChunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/IProcessor.h>
#include <bthread/mtx_cv_base.h>
#include <Poco/Logger.h>

namespace DB
{

/// Sink which broadcast data to ExchangeSource.
class BroadcastExchangeSink : public IExchangeSink
{
public:
    BroadcastExchangeSink(Block header_, BroadcastSenderPtrs senders_, ExchangeOptions options_);
    virtual ~BroadcastExchangeSink() override;
    String getName() const override { return "BroadcastExchangeSink"; }
    
protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;
    virtual void onCancel() override;

private:
    BroadcastSenderPtrs senders;
    ExchangeOptions options;
    BufferChunk buffer_chunk;
    Poco::Logger * logger;
};

}

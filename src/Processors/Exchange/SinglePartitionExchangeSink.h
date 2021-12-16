#pragma once
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>
#include <Functions/IFunction.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>

namespace DB
{
/// Send data to single partititon. Usually used with RepartitionTransform and BufferedCopyTransform: 
///                                                 ||-> SinglePartitionExchangeSink
/// RepartitionTransform--> BufferedCopyTransform-->||-> SinglePartitionExchangeSink
///                                                 ||-> SinglePartitionExchangeSink
/// This pipeline can keep data order and maximize the parallelism.
class SinglePartitionExchangeSink : public ISink
{
public:
    explicit SinglePartitionExchangeSink(Block header_, 
    BroadcastSenderPtr sender_, 
    size_t partition_id_,
    ExchangeOptions options_);
    String getName() const override { return "SinglePartitionExchangeSink"; }
    void onCancel() override;
    virtual ~SinglePartitionExchangeSink() override = default;

protected:
    void consume(Chunk) override;
    void onFinish() override;

private:
    const Block & header;
    BroadcastSenderPtr sender;
    size_t partition_id;
    size_t column_num;
    std::atomic<bool> was_finished = false;
    ExchangeOptions options;
    ExchangeBufferedSender buffered_sender;
    Poco::Logger * logger;
};

}

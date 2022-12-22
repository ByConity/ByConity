#pragma once
#include <Core/ColumnNumbers.h>
#include <Functions/IFunction.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/IProcessor.h>

namespace DB
{
/// Send data to single partititon. Usually used with RepartitionTransform and BufferedCopyTransform: 
///                                                 ||-> SinglePartitionExchangeSink[partition 0]
/// RepartitionTransform--> BufferedCopyTransform-->||-> SinglePartitionExchangeSink[partition 1]
///                                                 ||-> SinglePartitionExchangeSink[partition 2]
/// This pipeline can keep data order and maximize the parallelism.
class SinglePartitionExchangeSink : public IExchangeSink
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
    ExchangeOptions options;
    ExchangeBufferedSender buffered_sender;
    Poco::Logger * logger;
};

}

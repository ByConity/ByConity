#pragma once
#include <Core/ColumnNumbers.h>
#include <Functions/IFunction.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/ExchangeBufferedSender.h>
#include <Processors/Exchange/ExchangeOptions.h>
#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/IProcessor.h>

namespace DB
{
/// Send data to all partititons. Usually used with ResizeProcessor 
///                   ||-> MultiPartitionExchangeSink
/// ResizeProcessor-->||-> MultiPartitionExchangeSink
///                   ||-> MultiPartitionExchangeSink
/// This pipeline will not keep data order and maximize the performance.
class MultiPartitionExchangeSink : public IExchangeSink
{
public:
    explicit MultiPartitionExchangeSink(
        Block header_,
        BroadcastSenderPtrs partition_senders_,
        ExecutableFunctionPtr repartition_func_,
        ColumnNumbers repartition_keys,
        ExchangeOptions options_);
    virtual String getName() const override { return "MultiPartitionExchangeSink"; }
    virtual void onCancel() override;
    virtual ~MultiPartitionExchangeSink() override = default;


protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;

private:
    const Block & header;
    BroadcastSenderPtrs partition_senders;
    size_t partition_num;
    size_t column_num;
    ExecutableFunctionPtr repartition_func;
    const ColumnNumbers repartition_keys;
    ExchangeOptions options;
    ExchangeBufferedSenders buffered_senders;
    Poco::Logger * logger;
};

}

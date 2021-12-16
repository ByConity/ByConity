#pragma once
#include <memory>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>
#include <Poco/Logger.h>
#include <common/types.h>

namespace DB
{
/// Sink which send data to ExchangeSource with LoadBalanceSelector.
class LoadBalancedExchangeSink : public ISink
{
public:
    class LoadBalanceSelector : private boost::noncopyable
    {
    public:
        explicit LoadBalanceSelector(size_t partition_num_) : partition_num(partition_num_) { }
        virtual size_t selectNext() = 0;
        virtual ~LoadBalanceSelector() = default;

    protected:
        size_t partition_num;
    };
    using LoadBalanceSelectorPtr = std::unique_ptr<LoadBalanceSelector>;

    explicit LoadBalancedExchangeSink(Block header_, BroadcastSenderPtrs senders_);
    virtual ~LoadBalancedExchangeSink() override;
    virtual String getName() const override { return "LoadBalancedExchangeSink"; }


protected:
    virtual void consume(Chunk) override;
    virtual void onFinish() override;
    virtual void onCancel() override;

private:
    Block header = getPort().getHeader();
    BroadcastSenderPtrs senders;
    LoadBalanceSelectorPtr partition_selector;
    Poco::Logger * logger;
    std::atomic<bool> was_finished = false;
};

}

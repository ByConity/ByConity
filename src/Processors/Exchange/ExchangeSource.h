#pragma once

#include <atomic>

#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Pipe.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Exchange/ExchangeOptions.h>


namespace DB
{
/// Read chunk from ExchangeSink.
class ExchangeSource : public SourceWithProgress
{
public:
    ExchangeSource(Block header_, BroadcastReceiverPtr receiver_, ExchangeOptions options_);
    ExchangeSource(Block header_, BroadcastReceiverPtr receiver_, ExchangeOptions options_, bool fetch_exception_from_scheduler_);
    ~ExchangeSource() override;

    IProcessor::Status prepare() override;
    String getName() const override;
    BroadcastReceiverPtr & getReceiver() { return receiver; }

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    BroadcastReceiverPtr receiver;
    ExchangeOptions options;
    bool fetch_exception_from_scheduler;
    std::atomic<bool> was_query_canceled {false};
    std::atomic<bool> was_receiver_finished {false};
    Poco::Logger * logger;
};

}

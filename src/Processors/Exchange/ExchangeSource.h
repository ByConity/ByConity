#pragma once

#include <atomic>

#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Pipe.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{
/// Read chunk from ExchangeSink.
class ExchangeSource : public SourceWithProgress
{
public:
    explicit ExchangeSource(Block header_, BroadcastReceiverPtr receiver_);
    ~ExchangeSource() override;

    IProcessor::Status prepare() override;
    String getName() const override { return "ExchangeSource"; }
    void onUpdatePorts() override;

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    BroadcastReceiverPtr receiver;
    std::atomic<bool> was_query_canceled = false;
    std::atomic<bool> was_receiver_finished = false;
};

}

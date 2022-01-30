#include <atomic>
#include <Common/Exception.h>
#include <common/logger_useful.h>

#include <Processors/Exchange/IExchangeSink.h>
#include <Processors/Exchange/DataTrans/IBroadcastSender.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>

namespace DB
{
IExchangeSink::IExchangeSink(Block header_) : ISink(std::move(header_))
{
}

void IExchangeSink::finish()
{
    is_finished.store(true, std::memory_order_relaxed);
}

IExchangeSink::Status IExchangeSink::prepare()
{
    if (is_finished.load(std::memory_order_relaxed))
    {
        onFinish();
        input.close();
        return Status::Finished;
    }

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        onFinish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

}

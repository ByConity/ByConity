#pragma once

#include <atomic>
#include <Processors/ISource.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Common/Stopwatch.h>

namespace DB
{

/// CyclicProductionSource will send empty chunk in the given interval
/// once the pipeline executor is cancelled, it will immediately cancel the generate
class CyclicProductionSource : public ISource
{
public:
    explicit CyclicProductionSource(Block header, uint64_t interval_ms_, size_t rows_)
        : ISource(std::move(header)), interval_ms(interval_ms_), rows(rows_)
    {
    }
    Chunk generate() override;
    std::optional<Chunk> tryGenerate() override;
    String getName() const override
    {
        return "CyclicProductionSource";
    }
    void onCancel() override
    {
        std::unique_lock<bthread::Mutex> lock;
        cond_var.notify_all();
    }

private:
    uint64_t interval_ms;
    size_t rows;
    Stopwatch timer;
    bthread::Mutex mutex;
    bthread::ConditionVariable cond_var;
};

}

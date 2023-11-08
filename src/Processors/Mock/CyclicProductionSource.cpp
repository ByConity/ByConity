#include <mutex>
#include <Columns/IColumn.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Processors/Mock/CyclicProductionSource.h>
#include <bthread/mutex.h>

namespace DB
{
Chunk CyclicProductionSource::generate()
{
    const auto & header = inputs.begin()->getHeader();
    Columns cols;
    for (size_t i = 0; i < header.getColumns().size(); i++)
    {
        auto col = header.getColumns()[i]->cloneResized(rows);
        cols.emplace_back(std::move(col));
    }
    return Chunk{std::move(cols), rows};
}

std::optional<Chunk> CyclicProductionSource::tryGenerate()
{
    auto elapsed = timer.elapsedMilliseconds();
    if (elapsed < interval_ms)
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        cond_var.wait_for(lock, std::chrono::milliseconds(interval_ms - elapsed), [&] { return isCancelled(); });
    }
    if (!isCancelled())
    {
        Chunk chunk = generate();
        timer.restart();
        return chunk;
    }
    return {};
}
}

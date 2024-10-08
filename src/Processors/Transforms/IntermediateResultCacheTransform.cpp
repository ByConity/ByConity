#include <Processors/IntermediateResult/CacheManager.h>
#include <Processors/Transforms/IntermediateResultCacheTransform.h>
#include <common/logger_useful.h>

using namespace DB::IntermediateResult;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IntermediateResultCacheTransform::IntermediateResultCacheTransform(
    const Block & header_,
    IntermediateResultCachePtr cache_,
    CacheParam & cache_param_,
    UInt64 cache_max_bytes_,
    UInt64 cache_max_rows_,
    CacheHolderPtr cache_holder_)
    : ISimpleTransform(header_, header_, false)
    , cache(std::move(cache_))
    , cache_param(cache_param_)
    , cache_max_bytes(cache_max_bytes_)
    , cache_max_rows(cache_max_rows_)
    , cache_holder(std::move(cache_holder_))
    , log(getLogger("IntermediateResultCacheTransform"))
{
}

IProcessor::Status IntermediateResultCacheTransform::prepare()
{
    if (cache_holder->all_part_in_cache)
        stopReading();

    if (!cache_holder->early_finish && output.isFinished() && !input.isFinished())
    {
        cache_holder->early_finish = true;
        LOG_DEBUG(log, "Cache {} generate was early finish", cache_param.digest);
    }

    return ISimpleTransform::prepare();
}

void IntermediateResultCacheTransform::work()
{
    ISimpleTransform::work();
}

void IntermediateResultCacheTransform::transform(DB::Chunk & chunk)
{
    if (chunk.empty())
        return;

    const auto & owner_info = chunk.getOwnerInfo();
    CacheValuePtr value;
    if (!owner_info.empty())
    {
        CacheKey key{cache_param.digest, cache_param.cached_table.getFullTableName(), owner_info};
        if (!cache_holder->write_cache.contains(key))
            return;

        auto it = uncompleted_cache.find(key);
        if (it != uncompleted_cache.end())
            value = it->second;
        else
        {
            value = cache->tryGetUncompletedCache(key);
            uncompleted_cache.emplace(key, value);
        }

        if (!value)
            return;

        if ((cache_max_bytes < value->getBytes() + chunk.bytes()) || (cache_max_rows < value->getRows() + chunk.getNumRows()))
        {
            cache->modifyKeyStateToRefused(key);
            uncompleted_cache.emplace(key, nullptr);
            LOG_INFO(
                log,
                "Key:{} was refused because it was too large. (bytes,rows)->({},{})",
                key.toString(),
                value->getBytes() + chunk.bytes(),
                value->getRows() + chunk.getNumRows());
        }
    }

    auto cache_chunk = chunk.clone();
    size_t num_columns = cache_chunk.getNumColumns();
    size_t num_rows = cache_chunk.getNumRows();
    auto output_columns = cache_chunk.detachColumns();
    Columns cache_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        cache_columns[cache_param.output_pos_to_cache_pos[i]] = std::move(output_columns[i]);
    cache_chunk.setColumns(std::move(cache_columns), num_rows);

    if (value)
        value->addChunk(cache_chunk);
    else
    {
        CacheKey key{cache_param.digest, cache_param.cached_table.getFullTableName()};
        cache->emplaceCacheForEmptyResult(key, cache_chunk);
    }
}

}

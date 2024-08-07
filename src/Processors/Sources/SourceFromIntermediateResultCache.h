#pragma once
#include <Processors/ISource.h>
#include <Processors/IntermediateResult/CacheManager.h>

namespace DB
{

class SourceFromIntermediateResultCache : public ISource
{
public:
    SourceFromIntermediateResultCache(
        const Block & header,
        const std::list<IntermediateResult::CacheValuePtr> & values_,
        std::unordered_map<size_t, size_t> cache_pos_to_output_pos_)
        : ISource(header), values(values_), cache_pos_to_output_pos(std::move(cache_pos_to_output_pos_))
    {
    }

    String getName() const override
    {
        return "SourceFromIntermediateResultCache";
    }

protected:
    Chunk generate() override
    {
        while (!values.empty())
        {
            auto chunk = values.front()->getChunk();
            if (!chunk.empty())
            {
                size_t num_columns = chunk.getNumColumns();
                size_t num_rows = chunk.getNumRows();
                auto cache_columns = chunk.detachColumns();
                Columns output_columns(num_columns);
                for (size_t i = 0; i < num_columns; ++i)
                    output_columns[cache_pos_to_output_pos[i]] = std::move(cache_columns[i]);
                chunk.setColumns(std::move(output_columns), num_rows);
                return chunk;
            }
            else
                values.pop_front();
        }
        return Chunk();
    }

private:
    std::list<IntermediateResult::CacheValuePtr> values;
    std::unordered_map<size_t, size_t> cache_pos_to_output_pos;
};

}

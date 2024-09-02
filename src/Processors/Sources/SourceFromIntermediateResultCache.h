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
                auto columns = chunk.detachColumns();
                for (size_t i = 0; i < num_columns; ++i)
                    chunk.addColumn(std::move(columns[cache_pos_to_output_pos[i]]));
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

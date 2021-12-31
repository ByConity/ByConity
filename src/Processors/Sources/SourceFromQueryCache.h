#pragma once
#include <Processors/ISource.h>
#include <Processors/QueryCache.h>

namespace DB
{

class SourceFromQueryCache : public ISource
{
public:
    SourceFromQueryCache(const Block & header,
                         const QueryCachePtr & query_cache_,
                         const QueryKeyPtr & query_key_,
                         const QueryResultPtr & query_result_)
        : ISource(std::move(header)),
            query_cache(query_cache_), query_key(query_key_), query_result(query_result_) {}

    String getName() const override { return "SourceFromQueryCache"; }

protected:
    Chunk generate() override
    {
        if (query_result && query_result->result)
        {
            if (!query_end)
            {
                query_end = true;
                return Chunk(query_result->result.getColumns(), query_result->result.getNumRows());
            }
        }

        return Chunk();
    }

private:
    QueryCachePtr query_cache = nullptr;
    QueryKeyPtr query_key = nullptr;
    QueryResultPtr query_result = nullptr;
    bool query_end = false;
};

}

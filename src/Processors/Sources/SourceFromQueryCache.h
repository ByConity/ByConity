#pragma once
#include <Processors/ISource.h>
#include <Processors/QueryCache.h>

namespace DB
{

class SourceFromQueryCache : public ISource
{
public:
    SourceFromQueryCache(const Block & header,
                         const QueryResultPtr & query_result_)
        : ISource(std::move(header)), query_result(query_result_) {}

    String getName() const override { return "SourceFromQueryCache"; }

protected:
    Chunk generate() override
    {
        if (query_result)
            return query_result->getChunk();

        return Chunk();
    }

private:
    QueryResultPtr query_result = nullptr;
};

}

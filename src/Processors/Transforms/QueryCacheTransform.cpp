#include <Processors/Transforms/QueryCacheTransform.h>
#include <Common/PODArray.h>

namespace DB
{

QueryCacheTransform::QueryCacheTransform(const Block & header,
                                         const QueryCachePtr & query_cache_,
                                         const QueryKeyPtr & query_key_,
                                         const QueryResultPtr & query_result_,
                                         const std::set<String> & ref_db_and_table_)
    : ISimpleTransform(header, header, false),
    query_cache(query_cache_), query_key(query_key_), query_result(query_result_), ref_db_and_table(ref_db_and_table_)
{

}

QueryCacheTransform::~QueryCacheTransform()
{
    setQueryCache();
}

void QueryCacheTransform::setQueryCache()
{
    // How to update cache:
    // 1. Each database:table pair can have multiple queries
    // 2. Each query can be referenced by multiple database:table pair
    // Thus, we insert database:table and key for multiple times

    if (!isCancelled() && query_cache && query_key && query_result)
    {
        UInt128 key = QueryCache::hash(*query_key);

        // cache query only when it has reference tables, otherwise we cannot drop this query
        if (!ref_db_and_table.empty())
            query_cache->set(key, query_result);

        for (const auto & name : ref_db_and_table)
            query_cache->insert(name, key);
    }
}

void QueryCacheTransform::transform(Chunk & chunk)
{
    if (!query_result)
        return;

    query_result->addResult(chunk);
}

}

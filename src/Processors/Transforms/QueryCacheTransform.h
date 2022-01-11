#pragma once
#include <Processors/ISimpleTransform.h>
#include <Common/HashTable/HashMap.h>
#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryCache.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>


namespace DB
{

class QueryCacheTransform : public ISimpleTransform
{
public:
    QueryCacheTransform(const Block & header,
                        const QueryCachePtr & query_cache_,
                        const UInt128 & query_key_,
                        const QueryResultPtr & query_result_,
                        const std::set<String> & ref_db_and_table_,
                        UInt64 update_time_);

    ~QueryCacheTransform() override;

    String getName() const override { return "QueryCacheTransform"; }

    void setQueryCache();

protected:
    void transform(Chunk & chunk) override;

private:
    QueryCachePtr query_cache = nullptr;
    UInt128 query_key;
    QueryResultPtr query_result = nullptr;
    std::set<String> ref_db_and_table;
    UInt64 update_time;

};

}

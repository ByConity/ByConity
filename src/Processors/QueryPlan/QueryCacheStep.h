#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/Transforms/QueryCacheTransform.h>
#include <Processors/QueryCache.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

class QueryCacheStep : public IQueryPlanStep
{
public:
    QueryCacheStep(const DataStream & input_stream_,
                            const ASTPtr & query_ptr_,
                            const ContextPtr & context_,
                            QueryProcessingStage::Enum stage = QueryProcessingStage::Complete);

    String getName() const override { return "QueryCache"; }

    Type getType() const override { return Type::QueryCache; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &);
    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &);

    void checkDeterministic(const ASTPtr & node);
    bool isViableQuery();

    template <typename T> bool analyzeQuery();
    bool canDropQueryCache();
    bool needDropCache() const { return can_drop_cache; }
    void dropCache();

    bool hitCache() const { return hit_query_cache; }

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);

private:
    Processors processors;

    const ASTPtr & query_ptr;
    const ContextPtr context;

    QueryCachePtr query_cache = nullptr;
    QueryKeyPtr query_key = nullptr;
    QueryResultPtr query_result = nullptr;

    std::set<String> ref_db_and_table;

    bool is_deterministic = true;
    bool can_drop_cache = false;
    bool hit_query_cache = false;
    QueryProcessingStage::Enum stage;

    void init();

    void updateRefDatabaseAndTable(const String & database, const String & table)
    {
        ref_db_and_table.insert(database + "." + table);
    }
};

}

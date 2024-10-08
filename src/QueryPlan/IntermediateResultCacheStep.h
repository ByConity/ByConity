#pragma once
#include <Common/Logger.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterBuilder.h>
#include <Optimizer/IntermediateResult/CacheParam.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>

#include <string>
#include <unordered_map>

namespace Poco { class Logger; }

namespace DB
{

namespace IntermediateResult { struct CacheHolder; }
using CacheHolderPtr = std::shared_ptr<IntermediateResult::CacheHolder>;

class IntermediateResultCacheStep : public IQueryPlanStep
{
public:
    IntermediateResultCacheStep(const DataStream & input_stream_, CacheParam cache_param_, Aggregator::Params aggregator_params_);

    String getName() const override
    {
        return "IntermediateResultCache";
    }

    Type getType() const override
    {
        return Type::IntermediateResultCache;
    }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & build_settings) override;

    void updateInputStream(DataStream input_stream_);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    CacheParam getCacheParam() const { return cache_param; }
    Aggregator::Params getAggregatorParams() const { return aggregator_params; }

    // debug info below, already included in hash
    void setIncludedRuntimeFilters(std::unordered_set<RuntimeFilterId> ids) { included_runtime_filters = std::move(ids); }
    void setIgnoredRuntimeFilters(std::unordered_set<RuntimeFilterId> ids) { ignored_runtime_filters = std::move(ids); }
    void setCacheOrder(Block header) { cache_order = std::move(header); }
    const std::unordered_set<RuntimeFilterId> & getIncludedRuntimeFilters() const { return included_runtime_filters; }
    const std::unordered_set<RuntimeFilterId> & getIgnoredRuntimeFilters() const { return ignored_runtime_filters; }
    const Block & getCacheOrder() const { return cache_order; }

    void toProto(Protos::IntermediateResultCacheStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<IntermediateResultCacheStep> fromProto(const Protos::IntermediateResultCacheStep & proto, ContextPtr context);


private:
    QueryPipelinePtr processCacheTransform(QueryPipelines & pipelines, const BuildQueryPipelineSettings & build_settings, CacheHolderPtr cache_holder);

    CacheParam cache_param;
    Aggregator::Params aggregator_params;

    // debug info, already included in hash
    std::unordered_set<RuntimeFilterId> ignored_runtime_filters;
    std::unordered_set<RuntimeFilterId> included_runtime_filters;
    Block cache_order;
    LoggerPtr log;
};

}

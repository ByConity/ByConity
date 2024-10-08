#pragma once

#include <Common/Logger.h>
#include <Optimizer/IntermediateResult/CacheParam.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/IntermediateResult/CacheManager.h>

namespace Poco { class Logger; }

namespace DB
{

class IntermediateResultCacheTransform : public ISimpleTransform
{
private:
    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;
    };

public:
    IntermediateResultCacheTransform(
        const Block & header_,
        IntermediateResultCachePtr cache_,
        CacheParam & cache_param_,
        UInt64 cache_max_bytes_,
        UInt64 cache_max_rows_,
        CacheHolderPtr cache_holder_);

    String getName() const override
    {
        return "IntermediateResultCache";
    }

    Status prepare() override;
    void work() override;
    void transform(DB::Chunk & chunk) override;

private:
    IntermediateResultCachePtr cache;
    CacheParam cache_param;
    UInt64 cache_max_bytes = 0;
    UInt64 cache_max_rows = 0;
    CacheHolderPtr cache_holder;
    std::unordered_map<IntermediateResult::CacheKey, IntermediateResult::CacheValuePtr> uncompleted_cache;
    LoggerPtr log;
};

}

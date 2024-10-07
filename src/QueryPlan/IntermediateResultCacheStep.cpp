#include <Databases/DatabaseOnDisk.h>
#include <IO/Operators.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Sources/SourceFromIntermediateResultCache.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/IntermediateResultCacheTransform.h>
#include <QueryPlan/IntermediateResultCacheStep.h>
#include <common/logger_useful.h>

namespace DB
{

IntermediateResultCacheStep::IntermediateResultCacheStep(
    const DataStream & input_stream_, CacheParam cache_param_, Aggregator::Params aggregator_params_)
    : cache_param(std::move(cache_param_))
    , aggregator_params(std::move(aggregator_params_))
    , log(getLogger("IntermediateResultCacheStep"))
{
    input_streams.emplace_back(input_stream_);
    Block output;
    output = input_streams[0].header;
    output_stream = DataStream{output};
}

void IntermediateResultCacheStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void IntermediateResultCacheStep::updateInputStream(DataStream input_stream_)
{
    input_streams.clear();
    input_streams.emplace_back(std::move(input_stream_));
}

QueryPipelinePtr IntermediateResultCacheStep::processCacheTransform(
    QueryPipelines & pipelines, const BuildQueryPipelineSettings & build_settings, CacheHolderPtr cache_holder)
{
    const auto context = build_settings.context;
    auto cache = context->getIntermediateResultCache();
    if (!cache)
        return std::move(pipelines[0]);

    LOG_DEBUG(
        log,
        "process cache transform for digest:{}, write:{}, read:{}, all_part_in_cache:{}",
        cache_param.digest,
        cache_holder->write_cache.size(),
        cache_holder->read_cache.size(),
        cache_holder->all_part_in_cache);

    const auto & settings = build_settings.context->getSettingsRef();
    // write cache or skip pipeline
    if (!cache_holder->write_cache.empty() || cache_holder->all_part_in_cache)
    {
        pipelines[0]->addSimpleTransform([&](const Block & header) {
            auto cache_max_bytes = settings.intermediate_result_cache_max_bytes;
            auto cache_max_rows = settings.intermediate_result_cache_max_rows;
            return std::make_shared<IntermediateResultCacheTransform>(
                header, cache, cache_param, cache_max_bytes, cache_max_rows, cache_holder);
        });
    }

    // read cache
    QueryPipelinePtr read_pipeline;
    if (!cache_holder->read_cache.empty())
    {
        size_t read_cache_size = cache_holder->read_cache.size();
        size_t max_streams = std::min(size_t(settings.max_threads), read_cache_size);
        size_t read_cache_per_source = read_cache_size / max_streams;
        size_t read_cache_remain = read_cache_size % max_streams;
        auto iter = cache_holder->read_cache.begin();
        Pipes pipes;
        for (size_t i = 0; i < max_streams; ++i)
        {
            std::list<IntermediateResult::CacheValuePtr> cache_results;
            size_t per_source = read_cache_per_source;
            if (i < read_cache_remain)
                per_source++;
            for (size_t j = 0; j < per_source; j++, iter++)
                cache_results.emplace_back(std::move(iter->second));

            auto source = std::make_shared<SourceFromIntermediateResultCache>(
                getOutputStream().header, cache_results, cache_param.cache_pos_to_output_pos);
            pipes.emplace_back(std::move(source));
        }
        read_pipeline = std::make_unique<QueryPipeline>();
        read_pipeline->init(Pipe::unitePipes(std::move(pipes)));
    }

    // unite
    QueryPipelinePtr unite_pipeline;
    if (read_pipeline)
    {
        pipelines.emplace_back(std::move(read_pipeline));
        unite_pipeline = std::make_unique<QueryPipeline>();
        *unite_pipeline = QueryPipeline::unitePipelines(std::move(pipelines), settings.max_threads);
    }
    else
        unite_pipeline = std::move(pipelines[0]);

    if (!settings.enable_intermediate_result_cache_streaming)
    {
        Aggregator::Params new_params(
            aggregator_params.src_header,
            aggregator_params.keys,
            aggregator_params.aggregates,
            aggregator_params.overflow_row,
            settings.max_rows_to_group_by,
            settings.group_by_overflow_mode,
            settings.group_by_two_level_threshold,
            settings.group_by_two_level_threshold_bytes,
            settings.max_bytes_before_external_group_by,
            settings.spill_mode == SpillMode::AUTO,
            settings.spill_buffer_bytes_before_external_group_by,
            settings.empty_result_for_aggregation_by_empty_set,
            context->getTemporaryVolume(),
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            settings.compile_expressions,
            settings.min_count_to_compile_aggregate_expression,
            aggregator_params.intermediate_header,
            settings.enable_lc_group_by_opt);

        auto transform_params = std::make_shared<AggregatingTransformParams>(new_params, false);
        transform_params->only_merge = true;

        auto merge_threads = settings.max_threads;
        auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
            ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
            : static_cast<size_t>(settings.max_threads);

        auto many_data = std::make_shared<ManyAggregatedData>(unite_pipeline->getNumStreams());
        size_t counter = 0;
        unite_pipeline->addSimpleTransform([&](const Block & header) {
            return std::make_shared<AggregatingTransform>(
                header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });
    }

    return unite_pipeline;
}

QueryPipelinePtr IntermediateResultCacheStep::updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & build_settings)
{
    auto cache_holder = pipelines[0]->getCacheHolder();
    if (!cache_holder || cache_holder->all_part_in_storage)
        return std::move(pipelines[0]);

    return processCacheTransform(pipelines, build_settings, cache_holder);
}

void IntermediateResultCacheStep::toProto(Protos::IntermediateResultCacheStep & proto, [[maybe_unused]] bool for_hash_equals) const
{
    proto.set_step_description(step_description);
    input_streams[0].toProto(*proto.mutable_input_stream());
    cache_param.toProto(*proto.mutable_cache_param());
    aggregator_params.toProto(*proto.mutable_aggregator_params());
}

std::shared_ptr<IntermediateResultCacheStep>
IntermediateResultCacheStep::fromProto(const Protos::IntermediateResultCacheStep & proto, ContextPtr context)
{
    const auto & step_description = proto.step_description();
    DataStream input_stream;
    input_stream.fillFromProto(proto.input_stream());
    CacheParam cache_param;
    cache_param.fillFromProto(proto.cache_param());
    auto aggregator_params = Aggregator::Params::fromProto(proto.aggregator_params(), context);

    auto step = std::make_shared<IntermediateResultCacheStep>(input_stream, cache_param, aggregator_params);
    step->setStepDescription(step_description);
    return step;
}

std::shared_ptr<IQueryPlanStep> IntermediateResultCacheStep::copy(ContextPtr) const
{
    return std::make_shared<IntermediateResultCacheStep>(input_streams[0], cache_param, aggregator_params);
}

}

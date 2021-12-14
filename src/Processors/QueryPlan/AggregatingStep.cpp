#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/QueryPlan/PlanSerDerHelper.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/FinishAggregatingInOrderTransform.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    size_t max_block_size_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_,
    InputOrderInfoPtr group_by_info_,
    SortDescription group_by_sort_description_)
    : ITransformingStep(input_stream_, params_.getHeader(final_), getTraits(), false)
    , params(std::move(params_))
    , final(std::move(final_))
    , max_block_size(max_block_size_)
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , group_by_info(std::move(group_by_info_))
    , group_by_sort_description(std::move(group_by_sort_description_))
{
}

void AggregatingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final);

    if (group_by_info)
    {
        bool need_finish_sorting = (group_by_info->order_key_prefix_descr.size() < group_by_sort_description.size());

        if (need_finish_sorting)
        {
            /// TOO SLOW
        }
        else
        {
            if (pipeline.getNumStreams() > 1)
            {
                auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());
                size_t counter = 0;
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(header, transform_params, group_by_sort_description, max_block_size, many_data, counter++);
                });

                aggregating_in_order = collector.detachProcessors(0);

                for (auto & column_description : group_by_sort_description)
                {
                    if (!column_description.column_name.empty())
                    {
                        column_description.column_number = pipeline.getHeader().getPositionByName(column_description.column_name);
                        column_description.column_name.clear();
                    }
                }

                auto transform = std::make_shared<FinishAggregatingInOrderTransform>(
                    pipeline.getHeader(),
                    pipeline.getNumStreams(),
                    transform_params,
                    group_by_sort_description,
                    max_block_size);

                pipeline.addTransform(std::move(transform));
                aggregating_sorted = collector.detachProcessors(1);
            }
            else
            {
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<AggregatingInOrderTransform>(header, transform_params, group_by_sort_description, max_block_size);
                });

                aggregating_in_order = collector.detachProcessors(0);
            }

            pipeline.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FinalizingSimpleTransform>(header, transform_params);
            });

            finalizing = collector.detachProcessors(2);
            return;
        }
    }

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });

        pipeline.resize(1);

        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AggregatingTransform>(header, transform_params);
        });

        aggregating = collector.detachProcessors(0);
    }
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    if (!aggregating.empty())
        IQueryPlanStep::describePipeline(aggregating, settings);
    else
    {
        /// Processors are printed in reverse order.
        IQueryPlanStep::describePipeline(finalizing, settings);
        IQueryPlanStep::describePipeline(aggregating_sorted, settings);
        IQueryPlanStep::describePipeline(aggregating_in_order, settings);
    }
}

void AggregatingStep::serialize(WriteBuffer & buf) const
{
    serializeDataStream(input_streams[0], buf);
    params.serialize(buf);
    writeBinary(final, buf);
    writeBinary(max_block_size, buf);
    writeBinary(merge_threads, buf);
    writeBinary(temporary_data_merge_threads, buf);
    writeBinary(storage_has_evenly_distributed_read, buf);

    if (group_by_info)
    {
        writeBinary(true, buf);
        group_by_info->serialize(buf);
    }
    else
        writeBinary(false, buf);

    serializeSortDescription(group_by_sort_description, buf);
}

QueryPlanStepPtr AggregatingStep::deserialize(ReadBuffer & buf, ContextPtr context)
{
    DataStream input_stream = deserializeDataStream(buf);
    Aggregator::Params params = Aggregator::Params::deserialize(buf, context);
    bool final;
    readBinary(final, buf);
    size_t max_block_size;
    readBinary(max_block_size, buf);
    size_t merge_threads;
    readBinary(merge_threads, buf);
    size_t temporary_data_merge_threads;
    readBinary(temporary_data_merge_threads, buf);
    bool storage_has_evenly_distributed_read;
    readBinary(storage_has_evenly_distributed_read, buf);

    bool has_group_by_info = false;
    readBinary(has_group_by_info, buf);
    InputOrderInfoPtr group_by_info = nullptr;
    if (has_group_by_info)
        const_cast<InputOrderInfo &>(*group_by_info).deserialize(buf);

    SortDescription group_by_sort_description;
    deserializeSortDescription(group_by_sort_description, buf);

    return std::make_unique<AggregatingStep>(input_stream, params, final, max_block_size,
                                             merge_threads, temporary_data_merge_threads,
                                             storage_has_evenly_distributed_read,
                                             group_by_info, group_by_sort_description);
}

}

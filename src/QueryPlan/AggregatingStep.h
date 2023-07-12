/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;
class WriteBuffer;
class ReadBuffer;

bool hasNonParallelAggregateFunctions(const AggregateDescriptions &);

struct GroupingSetsParams
{
    GroupingSetsParams() = default;

    GroupingSetsParams(Names used_key_names_) : used_key_names(std::move(used_key_names_)) { }

    GroupingSetsParams(ColumnNumbers used_keys_, ColumnNumbers missing_keys_)
        : used_keys(std::move(used_keys_)), missing_keys(std::move(missing_keys_))
    {
    }

    Names used_key_names;

    ColumnNumbers used_keys;
    ColumnNumbers missing_keys;
};

using GroupingSetsParamsList = std::vector<GroupingSetsParams>;

struct GroupingDescription
{
    Names argument_names;
    String output_name;
};

using GroupingDescriptions = std::vector<GroupingDescription>;

Block appendGroupingSetColumn(Block header);

void computeGroupingFunctions(
    QueryPipeline & pipeline,
    const GroupingDescriptions & groupings,
    const Names & keys,
    const GroupingSetsParamsList & grouping_set_params,
    const BuildQueryPipelineSettings & build_settings);

/// Aggregation. See AggregatingTransform.
class AggregatingStep : public ITransformingStep
{
public:
    AggregatingStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescription group_by_sort_description_,
        bool should_produce_results_in_order_of_bucket_number_)
        : AggregatingStep(
            input_stream_,
            Names(),
            std::move(params_),
            std::move(grouping_sets_params_),
            final_,
            max_block_size_,
            merge_threads_,
            temporary_data_merge_threads_,
            storage_has_evenly_distributed_read_,
            std::move(group_by_info_),
            std::move(group_by_sort_description_),
            {},
            false,
            should_produce_results_in_order_of_bucket_number_)
    {
    }

    AggregatingStep(
        const DataStream & input_stream_,
        Names keys_,
        AggregateDescriptions aggregates_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        SortDescription group_by_sort_description_ = {},
        GroupingDescriptions groupings_ = {},
        bool /*totals_*/ = false,
        bool should_produce_results_in_order_of_bucket_number_ = false)
        : AggregatingStep(
            input_stream_,
            keys_,
            createParams(input_stream_.header, aggregates_, keys_),
            std::move(grouping_sets_params_),
            final_,
            0,
            0,
            0,
            false,
            nullptr,
            group_by_sort_description_,
            groupings_,
            false,
            should_produce_results_in_order_of_bucket_number_)
    {
    }


    AggregatingStep(
        const DataStream & input_stream_,
        Names keys_,
        Aggregator::Params params_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescription group_by_sort_description_,
        GroupingDescriptions groupings_ = {},
        bool totals_ = false,
        bool should_produce_results_in_order_of_bucket_number = true);

    String getName() const override { return "Aggregating"; }

    Type getType() const override { return Type::Aggregating; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }
    const AggregateDescriptions & getAggregates() const { return params.aggregates; }
    const Names & getKeys() const { return keys; }
    const GroupingSetsParamsList & getGroupingSetsParams() const { return grouping_sets_params; }
    const SortDescription & getGroupBySortDescription() const { return group_by_sort_description; }
    void setGroupBySortDescription(const SortDescription & group_by_sort_description_)
    {
        group_by_sort_description = group_by_sort_description_;
    }
    bool isFinal() const { return final; }
    bool isPartial() const { return !final; }
    bool isGroupingSet() const { return !grouping_sets_params.empty(); }
    const GroupingDescriptions & getGroupings() const { return groupings; }
    bool shouldProduceResultsInOrderOfBucketNumber() const { return should_produce_results_in_order_of_bucket_number; }

    bool isNormal() const { return final && !isGroupingSet() /*&& !totals && !having*/ && groupings.empty(); }

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;
    static Aggregator::Params createParams(Block header_before_aggregation, AggregateDescriptions aggregates, Names group_by_keys);
    GroupingSetsParamsList prepareGroupingSetsParams() const;

private:
    Poco::Logger * log = &Poco::Logger::get("TableScanStep");
    Names keys;
    Aggregator::Params params;
    GroupingSetsParamsList grouping_sets_params;
    bool final;

    size_t max_block_size;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    bool storage_has_evenly_distributed_read;

    InputOrderInfoPtr group_by_info;
    SortDescription group_by_sort_description;

    GroupingDescriptions groupings;
    /// It determines if we should resize pipeline to 1 at the end.
    /// Needed in case of distributed memory efficient aggregation over distributed table.
    /// Specifically, if there is a further MergingAggregatedStep and
    /// distributed_aggregation_memory_efficient=true
    /// then the pipeline should not be resized to > 1; otherwise,
    /// the data passed to GroupingAggregatedTransform are not in bucket order -> error.
    /// Set as to_stage==WithMergeableState && distributed_aggregation_memory_efficient
    /// which is equivalent to !final_ && && distributed_aggregation_memory_efficient
    /// distributed_aggregation_memory_efficient is not available inside this class,
    /// therefore, this variable is passed to the constructor.
    /// if the condition is unkown, the safe options is to set it to true to avoid errors
    /// therefore the default value is true in the constructor
    const bool should_produce_results_in_order_of_bucket_number;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

    Processors aggregating;
};

}

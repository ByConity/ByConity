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
#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context_fwd.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Core/Names.h>
#include <Processors/Transforms/AggregatingStreamingTransform.h>

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

public:
    void toProto(Protos::GroupingSetsParams & proto) const;
    void fillFromProto(const Protos::GroupingSetsParams & proto);
};

using GroupingSetsParamsList = std::vector<GroupingSetsParams>;

struct GroupingDescription
{
    Names argument_names;
    String output_name;

public:
    void toProto(Protos::GroupingDescription & proto) const;
    void fillFromProto(const Protos::GroupingDescription & proto);
};

using GroupingDescriptions = std::vector<GroupingDescription>;

/// AggregateStagePolicy represents partition requirements for aggregate node
///
/// 1 single, means insert a gather exchange node before aggregate node,
/// 2 perfect_shard, means the aggregate node match any partition requirement.
/// 3 merge_perfect_shard, needs insert a gather exchange before aggregate.
/// 4 default, in this case, use property enforcement to derive required exchange.
enum class AggregateStagePolicy : UInt8
{
    SINGLE = 0,
    PERFECT_SHARD,
    MERGE_PERFECT_SHARD,
    DEFAULT,
    STATE,
    MERGE
};

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
        const NameSet & keys_not_hashed_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        AggregateStagePolicy stage_policy_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescription group_by_sort_description_,
        bool should_produce_results_in_order_of_bucket_number_,
        bool no_shuffle_ = false,
        PlanHints hints_ = {})
        : AggregatingStep(
            input_stream_,
            Names(),
            keys_not_hashed_,
            std::move(params_),
            std::move(grouping_sets_params_),
            final_,
            stage_policy_,
            max_block_size_,
            merge_threads_,
            temporary_data_merge_threads_,
            storage_has_evenly_distributed_read_,
            std::move(group_by_info_),
            std::move(group_by_sort_description_),
            {},
            false,
            should_produce_results_in_order_of_bucket_number_,
            no_shuffle_,
            false,
            hints_)
    {
    }

    AggregatingStep(
        const DataStream & input_stream_,
        Names keys_,
        const NameSet & keys_not_hashed_,
        AggregateDescriptions aggregates_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        AggregateStagePolicy stage_policy_ = AggregateStagePolicy::DEFAULT,
        SortDescription group_by_sort_description_ = {},
        GroupingDescriptions groupings_ = {},
        bool overflow_row_ = false,
        bool should_produce_results_in_order_of_bucket_number_ = false,
        bool no_shuffle_ = false,
        bool streaming_for_cache_ = false,
        PlanHints hints_ = {})
        : AggregatingStep(
            input_stream_,
            keys_,
            keys_not_hashed_,
            createParams(input_stream_.header, aggregates_, keys_, overflow_row_),
            std::move(grouping_sets_params_),
            final_,
            stage_policy_,
            0,
            0,
            0,
            false,
            nullptr,
            group_by_sort_description_,
            groupings_,
            false,
            should_produce_results_in_order_of_bucket_number_,
            no_shuffle_,
            streaming_for_cache_,
            hints_)
    {
    }


    AggregatingStep(
        const DataStream & input_stream_,
        Names keys_,
        const NameSet & keys_not_hashed_,
        Aggregator::Params params_,
        GroupingSetsParamsList grouping_sets_params_,
        bool final_,
        AggregateStagePolicy stage_policy_,
        size_t max_block_size_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_,
        InputOrderInfoPtr group_by_info_,
        SortDescription group_by_sort_description_,
        GroupingDescriptions groupings_ = {},
        bool totals_ = false,
        bool should_produce_results_in_order_of_bucket_number = true,
        bool no_shuffle_ = false,
        bool streaming_for_cache_ = false,
        PlanHints hints_ = {});

    String getName() const override { return "Aggregating"; }

    Type getType() const override { return Type::Aggregating; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }
    const AggregateDescriptions & getAggregates() const { return params.aggregates; }
    const Names & getKeys() const { return keys; }
    const NameSet & getKeysNotHashed() const { return keys_not_hashed; }
    const GroupingSetsParamsList & getGroupingSetsParams() const { return grouping_sets_params; }
    const SortDescription & getGroupBySortDescription() const { return group_by_sort_description; }
    void setGroupBySortDescription(const SortDescription & group_by_sort_description_)
    {
        group_by_sort_description = group_by_sort_description_;
    }
    bool isFinal() const { return final; }
    bool isStreamingForCache() const
    {
        return streaming_for_cache;
    }
    void setStreamingForCache(bool streaming_for_cache_)
    {
        streaming_for_cache = streaming_for_cache_;
    }
    
    bool isPartial() const { return !final; }
    bool isGroupingSet() const { return !grouping_sets_params.empty(); }
    bool isNoShuffle() const
    {
        return no_shuffle;
    }
    void setNoShuffle(bool no_shuffle_)
    {
        no_shuffle = no_shuffle_;
    }

    const GroupingDescriptions & getGroupings() const { return groupings; }
    bool shouldProduceResultsInOrderOfBucketNumber() const { return should_produce_results_in_order_of_bucket_number; }
    void setShouldProduceResultsInOrderOfBucketNumber(bool value) { should_produce_results_in_order_of_bucket_number = value; }
    bool needOverflowRow() const
    {
        return params.overflow_row;
    }
    bool isNormal() const { return final && !isGroupingSet() /*&& !totals && !having*/ && groupings.empty(); }

    AggregateStagePolicy getStagePolicy() const { return stage_policy; }
    void setStagePolicy (AggregateStagePolicy policy){ stage_policy = policy; }

    void toProto(Protos::AggregatingStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<AggregatingStep> fromProto(const Protos::AggregatingStep & proto, ContextPtr context);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;
    static Aggregator::Params
    createParams(Block header_before_aggregation, AggregateDescriptions aggregates, Names group_by_keys, bool overflow_row);
    GroupingSetsParamsList prepareGroupingSetsParams() const;

private:
    LoggerPtr log = getLogger("TableScanStep");
    Names keys;

    NameSet keys_not_hashed; // keys which can be output directly, same as function `any`, but no type loss.

    Aggregator::Params params;
    GroupingSetsParamsList grouping_sets_params;
    bool final;

    /// stage_policy field doesn't need be serialize/deserialize
    AggregateStagePolicy stage_policy;

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
    bool should_produce_results_in_order_of_bucket_number;
    bool streaming_for_cache = false;

    // for bitengine sqls
    bool no_shuffle;

    Processors aggregating_in_order;
    Processors aggregating_sorted;
    Processors finalizing;

    Processors aggregating;
};

}

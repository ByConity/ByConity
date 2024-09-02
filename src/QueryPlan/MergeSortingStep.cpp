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

#include <QueryPlan/MergeSortingStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include "Core/SettingsEnums.h"
#include <Interpreters/Context.h>

namespace DB
{

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

MergeSortingStep::MergeSortingStep(
    const DataStream & input_stream_,
    const SortDescription & description_,
    size_t max_merged_block_size_,
    UInt64 limit_,
    size_t max_bytes_before_remerge_,
    double remerge_lowered_memory_bytes_ratio_,
    size_t max_bytes_before_external_sort_,
    VolumePtr tmp_volume_,
    size_t min_free_disk_space_,
    bool enable_adaptive_spill_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
    , max_bytes_before_remerge(max_bytes_before_remerge_)
    , remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio_)
    , max_bytes_before_external_sort(max_bytes_before_external_sort_), tmp_volume(tmp_volume_)
    , min_free_disk_space(min_free_disk_space_)
    , enable_adaptive_spill(enable_adaptive_spill_)
{
    /// TODO: check input_stream is partially sorted by the same description.
    output_stream->sort_description = description;
    output_stream->sort_mode = input_stream_.has_single_port ? DataStream::SortMode::Stream
                                                            : DataStream::SortMode::Port;
}

void MergeSortingStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void MergeSortingStep::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void MergeSortingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    max_merged_block_size = settings.context->getSettingsRef().external_sort_max_block_size;
    max_bytes_before_remerge = settings.context->getSettingsRef().max_bytes_before_remerge_sort;
    remerge_lowered_memory_bytes_ratio = settings.context->getSettingsRef().remerge_sort_lowered_memory_bytes_ratio;
    max_bytes_before_external_sort = settings.context->getSettingsRef().max_bytes_before_external_sort;
    tmp_volume = settings.context->getTemporaryVolume();
    min_free_disk_space = settings.context->getSettingsRef().min_free_disk_space_for_temporary_data;

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;
        enable_adaptive_spill = settings.context->getSettingsRef().spill_mode == SpillMode::AUTO;

        return std::make_shared<MergeSortingTransform>(
                header, description, max_merged_block_size, limit,
                max_bytes_before_remerge / pipeline.getNumStreams(),
                remerge_lowered_memory_bytes_ratio,
                max_bytes_before_external_sort,
                tmp_volume,
                min_free_disk_space,
                enable_adaptive_spill);
    });
}

void MergeSortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(description, input_streams.front().header, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void MergeSortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(description, input_streams.front().header));

    if (limit)
        map.add("Limit", limit);
}

std::shared_ptr<IQueryPlanStep> MergeSortingStep::copy(ContextPtr) const
{
    return std::make_shared<MergeSortingStep>(
        input_streams[0],
        description,
        max_merged_block_size,
        limit,
        max_bytes_before_remerge,
        remerge_lowered_memory_bytes_ratio,
        max_bytes_before_external_sort,
        tmp_volume,
        min_free_disk_space,
        enable_adaptive_spill);
}


void MergeSortingStep::toProto(Protos::MergeSortingStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    for (const auto & element : description)
        element.toProto(*proto.add_description());
    proto.set_max_merged_block_size(max_merged_block_size);
    proto.set_limit(limit);
    proto.set_max_bytes_before_remerge(max_bytes_before_remerge);
    proto.set_remerge_lowered_memory_bytes_ratio(remerge_lowered_memory_bytes_ratio);
    proto.set_max_bytes_before_external_sort(max_bytes_before_external_sort);

    proto.set_min_free_disk_space(min_free_disk_space);
    proto.set_enable_adaptive_spill(enable_adaptive_spill);
}

std::shared_ptr<MergeSortingStep> MergeSortingStep::fromProto(const Protos::MergeSortingStep & proto, ContextPtr context)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    SortDescription description;
    for (const auto & proto_element : proto.description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        description.emplace_back(std::move(element));
    }
    auto max_merged_block_size = proto.max_merged_block_size();
    auto limit = proto.limit();
    auto max_bytes_before_remerge = proto.max_bytes_before_remerge();
    auto remerge_lowered_memory_bytes_ratio = proto.remerge_lowered_memory_bytes_ratio();
    auto max_bytes_before_external_sort = proto.max_bytes_before_external_sort();
    auto enable_adaptive_spill = proto.enable_adaptive_spill();
    VolumePtr tmp_volume = context ? context->getTemporaryVolume() : nullptr;
    auto min_free_disk_space = proto.min_free_disk_space();
    auto step = std::make_shared<MergeSortingStep>(
        base_input_stream,
        description,
        max_merged_block_size,
        limit,
        max_bytes_before_remerge,
        remerge_lowered_memory_bytes_ratio,
        max_bytes_before_external_sort,
        tmp_volume,
        min_free_disk_space,
        enable_adaptive_spill);
    step->setStepDescription(step_description);
    return step;
}
}

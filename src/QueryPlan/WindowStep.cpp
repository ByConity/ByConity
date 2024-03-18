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

#include <QueryPlan/WindowStep.h>

#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/WindowTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingSortedStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <Common/JSONBuilder.h>

namespace DB
{
static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {.preserves_number_of_rows = true}};
}

static Block addWindowFunctionResultColumns(const Block & block, std::vector<WindowFunctionDescription> window_functions)
{
    auto result = block;

    for (const auto & f : window_functions)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = f.column_name;
        column_with_type.type = f.aggregate_function->getReturnType();
        column_with_type.column = column_with_type.type->createColumn();

        result.insert(column_with_type);
    }

    return result;
}

WindowStep::WindowStep(
    const DataStream & input_stream_, const WindowDescription & window_description_, bool need_sort_, SortDescription prefix_description_)
    : WindowStep(input_stream_, window_description_, window_description_.window_functions, need_sort_, prefix_description_)
{
}

WindowStep::WindowStep(
    const DataStream & input_stream_,
    const WindowDescription & window_description_,
    const std::vector<WindowFunctionDescription> & window_functions_,
    bool need_sort_,
    SortDescription prefix_description_)
    : ITransformingStep(input_stream_, addWindowFunctionResultColumns(input_stream_.header, window_functions_), getTraits())
    , window_description(window_description_)
    , window_functions(window_functions_)
    , need_sort(need_sort_)
    , prefix_description(prefix_description_)
{
    // We don't remove any columns, only add, so probably we don't have to update
    // the output DataStream::distinct_columns.

    window_description.checkValid();
}

void WindowStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = addWindowFunctionResultColumns(input_streams_[0].header, window_functions);
}

void WindowStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & s)
{
    auto enable_windows_parallel = s.context->getSettingsRef().enable_windows_parallel;
    if (need_sort && !window_description.full_sort_description.empty())
    {
        if (enable_windows_parallel)
            scatterByPartitionIfNeeded(pipeline);

        // finish sorting

        DataStream input_stream = input_streams[0];
        if (!prefix_description.empty() && !enable_windows_parallel)
        {
            bool need_finish_sorting = (prefix_description.size() < window_description.full_sort_description.size());
            if (pipeline.getNumStreams() > 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                    pipeline.getHeader(), pipeline.getNumStreams(), prefix_description, s.context->getSettingsRef().max_block_size, 0);

                pipeline.addTransform(std::move(transform));
            }

            if (need_finish_sorting)
            {
                pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr {
                    if (stream_type != QueryPipeline::StreamType::Main)
                        return nullptr;

                    return std::make_shared<PartialSortingTransform>(header, window_description.full_sort_description, 0);
                });

                /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
                pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr {
                    return std::make_shared<FinishSortingTransform>(
                        header,
                        prefix_description,
                        window_description.full_sort_description,
                        s.context->getSettingsRef().max_block_size,
                        0);
                });
            }
        }
        else
        {
            PartialSortingStep partial_sorting_step{input_stream, window_description.full_sort_description, 0};
            partial_sorting_step.transformPipeline(pipeline, s);

            MergeSortingStep merge_sorting_step{input_stream, window_description.full_sort_description, 0};
            merge_sorting_step.transformPipeline(pipeline, s);
        }
        if (!enable_windows_parallel || window_description.partition_by.empty())
        {
            MergingSortedStep merging_sorted_step{
                input_stream, window_description.full_sort_description, s.context->getSettingsRef().max_block_size, 0};
            merging_sorted_step.transformPipeline(pipeline, s);
        }
    }

    // This resize is needed for cases such as `over ()` when we don't have a
    // sort node, and the input might have multiple streams. The sort node would
    // have resized it.
    if (!enable_windows_parallel || window_description.full_sort_description.empty())
        pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & /*header*/) {
        return std::make_shared<WindowTransform>(
            input_streams[0].header, output_stream->header, window_description, window_functions, s.actions_settings.dialect_type);
    });

    assertBlocksHaveEqualStructure(
        pipeline.getHeader(), output_stream->header, "WindowStep transform for '" + window_description.window_name + "'");
}

void WindowStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Window: (";
    if (!window_description.partition_by.empty())
    {
        settings.out << "PARTITION BY ";
        for (size_t i = 0; i < window_description.partition_by.size(); ++i)
        {
            if (i > 0)
            {
                settings.out << ", ";
            }

            settings.out << window_description.partition_by[i].column_name;
        }
    }
    if (!window_description.partition_by.empty() && !window_description.order_by.empty())
    {
        settings.out << " ";
    }
    if (!window_description.order_by.empty())
    {
        settings.out << "ORDER BY " << dumpSortDescription(window_description.order_by);
    }
    settings.out << ")\n";

    for (size_t i = 0; i < window_functions.size(); ++i)
    {
        settings.out << prefix << (i == 0 ? "Functions: " : "           ");
        settings.out << window_functions[i].column_name << "\n";
    }
}

void WindowStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!window_description.partition_by.empty())
    {
        auto partion_columns_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & descr : window_description.partition_by)
            partion_columns_array->add(descr.column_name);

        map.add("Partition By", std::move(partion_columns_array));
    }

    if (!window_description.order_by.empty())
        map.add("Sort Description", explainSortDescription(window_description.order_by, {}));

    auto functions_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & func : window_functions)
        functions_array->add(func.column_name);

    map.add("Functions", std::move(functions_array));
}

std::shared_ptr<IQueryPlanStep> WindowStep::copy(ContextPtr) const
{
    return std::make_shared<WindowStep>(input_streams[0], window_description, window_functions, need_sort);
}

void WindowStep::toProto(Protos::WindowStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    window_description.toProto(*proto.mutable_window_description());
    for (const auto & element : window_functions)
        element.toProto(*proto.add_window_functions());
    proto.set_need_sort(need_sort);
    for (const auto & element : prefix_description)
        element.toProto(*proto.add_prefix_description());
}

std::shared_ptr<WindowStep> WindowStep::fromProto(const Protos::WindowStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    WindowDescription window_description;
    window_description.fillFromProto(proto.window_description());
    std::vector<WindowFunctionDescription> window_functions;
    for (const auto & proto_element : proto.window_functions())
    {
        WindowFunctionDescription element;
        element.fillFromProto(proto_element);
        window_functions.emplace_back(std::move(element));
    }
    auto need_sort = proto.need_sort();
    SortDescription prefix_description;
    for (const auto & proto_element : proto.prefix_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        prefix_description.emplace_back(std::move(element));
    }
    auto step = std::make_shared<WindowStep>(base_input_stream, window_description, window_functions, need_sort, prefix_description);
    step->setStepDescription(step_description);
    return step;
}

void WindowStep::scatterByPartitionIfNeeded(QueryPipeline& pipeline)
{
    size_t threads = pipeline.getNumThreads();
    size_t streams = pipeline.getNumStreams();

    if (!window_description.partition_by.empty() && threads > 1)
    {
        Block stream_header = pipeline.getHeader();

        ColumnNumbers key_columns;
        key_columns.reserve(window_description.partition_by.size());
        for (auto & col : window_description.partition_by)
        {
            key_columns.push_back(stream_header.getPositionByName(col.column_name));
        }

        pipeline.transform([&](OutputPortRawPtrs ports)
        {
            Processors processors;
            for (auto * port : ports)
            {
                auto scatter = std::make_shared<ScatterByPartitionTransform>(stream_header, threads, key_columns);
                connect(*port, scatter->getInputs().front());
                processors.push_back(scatter);
            }
            return processors;
        });

        if (streams > 1)
        {
            pipeline.transform([&](OutputPortRawPtrs ports)
            {
                Processors processors;
                for (size_t i = 0; i < threads; ++i)
                {
                    size_t output_it = i;
                    auto resize = std::make_shared<ResizeProcessor>(ports[output_it]->getHeader(), streams, 1);
                    auto & inputs = resize->getInputs();

                    for (auto input_it = inputs.begin(); input_it != inputs.end(); output_it += threads, ++input_it)
                        connect(*ports[output_it], *input_it);
                    processors.push_back(resize);
                }
                return processors;
            });
        }
    }
}

}

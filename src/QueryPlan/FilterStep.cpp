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

#include <utility>
#include <QueryPlan/FilterStep.h>

#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/RuntimeFilter/RuntimeFilterManager.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/RuntimeFilterUtils.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Processors/Port.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Common/JSONBuilder.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <Processors/Port.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ActionsDAGPtr & expression)
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = !expression->hasArrayJoin(), /// I suppose it actually never happens
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

FilterStep::FilterStep(const DataStream & input_stream_, ActionsDAGPtr actions_dag_, String filter_column_name_, bool remove_filter_column_)
    : ITransformingStep(
        input_stream_,
        FilterTransform::transformHeader(input_stream_.header, *actions_dag_, filter_column_name_, remove_filter_column_),
        getTraits(actions_dag_))
    , actions_dag(std::move(actions_dag_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
    /// TODO: it would be easier to remove all expressions from filter step. It should only filter by column name.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

// todo optimizer
FilterStep::FilterStep(const DataStream & input_stream_, const ConstASTPtr & filter_, bool remove_filter_column_)
    : ITransformingStep(input_stream_, input_stream_.header, {})
    , filter(filter_)
    , filter_column_name(filter->getColumnName())
    , remove_filter_column(remove_filter_column_)
{
}

void FilterStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
    output_stream->header = input_streams_[0].header;
}

void FilterStep::updateInputStream(DataStream input_stream, bool keep_header)
{
    Block out_header = std::move(output_stream->header);
    if (keep_header)
        out_header = FilterTransform::transformHeader(input_stream.header, *actions_dag, filter_column_name, remove_filter_column);

    output_stream = createOutputStream(input_stream, std::move(out_header), getDataStreamTraits());

    input_streams.clear();
    input_streams.emplace_back(std::move(input_stream));
}

void FilterStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    ConstASTPtr rewrite_filter = filter;
    if (!actions_dag)
    {
        rewrite_filter = rewriteRuntimeFilter(filter, pipeline, settings);
        actions_dag = IQueryPlanStep::createFilterExpressionActions(settings.context, rewrite_filter->clone(), input_streams[0].header);
        filter_column_name = rewrite_filter->getColumnName();
    }

    bool contains_runtime_filter = RuntimeFilterUtils::containsRuntimeFilters(rewrite_filter);
    auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<FilterTransform>(
            header, expression, filter_column_name, remove_filter_column, on_totals, contains_runtime_filter);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(),
            output_stream->header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
    }
}

void FilterStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Filter column: " << filter_column_name;

    if (remove_filter_column)
        settings.out << " (removed)";
    settings.out << '\n';

    bool first = true;
    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: " : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }

    settings.out << prefix << "Positions:";
    for (const auto & pos : expression->getResultPositions())
        settings.out << ' ' << pos;
    settings.out << '\n';
}

void FilterStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Filter Column", filter_column_name);
    map.add("Removes Filter", remove_filter_column);

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    map.add("Expression", expression->toTree());
}

std::shared_ptr<FilterStep> FilterStep::fromProto(const Protos::FilterStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto filter = deserializeASTFromProto(proto.filter());
    auto remove_filter_column = proto.remove_filter_column();
    auto step = std::make_shared<FilterStep>(base_input_stream, filter, remove_filter_column);
    step->setStepDescription(step_description);
    return step;
}

void FilterStep::toProto(Protos::FilterStep & proto, bool) const
{
    if (actions_dag)
    {
        throw Exception("actions dag is not supported in protobuf", ErrorCodes::PROTOBUF_BAD_CAST);
    }
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    serializeASTToProto(filter, *proto.mutable_filter());
    proto.set_remove_filter_column(remove_filter_column);
}

std::shared_ptr<IQueryPlanStep> FilterStep::copy(ContextPtr) const
{
    return std::make_shared<FilterStep>(input_streams[0], filter->clone(), remove_filter_column);
}

ConstASTPtr FilterStep::rewriteRuntimeFilter(const ConstASTPtr & filter, QueryPipeline &, const BuildQueryPipelineSettings & build_context)
{
    auto filters = RuntimeFilterUtils::extractRuntimeFilters(filter);
    if (filters.first.empty())
        return filter;

    bool only_bf = build_context.context->getSettingsRef().enable_rewrite_bf_into_prewhere;

    std::vector<ConstASTPtr> predicates = std::move(filters.second);

    if (build_context.context->getSettingsRef().enable_two_stages_prewhere)
    {
        //skip all runtime_filters in FilterStep, since all runtime_filters has been moved into TableScanStep.
    }
    else
    {
        for (auto & runtime_filter : filters.first)
        {
            auto description = RuntimeFilterUtils::extractDescription(runtime_filter).value();
            auto runtime_filters
                = RuntimeFilterUtils::createRuntimeFilterForFilter(description, build_context.context->getInitialQueryId(), only_bf);
            predicates.insert(predicates.end(), runtime_filters.begin(), runtime_filters.end());
        }
    }

    return PredicateUtils::combineConjuncts(predicates);
}

void FilterStep::prepare(const PreparedStatementContext & prepared_context)
{
    prepared_context.prepare(filter);
}

std::pair<ConstASTPtr, ConstASTPtr> FilterStep::splitLargeInValueList(const ConstASTPtr & filter, UInt64 limit)
{
    std::vector<ConstASTPtr> removed_large_in_value_list;
    std::vector<ConstASTPtr> large_in_value_list;
    for (auto & predicate : PredicateUtils::extractConjuncts(filter))
    {
        LOG_DEBUG(getLogger("FilterStep"), " predicate : {}", predicate->formatForErrorMessage());

        if (predicate->as<ASTFunction>() &&
            (predicate->as<const ASTFunction &>().name == "in" ||
             predicate->as<const ASTFunction &>().name == "globalIn" ||
             predicate->as<const ASTFunction &>().name == "notIn" ||
             predicate->as<const ASTFunction &>().name == "globalNotIn"))
        {
            const auto & function = predicate->as<const ASTFunction &>();
            if (function.arguments->getChildren()[1]->as<ASTFunction>())
            {
                ASTFunction & tuple = function.arguments->getChildren()[1]->as<ASTFunction &>();
                size_t size = tuple.arguments->getChildren().size();
                if (size > limit)
                {
                    large_in_value_list.emplace_back(predicate);
                    continue;
                }
            }
        }
        removed_large_in_value_list.emplace_back(predicate);
    }

    return std::make_pair(
        PredicateUtils::combineConjuncts(removed_large_in_value_list), PredicateUtils::combineConjuncts(large_in_value_list));
}

std::vector<ConstASTPtr> FilterStep::removeLargeInValueList(const std::vector<ConstASTPtr> & filters, UInt64 limit)
{
    std::vector<ConstASTPtr> removed_large_in_value_list;
    for (const auto & predicate : filters)
    {
        if (predicate->as<ASTFunction>() &&
           (predicate->as<const ASTFunction &>().name == "in" ||
            predicate->as<const ASTFunction &>().name == "globalIn" ||
            predicate->as<const ASTFunction &>().name == "notIn" ||
            predicate->as<const ASTFunction &>().name == "globalNotIn")
        )
        {
            const auto & function = predicate->as<const ASTFunction &>();
            if (function.arguments->getChildren()[1]->as<ASTFunction>())
            {
                ASTFunction & tuple = function.arguments->getChildren()[1]->as<ASTFunction &>();
                size_t size = tuple.arguments->getChildren().size();
                if (size > limit)
                {
                    continue;
                }
            }
        }
        removed_large_in_value_list.emplace_back(predicate);
    }
    return removed_large_in_value_list;
}

}

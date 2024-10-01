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

#include <QueryPlan/IQueryPlanStep.h>

#include <Functions/FunctionsHashing.h>
#include <IO/Operators.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Processors/Exchange/ExchangeSource.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include "QueryPlan/PlanSerDerHelper.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void DataStream::toProto(Protos::DataStream & proto) const
{
    serializeHeaderToProto(header, *proto.mutable_header());
    for (const auto & element : distinct_columns)
        proto.add_distinct_columns(element);
    std::sort(proto.mutable_distinct_columns()->begin(), proto.mutable_distinct_columns()->end());
    proto.set_has_single_port(has_single_port);
    for (const auto & element : sort_description)
        element.toProto(*proto.add_sort_description());
    proto.set_sort_mode(DataStream::SortModeConverter::toProto(sort_mode));
}

void DataStream::fillFromProto(const Protos::DataStream & proto)
{
    header = deserializeHeaderFromProto(proto.header());
    for (const auto & element : proto.distinct_columns())
        distinct_columns.emplace(element);
    has_single_port = proto.has_single_port();
    for (const auto & proto_element : proto.sort_description())
    {
        SortColumnDescription element;
        element.fillFromProto(proto_element);
        sort_description.emplace_back(std::move(element));
    }
    sort_mode = DataStream::SortModeConverter::fromProto(proto.sort_mode());
}

void RuntimeAttributeDescription::fillFromProto(const Protos::RuntimeAttributeDescription & proto)
{
    description = proto.description();
    for (const auto & proto_element : proto.details())
    {
        auto name = proto_element.name();
        auto alias = proto_element.alias();
        name_and_detail.emplace_back(name, alias);
    }
    if (proto.has_additional())
        additional = proto.additional();
}

void RuntimeAttributeDescription::toProto(Protos::RuntimeAttributeDescription & proto) const
{
    proto.set_description(description);
    for (const auto & [name, detail] : name_and_detail)
    {
        auto * proto_element = proto.add_details();
        proto_element->set_name(name);
        proto_element->set_alias(detail);
    }
    proto.set_additional(additional);
}

const DataStream & IQueryPlanStep::getOutputStream() const
{
    if (!hasOutputStream())
        throw Exception("QueryPlanStep " + getName() + " does not have output stream.", ErrorCodes::LOGICAL_ERROR);

    return *output_stream;
}

static void doDescribeHeader(const Block & header, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    String prefix(settings.offset, settings.indent_char);
    prefix += "Header";

    if (count > 1)
        prefix += " × " + std::to_string(count) + " ";

    prefix += ": ";

    settings.out << prefix;

    if (!header)
    {
        settings.out << " empty\n";
        return;
    }

    prefix.assign(prefix.size(), settings.indent_char);
    bool first = true;

    for (const auto & elem : header)
    {
        if (!first)
            settings.out << prefix;

        first = false;
        elem.dumpNameAndType(settings.out);
        settings.out << ": ";
        elem.dumpStructure(settings.out);
        settings.out << '\n';
    }
}

static void doDescribeProcessor(const IProcessor & processor, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    auto exchange_source = dynamic_cast<const ExchangeSource *>(&processor);
    if (exchange_source)
        settings.out << String(settings.offset, settings.indent_char) << exchange_source->getClassName();
    else
        settings.out << String(settings.offset, settings.indent_char) << processor.getName();

    if (count > 1)
        settings.out << " × " << std::to_string(count);

    size_t num_inputs = processor.getInputs().size();
    size_t num_outputs = processor.getOutputs().size();
    if (num_inputs != 1 || num_outputs != 1)
        settings.out << " " << std::to_string(num_inputs) << " → " << std::to_string(num_outputs);

    settings.out << '\n';

    if (settings.write_header)
    {
        const Block * last_header = nullptr;
        size_t num_equal_headers = 0;

        for (const auto & port : processor.getOutputs())
        {
            if (last_header && !blocksHaveEqualStructure(*last_header, port.getHeader()))
            {
                doDescribeHeader(*last_header, num_equal_headers, settings);
                num_equal_headers = 0;
            }

            ++num_equal_headers;
            last_header = &port.getHeader();
        }

        if (last_header)
            doDescribeHeader(*last_header, num_equal_headers, settings);
    }

    settings.offset += settings.indent;
}

void IQueryPlanStep::describePipeline(const Processors & processors, FormatSettings & settings)
{
    const IProcessor * prev = nullptr;
    size_t count = 0;

    for (auto it = processors.rbegin(); it != processors.rend(); ++it)
    {
        if (prev && prev->getName() != (*it)->getName())
        {
            doDescribeProcessor(*prev, count, settings);
            count = 0;
        }

        ++count;
        prev = it->get();
    }

    if (prev)
        doDescribeProcessor(*prev, count, settings);
}


ActionsDAGPtr IQueryPlanStep::createFilterExpressionActions(ContextPtr context, const ASTPtr & filter, const Block & header)
{
    Names output;
    for (const auto & item : header)
        output.emplace_back(item.name);
    output.push_back(filter->getColumnName());

    return createExpressionActions(context, header.getNamesAndTypesList(), output, filter);
}

ActionsDAGPtr IQueryPlanStep::createExpressionActions(
    ContextPtr context, const NamesAndTypesList & source, const NamesWithAliases & output, const ASTPtr & ast, bool add_project)
{
    PreparedSets prepared_sets;
    SubqueriesForSets subqueries_for_sets;
    auto settings = context->getSettingsRef();
    SizeLimits size_limits_for_set(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
    auto actions = std::make_shared<ActionsDAG>(source);
    const NamesAndTypesList aggregation_keys;
    const ColumnNumbersList grouping_set_keys;
    ActionsVisitor::Data visitor_data(
        context,
        size_limits_for_set,
        0,
        source,
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        true,
        false,
        false,
        false,
        {aggregation_keys, grouping_set_keys, GroupByKind::NONE});
    ActionsVisitor(visitor_data).visit(ast);
    actions = visitor_data.getActions();

    if (add_project)
        actions->project(output);
    else
        actions->addAliases(output);

    Names output_columns;
    for (const auto & item : output)
        if (!item.second.empty())
            output_columns.emplace_back(item.second);
        else
            output_columns.emplace_back(item.first);

    actions->removeUnusedActions(output_columns);

    return actions;
}

ActionsDAGPtr IQueryPlanStep::createExpressionActions(
    ContextPtr context, const NamesAndTypesList & source, const Names & output, const ASTPtr & ast, bool add_project)
{
    NamesWithAliases names_with_aliases;
    for (const auto & item : output)
        names_with_aliases.emplace_back(NameWithAlias{item, ""});

    return createExpressionActions(context, source, names_with_aliases, ast, add_project);
}

void IQueryPlanStep::projection(QueryPipeline & pipeline, const Block & target, const BuildQueryPipelineSettings & settings)
{
    if (!blocksHaveEqualStructure(pipeline.getHeader(), target))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(), target.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
    }
}

void IQueryPlanStep::aliases(QueryPipeline & pipeline, const Block & target, const BuildQueryPipelineSettings & settings)
{
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        pipeline.getHeader().getColumnsWithTypeAndName(), target.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Position);
    auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
}

void IQueryPlanStep::addHints(SqlHints & sql_hints, ContextMutablePtr & context, bool check_type)
{
    PlanHintPtr plan_hint;
    for (auto & hint : sql_hints)
    {
        plan_hint = PlanHintFactory::instance().tryGet(hint.getName(), context, hint);
        if (plan_hint)
        {
            if (check_type && !plan_hint->checkStepType(*this))
                continue;
            hints.emplace_back(plan_hint);
        }
    }
}

String IQueryPlanStep::toString(Type type)
{
#define DISPATCH_DEF(TYPE) \
    if (type == IQueryPlanStep::Type::TYPE) \
    { \
        return #TYPE; \
    }
    APPLY_STEP_TYPES(DISPATCH_DEF)

#undef DISPATCH_DEF
    return "Unknown";
}

size_t IQueryPlanStep::hash(bool ignore_output_stream) const
{
    return hashPlanStep(*this, ignore_output_stream);
}

}

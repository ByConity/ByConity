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

#include <QueryPlan/ProjectionStep.h>

#include <DataTypes/DataTypeHelper.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/ExpressionTransform.h>

namespace DB
{
ProjectionStep::ProjectionStep(
    const DataStream & input_stream_,
    Assignments assignments_,
    NameToType name_to_type_,
    bool final_project_,
    bool index_project_,
    PlanHints hints_)
    : ITransformingStep(input_stream_, {}, {}, true, hints_)
    , assignments(std::move(assignments_))
    , name_to_type(std::move(name_to_type_))
    , final_project(final_project_)
    , index_project(index_project_)
{
    for (const auto & item : assignments)
    {
        if (unlikely(!name_to_type[item.first]))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ProjectionStep miss type info for column " + item.first);
        output_stream->header.insert(ColumnWithTypeAndName{name_to_type[item.first], item.first});
    }
}

void ProjectionStep::setInputStreams(const DataStreams & input_streams_)
{
    input_streams = input_streams_;
}

void ProjectionStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto actions = createActions(settings.context);
    auto expression = std::make_shared<ExpressionActions>(actions, settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, expression); });
    projection(pipeline, output_stream->header, settings);
}

void ProjectionStep::toProto(Protos::ProjectionStep & proto, bool) const
{
    ITransformingStep::serializeToProtoBase(*proto.mutable_query_plan_base());
    serializeAssignmentsToProto(assignments, *proto.mutable_assignments());
    serializeOrderedMapToProto(name_to_type, *proto.mutable_name_to_type());
    proto.set_final_project(final_project);
    proto.set_index_project(index_project);
}

std::shared_ptr<ProjectionStep> ProjectionStep::fromProto(const Protos::ProjectionStep & proto, ContextPtr)
{
    auto [step_description, base_input_stream] = ITransformingStep::deserializeFromProtoBase(proto.query_plan_base());
    auto assignments = deserializeAssignmentsFromProto(proto.assignments());
    auto name_to_type = deserializeOrderedMapFromProto<String, DataTypePtr>(proto.name_to_type());
    auto final_project = proto.final_project();
    auto index_project = proto.index_project();
    auto step = std::make_shared<ProjectionStep>(base_input_stream, assignments, name_to_type, final_project, index_project);
    step->setStepDescription(step_description);
    return step;
}

std::shared_ptr<IQueryPlanStep> ProjectionStep::copy(ContextPtr) const
{
    return std::make_shared<ProjectionStep>(input_streams[0], assignments.copy(), name_to_type, final_project, index_project, hints);
}

ActionsDAGPtr ProjectionStep::createActions(ContextPtr context) const
{
    ASTPtr expr_list = std::make_shared<ASTExpressionList>();

    NamesWithAliases output;
    for (const auto & item : assignments)
    {
        expr_list->children.emplace_back(item.second->clone());
        output.emplace_back(NameWithAlias{item.second->getColumnName(), item.first});
    }
    return createExpressionActions(context, input_streams[0].header.getNamesAndTypesList(), output, expr_list);
}

ActionsDAGPtr ProjectionStep::createActions(const Assignments & assignments, const NamesAndTypesList & source, ContextPtr context)
{
    ASTPtr expr_list = std::make_shared<ASTExpressionList>();

    NamesWithAliases output;
    for (const auto & item : assignments)
    {
        expr_list->children.emplace_back(item.second->clone());
        output.emplace_back(NameWithAlias{item.second->getColumnName(), item.first});
    }
    return createExpressionActions(context, source, output, expr_list);
}

void ProjectionStep::prepare(const PreparedStatementContext & prepared_context)
{
    for (auto & assign : assignments)
        prepared_context.prepare(assign.second);
}
}

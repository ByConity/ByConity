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
    PlanHints hints_)
    : ITransformingStep(input_stream_, {}, {}, true, hints_)
    , assignments(std::move(assignments_))
    , name_to_type(std::move(name_to_type_))
    , final_project(final_project_)
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

void ProjectionStep::serialize(WriteBuffer & buf) const
{
    IQueryPlanStep::serializeImpl(buf);

    writeVarUInt(assignments.size(), buf);
    for (const auto & item : assignments)
    {
        writeStringBinary(item.first, buf);
        serializeAST(item.second, buf);
    }

    writeVarUInt(name_to_type.size(), buf);
    for (const auto & item : name_to_type)
    {
        writeStringBinary(item.first, buf);
        serializeDataType(item.second, buf);
    }

    writeBinary(final_project, buf);
}

QueryPlanStepPtr ProjectionStep::deserialize(ReadBuffer & buf, ContextPtr)
{
    String step_description;
    readBinary(step_description, buf);

    DataStream input_stream = deserializeDataStream(buf);
    size_t size;
    readVarUInt(size, buf);
    Assignments assignments;
    for (size_t index = 0; index < size; ++index)
    {
        String name;
        readStringBinary(name, buf);
        auto ast = deserializeAST(buf);
        assignments.emplace_back(name, ast);
    }

    readVarUInt(size, buf);
    NameToType name_to_type;
    for (size_t index = 0; index < size; ++index)
    {
        String name;
        readStringBinary(name, buf);
        auto data_type = deserializeDataType(buf);
        name_to_type[name] = data_type;
    }

    bool final_project;
    readBinary(final_project, buf);

    return std::make_shared<ProjectionStep>(input_stream, assignments, name_to_type, final_project);
}

std::shared_ptr<IQueryPlanStep> ProjectionStep::copy(ContextPtr) const
{
    return std::make_shared<ProjectionStep>(input_streams[0], assignments, name_to_type, final_project, hints);
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
}

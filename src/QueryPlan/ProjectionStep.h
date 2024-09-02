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

#include <Core/NameToType.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class RuntimeFilterBuilder;
using RuntimeFilterBuilderPtr = std::shared_ptr<RuntimeFilterBuilder>;

class ProjectionStep : public ITransformingStep
{
public:
    explicit ProjectionStep(
        const DataStream & input_stream_,
        Assignments assignments_,
        NameToType name_to_type_,
        bool final_project_ = false,
        bool index_project_ = false,
        PlanHints hints_ = {});

    String getName() const override { return "Projection"; }
    Type getType() const override { return Type::Projection; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
    void toProto(Protos::ProjectionStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ProjectionStep> fromProto(const Protos::ProjectionStep & proto, ContextPtr context);

    ActionsDAGPtr createActions(ContextPtr context) const;
    const Assignments & getAssignments() const { return assignments; }
    const NameToType & getNameToType() const { return name_to_type; }

    bool isFinalProject() const { return final_project; }
    bool isIndexProject() const { return index_project; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    void prepare(const PreparedStatementContext & prepared_context) override;

    static ActionsDAGPtr createActions(const Assignments & assignments, const NamesAndTypesList & source, ContextPtr context);

private:
    Assignments assignments;
    NameToType name_to_type;
    // final output step
    bool final_project;
    bool index_project;
};

}

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
#include <QueryPlan/ITransformingStep.h>
#include <Interpreters/ArrayJoinAction.h>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinStep : public ITransformingStep
{
public:
    explicit ArrayJoinStep(const DataStream & input_stream_, ArrayJoinActionPtr array_join_);
    String getName() const override { return "ArrayJoin"; }

    Type getType() const override { return Type::ArrayJoin; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void updateInputStream(DataStream input_stream, Block result_header);

    const ArrayJoinActionPtr & arrayJoin() const { return array_join; }

    void serialize(WriteBuffer & buf) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    ArrayJoinActionPtr getArrayJoinAction() const { return array_join; }
    void setInputStreams(const DataStreams & input_streams_) override;

    NameSet & getResultNameSet() const { return array_join->columns; }

    bool isLeft() const { return array_join->is_left; }

private:
    ArrayJoinActionPtr array_join;
    Block res_header;
};

}

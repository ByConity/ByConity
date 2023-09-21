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
#include <QueryPlan/SetOperationStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public SetOperationStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    UnionStep(DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_, size_t max_threads_, bool local_);

    explicit UnionStep(DataStreams input_streams_, DataStream output_stream_ = {}, OutputToInputs output_to_inputs_ = {})
        : UnionStep(std::move(input_streams_), std::move(output_stream_), std::move(output_to_inputs_), 0, false)
    {
    }

    String getName() const override { return "Union"; }

    Type getType() const override { return Type::Union; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    size_t getMaxThreads() const { return max_threads; }
    bool isLocal() const { return local; }

    void toProto(Protos::UnionStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<UnionStep> fromProto(const Protos::UnionStep & proto, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;

private:
    Block header;
    size_t max_threads;
    bool local;
    Processors processors;
};

}

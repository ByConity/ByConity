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
#include <QueryPlan/IQueryPlanStep.h>

namespace DB
{
using OutputToInputs = std::unordered_map<String, std::vector<String>>;

class SetOperationStep : public IQueryPlanStep
{
public:
    SetOperationStep(DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_);
    const OutputToInputs & getOutToInputs() const;
    NameToNameMap getOutToInput(size_t source_idx) const;
    void setInputStreams(const DataStreams & input_streams_) override;

    void serializeToProtoBase(Protos::SetOperationStep & proto) const;
    static std::tuple<DataStreams, DataStream, std::unordered_map<String, std::vector<String>>>
    deserializeFromProtoBase(const Protos::SetOperationStep & proto);

protected:
    OutputToInputs output_to_inputs;
};

}

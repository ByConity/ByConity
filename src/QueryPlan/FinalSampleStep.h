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
#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <DataStreams/SizeLimits.h>
#include <QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/FinalSampleTransform.h>
#include <Processors/QueryPipeline.h>
#include <Common/JSONBuilder.h>

namespace DB
{
/// Executes Sample. See FinalSampleTransform.
class FinalSampleStep : public ITransformingStep
{
public:
    FinalSampleStep(const DataStream & input_stream_, size_t sample_size_, size_t max_chunk_size_)
        : ITransformingStep(input_stream_, input_stream_.header, getTraits()), sample_size(sample_size_), max_chunk_size(max_chunk_size_)
    {
    }

    size_t getSampleSize() const { return sample_size; }
    size_t getMaxChunkSize() const { return max_chunk_size; }

    void setSampleSize(size_t sample_size_) { sample_size = sample_size_; }

    String getName() const override { return "FinalSample"; }

    Type getType() const override { return Type::FinalSample; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto transform = std::make_shared<FinalSampleTransform>(pipeline.getHeader(), sample_size, max_chunk_size, pipeline.getNumStreams());
        pipeline.addTransform(std::move(transform));
    }

    void describeActions(FormatSettings & settings) const override
    {
        String prefix(settings.offset, ' ');
        settings.out << prefix << "sample_size " << sample_size << '\n';
        settings.out << prefix << "max_chunk_size " << max_chunk_size << '\n';
    }

    void describeActions(JSONBuilder::JSONMap & map) const override
    {
        map.add("sample_size", sample_size);
        map.add("max_chunk_size", max_chunk_size);
    }

    void toProto(Protos::FinalSampleStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<FinalSampleStep> fromProto(const Protos::FinalSampleStep & proto, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override
    {
        return std::make_shared<FinalSampleStep>(input_streams[0], sample_size, max_chunk_size);
    }


    static ITransformingStep::Traits getTraits()
    {
        return ITransformingStep::Traits{
            {
                .preserves_distinct_columns = true,
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
            },
            {
                .preserves_number_of_rows = false,
            }};
    }
    void setInputStreams(const DataStreams & input_streams_) override
    {
        input_streams = input_streams_;
        output_stream->header = input_streams_[0].header;
    }

private:
    size_t sample_size;
    size_t max_chunk_size;
};

}

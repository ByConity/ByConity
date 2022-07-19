#pragma once
#include <QueryPlan/SetOperationStep.h>

namespace DB
{
class ExceptStep : public SetOperationStep
{
public:
    ExceptStep(
        DataStreams input_streams_,
        DataStream output_stream_,
        std::unordered_map<String, std::vector<String>> output_to_inputs_,
        bool distinct_);

    ExceptStep(DataStreams input_streams_, DataStream output_stream_, bool distinct_) : ExceptStep(input_streams_, output_stream_, {}, distinct_) { }

    String getName() const override { return "Except"; }
    Type getType() const override { return Type::Except; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & context) override;
    void serialize(WriteBuffer & buffer) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr context);

    bool isDistinct() const;
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    bool distinct;
};
}

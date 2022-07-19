#pragma once
#include <QueryPlan/IQueryPlanStep.h>

namespace DB
{
class SetOperationStep : public IQueryPlanStep
{
public:
    SetOperationStep(
        DataStreams input_streams_, DataStream output_stream_, std::unordered_map<String, std::vector<String>> output_to_inputs_);
    const std::unordered_map<String, std::vector<String>> & getOutToInputs() const;
    void setInputStreams(const DataStreams & input_streams_) override;

protected:
    std::unordered_map<String, std::vector<String>> output_to_inputs;
};

}

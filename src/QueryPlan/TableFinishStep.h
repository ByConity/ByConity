#pragma once

#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include <QueryPlan/TableWriteStep.h>

namespace DB
{
class TableFinishStep : public ITransformingStep
{
public:
    TableFinishStep(const DataStream & input_stream_, TableWriteStep::TargetPtr target_, String output_affected_row_count_symbol_);

    String getName() const override
    {
        return "TableFinish";
    }
    Type getType() const override
    {
        return Type::TableFinish;
    }

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;
    void serialize(WriteBuffer & buffer) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr & context);
    void setInputStreams(const DataStreams & input_streams_) override
    {
        input_streams = input_streams_;
        output_stream = DataStream{.header = std::move((input_streams_[0].header))};
    }

    TableWriteStep::TargetPtr getTarget() const
    {
        return target;
    }

private:
    TableWriteStep::TargetPtr target;
    String output_affected_row_count_symbol;
    Poco::Logger * log;
};
}

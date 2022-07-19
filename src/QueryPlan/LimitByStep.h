#pragma once
#include <QueryPlan/ITransformingStep.h>

namespace DB
{

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    explicit LimitByStep(
            const DataStream & input_stream_,
            size_t group_length_, size_t group_offset_, const Names & columns_);

    String getName() const override { return "LimitBy"; }

    Type getType() const override { return Type::LimitBy; }

    size_t getGroupLength() const { return group_length; }
    size_t getGroupOffset() const { return group_offset; }
    const Names & getColumns() const { return columns; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    size_t group_length;
    size_t group_offset;
    Names columns;
};

}

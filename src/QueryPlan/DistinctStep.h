#pragma once
#include <QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
            const DataStream & input_stream_,
            const SizeLimits & set_size_limits_,
            UInt64 limit_hint_,
            const Names & columns_,
            bool pre_distinct_); /// If is enabled, execute distinct for separate streams. Otherwise, merge streams.

    String getName() const override { return "Distinct"; }

    Type getType() const override { return Type::Distinct; }
    const SizeLimits & getSetSizeLimits() const { return set_size_limits; }
    UInt64 getLimitHint() const { return limit_hint; }
    const Names & getColumns() const { return columns; }
    bool preDistinct() const { return pre_distinct; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    SizeLimits set_size_limits;
    UInt64 limit_hint;
    Names columns;
    bool pre_distinct;
};

}

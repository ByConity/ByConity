#pragma once
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{

/// Sort separate chunks of data.
class PartialSortingStep : public ITransformingStep
{
public:
    explicit PartialSortingStep(const DataStream & input_stream, SortDescription sort_description_, UInt64 limit_, SizeLimits size_limits_ = {});

    String getName() const override { return "PartialSorting"; }

    Type getType() const override { return Type::PartialSorting; }
    const SortDescription & getSortDescription() const { return sort_description; }
    UInt64 getLimit() const { return limit; }
    const SizeLimits & getSizeLimits() const { return size_limits; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    SortDescription sort_description;
    UInt64 limit;
    SizeLimits size_limits;
};

}

#pragma once
#include <QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Disks/IVolume.h>

namespace DB
{

/// Sorts stream of data. See MergeSortingTransform.
class SortingStep : public ITransformingStep
{
public:
    explicit SortingStep(
            const DataStream & input_stream,
            const SortDescription & description_,
            UInt64 limit_,
            bool partial_);

    String getName() const override { return "Sorting"; }

    Type getType() const override { return Type::Sorting; }
    const SortDescription & getSortDescription() const { return description; }
    UInt64 getLimit() const { return limit; }
    bool isPartial() const { return partial; }

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
    SortDescription description;
    UInt64 limit;
    bool partial;
};

}

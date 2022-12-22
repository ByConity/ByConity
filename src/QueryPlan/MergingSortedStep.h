#pragma once
#include <QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Disks/IVolume.h>

namespace DB
{

/// Merge streams of data into single sorted stream.
class MergingSortedStep : public ITransformingStep
{
public:
    explicit MergingSortedStep(
        const DataStream & input_stream,
        SortDescription sort_description_,
        size_t max_block_size_,
        UInt64 limit_ = 0);

    String getName() const override { return "MergingSorted"; }

    Type getType() const override { return Type::MergingSorted; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);
    UInt64 getLimit() const { return limit; }
    size_t getMaxBlockSize() const { return max_block_size; }
    const SortDescription & getSortDescription() const { return sort_description; }

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    SortDescription sort_description;
    size_t max_block_size;
    UInt64 limit;
};

}



#pragma once
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Disks/IVolume.h>
#include <Processors/Transforms/PartitionTopNTransform.h>
#include <QueryPlan/ITransformingStep.h>

namespace DB
{
/// Sorts stream of data. See MergeSortingTransform.
class PartitionTopNStep : public ITransformingStep
{
public:
    explicit PartitionTopNStep(const DataStream & input_stream_, const Names & partition_, const Names & order_by_, UInt64 limit_, PartitionTopNModel model_);

    String getName() const override { return "PartitionTopN"; }

    Type getType() const override { return Type::PartitionTopN; }
    const Names & getPartition() const { return partition; }
    const Names & getOrderBy() const { return order_by; }
    UInt64 getLimit() const { return limit; }
    PartitionTopNModel getModel() const { return model; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

private:
    Names partition;
    Names order_by;
    UInt64 limit;
    PartitionTopNModel model;
};

}

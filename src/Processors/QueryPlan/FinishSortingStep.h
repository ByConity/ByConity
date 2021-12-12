#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Finish sorting of pre-sorted data. See FinishSortingTransform.
class FinishSortingStep : public ITransformingStep
{
public:
    FinishSortingStep(
        const DataStream & input_stream_,
        SortDescription prefix_description_,
        SortDescription result_description_,
        size_t max_block_size,
        UInt64 limit);

    String getName() const override { return "FinishSorting"; }

    Type getType() const override { return Type::FinishSorting; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);

private:
    DataStream input_stream;
    SortDescription prefix_description;
    SortDescription result_description;
    size_t max_block_size;
    UInt64 limit;
};

}

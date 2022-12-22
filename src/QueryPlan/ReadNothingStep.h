#pragma once
#include <QueryPlan/ISourceStep.h>

namespace DB
{

/// Create NullSource with specified structure.
class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(Block output_header);

    String getName() const override { return "ReadNothing"; }

    Type getType() const override { return Type::ReadNothing; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(WriteBuffer &) const override;
    static QueryPlanStepPtr deserialize(ReadBuffer &, ContextPtr context_ = nullptr);
    void setUniqueId(Int32 unique_id_) { unique_id = unique_id_; }
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const override;

private:
    Int32 unique_id;
};

}

#pragma once
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ITransformingStep.h>
#include "Parsers/IAST_fwd.h"

namespace DB
{

class OutfileWriteStep : public ITransformingStep
{
public:

    OutfileWriteStep(const DataStream & input_stream_, OutfileTargetPtr outfile_target);

    String getName() const override
    {
        return "OutfileWrite";
    }

    Type getType() const override
    {
        return Type::OutfileWrite;
    }

    void setInputStreams(const DataStreams & input_streams_) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const override;

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_context) override;

    void toProto(Protos::OutfileWriteStep & proto, bool for_hash_equals = false) const;

    static std::shared_ptr<OutfileWriteStep> fromProto(const Protos::OutfileWriteStep & proto, ContextPtr context);
    
    OutfileTargetPtr outfile_target;
};


}

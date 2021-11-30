#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <DataStreams/IBlockStream_fwd.h>

namespace DB
{

class InterpreterPlanSegment : public IInterpreter
{
public:
    InterpreterPlanSegment(
        PlanSegment * plan_segment_,
        ContextPtr context_);

    BlockIO execute() override;

private:
    PlanSegment * plan_segment = nullptr;
    ContextPtr context;
};

}


#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/DistributedStages/PlanSegment.h>

namespace DB
{

class SegmentScheduler;
using SegmentSchedulerPtr = std::shared_ptr<SegmentScheduler>;

struct DistributedStagesSettings
{
    bool enable_distributed_stages = false;
    bool fallback_to_simple_query = false;

    DistributedStagesSettings(
        bool enable_distributed_stages_,
        bool fallback_to_simple_query_
        )
        : enable_distributed_stages(enable_distributed_stages_)
        , fallback_to_simple_query(fallback_to_simple_query_)
        {}
};
class InterpreterDistributedStages : public IInterpreter
{
public:
    InterpreterDistributedStages(const ASTPtr & query_ptr_, ContextPtr context_);

    void createPlanSegments();

    BlockIO execute() override;

    BlockIO executePlanSegment();

    ASTPtr getQuery() { return query_ptr; }

    void initSettings();

    static bool isDistributedStages(const ASTPtr & query, ContextPtr context_);
    static DistributedStagesSettings extractDistributedStagesSettingsImpl(const ASTPtr & query, ContextPtr context_);
    static DistributedStagesSettings extractDistributedStagesSettings(const ASTPtr & query, ContextPtr context_);

private:

    ASTPtr query_ptr;
    ContextMutablePtr context;
    Poco::Logger * log;

    PlanSegmentTreePtr plan_segment_tree = nullptr;
};

}

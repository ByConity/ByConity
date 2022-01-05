#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/ExchangeStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/FinishSortingStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergeSortingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/PartialSortingStep.h>
#include <Processors/QueryPlan/PlanSegmentSourceStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/QueryPlan.h>


namespace DB
{


template <typename R, typename C>
class StepVisitor
{
public:
    virtual ~StepVisitor() = default;

    virtual R visitPlan(QueryPlan::Node *, C &) { throw Exception("Visitor does not supported this step.", ErrorCodes::NOT_IMPLEMENTED); }

#define VISITOR_DEF(TYPE) \
    virtual R visit##TYPE##Step(QueryPlan::Node * node, C & context) { return visitPlan(node, context); }
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF
};

class VisitorUtil
{
public:
    template <typename R, typename C>
    static R accept(QueryPlan::Node * node, StepVisitor<R, C> & visitor, C & context)
    {
        #define VISITOR_DEF(TYPE) \
        if (node && node->step->getType() == IQueryPlanStep::Type::TYPE) \
        { \
            return visitor.visit##TYPE##Step(node, context); \
        }
        APPLY_STEP_TYPES(VISITOR_DEF)
        #undef VISITOR_DEF
        return visitor.visitPlan(node, context);
    }
};


}

#pragma once

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/TableScanStep.h>
#include <Poco/Logger.h>
#include "QueryPlan/ExchangeStep.h"

#include <string>
#include <vector>

namespace DB
{
/**
 * @class StepNormalizer normalizes each step without considering the tree structure. The post-order visit is handled in PlanNormalizer.\n
 *
 * Given the normalized StepsAndOutputOrders of the children, normalizing a step will have three phases: \n
 *
 * 1. Normalizing input symbols. Each input symbol in the step is mapped to its "$idx" of appearance in children's normalized output headers.
 *    e.g. original step Projection{input_stream=[a,b] assignment=[c=a+1]}, and children normalized its output stream to [b,a].
 *         Then a SymbolMapping{b->$0, a->$1} will be created for this Projection step, and the assignment becomes [c=$1+1]. \n
 *
 * 2. (optional) Reordering ASTs and vectors. This is for order-independent matching.
 *    e.g. (a=1 and b=2) matches with (b=2 and a=1) / [x<-a, y<-b] matches with [y<-b, x<-a]
 *    currently supports TableScan, Filter, Project and Aggregation. This must be done after phase 1. \n
 *
 * 3. (optional) normalize literals. Used by normalized_query_plan_hash in query log
 *    e.g (a > 1) will be normalized to (a > '?')
 *    currently supports TableScan, Filter
 * 
 * 4. Normalizing output symbols. This only needs to be done for steps that create new symbols.
 *    currently supports TableScan, Project and Aggregation. This must be done after phase 2 (if exists). \n
 *
 * In current implementation, the four phases (if exists) are packed in the same visitor to avoid creating duplicated steps.
 */
class StepNormalizer;

/**
 * @class StepAndOutputOrder is the outcome of normalizing a step
 *
 * normal_step is the normalized step for each original step, which is in index_ref and can be used to calculate hash etc.
 * All normal_steps implicitly formulate a tree, whose structure is implied by the original plan. Handling this is left to PlanNormalizer
 * reordered_header is an reordering of the original output header.
 * It is the same as the header of original step if no reordering take place.
 * It is different from normal_step->getOutputStream().header, as the symbols in reordered_header are still the original symbols.
 * The parent must use this information to normalize.
 */
struct StepAndOutputOrder
{
    QueryPlanStepPtr normal_step;
    // if there is no reorder, this is exactly the original output header
    // if there is reorder, this header has the original symbols, but ordered according to normal_step
    Block output_order;
};
using StepsAndOutputOrders = std::vector<StepAndOutputOrder>;

class StepNormalizer : public StepVisitor<StepAndOutputOrder, StepsAndOutputOrders>
{
public:
    explicit StepNormalizer(ContextPtr _context, bool normalize_literals_ = false, bool normalize_storage_ = false)
        : context(_context), normalize_literals(normalize_literals_), normalize_storage(normalize_storage_)
    {
    }
    StepAndOutputOrder normalize(QueryPlanStepPtr step, StepsAndOutputOrders && input);
protected:
    StepAndOutputOrder visitStep(const IQueryPlanStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitTableScanStep(const TableScanStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitFilterStep(const FilterStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitProjectionStep(const ProjectionStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitAggregatingStep(const AggregatingStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitCTERefStep(const CTERefStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitJoinStep(const JoinStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep & step, StepsAndOutputOrders & inputs) override;
    StepAndOutputOrder visitTableWriteStep(const TableWriteStep & step, StepsAndOutputOrders & inputs) override;

private:
    ContextPtr context;
    bool normalize_literals;
    bool normalize_storage;
    LoggerPtr log = getLogger("StepNormalizer");
};

}

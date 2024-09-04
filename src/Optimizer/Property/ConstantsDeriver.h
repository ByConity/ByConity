#pragma once

#include <Optimizer/Property/Constants.h>
#include <QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
class ConstantsDeriver
{
public:
    static Constants deriveConstants(QueryPlanStepPtr step, CTEInfo & cte_info, ContextMutablePtr & context);
    static Constants deriveConstants(QueryPlanStepPtr step, Constants & input_constants, CTEInfo & cte_info, ContextMutablePtr & context);
    static Constants
    deriveConstants(QueryPlanStepPtr step, ConstantsSet & input_constants, CTEInfo & cte_info, ContextMutablePtr & context);
    static Constants deriveConstantsFromTree(PlanNodePtr node, CTEInfo & cte_info, ContextMutablePtr & context);
};

class ConstantsDeriverContext
{
public:
    ConstantsDeriverContext(ConstantsSet input_properties_, CTEInfo & cte_info_, ContextMutablePtr & context_)
        : input_properties(std::move(input_properties_)), cte_info(cte_info_), context(context_)
    {
    }
    const ConstantsSet & getInput()
    {
        return input_properties;
    }
    CTEInfo & getCTEInfo()
    {
        return cte_info;
    }
    ContextMutablePtr & getContext()
    {
        return context;
    }

private:
    ConstantsSet input_properties;
    CTEInfo & cte_info;
    ContextMutablePtr & context;
};

class ConstantsDeriverVisitor : public StepVisitor<Constants, ConstantsDeriverContext>
{
public:
    Constants visitStep(const IQueryPlanStep &, ConstantsDeriverContext &) override;

    Constants visitFilterStep(const FilterStep &, ConstantsDeriverContext & context) override;
    Constants visitJoinStep(const JoinStep & step, ConstantsDeriverContext & context) override;
    Constants visitProjectionStep(const ProjectionStep & step, ConstantsDeriverContext & context) override;
    Constants visitMarkDistinctStep(const MarkDistinctStep & step, ConstantsDeriverContext & context) override;
    Constants visitAggregatingStep(const AggregatingStep & step, ConstantsDeriverContext & context) override;
    Constants visitUnionStep(const UnionStep & step, ConstantsDeriverContext & context) override;
    Constants visitTableScanStep(const TableScanStep &, ConstantsDeriverContext &) override;
    Constants visitReadNothingStep(const ReadNothingStep &, ConstantsDeriverContext &) override;
    Constants visitReadStorageRowCountStep(const ReadStorageRowCountStep &, ConstantsDeriverContext &) override;
    Constants visitValuesStep(const ValuesStep &, ConstantsDeriverContext &) override;
    Constants visitCTERefStep(const CTERefStep &, ConstantsDeriverContext & context) override;
};

struct ConstantsDeriverTreeVisitorContext
{
    CTEInfo & cte_info;
    ContextMutablePtr context;
};

class ConstantsDeriverTreeVisitor : public PlanNodeVisitor<Constants, ConstantsDeriverTreeVisitorContext>
{
public:
    Constants visitPlanNode(PlanNodeBase & node, ConstantsDeriverTreeVisitorContext &) override;
    Constants visitCTERefNode(CTERefNode &, ConstantsDeriverTreeVisitorContext &) override
    {
        return {};
    }
};
}

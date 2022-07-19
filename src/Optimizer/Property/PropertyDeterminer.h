#pragma once

#include <Optimizer/Property/Property.h>
#include <QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
class PropertyDeterminer
{
public:
    static PropertySets determineRequiredProperty(ConstQueryPlanStepPtr step, const Property & property);

    static PropertySets determineRequiredProperty(
        ConstQueryPlanStepPtr step,
        const Property & property,
        const std::vector<std::unordered_set<CTEId>> & child_with_clause);
};

class DeterminerContext
{
public:
    DeterminerContext(Property required_) : required(required_){}
    Property getRequired() { return required; }

private:
    Property required;
};

class DeterminerVisitor : public StepVisitor<PropertySets, DeterminerContext>
{
public:
    PropertySets visitStep(const IQueryPlanStep &, DeterminerContext &) override;
    PropertySets visitProjectionStep(const ProjectionStep & step, DeterminerContext & ctx) override;
    PropertySets visitFilterStep(const FilterStep &, DeterminerContext & context) override;
    PropertySets visitJoinStep(const JoinStep & step, DeterminerContext &) override;
    PropertySets visitAggregatingStep(const AggregatingStep & step, DeterminerContext &) override;
    PropertySets visitMergingAggregatedStep(const MergingAggregatedStep & step, DeterminerContext &) override;
    PropertySets visitUnionStep(const UnionStep & step, DeterminerContext & context) override;
    PropertySets visitIntersectStep(const IntersectStep & node, DeterminerContext & context) override;
    PropertySets visitExceptStep(const ExceptStep & node, DeterminerContext & context) override;
    PropertySets visitExchangeStep(const ExchangeStep & node, DeterminerContext & context) override;
    PropertySets visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep & node, DeterminerContext & context) override;
    PropertySets visitTableScanStep(const TableScanStep &, DeterminerContext &) override;
    PropertySets visitReadNothingStep(const ReadNothingStep &, DeterminerContext &) override;
    PropertySets visitValuesStep(const ValuesStep &, DeterminerContext &) override;
    PropertySets visitLimitStep(const LimitStep & step, DeterminerContext & context) override;
    PropertySets visitLimitByStep(const LimitByStep & node, DeterminerContext & context) override;
    PropertySets visitMergeSortingStep(const MergeSortingStep &, DeterminerContext &) override;
    PropertySets visitPartialSortingStep(const PartialSortingStep &, DeterminerContext &) override;
    PropertySets visitMergingSortedStep(const MergingSortedStep & node, DeterminerContext & context) override;
    PropertySets visitDistinctStep(const DistinctStep &, DeterminerContext &) override;
    PropertySets visitExtremesStep(const ExtremesStep &, DeterminerContext &) override;
    PropertySets visitWindowStep(const WindowStep & step, DeterminerContext &) override;
    PropertySets visitApplyStep(const ApplyStep &, DeterminerContext &) override;
    PropertySets visitEnforceSingleRowStep(const EnforceSingleRowStep &, DeterminerContext &) override;
    PropertySets visitAssignUniqueIdStep(const AssignUniqueIdStep & node, DeterminerContext & context) override;
    PropertySets visitCTERefStep(const CTERefStep &, DeterminerContext &) override;

private:
    static PropertySet single()
    {
        return {Property{Partitioning{Partitioning::Handle::SINGLE}, Partitioning{Partitioning::Handle::SINGLE}}};
    }
};

}

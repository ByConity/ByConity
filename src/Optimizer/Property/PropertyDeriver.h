#pragma once

#include <Optimizer/Property/Property.h>
#include <QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
class PropertyDeriver
{
public:
    static Property deriveProperty(ConstQueryPlanStepPtr step);
    static Property deriveProperty(ConstQueryPlanStepPtr step, Property & input_property);
    static Property deriveProperty(ConstQueryPlanStepPtr step, PropertySet & input_properties);
};

class DeriverContext
{
public:
    explicit DeriverContext(PropertySet input_properties_) : input_properties(std::move(input_properties_)) { }
    PropertySet getInput() { return input_properties; }

private:
    PropertySet input_properties;
};

class DeriverVisitor : public StepVisitor<Property, DeriverContext>
{
public:
    Property visitStep(const IQueryPlanStep &, DeriverContext &) override;

    Property visitProjectionStep(const ProjectionStep & step, DeriverContext & context) override;
    Property visitFilterStep(const FilterStep &, DeriverContext & context) override;
    Property visitJoinStep(const JoinStep & step, DeriverContext & context) override;
    Property visitAggregatingStep(const AggregatingStep & step, DeriverContext & context) override;
    Property visitMergingAggregatedStep(const MergingAggregatedStep &, DeriverContext & context) override;
    Property visitUnionStep(const UnionStep & step, DeriverContext & context) override;
    Property visitExceptStep(const ExceptStep &, DeriverContext & context) override;
    Property visitIntersectStep(const IntersectStep &, DeriverContext & context) override;
    Property visitExchangeStep(const ExchangeStep & step, DeriverContext & context) override;
    Property visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep &, DeriverContext & context) override;
    Property visitTableScanStep(const TableScanStep &, DeriverContext &) override;
    Property visitReadNothingStep(const ReadNothingStep &, DeriverContext &) override;
    Property visitValuesStep(const ValuesStep &, DeriverContext &) override;
    Property visitLimitStep(const LimitStep &, DeriverContext & context) override;
    Property visitLimitByStep(const LimitByStep &, DeriverContext & context) override;
    Property visitMergeSortingStep(const MergeSortingStep &, DeriverContext & context) override;
    Property visitPartialSortingStep(const PartialSortingStep &, DeriverContext & context) override;
    Property visitMergingSortedStep(const MergingSortedStep &, DeriverContext & context) override;
    Property visitDistinctStep(const DistinctStep &, DeriverContext & context) override;
    Property visitExtremesStep(const ExtremesStep &, DeriverContext & context) override;
    Property visitWindowStep(const WindowStep &, DeriverContext & context) override;
    Property visitApplyStep(const ApplyStep &, DeriverContext & context) override;
    Property visitEnforceSingleRowStep(const EnforceSingleRowStep &, DeriverContext & context) override;
    Property visitAssignUniqueIdStep(const AssignUniqueIdStep &, DeriverContext & context) override;
    Property visitCTERefStep(const CTERefStep &, DeriverContext & context) override;
};

}

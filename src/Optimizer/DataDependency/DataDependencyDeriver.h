#pragma once

#include <unordered_map>
#include <utility>
#include <Optimizer/DataDependency/DataDependency.h>
#include <Optimizer/DataDependency/DependencyUtils.h>
#include <QueryPlan/PlanVisitor.h>


namespace DB
{
class DataDependencyDeriver
{
public:
    static DataDependency
    deriveDataDependency(QueryPlanStepPtr step, CTEInfo & cte_info, ContextMutablePtr & context);
    static DataDependency deriveDataDependency(
        QueryPlanStepPtr step,
        DataDependency & input_property,
        CTEInfo & cte_info,
        ContextMutablePtr & context);
    static DataDependency deriveDataDependency(
        QueryPlanStepPtr step,
        DataDependencyVector & input_data_dependencies,
        CTEInfo & cte_info,
        ContextMutablePtr & context);
    static DataDependency
    deriveStorageDataDependency(const StoragePtr & storage, ContextMutablePtr & context);
};


class DataDependencyDeriverContext
{
public:
    DataDependencyDeriverContext(
        DataDependencyVector input_properties_, CTEInfo & cte_info_, ContextMutablePtr & context_)
        : input_data_dependencies(std::move(input_properties_)), cte_helper(cte_info_), context(context_)
    {
    }
    const DataDependencyVector & getInput() const
    {
        return input_data_dependencies;
    }

    auto & getCTEHelper()
    {
        return cte_helper;
    }

    ContextMutablePtr & getContext()
    {
        return context;
    }

private:
    DataDependencyVector input_data_dependencies;
    StepCTEVisitHelper<DataDependency, DataDependencyDeriverContext> cte_helper;
    ContextMutablePtr & context;
};

class DataDependencyDeriverVisitor : public StepVisitor<DataDependency, DataDependencyDeriverContext>
{
public:
    DataDependency visitStep(const IQueryPlanStep &, DataDependencyDeriverContext &) override;

    DataDependency visitProjectionStep(const ProjectionStep & step, DataDependencyDeriverContext & context) override;
    DataDependency visitJoinStep(const JoinStep & step, DataDependencyDeriverContext & context) override;
    DataDependency visitTableScanStep(const TableScanStep &, DataDependencyDeriverContext &) override;
    DataDependency visitFilterStep(const FilterStep &, DataDependencyDeriverContext & context) override;
    DataDependency visitAggregatingStep(const AggregatingStep & step, DataDependencyDeriverContext & context) override;
    DataDependency visitUnionStep(const UnionStep & step, DataDependencyDeriverContext & context) override;
    DataDependency visitExchangeStep(const ExchangeStep & step, DataDependencyDeriverContext & context) override;
    DataDependency visitLimitStep(const LimitStep &, DataDependencyDeriverContext & context) override;
    DataDependency visitSortingStep(const SortingStep &, DataDependencyDeriverContext & context) override;
    DataDependency visitCTERefStep(const CTERefStep &, DataDependencyDeriverContext & context) override;

private:
    static void visitFilterExpression(const ConstASTPtr & filter, DataDependency & data_dependency, DataDependencyDeriverContext & context);
    
    // DataDependency visitMergingAggregatedStep(const MergingAggregatedStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitArrayJoinStep(const ArrayJoinStep & step, DataDependencyDeriverContext & context) override;
    // DataDependency visitMarkDistinctStep(const MarkDistinctStep & step, DataDependencyDeriverContext & context) override;
    // DataDependency visitExceptStep(const ExceptStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitIntersectStep(const IntersectStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitIntersectOrExceptStep(const IntersectOrExceptStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitRemoteExchangeSourceStep(const RemoteExchangeSourceStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitSortingStep(const SortingStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitMergeSortingStep(const MergeSortingStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitPartialSortingStep(const PartialSortingStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitMergingSortedStep(const MergingSortedStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitDistinctStep(const DistinctStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitExtremesStep(const ExtremesStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitWindowStep(const WindowStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitApplyStep(const ApplyStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitEnforceSingleRowStep(const EnforceSingleRowStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitAssignUniqueIdStep(const AssignUniqueIdStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitGlobalDecodeStep(const GlobalDecodeStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitExplainAnalyzeStep(const ExplainAnalyzeStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitTopNFilteringStep(const TopNFilteringStep &, DataDependencyDeriverContext & context) override;
    // DataDependency visitFillingStep(const FillingStep &, DataDependencyDeriverContext & context) override;
};

}

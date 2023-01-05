#pragma once

#include <QueryPlan/PlanVisitor.h>
#include <Optimizer/Equivalences.h>

#include <utility>

namespace DB
{
using SymbolEquivalences = Equivalences<String>;
using SymbolEquivalencesPtr = std::shared_ptr<SymbolEquivalences>;

class SymbolEquivalencesDeriver
{
public:
    static SymbolEquivalencesPtr deriveEquivalences(ConstQueryPlanStepPtr step, std::vector<SymbolEquivalencesPtr> children_equivalences);
};

class SymbolEquivalencesDeriverVisitor : public StepVisitor<SymbolEquivalencesPtr, std::vector<SymbolEquivalencesPtr>>
{
public:
    SymbolEquivalencesPtr visitStep(const IQueryPlanStep & step, std::vector<SymbolEquivalencesPtr> & c) override;
    SymbolEquivalencesPtr visitJoinStep(const JoinStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitFilterStep(const FilterStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitProjectionStep(const ProjectionStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitAggregatingStep(const AggregatingStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitExchangeStep(const ExchangeStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
};
}

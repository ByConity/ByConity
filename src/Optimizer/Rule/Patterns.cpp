#include <Optimizer/Rule/Patterns.h>
#include <QueryPlan/SortingStep.h>

namespace DB::Patterns
{

PatternPtr topN()
{
    return typeOf(IQueryPlanStep::Type::Sorting)
           ->matchingStep<SortingStep>([&](const SortingStep & s) { return s.getLimit() != 0; });
}
}

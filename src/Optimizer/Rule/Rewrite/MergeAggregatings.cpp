#include <Optimizer/Rule/Rewrite/MergeAggregatings.h>

#include <Optimizer/Rule/Pattern.h>
#include <Optimizer/Rule/Patterns.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/QueryPlan.h>

namespace DB
{

PatternPtr MergeAggregatings::getPattern() const
{
    return Patterns::aggregating()
        ->matchingStep<AggregatingStep>([](const AggregatingStep & step) { return step.getAggregates().empty(); })
        ->withSingle(Patterns::aggregating());
}

TransformResult MergeAggregatings::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto & parent = dynamic_cast<const AggregatingStep &>(*node->getStep().get());
    const auto & children = dynamic_cast<const AggregatingStep &>(*node->getChildren()[0]->getStep().get());

    NameSet children_group_keys{children.getKeys().begin(), children.getKeys().end()};

    bool all_contains = std::all_of(
        parent.getKeys().begin(), parent.getKeys().end(), [&](const auto & name) { return children_group_keys.contains(name); });
    if (all_contains)
    {
        return PlanNodeBase::createPlanNode(
            rule_context.context->nextNodeId(), parent.copy(rule_context.context), node->getChildren()[0]->getChildren());
    }

    return {};
}

}

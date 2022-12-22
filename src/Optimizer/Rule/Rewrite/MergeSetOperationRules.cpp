#include <Optimizer/MergeSetOperation.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/MergeSetOperationRules.h>

namespace DB
{
PatternPtr MergeUnionRule::getPattern() const
{
    return Patterns::unionn();
}

TransformResult MergeUnionRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    SetOperationMerge merge_operation(node, *rule_context.context);
    auto result = merge_operation.merge();

    if (result)
        return result;
    else
        return {};
}

PatternPtr MergeExceptRule::getPattern() const
{
    return Patterns::except();
}

TransformResult MergeExceptRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    SetOperationMerge merge_operation(node, *rule_context.context);
    auto result = merge_operation.mergeFirstSource();

    if (result)
        return result;
    else
        return {};
}

PatternPtr MergeIntersectRule::getPattern() const
{
    return Patterns::intersect();
}

TransformResult MergeIntersectRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    SetOperationMerge merge_operation(node, *rule_context.context);
    auto result = merge_operation.merge();

    if (result)
        return result;
    else
        return {};
}
}

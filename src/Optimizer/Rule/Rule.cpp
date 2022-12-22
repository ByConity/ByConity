#include <Optimizer/Rule/Rule.h>

namespace DB
{
TransformResult Rule::transform(const PlanNodePtr & node, RuleContext & context)
{
    auto match_opt = getPattern()->match(node);

    if (match_opt)
        return transformImpl(node, match_opt->captures, context);
    else
        return {};
}

TransformResult TransformResult::of(const std::optional<PlanNodePtr> & plan_)
{
    if (plan_)
        return {plan_.value()};
    else
        return {};
}
}

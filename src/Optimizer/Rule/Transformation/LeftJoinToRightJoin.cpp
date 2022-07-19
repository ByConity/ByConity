#include <Optimizer/Cascades/CascadesOptimizer.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Transformation/LeftJoinToRightJoin.h>
#include <QueryPlan/AnyStep.h>

namespace DB
{
PatternPtr LeftJoinToRightJoin::getPattern() const
{
    return Patterns::join()
        ->matchingStep<JoinStep>([&](const JoinStep & s) { return supportSwap(s); })
        ->with({Patterns::any(), Patterns::any()});
}

TransformResult LeftJoinToRightJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    if (!rule_context.context->getSettingsRef().enable_left_join_to_right_join)
    {
        return {};
    }

    auto *old_join_node = dynamic_cast<JoinNode *>(node.get());
    if (!old_join_node)
        return {};
    const auto & step = *old_join_node->getStep();

    DataStreams streams = {step.getInputStreams()[1], step.getInputStreams()[0]};
    auto join_step = std::make_shared<JoinStep>(
        streams,
        step.getOutputStream(),
        ASTTableJoin::Kind::Right,
        step.getStrictness(),
        step.getRightKeys(),
        step.getLeftKeys(),
        step.getFilter(),
        step.isHasUsing(),
        step.getRequireRightKeys(),
        step.getAsofInequality(),
        step.getDistributionType());
    PlanNodePtr join_node = std::make_shared<JoinNode>(
        rule_context.context->nextNodeId(), std::move(join_step), PlanNodes{node->getChildren()[1], node->getChildren()[0]});

    return {join_node};
}
}

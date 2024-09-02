#pragma once

#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{

/**
 * Semi/Anti and other Join Reorder
 * Semi(Join(A, B), C) => Join(Semi(A, C), B)
 *
 *       Semi-Join                 Join
 *       /       \               /      \
 *     Join       C  ===>    Semi-Join   B
 *    /    \                 /       \
 *   A      B               A        C
 */
class SemiJoinPushDown : public Rule
{
public:
    RuleType getType() const override { return RuleType::SEMI_JOIN_PUSH_DOWN; }
    String getName() const override { return "SEMI_JOIN_PUSH_DOWN"; }
    bool isEnabled(ContextPtr context) const override {return context->getSettingsRef().enable_semi_join_push_down; }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

/**
 * Transforms:
 * <pre>
 * - Left Semi Join
 *     - Projection
 *         - X
 *     - Y
 * </pre>
 * Into:
 * <pre>
 * - Projection
 *     - Left Semi Join
 *         - X
 *         - Y
 * </pre>
 */
class SemiJoinPushDownProjection : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::SEMI_JOIN_PUSH_DOWN_PROJECTION;
    }
    String getName() const override
    {
        return "SEMI_JOIN_PUSH_DOWN_PROJECTION";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_semi_join_push_down;
    }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

/**
 * Transforms:
 * <pre>
 * - Left Semi Join
 *     - Aggregate
 *         - X
 *     - Y
 * </pre>
 * Into:
 * <pre>
 * - Aggregate
 *     - Left Semi Join
 *         - X
 *         - Y
 * </pre>
 */
class SemiJoinPushDownAggregate : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::SEMI_JOIN_PUSH_DOWN_AGGREAGTE;
    }
    String getName() const override
    {
        return "SEMI_JOIN_PUSH_DOWN_AGGREAGTE";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_semi_join_push_down;
    }
    ConstRefPatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};
}

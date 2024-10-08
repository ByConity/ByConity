#pragma once

#include <Common/Logger.h>
#include <Optimizer/Rule/Rule.h>

namespace DB
{
/**
 * Pull projection for join graph rewrite.
 *
 * Transforms:
 * <pre>
 * - Union
 *     - Inner Join
 *         - X
 *         - Z
 *     - Inner Join
 *         - Y
 *         - Z
 * </pre>
 * Into:
 * - Inner Join
 *     - Union
 *         - X
 *         - Y
 *     - Z
 * <pre>
 * </pre>
 */
class PushUnionThroughJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_UNION_THROUGH_JOIN; }
    String getName() const override { return "PUSH_UNION_THROUGH_JOIN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_union_through_join; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

    LoggerPtr log = getLogger("PushUnionThroughJoin");
};

class PushUnionThroughProjection : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_UNION_THROUGH_PROJECTION; }
    String getName() const override { return "PUSH_UNION_THROUGH_PROJECTION"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_union_through_projection; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushUnionThroughAgg : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_UNION_THROUGH_AGG; }
    String getName() const override { return "PUSH_UNION_THROUGH_AGG"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_union_through_agg; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}

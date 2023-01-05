#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

/**
 * Pull projection for join graph rewrite.
 *
 * Transforms:
 * <pre>
 * - Inner Join / Left Join X
 *     - Projection
 *         - Join Y
 *     - Z
 * </pre>
 * Into:
 * <pre>
 * - Projection
 *     - Inner Join / Left Join X
 *         - Join Y
 *         - Z
 * </pre>
 */
class PullProjectionOnJoinThroughJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::PULL_PROJECTION_ON_JOIN_THROUGH_JOIN; }
    String getName() const override { return "PULL_PROJECTION_ON_JOIN_THROUGH_JOIN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}


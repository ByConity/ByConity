#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class PushLimitIntoDistinct: public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_INTO_DISTINCT; }
    String getName() const override { return "PUSH_LIMIT_INTO_DISTINCT"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushLimitThroughProjection: public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_THROUGH_PROJECTION; }
    String getName() const override { return "PUSH_LIMIT_THROUGH_PROJECTION"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushLimitThroughExtremesStep : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_THROUGH_EXTREMES; }
    String getName() const override { return "PUSH_LIMIT_THROUGH_EXTREMES"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};


/**
 * Transforms:
 * <pre>
 * - Limit
 *    - Union
 *       - relation1
 *       - relation2
 *       ..
 * </pre>
 * Into:
 * <pre>
 * - Limit
 *    - Union
 *       - Limit
 *          - relation1
 *       - Limit
 *          - relation2
 *       ..
 * </pre>
 * Applies to LimitNode without ties only to avoid optimizer loop.
 */
class PushLimitThroughUnion: public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_THROUGH_UNION; }
    String getName() const override { return "PUSH_LIMIT_THROUGH_UNION"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

/**
 * Transforms:
 * <pre>
 * - Limit
 *    - Join
 *       - left source
 *       - right source
 * </pre>
 * Into:
 * <pre>
 * - Limit
 *    - Join
 *       - Limit (present if Join is left or outer)
 *          - left source
 *       - Limit (present if Join is right or outer)
 *          - right source
 * </pre>
 * Applies to LimitNode without ties only to avoid optimizer loop.
 */
class PushLimitThroughOuterJoin: public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_LIMIT_THROUGH_OUTER_JOIN; }
    String getName() const override { return "PUSH_LIMIT_THROUGH_OUTER_JOIN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}


#pragma once
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/Assignment.h>
#include <Optimizer/Rule/Rule.h>

namespace DB
{

/**
 * Inlines expressions from a child project node into a parent project node
 * as long as they are all identity or not used, or they are simple constants,
 * or they are referenced only once (to avoid introducing duplicate computation)
 * and the references don't appear within a TRY block (to avoid changing semantics).
 */
class InlineProjections : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_PROJECTION; }
    String getName() const override { return "INLINE_PROJECTION"; }

    PatternPtr getPattern() const override;
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    static std::optional<PlanNodePtr> inlineProjections(PlanNodePtr & parent, PlanNodePtr & child, ContextMutablePtr & context);

private:
    static std::set<String> extractInliningTargets(ProjectionNode * parent, ProjectionNode * child, ContextMutablePtr & context);
    static ASTPtr inlineReferences(const ConstASTPtr & expression, Assignments & assignments);

};

class InlineProjectionIntoJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_PROJECTION_INTO_JOIN; }
    String getName() const override { return "INLINE_PROJECTION_INTO_JOIN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class InlineProjectionOnJoinIntoJoin : public Rule
{
public:
    RuleType getType() const override { return RuleType::INLINE_PROJECTION_ON_JOIN_INTO_JOIN; }
    String getName() const override { return "INLINE_PROJECTION_ON_JOIN_INTO_JOIN"; }

    PatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}

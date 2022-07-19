#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB {

class CommonPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::COMMON_PREDICATE_REWRITE; }
    String getName() const override { return "COMMON_PREDICATE_REWRITE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SwapPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SWAP_PREDICATE_REWRITE; }
    String getName() const override { return "SWAP_PREDICATE_REWRITE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_PREDICATE_REWRITE; }
    String getName() const override { return "SIMPLIFY_PREDICATE_REWRITE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class UnWarpCastInPredicateRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::UN_WARP_CAST_IN_PREDICATE_REWRITE; }
    String getName() const override { return "UN_WARP_CAST_IN_PREDICATE_REWRITE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyJoinFilterRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_JOIN_FILTER_REWRITE; }
    String getName() const override { return "SIMPLIFY_JOIN_FILTER_REWRITE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class SimplifyExpressionRewriteRule : public Rule
{
public:
    RuleType getType() const override { return RuleType::SIMPLIFY_EXPRESSION_REWRITE; }
    String getName() const override { return "SIMPLIFY_EXPRESSION_REWRITE"; }

    PatternPtr getPattern() const override;

protected:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

}


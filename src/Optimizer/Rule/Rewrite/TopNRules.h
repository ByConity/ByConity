#pragma once
#include <Optimizer/Rule/Rule.h>

namespace DB
{

class CreateTopNFilteringForAggregating : public Rule
{
public:
    RuleType getType() const override { return RuleType::CREATE_TOPN_FILTERING_FOR_AGGREGATING; }
    String getName() const override { return "CREATE_TOPN_FILTERING_FOR_AGGREGATING"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_create_topn_filtering_for_aggregating;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushTopNThroughProjection : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_TOPN_THROUGH_PROJECTION; }
    String getName() const override { return "PUSH_TOPN_THROUGH_PROJECTION"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_push_topn_through_projection;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushTopNFilteringThroughProjection : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_TOPN_FILTERING_THROUGH_PROJECTION; }
    String getName() const override { return "PUSH_TOPN_FILTERING_THROUGH_PROJECTION"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_push_topn_filtering_through_projection;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushTopNFilteringThroughUnion : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::PUSH_TOPN_FILTERING_THROUGH_UNION;
    }
    String getName() const override
    {
        return "PUSH_TOPN_FILTERING_THROUGH_UNION";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_push_topn_filtering_through_union;
    }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};
}

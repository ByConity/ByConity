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

    bool excludeIfTransformSuccess() const override
    {
        return true;
    } // should only apply once
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class CreateTopNFilteringForDistinct : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::CREATE_TOPN_FILTERING_FOR_DISTINCT;
    }
    String getName() const override
    {
        return "CREATE_TOPN_FILTERING_FOR_DISTINCT";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_create_topn_filtering_for_aggregating;
    }
    ConstRefPatternPtr getPattern() const override;

    bool excludeIfTransformSuccess() const override
    {
        return true;
    } // should only apply once
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class CreateTopNFilteringForAggregatingLimit : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::CREATE_TOPN_FILTERING_FOR_AGGREGATING_LIMIT;
    }
    String getName() const override
    {
        return "CREATE_TOPN_FILTERING_FOR_AGGREGATING_LIMIT";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_create_topn_filtering_for_aggregating;
    }
    ConstRefPatternPtr getPattern() const override;

    bool excludeIfTransformSuccess() const override
    {
        return true;
    } // should only apply once
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class CreateTopNFilteringForDistinctLimit : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::CREATE_TOPN_FILTERING_FOR_DISTINCT_LIMIT;
    }
    String getName() const override
    {
        return "CREATE_TOPN_FILTERING_FOR_DISTINCT_LIMIT";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_create_topn_filtering_for_aggregating;
    }
    ConstRefPatternPtr getPattern() const override;

    bool excludeIfTransformSuccess() const override
    {
        return true;
    } // should only apply once
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

class PushSortThroughProjection : public PushTopNThroughProjection
{
public:
    RuleType getType() const override { return RuleType::PUSH_SORT_THROUGH_PROJECTION; }
    String getName() const override { return "PUSH_SORT_THROUGH_PROJECTION"; }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().enable_push_sort_through_projection;
    }
    ConstRefPatternPtr getPattern() const override
    {
        static auto pattern = Patterns::sorting().withSingle(Patterns::project()).result();
        return pattern;
    }

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

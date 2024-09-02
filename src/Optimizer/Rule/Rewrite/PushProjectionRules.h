#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class PushProjectionThroughFilter : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PROJECTION_THROUGH_FILTER; }
    String getName() const override { return "PUSH_PROJECTION_THROUGH_FILTER"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_projection; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};

class PushProjectionThroughProjection : public Rule
{
public:
    RuleType getType() const override { return RuleType::PUSH_PROJECTION_THROUGH_PROJECTION; }
    String getName() const override { return "PUSH_PROJECTION_THROUGH_PROJECTION"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_push_projection; }
    ConstRefPatternPtr getPattern() const override;

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};
}

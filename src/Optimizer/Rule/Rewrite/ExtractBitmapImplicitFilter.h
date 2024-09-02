#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

// see also optimizeBitMapParametersToWhere
class ExtractBitmapImplicitFilter : public Rule
{
public:
    RuleType getType() const override
    {
        return RuleType::EXTRACT_BITMAP_IMPLICIT_FILTER;
    }
    String getName() const override
    {
        return "EXTRACT_BITMAP_IMPLICIT_FILTER";
    }
    bool isEnabled(ContextPtr context) const override
    {
        return context->getSettingsRef().extract_bitmap_implicit_filter;
    }
    bool excludeIfTransformSuccess() const override
    {
        return true;
    }
    bool excludeIfTransformFailure() const override
    {
        return true;
    }
    ConstRefPatternPtr getPattern() const override;
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;

private:
    constexpr static const char * aggregate_capture = "aggregate";
};

}

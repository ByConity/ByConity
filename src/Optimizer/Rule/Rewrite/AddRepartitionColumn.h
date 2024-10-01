#pragma once
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rule.h>

namespace DB
{

class AddRepartitionColumn : public Rule
{
public:
    RuleType getType() const override { return RuleType::ADD_REPARTITION_COLUMN; }
    String getName() const override { return "ADD_REPARTITION_COLUMN"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_bucket_shuffle; }
    ConstRefPatternPtr getPattern() const override
    {
        static auto pattern = Patterns::exchange()
                                  .matchingStep<ExchangeStep>([&](const ExchangeStep & step) {
                                      return step.getSchema().isExchangeSchema(true) && !step.getSchema().isSimpleExchangeSchema(true);
                                  })
                                  .result();
        return pattern;
    }

    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
};
}

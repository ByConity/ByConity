#include <Optimizer/Rule/Rewrite/OptimizeAggregateRules.h>

#include <Optimizer/Rule/Patterns.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/MergingAggregatedStep.h>

namespace DB
{
bool OptimizeMemoryEfficientAggregation::isConvertibleToTwoLevel(const Aggregator::Params & params)
{
    Aggregator::ChooseMethodOption option{
        .header = (params.src_header ? params.src_header : params.intermediate_header),
        .keys = params.keys,
        .enable_lc_group_by_opt = params.enable_lc_group_by_opt};
    AggregatedDataVariants::Type method_chosen;
    Sizes key_sizes;
    Aggregator::chooseAggregationMethodByOption(option, key_sizes, method_chosen);

    switch (method_chosen)
    {
#define M(NAME) \
    case AggregatedDataVariants::Type::NAME: \
        return true;
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
        default:
            return false;
    }
}

ConstRefPatternPtr OptimizeMemoryEfficientAggregation::getPattern() const
{
    static auto pattern = Patterns::mergingAggregated()
        .withSingle(Patterns::exchange().withSingle(Patterns::aggregating().matchingStep<AggregatingStep>([](const AggregatingStep & agg) {
            return agg.isPartial() && !agg.shouldProduceResultsInOrderOfBucketNumber() && isConvertibleToTwoLevel(agg.getParams());
        })))
        .result();
    return pattern;
}

TransformResult OptimizeMemoryEfficientAggregation::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    auto & final_agg_step = dynamic_cast<MergingAggregatedStep &>(*node->getStep());
    final_agg_step.setShouldProduceResultsInOrderOfBucketNumber(true);
    final_agg_step.setMemoryEfficientAggregation(true);

    auto exchange_node = node->getChildren()[0];
    auto & exchange_step = dynamic_cast<ExchangeStep &>(*exchange_node->getStep());
    exchange_step.setKeepOrder(true);

    auto partial_aggregate_node = exchange_node->getChildren()[0];
    auto & partial_aggregate_step = dynamic_cast<AggregatingStep &>(*partial_aggregate_node->getStep());
    partial_aggregate_step.setShouldProduceResultsInOrderOfBucketNumber(true);
    return {};
}
}

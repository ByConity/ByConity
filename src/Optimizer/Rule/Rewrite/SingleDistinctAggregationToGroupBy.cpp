#include <unordered_set>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/SingleDistinctAggregationToGroupBy.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/DistinctStep.h>

namespace DB
{

ConstRefPatternPtr SingleDistinctAggregationToGroupBy::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStep>([](const AggregatingStep & s) {
            if (!s.isNormal())
            {
                return false;
            }
            return allDistinctAggregates(s) && hasSingleDistinctInput(s) && noMasks(s) && allCountHasAtMostOneArguments(s);
        })
        .result();
    return pattern;
}

const std::set<String> SingleDistinctAggregationToGroupBy::distinct_func{
    "uniqexact", "countdistinct", "avgdistinct", "maxdistinct", "mindistinct", "sumdistinct"};

const std::unordered_map<String, String> SingleDistinctAggregationToGroupBy::distinct_func_normal_func{
    {"uniqexact", "count"},
    {"countdistinct", "count"},
    {"avgdistinct", "avg"},
    {"maxdistinct", "max"},
    {"mindistinct", "min"},
    {"sumdistinct", "sum"}};

bool SingleDistinctAggregationToGroupBy::allDistinctAggregates(const AggregatingStep & s)
{
    for (const auto & agg : s.getAggregates())
    {
        if (!distinct_func.contains(Poco::toLower(agg.function->getName())))
            return false;
    }
    return true;
}

bool SingleDistinctAggregationToGroupBy::hasSingleDistinctInput(const AggregatingStep & s)
{
    std::set<Names> names;
    for (const auto & agg : s.getAggregates())
    {
        names.insert(agg.argument_names);
    }
    return names.size() == 1;
}

bool SingleDistinctAggregationToGroupBy::noMasks(const AggregatingStep & s)
{
    for (const auto & agg : s.getAggregates())
    {
        if (!agg.mask_column.empty())
            return false;
    }
    return true;
}

bool SingleDistinctAggregationToGroupBy::allCountHasAtMostOneArguments(const AggregatingStep & s)
{
    for (const auto & agg : s.getAggregates())
    {
        if (Poco::toLower(agg.function->getName()) == "uniqexact" || Poco::toLower(agg.function->getName()) == "countdistinct")
        {
            if (agg.argument_names.size() > 1)
                return false;   
        }
    }
    return true;
}

TransformResult SingleDistinctAggregationToGroupBy::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto step_ptr = node->getStep();
    const auto & step = dynamic_cast<const AggregatingStep &>(*step_ptr);

    // insert a extra Group-by Aggregate, perform distinct operation
    NameSet distinct_keys;
    Names keys;
    for (const auto & symbol : step.getKeys())
        if (distinct_keys.emplace(symbol).second)
            keys.emplace_back(symbol);
    for (const auto & symbol : step.getAggregates()[0].argument_names)
        if (distinct_keys.emplace(symbol).second)
            keys.emplace_back(symbol);

    AggregateDescriptions aggregate_descriptions;
    auto group_by_step = std::make_shared<AggregatingStep>(
        node->getChildren()[0]->getStep()->getOutputStream(),
        keys,
        step.getKeysNotHashed(),
        aggregate_descriptions,
        GroupingSetsParamsList{},
        true);
    auto group_by_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(group_by_step), node->getChildren());

    // remove distinct from original Aggregate
    AggregateDescriptions remove_distinct_agg_descs;
    for (const auto & agg : step.getAggregates())
    {
        auto remove_distinct_agg_desc = agg;
        String fun_remove_distinct = distinct_func_normal_func.at(Poco::toLower(agg.function->getName()));
        DataTypes arg_types = agg.function->getArgumentTypes();
        AggregateFunctionProperties properties;
        remove_distinct_agg_desc.function = AggregateFunctionFactory::instance().get(fun_remove_distinct, arg_types, {}, properties);
        remove_distinct_agg_descs.emplace_back(remove_distinct_agg_desc);
    }

    auto remove_distinct_agg_step = std::make_shared<AggregatingStep>(
        group_by_node->getStep()->getOutputStream(),
        step.getKeys(),
        step.getKeysNotHashed(),
        remove_distinct_agg_descs,
        GroupingSetsParamsList{},
        true);
    remove_distinct_agg_step->setHints(step.getHints());
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(remove_distinct_agg_step), {group_by_node});
}

}

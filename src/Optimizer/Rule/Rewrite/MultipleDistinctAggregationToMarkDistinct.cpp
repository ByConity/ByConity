#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/NameToType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/MultipleDistinctAggregationToMarkDistinct.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/MarkDistinctStep.h>

namespace DB
{

const std::set<String> MultipleDistinctAggregationToMarkDistinct::distinct_func{
    "uniqexact", "countdistinct", "avgdistinct", "maxdistinct", "mindistinct", "sumdistinct"};

const std::unordered_map<String, String> MultipleDistinctAggregationToMarkDistinct::distinct_func_normal_func{
    {"uniqexact", "countIf"},
    {"countdistinct", "countIf"},
    {"avgdistinct", "avgIf"},
    {"maxdistinct", "maxIf"},
    {"mindistinct", "minIf"},
    {"sumdistinct", "sumIf"}};

bool MultipleDistinctAggregationToMarkDistinct::hasNoDistinctWithFilterOrMask(const AggregatingStep & step)
{
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())) && !agg_desc.mask_column.empty())
            return false;
    }
    return true;
}

bool MultipleDistinctAggregationToMarkDistinct::hasMultipleDistincts(const AggregatingStep & step)
{
    std::set<Names> multiple_distinct_aggs;
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())))
            multiple_distinct_aggs.emplace(agg_desc.argument_names);
    }
    return multiple_distinct_aggs.size() > 1;
}

bool MultipleDistinctAggregationToMarkDistinct::hasMixedDistinctAndNonDistincts(const AggregatingStep & step)
{
    size_t distinct_aggs = 0;
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())))
        {
            distinct_aggs++;
        }
    }
    return distinct_aggs > 0 && distinct_aggs < agg_descs.size();
}

ConstRefPatternPtr MultipleDistinctAggregationToMarkDistinct::getPattern() const
{
    static auto pattern = Patterns::aggregating().matchingStep<AggregatingStep>([](const AggregatingStep & s) {
        return hasNoDistinctWithFilterOrMask(s) && (hasMultipleDistincts(s) || hasMixedDistinctAndNonDistincts(s));
    }).result();
    return pattern;
}

TransformResult MultipleDistinctAggregationToMarkDistinct::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto step_ptr = node->getStep();
    const auto & step = dynamic_cast<const AggregatingStep &>(*step_ptr);
    const AggregateDescriptions & agg_descs = step.getAggregates();

    // the distinct marker for the given set of input columns
    std::map<std::set<String>, String> markers;

    AggregateDescriptions new_agg_descs;
    PlanNodePtr child = node->getChildren()[0];

    Assignments new_argument_assignments;

    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())) && agg_desc.mask_column.empty())
        {
            String marker;

            std::set<String> names;
            String last_name;
            for (const auto & name : agg_desc.argument_names)
            {
                names.emplace(name);
                last_name = name;
            }
            if (markers.contains(names))
            {
                marker = markers.at(names);
            }
            else
            {
                marker = rule_context.context->getSymbolAllocator()->newSymbol(last_name + "$distinct");
                markers[names] = marker;

                std::set<String> distinct_symbols;
                for (const auto & key : step.getKeys())
                {
                    distinct_symbols.emplace(key);
                }
                for (const auto & marker_name : names)
                {
                    distinct_symbols.emplace(marker_name);
                }

                std::vector<String> distinct_symbols_list(distinct_symbols.begin(), distinct_symbols.end());

                auto mark_distinct_step
                    = std::make_shared<MarkDistinctStep>(child->getStep()->getOutputStream(), marker, distinct_symbols_list);
                child = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(mark_distinct_step), PlanNodes{child});
            }

            // remove the distinct flag and set the distinct marker
            String fun_remove_distinct = distinct_func_normal_func.at(Poco::toLower(agg_desc.function->getName()));
            Names argument_names;
            DataTypes data_types;
            if (fun_remove_distinct == "countIf" && agg_desc.argument_names.size() > 1)
            {
                // countDistinct(arg1, arg2) cannot convert to count(arg1, arg2), because clickhousedon't support count multi arguments.
                // As an alternative we can rewrite it to count(IF(arg1 is null, null, arg2 is null, null, 1)),
                // or sum((arg1 is not null) AND (arg2 is not null))
                fun_remove_distinct = "sumIf";

                ASTs argument_functions;
                for (const auto & argument : agg_desc.argument_names)
                    argument_functions.emplace_back(makeASTFunction("isNotNull", makeASTIdentifier(argument)));
                auto new_argument = PredicateUtils::combineConjuncts(argument_functions);
                auto new_argument_name = rule_context.context->getSymbolAllocator()->newSymbol(new_argument);
                new_argument_assignments.emplace_back(new_argument_name, new_argument);

                argument_names.emplace_back(new_argument_name);
                data_types.emplace_back(std::make_shared<DataTypeUInt8>());
            }
            else
            {
                argument_names = agg_desc.argument_names;
                data_types = agg_desc.function->getArgumentTypes();
            }
            
            argument_names.emplace_back(marker);
            data_types.emplace_back(std::make_shared<DataTypeUInt8>());

            Array parameters = agg_desc.function->getParameters();
            AggregateFunctionProperties properties;
            AggregateFunctionPtr new_agg_fun
                = AggregateFunctionFactory::instance().get(fun_remove_distinct, data_types, parameters, properties);

            AggregateDescription new_agg_desc;

            new_agg_desc.mask_column = marker;
            new_agg_desc.function = new_agg_fun;
            new_agg_desc.parameters = agg_desc.parameters;
            new_agg_desc.column_name = agg_desc.column_name;
            new_agg_desc.argument_names = argument_names;
            new_agg_desc.parameters = agg_desc.parameters;
            new_agg_desc.arguments = agg_desc.arguments;

            new_agg_descs.emplace_back(new_agg_desc);
        }
        else
        {
            new_agg_descs.emplace_back(agg_desc);
        }
    }

    if (!new_argument_assignments.empty())
    {
        NameToType name_to_type;
        for (const auto & assignment : new_argument_assignments)
            name_to_type.emplace(assignment.first, std::make_shared<DataTypeUInt8>());

        for (const auto & input : child->getStep()->getOutputStream().header)
        {
            new_argument_assignments.emplace(input.name, makeASTIdentifier(input.name));
            name_to_type.emplace(input.name, input.type);
        }
        auto new_argument_projection_step
            = std::make_shared<ProjectionStep>(child->getStep()->getOutputStream(), new_argument_assignments, name_to_type);
        child = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(new_argument_projection_step), {child});
    }

    auto count_agg_step = std::make_shared<AggregatingStep>(
        child->getStep()->getOutputStream(),
        step.getKeys(),
        step.getKeysNotHashed(),
        new_agg_descs,
        step.getGroupingSetsParams(),
        step.isFinal(),
        step.getStagePolicy(),
        step.getGroupBySortDescription(),
        step.getGroupings(),
        step.needOverflowRow(),
        step.shouldProduceResultsInOrderOfBucketNumber(),
        step.isNoShuffle(),
        step.isStreamingForCache(),
        step.getHints());
    auto count_agg_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(count_agg_step), {child});
    return count_agg_node;
}
}

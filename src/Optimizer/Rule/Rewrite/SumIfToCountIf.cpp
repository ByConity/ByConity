#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/SumIfToCountIf.h>
#include <QueryPlan/AggregatingStep.h>
#include <common/types.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{

ConstRefPatternPtr SumIfToCountIf::getPattern() const
{
    static auto pattern = Patterns::aggregating().withSingle(Patterns::project()).result();
    return pattern;
}

TransformResult SumIfToCountIf::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto & projection_node = node->getChildren()[0];

    auto & agg_step = dynamic_cast<AggregatingStep &>(*node->getStep());
    auto & projection_step = dynamic_cast<ProjectionStep &>(*projection_node->getStep());

    auto aggregates = agg_step.getAggregates();
    
    auto assignments = projection_step.getAssignments();
    auto name_to_type = projection_step.getNameToType();

    auto bottom_name_to_type = projection_node->getChildren()[0]->getOutputNamesToTypes();

    bool need_reconstruct_projection = false, need_reconstruct_aggregation = false;

    for (auto & aggregate : aggregates)
    {
        auto lower_name = Poco::toLower(aggregate.function->getName());
        auto func_arguments = aggregate.argument_names;
        // Rewrite `sumIf(1, cond)` into `countIf(cond)`
        if (lower_name == "sumif")
        {
            const auto * literal = assignments.at(func_arguments[0])->as<ASTLiteral>();
            if (!literal || !DB::isInt64OrUInt64FieldType(literal->value.getType()))
                continue;

            if (func_arguments.size() == 2 && literal->value.get<UInt64>() == 1)
            {
                auto cond_type = ExpressionInterpreter::optimizeExpression(assignments.at(func_arguments[1]), bottom_name_to_type, rule_context.context).first;

                need_reconstruct_aggregation = true;
                AggregateFunctionProperties properties;
                aggregate.function = AggregateFunctionFactory::instance().get("countIf", DataTypes{name_to_type.at(func_arguments[1])}, aggregate.parameters, properties);
                aggregate.argument_names = Names{func_arguments[1]};
            }
        }
        // Rewrite `sum(if(cond, 1, 0))` into `countIf(cond)`.
        // Rewrite `sum(if(cond, 0, 1))` into `countIf(not(cond))` if cond is not Nullable (otherwise the result can be different).
        else if (lower_name == "sum")
        {
            const auto * nested_func = assignments.at(func_arguments[0])->as<ASTFunction>();

            if (!nested_func || Poco::toLower(nested_func->name) != "if" || nested_func->arguments->children.size() != 3)
                continue;

            // e.g. sum(if(cond, 1, 0)) -> countIf(cond) deduce by steps:
            // aggregating: sum(`expr#if`) -> sum(`new_cond_name`)
            // projection: `expr#if` := if(cond, 1, 0) -> `new_cond_name` := cond
            const auto & if_arguments = nested_func->arguments->children;

            const auto * first_literal = if_arguments[1]->as<ASTLiteral>();
            const auto * second_literal = if_arguments[2]->as<ASTLiteral>();

            if (first_literal && second_literal)
            {
                if (!DB::isInt64OrUInt64FieldType(first_literal->value.getType()) || !DB::isInt64OrUInt64FieldType(second_literal->value.getType()))
                    continue;

                auto first_value = first_literal->value.get<UInt64>();
                auto second_value = second_literal->value.get<UInt64>();
                if (first_value + second_value != 1)
                    continue;


                ASTPtr new_cond;
                auto cond_type = ExpressionInterpreter::optimizeExpression(if_arguments[0], bottom_name_to_type, rule_context.context).first;

                /// sum(if(cond, 1, 0)) -> countIf(cond)
                if (first_value == 1 && second_value == 0)
                {
                    new_cond = if_arguments[0];
                }
                /// sum(if(cond, 0, 1)) -> countIf(not(cond)) if cond is not Nullable (otherwise the result can be different)
                else if (first_value == 0 && second_value == 1 && !cond_type->isNullable())
                {
                    new_cond = makeASTFunction("not", if_arguments[0]);
                }

                if (new_cond)
                {
                    need_reconstruct_aggregation = true;
                    need_reconstruct_projection = true;

                    String new_cond_name = rule_context.context->getSymbolAllocator()->newSymbol(new_cond);

                    AggregateFunctionProperties properties;
                    aggregate.function = AggregateFunctionFactory::instance().get("countIf", DataTypes{cond_type}, aggregate.parameters, properties);
                    aggregate.argument_names = Names{new_cond_name};

                    assignments.erase(func_arguments[0]);
                    name_to_type.erase(func_arguments[0]);

                    assignments.emplace(new_cond_name, new_cond);
                    name_to_type[new_cond_name] = cond_type;
                }
            }
        }
    }
    if (!need_reconstruct_aggregation)
        return node;

    auto new_projection_node = projection_node;

    if (need_reconstruct_projection)
    {
        auto new_projection_step = std::make_shared<ProjectionStep>(
            projection_node->getChildren()[0]->getCurrentDataStream(),
            assignments,
            name_to_type,
            projection_step.isFinalProject(),
            projection_step.isIndexProject(),
            projection_step.getHints());
        
        new_projection_node = PlanNodeBase::createPlanNode(projection_node->getId(), std::move(new_projection_step), projection_node->getChildren(), projection_node->getStatistics());
    }

    auto new_agg_step = std::make_shared<AggregatingStep>(
        new_projection_node->getCurrentDataStream(),
        agg_step.getKeys(),
        agg_step.getKeysNotHashed(),
        aggregates,
        agg_step.getGroupingSetsParams(),
        agg_step.isFinal(),
        AggregateStagePolicy::DEFAULT,
        agg_step.getGroupBySortDescription(),
        agg_step.getGroupings(),
        agg_step.needOverflowRow(),
        agg_step.shouldProduceResultsInOrderOfBucketNumber(),
        agg_step.isNoShuffle(),
        agg_step.isStreamingForCache(),
        agg_step.getHints());

    return PlanNodeBase::createPlanNode(node->getId(), std::move(new_agg_step), {new_projection_node}, node->getStatistics());
}

}

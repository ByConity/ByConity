#include <algorithm>
#include <iterator>
#include <vector>
#include <Optimizer/Rewriter/GroupByKeysPruning.h>
#include <Poco/Format.h>
#include <common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Optimizer/DataDependency/DataDependency.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/ConstantsDeriver.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPlan/AggregatingStep.h>
#include <Optimizer/LiteralEncoder.h>

namespace DB
{
bool GroupByKeysPruning::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    GroupByKeysPruning::Rewriter rewriter{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), rewriter, v);

    plan.update(result.plan);
    return true;
}

PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitPlanNode(PlanNodeBase & node, Void &)
{
    PlanNodes children;
    DataStreams inputs;
    Void require;
    DataDependencyVector input_data_dependencies;
    ConstantsSet input_constants;

    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, require);
        children.emplace_back(result.plan);
        inputs.push_back(result.plan->getStep()->getOutputStream());
        input_data_dependencies.emplace_back(result.data_dependency);
        input_constants.emplace_back(result.constants);
    }

    node.getStep()->setInputStreams(inputs);

    node.replaceChildren(children);
    DataDependency depend = DataDependencyDeriver::deriveDataDependency(
        node.getStep(), input_data_dependencies, cte_helper.getCTEInfo(), context);
    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), input_constants, cte_helper.getCTEInfo(), context);

    return {node.shared_from_this(), depend, constants};
}


PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitAggregatingNode(AggregatingNode & node, Void & v)
{
    if (node.getStep()->getKeys().empty() || !node.getStep()->getGroupBySortDescription().empty() || !node.getStep()->getGroupings().empty() || !node.getStep()->getGroupingSetsParams().empty())
        return visitPlanNode(node, v);

    auto result = VisitorUtil::accept(node.getChildren()[0], *this, v);

    auto agg_step = node.getStep();

    NameSet all_agg_argument_names;
    for (const auto & aggregator : agg_step->getAggregates())
            for (const auto & argument_name : aggregator.argument_names)
                all_agg_argument_names.insert(argument_name);

    NameSet new_keys_not_participate_in_calculating = agg_step->getKeysNotHashed();

    // if a key used in aggregator or also be simplified, it can't be simplified again.
    NameSet agg_keys;
    for (const String & name : agg_step->getKeys())
        if (!all_agg_argument_names.contains(name) && !new_keys_not_participate_in_calculating.contains(name))
            agg_keys.insert(name);

    // prune group by keys which is dependent.
    NameSet simplified_agg_keys = result.data_dependency.getFunctionalDependencies().simplify(agg_keys);
    if (!agg_keys.empty() && simplified_agg_keys.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "logic error in GroupByKeysPruning -> simplify by fd: simplified_agg_keys is empty!");

    // add back columns which not participate in simplify().
    for (const String & name :  agg_step->getKeys())
        if (all_agg_argument_names.contains(name) || new_keys_not_participate_in_calculating.contains(name))
            simplified_agg_keys.insert(name);

    if (NameSet(agg_step->getKeys().begin(), agg_step->getKeys().end()) != simplified_agg_keys)
    {
        std::string str;
        for (const auto & name : simplified_agg_keys)
            str += name + ",";
        LOG_INFO(getLogger("DataDependency"), "after GroupByKeysPruning by functional dependecy, new_agg_keys -- " + str + ". note we don't remove unused keys, but just add it to keys_not_hashed");
    }

    auto node_ptr = node.shared_from_this();
    auto constants_values = result.constants.getValues();


    AggregateDescriptions new_aggregators;
    for (const auto & aggregator : agg_step->getAggregates())
    {
        // agg_func(key) can be removed by constants.
        if (Poco::toLower(aggregator.function->getName()) == "any" && result.constants.contains(aggregator.argument_names[0]))
            continue;
        else
        {
            // Remove unreducible names from constants.
            for (const auto & argument_name : aggregator.argument_names)
                constants_values.erase(argument_name);
            new_aggregators.push_back(aggregator);
        }
    }

    // reserve original keys order, avoid unnecesary repartition.
    Names new_agg_keys = agg_step->getKeys();

    // prune constants group by keys.
    bool has_eliminated_agg_by_constants = false;
    if (!constants_values.empty())
    {
        std::erase_if(new_agg_keys, [&](const String & name){ return constants_values.contains(name); });
        // It is currently not possible to optimize all keys because sql syntax dictates that group by () must produce a single line of results (the expected behavior of rewrite here is no result without data).
        // TODO@lijinzhi.zx: If new_agg_keys are empty and aggregators are empty, we can simply remove the current agg.
        if (new_agg_keys.empty())
        {
            String one_of_simplified_key = agg_step->getKeys()[0];
            new_agg_keys.emplace_back(one_of_simplified_key);
            constants_values.erase(one_of_simplified_key);
        }

        if (new_agg_keys.size() < agg_step->getKeys().size())
        {
            has_eliminated_agg_by_constants = true;
            std::string str;
            for (const auto & name : new_agg_keys)
                str += name + ",";
            LOG_INFO(getLogger("DataDependency"), "after GroupByKeysPruning by constants, new_agg_keys -- " + str);
        }
    }

    // Insert agg keys which has been simplified to new_keys_not_participate_in_calculating.
    // Otherwise, if it belongs to constants values, we can simply remove it.
    for (const auto & agg_key : agg_step->getKeys())
    {
        if (!simplified_agg_keys.contains(agg_key) && !constants_values.contains(agg_key))
        {
            new_keys_not_participate_in_calculating.insert(agg_key);
        }
    }

    auto new_agg_step = std::make_shared<AggregatingStep>(
        node.getChildren()[0]->getStep()->getOutputStream(),
        new_agg_keys,
        new_keys_not_participate_in_calculating,
        new_aggregators,
        agg_step->getGroupingSetsParams(),
        agg_step->isFinal(),
        agg_step->getGroupBySortDescription(),
        agg_step->getGroupings(),
        false,
        agg_step->shouldProduceResultsInOrderOfBucketNumber(),
        agg_step->isNoShuffle(),
        agg_step->isStreamingForCache(),
        agg_step->getHints());
    node_ptr = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(new_agg_step), node.getChildren());


    // add new projection for outputing pruning constants.
    if (has_eliminated_agg_by_constants)
    {
        Assignments new_assignments;
        NameToType new_name_to_type;

        for (const auto & [name, type] : node_ptr->getCurrentDataStream().getNamesAndTypes())
        {
            new_assignments.emplace(name, std::make_shared<ASTIdentifier>(name));
            new_name_to_type[name] = type;
        }
        for (const auto & [name, literal] : constants_values)
        {
            // date/datetime should make a cast function
            // but nullable(UInt64) shouldn't make a cast function
            auto literal_ast = LiteralEncoder::encodeForComparisonExpr(literal.value, literal.type, context);
            new_assignments.emplace(name, std::move(literal_ast));
            new_name_to_type[name] = literal.type;
        }

        auto new_projection_step
            = std::make_shared<ProjectionStep>(node_ptr->getCurrentDataStream(), new_assignments, new_name_to_type);
        node_ptr = ProjectionNode::createPlanNode(context->nextNodeId(), std::move(new_projection_step), {node_ptr});
    }

    DataDependency data_dependency = DataDependencyDeriver::deriveDataDependency(
        node.getStep(), {result.data_dependency}, cte_helper.getCTEInfo(), context);
    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);

    return {node_ptr, data_dependency, constants};
}

PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitCTERefNode(CTERefNode & node, Void & c)
{
    auto result = cte_helper.accept(node.getStep()->getId(), *this, c);

    DataDependency data_dependency = DataDependencyDeriver::deriveDataDependency(
        node.getStep(), {result.data_dependency}, cte_helper.getCTEInfo(), context);

    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);

    return {node.shared_from_this(), data_dependency, constants};
}

// PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitJoinNode(JoinNode &, Void &)
// {

// }

// PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitTableScanNode(TableScanNode & node, Void & v)
// {

// }

// PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitFilterNode(FilterNode &, Void &)
// {

// }

// PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitUnionNode(UnionNode &, Void &)
// {

// }

// PlanAndDataDependencyWithConstants GroupByKeysPruning::Rewriter::visitExchangeNode(ExchangeNode &, Void &)
// {

// }

}

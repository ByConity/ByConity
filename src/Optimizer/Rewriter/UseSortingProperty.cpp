#include <algorithm>
#include <iterator>
#include <Optimizer/Rewriter/UseSortingProperty.h>

#include <Core/SortDescription.h>
#include <Optimizer/Property/ConstantsDeriver.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/PropertyDeriver.h>
#include <Optimizer/Property/PropertyMatcher.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolMapper.h>
#include <fmt/core.h>
#include <common/logger_useful.h>

namespace DB
{
bool SortingOrderedSource::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    SortingOrderedSource::Rewriter rewriter{context, plan.getCTEInfo()};
    SortDescription required;
    auto result = VisitorUtil::accept(plan.getPlanNode(), rewriter, required);

    PruneSortingInfoRewriter push_rewriter{context, plan.getCTEInfo()};
    SortInfo sort_info;
    auto plan_node = VisitorUtil::accept(result.plan, push_rewriter, sort_info);
    plan.update(plan_node);
    return true;
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitPlanNode(PlanNodeBase & node, SortDescription &)
{
    PlanNodes children;
    SortDescription required;
    PropertySet input_properties;
    ConstantsSet input_constants;
    for (const auto & child : node.getChildren())
    {
        auto result = VisitorUtil::accept(child, *this, required);
        children.emplace_back(result.plan);
        input_properties.emplace_back(result.property);
        input_constants.emplace_back(result.constants);
    }

    node.replaceChildren(children);
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), input_properties, any_prop, context);
    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), input_constants, cte_helper.getCTEInfo(), context);
    return {node.shared_from_this(), prop, constants};
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitSortingNode(SortingNode & node, SortDescription &)
{
    auto step = node.getStep();
    auto required = step->getSortDescription();
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, required);

    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);
    auto prefix_sorting = PropertyMatcher::matchSorting(*context, step->getSortDescription(), result.property.getSorting(), {}, constants);
    step->setPrefixDescription(prefix_sorting.toSortDesc());
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
    return {node.shared_from_this(), prop, constants};
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitAggregatingNode(AggregatingNode & node, SortDescription & required)
{
    const auto & settings = context->getSettingsRef();
    if (settings.optimize_aggregation_in_order /* && !settings.optimize_aggregate_function_type */)
    {
        auto step = node.getStep();

        SortDescription order_descr;
        order_descr.reserve(step->getKeys().size());
        for (const auto & name : step->getKeys())
        {
            order_descr.emplace_back(name, 1, 1);
        }

        PlanAndPropConstants result = VisitorUtil::accept(node.getChildren()[0], *this, order_descr);

        Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);
        auto prefix_sorting = PropertyMatcher::matchSorting(*context, order_descr, result.property.getSorting(), {}, constants);
        step->setGroupBySortDescription(prefix_sorting.toSortDesc());

        Property any_prop;
        Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
        return {node.shared_from_this(), prop, constants};
    }
    return visitPlanNode(node, required);
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitWindowNode(WindowNode & node, SortDescription & required)
{
#if 0
    if (context->getSettingsRef().optimize_read_in_window_order)
    {
        auto step = node.getStep();

        SortDescription order_descr;
        const auto & partition_by = step->getWindowDescription().partition_by;
        order_descr.insert(order_descr.end(), partition_by.begin(), partition_by.end());

        const auto & order_by = step->getWindowDescription().order_by;
        order_descr.insert(order_descr.end(), order_by.begin(), order_by.end());

        auto result = VisitorUtil::accept(node.getChildren()[0], *this, order_descr);

        Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);
        auto prefix_sorting = PropertyMatcher::matchSorting(*context, order_descr, result.property.getSorting(), {}, constants);
        step->setPrefixDescription(prefix_sorting.toSortDesc());

        Property any_prop;
        Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
        return {node.shared_from_this(), prop, constants};
    }
#endif

    return visitPlanNode(node, required);
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitCTERefNode(CTERefNode & node, SortDescription &)
{
    const auto * step = node.getStep().get();
    SortDescription required;
    auto cte_plan = cte_helper.acceptAndUpdate(step->getId(), *this, required, [](auto & result) { return result.plan; });
    return {node.shared_from_this(), Property{}, cte_plan.constants};
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitTopNFilteringNode(TopNFilteringNode & node, SortDescription &)
{
    auto & topn_filtering = node.getStep();
    auto required_sorting = topn_filtering->getSortDescription();

    auto result = VisitorUtil::accept(node.getChildren()[0], *this, required_sorting);

    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);
    auto prefix_sorting = PropertyMatcher::matchSorting(*context, required_sorting, result.property.getSorting(), {}, constants);
    if (prefix_sorting.size() == required_sorting.size())
        topn_filtering->setAlgorithm(TopNFilteringAlgorithm::Limit);

    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
    return {node.shared_from_this(), prop, constants};
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitTableScanNode(TableScanNode & node, SortDescription & required)
{
    auto & step = node.getStep();

    Property any_prop;
    any_prop.setSorting(Sorting{required});
    Property prop = PropertyDeriver::deriveProperty(step, context, any_prop);
    step->setReadOrder(prop.getSorting().translate(node.getStep()->getAliasToColumnMap()).toSortDesc());
    Constants constants = ConstantsDeriver::deriveConstants(step, cte_helper.getCTEInfo(), context);
    return {node.shared_from_this(), prop, constants};
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitFilterNode(FilterNode & node, SortDescription & required)
{
    auto result = VisitorUtil::accept(node.getChildren()[0], *this, required);
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);
    return {node.shared_from_this(), prop, constants};
}

PlanAndPropConstants SortingOrderedSource::Rewriter::visitProjectionNode(ProjectionNode & node, SortDescription & require)
{
    auto mappings = Utils::computeIdentityTranslations(node.getStep()->getAssignments());

    SortDescription push_down_sort_description;
    for (const auto & column : require)
    {
        if (!mappings.contains(column.column_name))
            break;
        push_down_sort_description.emplace_back(
            SortColumnDescription{mappings.at(column.column_name), column.direction, column.nulls_direction});
    }

    auto result = VisitorUtil::accept(node.getChildren()[0], *this, push_down_sort_description);
    
    Property any_prop;
    Property prop = PropertyDeriver::deriveProperty(node.getStep(), {result.property}, any_prop, context);
    Constants constants = ConstantsDeriver::deriveConstants(node.getStep(), {result.constants}, cte_helper.getCTEInfo(), context);
    return {node.shared_from_this(), prop, constants};
}

PlanNodePtr PruneSortingInfoRewriter::visitPlanNode(PlanNodeBase & node, SortInfo & required)
{
    SortInfo s{required.sort_desc, size_t{0}};
    return SimplePlanRewriter::visitPlanNode(node, s);
}

PlanNodePtr PruneSortingInfoRewriter::visitSortingNode(SortingNode & node, SortInfo &)
{
    auto prefix_desc = node.getStep()->getPrefixDescription();
    SortInfo s{prefix_desc, node.getStep()->getLimit()};
    return SimplePlanRewriter::visitPlanNode(node, s);
}

PlanNodePtr PruneSortingInfoRewriter::visitAggregatingNode(AggregatingNode & node, SortInfo &)
{
    auto prefix_desc = node.getStep()->getGroupBySortDescription();
    SortInfo s{prefix_desc, size_t{0}};
    return SimplePlanRewriter::visitPlanNode(node, s);
}

PlanNodePtr PruneSortingInfoRewriter::visitWindowNode(WindowNode & node, SortInfo &)
{
    auto prefix_desc = node.getStep()->getPrefixDescription();
    SortInfo s{prefix_desc, size_t{0}};
    return SimplePlanRewriter::visitPlanNode(node, s);
}

PlanNodePtr PruneSortingInfoRewriter::visitTopNFilteringNode(TopNFilteringNode & node, SortInfo &)
{
    auto prefix_desc = node.getStep()->getSortDescription();
    SortInfo s = node.getStep()->getAlgorithm() == TopNFilteringAlgorithm::Limit ? SortInfo{prefix_desc, size_t{0}} : SortInfo{};
    return SimplePlanRewriter::visitPlanNode(node, s);
}

PlanNodePtr PruneSortingInfoRewriter::visitTableScanNode(TableScanNode & node, SortInfo & required)
{
    auto & step = node.getStep();

    NameSet required_columns;
    auto mappings = step->getAliasToColumnMap();
    for (const auto & column : required.sort_desc)
    {
        auto column_name = mappings.contains(column.column_name) ? mappings.at(column.column_name) : column.column_name;
        required_columns.emplace(column_name);
    }

    // prune unused read order columns
    // eg, select * from table(order by a,b,c) where a = 'x' and d = 'y' order by b,d
    // required sort columns may be: b,d; read order columns should be a,b
    auto read_order = step->getReadOrder();
    auto it = std::find_if(read_order.rbegin(), read_order.rend(), [&](const SortColumnDescription & sort_column) {
        return required_columns.contains(sort_column.column_name);
    });

    SortDescription pruned_read_order(read_order.begin(), read_order.begin() + std::distance(it, read_order.rend()));

    if (!required.sort_desc.empty() && pruned_read_order.empty())
    {
        // do nothing if all columns in required don't exist in table
        if (logger->error())
        {
            Names names;
            for (const auto & desc : required.sort_desc)
                names.emplace_back(desc.column_name);
            LOG_WARNING(logger, "unkown required sorting: {}", fmt::format("{}", fmt::join(names, ", ")));
        }
    }
    else
    {
        node.getStep()->setReadOrder(pruned_read_order);
    }

    return node.shared_from_this();
}

PlanNodePtr PruneSortingInfoRewriter::visitProjectionNode(ProjectionNode & node, SortInfo & required)
{
    auto mappings = Utils::computeIdentityTranslations(node.getStep()->getAssignments());

    SortDescription push_down_sort_description;
    for (const auto & column : required.sort_desc)
    {
        if (!mappings.contains(column.column_name))
            break;
        push_down_sort_description.emplace_back(
            SortColumnDescription{mappings.at(column.column_name), column.direction, column.nulls_direction});
    }

    SortInfo child_required{push_down_sort_description, Utils::canChangeOutputRows(*node.getStep(), context) ? required.limit : 0ul};
    return SimplePlanRewriter::visitPlanNode(node, child_required);
}

}

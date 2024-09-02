#include <Advisor/ColumnUsage.h>

#include <Advisor/WorkloadQuery.h>
#include <Analyzers/QualifiedColumnName.h>
#include <Core/Types.h>
#include <Functions/FunctionsComparison.h>
#include <Interpreters/StorageID.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/PredicateUtils.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>

#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/MergingAggregatedStep.h>
#include <QueryPlan/CTERefStep.h>

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace
{
    bool isEqualityPredicate(const ASTFunction & function)
    {
        return function.name == NameEquals::name && function.arguments && function.arguments->children.size() == 2
            && dynamic_pointer_cast<const ASTLiteral>(function.arguments->children[1]);
    }

    bool isRangePredicate(const ASTFunction & function)
    {
        return (function.name == NameGreater::name || function.name == NameLess::name
                || function.name == NameGreaterOrEquals::name || function.name == NameLessOrEquals::name)
            && function.arguments && function.arguments->children.size() == 2
            && dynamic_pointer_cast<const ASTLiteral>(function.arguments->children[1]);
    }

    bool isInPredicate(const ASTFunction & function)
    {
        return function.name == "in" && function.arguments && function.arguments->children.size() == 2;
    }

    std::optional<std::pair<std::string, ColumnUsageType>> extractPredicateUsage(ConstASTPtr expression)
    {
        auto fun = dynamic_pointer_cast<const ASTFunction>(expression);
        if (!fun || !fun->arguments || fun->arguments->children.size() != 2)
            return std::nullopt;
        auto identifier = dynamic_pointer_cast<const ASTIdentifier>(fun->arguments->children[0]);
        if (!identifier)
            return std::nullopt;
        const std::string & symbol = identifier->name();

        if (isEqualityPredicate(*fun))
            return std::make_optional(std::make_pair(symbol, ColumnUsageType::EQUALITY_PREDICATE));
        else if (isRangePredicate(*fun))
            return std::make_optional(std::make_pair(symbol, ColumnUsageType::RANGE_PREDICATE));
        else if (isInPredicate(*fun))
            return std::make_optional(std::make_pair(symbol, ColumnUsageType::IN_PREDICATE));
        else
            return std::make_optional(std::make_pair(symbol, ColumnUsageType::OTHER_PREDICATE));
    }

    std::optional<std::pair<std::string, std::string>> extractNonEquiJoinUsage(ConstASTPtr expression)
    {
        auto fun = dynamic_pointer_cast<const ASTFunction>(expression);
        if (!fun || !fun->arguments || fun->arguments->children.size() != 2)
            return std::nullopt;
        auto function = *fun;
        if ((function.name == NameGreater::name || function.name == NameLess::name
             || function.name == NameGreaterOrEquals::name || function.name == NameLessOrEquals::name
             || function.name == NameNotEquals::name)
            && function.arguments && function.arguments->children.size() == 2)
        {
            auto left = dynamic_pointer_cast<const ASTIdentifier>(function.arguments->children[0]);
            auto right = dynamic_pointer_cast<const ASTIdentifier>(function.arguments->children[1]);
            if (left && right)
                return std::make_optional(std::make_pair(left->name(), right->name()));
        }
        return std::nullopt;
    }

    using ColumnNameWithSourceTableFlag = std::pair<QualifiedColumnName, bool>;
}

class ColumnUsageVisitor : public PlanNodeVisitor<void, ColumnUsages>
{
public:
    explicit ColumnUsageVisitor(const CTEInfo & cte_info_): cte_info(cte_info_)
    {
    }

protected:
    void addUsage(
        ColumnUsages & column_usages,
        const std::string & symbol,
        ColumnUsageType type,
        PlanNodePtr node,
        ConstASTPtr expression = nullptr);

    void processChildren(PlanNodeBase & node, ColumnUsages & column_usages);
    void visitPlanNode(PlanNodeBase & node, ColumnUsages & column_usages) override;

    void visitTableScanNode(TableScanNode & node, ColumnUsages & column_usages) override;
    void visitFilterNode(FilterNode & node, ColumnUsages & column_usages) override;
    void visitJoinNode(JoinNode & node, ColumnUsages & column_usages) override;
    void visitProjectionNode(ProjectionNode & node, ColumnUsages & column_usages) override;
    void visitAggregatingNode(AggregatingNode & node, ColumnUsages & column_usages) override;
    void visitCTERefNode(CTERefNode & node, ColumnUsages & column_usages) override;

private:
    std::unordered_map<std::string, ColumnNameWithSourceTableFlag> symbol_to_table_column_map;
    std::unordered_set<CTEId> visited_ctes;
    const CTEInfo & cte_info;
};

ColumnUsages buildColumnUsages(const WorkloadQueries & queries)
{
    ColumnUsages column_usages;
    for (const auto & query : queries)
    {
        const auto & plan = query->getPlan();
        ColumnUsageVisitor visitor(plan->getCTEInfo());
        VisitorUtil::accept(*plan->getPlanNode(), visitor, column_usages);
    }
    return column_usages;
}

void ColumnUsageInfo::update(ColumnUsage usage, bool is_source_table)
{
    ColumnUsageType type = usage.type;
    if (is_source_table)
        usages_only_source_table.emplace(type, std::move(usage));
    else
        usages_non_source_table.emplace(type, std::move(usage));
}

size_t ColumnUsageInfo::getFrequency(ColumnUsageType type, bool only_source_table) const
{
    size_t freq = usages_only_source_table.count(type);
    if (!only_source_table)
        freq += usages_non_source_table.count(type);
    return freq;
}

std::vector<ColumnUsage> ColumnUsageInfo::getUsages(ColumnUsageType type, bool only_source_table) const
{
    std::vector<ColumnUsage> res{};
    auto range = usages_only_source_table.equal_range(type);
    for (auto it = range.first; it != range.second; ++it)
        res.emplace_back(it->second);
    if (!only_source_table)
    {
        range = usages_non_source_table.equal_range(type);
        for (auto it = range.first; it != range.second; ++it)
            res.emplace_back(it->second);
    }
    return res;
}

void ColumnUsageVisitor::addUsage(
    ColumnUsages & column_usages, const std::string & symbol, ColumnUsageType type, PlanNodePtr node, ConstASTPtr expression)
{
    auto it = symbol_to_table_column_map.find(symbol);
    if (it == symbol_to_table_column_map.end()) // no matching column
        return;
    const auto & [column, is_source_table] = it->second;
    column_usages[column].update(ColumnUsage{type, node, column, expression}, is_source_table);
}

void ColumnUsageVisitor::processChildren(PlanNodeBase & node, ColumnUsages & column_usages)
{
    for (auto & child : node.getChildren())
        VisitorUtil::accept(*child, *this, column_usages);
}

void ColumnUsageVisitor::visitPlanNode(PlanNodeBase & node, ColumnUsages & column_usages)
{
    processChildren(node, column_usages);
}

void ColumnUsageVisitor::visitTableScanNode(TableScanNode & node, ColumnUsages & column_usages)
{
    auto table_step = dynamic_pointer_cast<TableScanStep>(node.getStep());
    const StorageID & storage_id = table_step->getStorageID();

    for (const auto & [column_name, alias] : table_step->getColumnAlias())
    {
        QualifiedColumnName column{storage_id.getDatabaseName(), storage_id.getTableName(), column_name};
        symbol_to_table_column_map.emplace(alias, ColumnNameWithSourceTableFlag{column, true});
        addUsage(column_usages, alias, ColumnUsageType::SCANNED, node.shared_from_this());
    }
}

void ColumnUsageVisitor::visitFilterNode(FilterNode & node, ColumnUsages & column_usages)
{
    processChildren(node, column_usages);
    auto filter_step = dynamic_pointer_cast<FilterStep>(node.getStep());
    for (const ConstASTPtr & expression : PredicateUtils::extractConjuncts(filter_step->getFilter()))
    {
        auto usage_opt = extractPredicateUsage(expression);
        if (usage_opt.has_value())
            addUsage(column_usages, usage_opt.value().first, usage_opt.value().second, node.shared_from_this(), expression);
    }
}

void ColumnUsageVisitor::visitJoinNode(JoinNode & node, ColumnUsages & column_usages)
{
    processChildren(node, column_usages);
    auto join_step = dynamic_pointer_cast<JoinStep>(node.getStep());

    for (const std::string & name : join_step->getLeftKeys())
        addUsage(column_usages, name, ColumnUsageType::EQUI_JOIN, node.shared_from_this());
    for (const std::string & name : join_step->getRightKeys())
        addUsage(column_usages, name, ColumnUsageType::EQUI_JOIN, node.shared_from_this());

    if (join_step->getFilter())
    {
        for (const ConstASTPtr & expression : PredicateUtils::extractConjuncts(join_step->getFilter()))
        {
            auto usage_opt = extractNonEquiJoinUsage(expression);
            if (usage_opt.has_value())
            {
                addUsage(column_usages, usage_opt.value().first, ColumnUsageType::NON_EQUI_JOIN, node.shared_from_this(), expression);
                addUsage(column_usages, usage_opt.value().second, ColumnUsageType::NON_EQUI_JOIN, node.shared_from_this(), expression);
            }
        }
    }
    // after join, the symbols are not considered source-table
    const auto & outputs = join_step->getOutputStream().header;
    for (auto & symbol_column_flag : symbol_to_table_column_map)
    {
        if (outputs.has(symbol_column_flag.first))
            symbol_column_flag.second.second = false;
    }
}

void ColumnUsageVisitor::visitProjectionNode(ProjectionNode & node, ColumnUsages & column_usages)
{
    processChildren(node, column_usages);
    auto project_step = dynamic_pointer_cast<ProjectionStep>(node.getStep());
    for (const auto & [out_symbol, in_ast] : project_step->getAssignments())
    {
        if (const auto * identifier = in_ast->as<const ASTIdentifier>())
        {
            auto it = symbol_to_table_column_map.find(identifier->name());
            if (it != symbol_to_table_column_map.end())
                symbol_to_table_column_map.emplace(out_symbol, it->second);
        }
    }
}

void ColumnUsageVisitor::visitAggregatingNode(AggregatingNode & node, ColumnUsages & column_usages)
{
    processChildren(node, column_usages);
    auto agg_step = dynamic_pointer_cast<AggregatingStep>(node.getStep());
    for (const auto & grouping_key : agg_step->getKeys())
        addUsage(column_usages, grouping_key, ColumnUsageType::GROUP_BY, node.shared_from_this());
    // after agg, the symbols are not considered source-table
    const auto & outputs = agg_step->getOutputStream().header;
    for (auto & symbol_column_flag : symbol_to_table_column_map)
    {
        if (outputs.has(symbol_column_flag.first))
            symbol_column_flag.second.second = false;
    }
}

void ColumnUsageVisitor::visitCTERefNode(CTERefNode & node, ColumnUsages & column_usages)
{
    auto cte = dynamic_pointer_cast<CTERefStep>(node.getStep());
    auto cte_id = cte->getId();
    if (visited_ctes.contains(cte_id))
        return;
    visited_ctes.insert(cte_id);
    VisitorUtil::accept(cte_info.getCTEs().at(cte_id), *this, column_usages);
}

}

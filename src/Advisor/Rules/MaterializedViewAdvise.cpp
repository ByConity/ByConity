#include <Advisor/Rules/MaterializedViewAdvise.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/SignatureUsage.h>
#include <Advisor/WorkloadQuery.h>
#include <Analyzers/ASTEquals.h>
#include <Core/Names.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Optimizer/SimplifyExpressions.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/Void.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Logger.h>
#include <Common/SipHash.h>
#include "QueryPlan/PlanPrinter.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

namespace
{
    // MV size is very important because it also consumes storage, so we give more importance to the MV size
    constexpr float MV_SCAN_COST_SCALE_UP = 3.0;
    // min size of table for its mv to be recommended
    // constexpr size_t ADVISE_MIN_TABLE_SIZE = 10000;
}

WorkloadAdvises MaterializedViewAdvisor::analyze(AdvisorContext & context) const
{
    std::unordered_set<PlanSignature> blacklist;
    std::unordered_map<QualifiedTableName, std::vector<MaterializedViewCandidate>> candidates_by_table;
    for (const auto & [signature, queries] : context.signature_usages.signature_to_query_map)
    {
        if (blacklist.contains(signature))
            continue;
        const PlanNodePtr & query_plan = queries[0].first;
        const auto & query = queries[0].second;
        if (query_plan->getStep()->getType() == IQueryPlanStep::Type::TableScan)
            continue;

        auto benefit = calculateMaterializeBenefit(query_plan, query, context.session_context).value_or(1.);
        if (benefit < 0)
            continue;
        double total_benefit = queries.size() * benefit;

        std::vector<String> related_queries;
        for (const auto & item : queries)
            related_queries.emplace_back(item.second->getQueryId());

        std::optional<MaterializedViewCandidate> candidate
            = MaterializedViewCandidate::from(query_plan, related_queries, signature, total_benefit, only_aggregate, ignore_filter);
        if (!candidate.has_value())
                    continue;
                
        QueryPlan plan{query_plan, std::make_shared<PlanNodeIdAllocator>()};
        addChildrenToBlacklist(query_plan, context.signature_usages.plan_to_signature_map, blacklist);

        bool merged = false;
        auto & candidates = candidates_by_table[candidate->table_name];
        for (auto & other : candidates)
            {
                if (blacklist.contains(other.plan_signature))
                    continue;
                
            if (other.tryMerge(*candidate))
                {
                    merged = true;
                break;
                }
            }
        if (!merged)
            candidates.emplace_back(*candidate);
    }

    WorkloadAdvises res;
    for (const auto & candidates : candidates_by_table)
    {
            for (const auto & candidate : candidates.second)
        {
            if (candidate.related_queries.size() <= 1)
                continue;
            if (blacklist.contains(candidate.plan_signature))
                continue;
            res.emplace_back(std::make_shared<MaterializedViewAdvise>(
                candidates.first,
                candidate.toSql(output_type),
                getRelatedQueries(candidate.related_queries, context),
                candidate.total_cost));
        }
    }

    std::sort(res.begin(), res.end(), [&](const auto & a, const auto & b) { return a->getBenefit() > b->getBenefit(); });
    return res;
}

    std::optional<double>
MaterializedViewAdvisor::calculateMaterializeBenefit(const PlanNodePtr & node, const WorkloadQueryPtr & query, ContextPtr context)
{
    if (!query->getCosts().contains(node->getId()))
        return std::nullopt;
    auto original_cost = query->getCosts().at(node->getId());
    auto node_stats = node->getStatistics();
    if (!node_stats)
        return std::nullopt;

    // see TableScanCost.h
    PlanNodeCost scan_output_cost = PlanNodeCost::cpuCost(node_stats.value()->getRowCount()) * CostModel{*context}.getTableScanCostWeight();
    double scaled_mv_scan_cost = MV_SCAN_COST_SCALE_UP * scan_output_cost.getCost(CostModel(*context));
    return original_cost - scaled_mv_scan_cost;
}

void MaterializedViewAdvisor::addChildrenToBlacklist(
    PlanNodePtr node, const PlanNodeToSignatures & signatures, std::unordered_set<PlanSignature> & blacklist)
{
    for (const auto & child : node->getChildren())
    {
                  if (auto it = signatures.find(child); it != signatures.end())
            blacklist.emplace(it->second);
        addChildrenToBlacklist(child, signatures, blacklist);
    }
}

std::vector<String> MaterializedViewAdvisor::getRelatedQueries(const std::vector<String> & related_query_ids, AdvisorContext & context)
{
        std::vector<String> queries;

    std::set<String> unique_query_ids(related_query_ids.begin(), related_query_ids.end());
    for (const auto & id : unique_query_ids)
    {
        if (auto it = context.query_id_to_query.find(id); it != context.query_id_to_query.end())
            queries.emplace_back(it->second->getSQL());
    }

    return queries;
}

class MaterializedViewCandidateBuilder : public PlanNodeVisitor<void, Void>
{
public:
    bool isValid() const
    {
        return is_valid && table.has_value();
    }
    const Names & getOutputSymbols() const
    {
        return output_symbols;
    }
    const Names & getGroupingSymbols() const
    {
        return grouping_symbols;
    }
    const ASTPtr & getWhere() const
    {
        return where_filter;
    }
    const QualifiedTableName & getTableName() const
    {
        return *table;
    }
    std::shared_ptr<AggregatingStep> getAggregate() const
    {
        return aggregate;
    }

protected:
    void processChildren(PlanNodeBase & node, Void & context)
    {
        for (auto & child : node.getChildren())
        {
            if (!is_valid)
                return;
            VisitorUtil::accept(*child, *this, context);
        }
    }

    void visitPlanNode(PlanNodeBase &, Void &) override { invalidate(); }

    void visitTableScanNode(TableScanNode & node, Void &) override
    {
        if (table.has_value())
        {
            invalidate();
            return;
        }
        auto table_step = dynamic_pointer_cast<TableScanStep>(node.getStep());
        table = table_step->getStorageID().getQualifiedName();
        for (const auto & out_column : table_step->getOutputStream().header)
            output_symbols.emplace_back(out_column.name);
    }

    void visitFilterNode(FilterNode & node, Void & context) override
    {
        processChildren(node, context);
        if (!is_valid || aggregate)
        {
            invalidate();
            return;
        }
        auto filter_step = dynamic_pointer_cast<FilterStep>(node.getStep());
        auto conjuncts = RuntimeFilterUtils::extractRuntimeFilters(filter_step->getFilter()).second;
        if (conjuncts.empty())
            return;
        if (where_filter)
            conjuncts.emplace_back(std::move(where_filter));
        where_filter = PredicateUtils::combineConjuncts(conjuncts);
    }

    void visitProjectionNode(ProjectionNode & node, Void & context) override
    {
        processChildren(node, context);
        if (!is_valid)
            return;
        auto projection_step = dynamic_pointer_cast<ProjectionStep>(node.getStep());
        output_symbols.clear();
        for (const auto & out_column : projection_step->getOutputStream().header)
            output_symbols.emplace_back(out_column.name);
    }

    void visitAggregatingNode(AggregatingNode & node, Void & context) override
    {
        processChildren(node, context);
        auto agg_step = dynamic_pointer_cast<AggregatingStep>(node.getStep());
        if (!is_valid || aggregate || agg_step->isGroupingSet())
        {
            invalidate();
            return;
        }

        aggregate = agg_step;
        grouping_symbols = agg_step->getKeys();
        output_symbols.clear();
        for (const auto & out_column : agg_step->getOutputStream().header)
            output_symbols.emplace_back(out_column.name);
    }

    void visitExchangeNode(ExchangeNode & node, Void & context) override { return processChildren(node, context); }

private:
    bool is_valid = true;
    std::shared_ptr<AggregatingStep> aggregate = nullptr;
    std::optional<QualifiedTableName> table = std::nullopt;
    ASTPtr where_filter;
    Names grouping_symbols;
    Names output_symbols;

    void invalidate() { is_valid = false; }
};

class TableColumnToIdentifierRewriter : public SimpleExpressionRewriter<Void>
{
public:
    static ASTPtr rewrite(ASTPtr expr)
    {
    TableColumnToIdentifierRewriter rewriter;
        Void c;
        return ASTVisitorUtil::accept(expr, rewriter, c);
    }

    ASTPtr visitASTTableColumnReference(ASTPtr & expr, Void &) override
    {
        const auto & name = expr->as<ASTTableColumnReference &>().column_name;
        return std::make_shared<ASTIdentifier>(name);
    }
};

std::optional<MaterializedViewCandidate> MaterializedViewCandidate::from(
    PlanNodePtr plan,
    const std::vector<String> & related_queries,
    PlanSignature plan_signature,
    double total_cost,
    bool only_aggregate,
    bool ignore_filter)
{
    MaterializedViewCandidateBuilder builder;
    Void c;
    VisitorUtil::accept(plan, builder, c);
    if (!builder.isValid())
        return std::nullopt;

bool contains_aggregate = builder.getAggregate() ? !builder.getAggregate()->getAggregates().empty() : false;
    if (only_aggregate && !contains_aggregate)
        return std::nullopt;

    auto symbol_map = SymbolTransformMap::buildFrom(*plan);
    if (!symbol_map.has_value())
        return std::nullopt;

    EqualityASTSet wheres;
    EqualityASTSet group_bys;
    EqualityASTSet outputs;
    if (builder.getWhere())
    {
        if (ignore_filter)
        {
            auto columns
                = SymbolsExtractor::extract(TableColumnToIdentifierRewriter::rewrite(symbol_map->inlineReferences(builder.getWhere())));
            for (const auto & column : columns)
            {
                group_bys.emplace(std::make_shared<ASTIdentifier>(column));
                outputs.emplace(std::make_shared<ASTIdentifier>(column));
            }
        }
        else
        {
            wheres.emplace(TableColumnToIdentifierRewriter::rewrite(symbol_map->inlineReferences(builder.getWhere())));
        }
    }

    for (const auto & symbol : builder.getGroupingSymbols())
        group_bys.emplace(TableColumnToIdentifierRewriter::rewrite(symbol_map->inlineReferences(symbol)));
    for (const auto & symbol : builder.getOutputSymbols())
        outputs.emplace(TableColumnToIdentifierRewriter::rewrite(symbol_map->inlineReferences(symbol)));

    return MaterializedViewCandidate{
        plan_signature,
        builder.getTableName(),
        total_cost,
        contains_aggregate,
        std::move(wheres),
        std::move(group_bys),
        std::move(outputs),
        related_queries};
}

std::string MaterializedViewCandidate::toSql(MaterializedViewAdvisor::OutputType output_type) const
{
    if (output_type == MaterializedViewAdvisor::OutputType::PROJECTION)
        return toProjection();
    return toQuery();
}

std::string MaterializedViewCandidate::toQuery() const
{
    auto select = std::make_shared<ASTSelectQuery>();
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    
    auto table_expression = std::make_shared<ASTTableExpression>();
    auto ast = std::make_shared<ASTTableIdentifier>(table_name.database, table_name.table);
    table_expression->database_and_table_name = ast;
    table_expression->children.push_back(ast);
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->table_expression = table_expression;
    table_element->children.push_back(table_expression);
    select->tables()->children.emplace_back(table_element);

    auto output_list = std::make_shared<ASTExpressionList>();
    for (const auto & item : outputs)
        output_list->children.emplace_back(item->clone());
    select->setExpression(ASTSelectQuery::Expression::SELECT, output_list->clone());

    if (!wheres.empty())
    {
        std::vector<ConstASTPtr> disjucts;
        for (const auto & filter : wheres)
            disjucts.emplace_back(filter.getPtr());
        select->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineDisjuncts(disjucts));
    }

    if (!group_bys.empty())
    {
        auto group_by = std::make_shared<ASTExpressionList>();
        for (const auto & item : group_bys)
            group_by->children.emplace_back(item->clone());
        select->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);
    }

    return serializeAST(*select);
}

std::string MaterializedViewCandidate::toProjection() const
{
    auto select = std::make_shared<ASTSelectQuery>();
    {
        auto output_list = std::make_shared<ASTExpressionList>();
        for (const auto & ast : outputs)
            output_list->children.emplace_back(ast->clone());
        select->setExpression(ASTSelectQuery::Expression::SELECT, output_list->clone());
    }
    if (!wheres.empty())
    {
        std::vector<ConstASTPtr> disjucts;
        for (const auto & filter : wheres)
            disjucts.emplace_back(filter.getPtr());
        select->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineDisjuncts(disjucts));
    }
    if (!group_bys.empty())
    {
        auto group_by = std::make_shared<ASTExpressionList>();
        for (const auto & ast : group_bys)
            group_by->children.emplace_back(ast->clone());
        select->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);
    }

    auto command = std::make_shared<ASTAlterCommand>();
    command->type = ASTAlterCommand::Type::ADD_PROJECTION;
    command->projection_decl = select;

    auto alter = std::make_shared<ASTAlterQuery>();
    alter->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
    alter->database = table_name.database;
    alter->table = table_name.table;
    auto command_list = std::make_shared<ASTExpressionList>();
    command_list->children.emplace_back(command);
    alter->command_list = command_list.get();
    alter->children.emplace_back(command_list);

    return serializeAST(*alter);
}

bool MaterializedViewCandidate::tryMerge(const MaterializedViewCandidate & other)
{
    // cannot merge mv on different tables
    if (table_name != other.table_name)
        return false;

    // cannot merge different wheres
    if (other.wheres.size() != wheres.size())
        return false;
    if (!std::all_of(other.wheres.begin(), other.wheres.end(), [&](const auto & predicate) { return wheres.contains(predicate); }))
        return false;

    // cannot merge mv containing agg vs mv not containing agg
    if (contains_aggregate ^ other.contains_aggregate)
        return false;
    // cannot merge mv with group by vs mv without group by
    if (group_bys.empty() ^ other.group_bys.empty())
        return false;
if (!std::all_of(other.group_bys.begin(), other.group_bys.end(), [&](const auto & predicate) { return group_bys.contains(predicate); })
        && !std::all_of(group_bys.begin(), group_bys.end(), [&](const auto & predicate) { return other.group_bys.contains(predicate); }))
    {
        return false;
    }
    for (const auto & group_by : other.group_bys)
        group_bys.emplace(group_by);

    for (const auto & output : other.outputs)
        outputs.emplace(output);

    for (const auto & query_id : other.related_queries)
        related_queries.emplace_back(query_id);

    total_cost += other.total_cost;
    return true;
}

} // namespace DB

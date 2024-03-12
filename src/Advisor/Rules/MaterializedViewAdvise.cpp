#include <Advisor/Rules/MaterializedViewAdvise.h>

#include <Advisor/AdvisorContext.h>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <Advisor/SignatureUsage.h>
#include <Analyzers/ASTEquals.h>
#include <Core/Names.h>
#include <Core/QualifiedTableName.h>
#include <Core/Types.h>
#include <Common/SipHash.h>
#include "Parsers/ASTTableColumnReference.h"
#include <Interpreters/Context_fwd.h>
#include <Optimizer/CostModel/PlanNodeCost.h>
#include <Optimizer/SimplifyExpressions.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/Void.h>
#include <Poco/Logger.h>

#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/AggregatingStep.h>

#include <algorithm>
#include <optional>
#include <memory>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

class MaterializedViewAdvise : public IWorkloadAdvise
{
public:
    explicit MaterializedViewAdvise(QualifiedTableName table_,
                                    String sql_,
                                    double benefit_)
        : table(std::move(table_)), sql(std::move(sql_)), benefit(benefit_) {}

    String apply(WorkloadTables &) override { return ""; }
    QualifiedTableName getTable() override { return table; }
    String getAdviseType() override { return "MaterializedView"; }
    String getOriginalValue() override { return ""; }
    String getOptimizedValue() override { return sql; }
    double getBenefit() override {return benefit; }

    QualifiedTableName table;
    String sql;
    double benefit;
};

namespace
{
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

    void addSkipSignaturesRecursive(const PlanSignature & signature, std::unordered_set<PlanSignature> & dependents, const SignatureUsages & usages)
    {
        if (dependents.contains(signature))
            return;

        dependents.insert(signature);

        // add children recursively
        std::stack<PlanSignature> signature_stack;
        signature_stack.push(signature);
        while (!signature_stack.empty())
        {
            auto sig = signature_stack.top();
            signature_stack.pop();
            for (const auto & child : usages.at(sig).getChildren())
            {
                if (dependents.contains(child))
                    continue;
                dependents.insert(child);
                signature_stack.push(child);
            }
        }

        // add parent recursively
        signature_stack.push(signature);
        while (!signature_stack.empty())
        {
            auto sig = signature_stack.top();
            signature_stack.pop();
            for (const auto & [parent, usage_info] : usages)
            {
                if (dependents.contains(parent))
                    continue;
                if (usage_info.getChildren().contains(sig))
                {
                    dependents.insert(parent);
                    signature_stack.push(parent);
                }
            }
        }
    }

    WorkloadAdvises collectAdvises(std::unordered_multimap<QualifiedTableName, MaterializedViewCandidateWithBenefit> && candidates_by_table)
    {
        WorkloadAdvises res;
        // currently for each table, we only recommend one view with agg and one view without agg
        for (auto it = candidates_by_table.begin(); it != candidates_by_table.end();)
        {
            const auto & table = it->first;
            auto [begin, end] = candidates_by_table.equal_range(table);
            if (std::distance(begin, end) == 1)
            {
                auto & view_with_benefit = begin->second;
                res.emplace_back(std::make_shared<MaterializedViewAdvise>(table, view_with_benefit.first.toSQL(), view_with_benefit.second));
            }
            else
            {
                std::vector<MaterializedViewCandidateWithBenefit> views;
                for (auto inner_it = begin; inner_it != end; ++inner_it)
                {
                    auto & view_with_benefit = inner_it->second;
                    bool merged = false;
                    for (auto & existing : views)
                    {
                        merged |= existing.first.tryMerge(view_with_benefit.first);
                        if (merged)
                        {
                            // update benefit
                            existing.second = std::max(existing.second, view_with_benefit.second);
                            break;
                        }
                    }
                    if (!merged)
                        views.emplace_back(view_with_benefit);
                }

                for (auto & view : views)
                    res.emplace_back(std::make_shared<MaterializedViewAdvise>(table, view.first.toSQL(), view.second));
            }
            it = end;
        }

        return res;
    }

    ASTPtr toAST(const QualifiedTableName & table)
    {
        auto table_expression = std::make_shared<ASTTableExpression>();
        auto ast = std::make_shared<ASTTableIdentifier>(table.database, table.table);
        table_expression->database_and_table_name = ast;
        table_expression->children.push_back(ast);
        auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
        table_element->table_expression = table_expression;
        table_element->children.push_back(table_expression);
        return table_element;
    }

    ASTs toVector(MaterializedViewCandidate::EqASTBySerializeSet && asts)
    {
        ASTs res{};
        for (const auto & ast : asts)
            res.emplace_back(ast.getAST());
        return res;
    }

    using SignatureToCandidate = std::pair<PlanSignature, MaterializedViewCandidateWithBenefit>;
}

WorkloadAdvises MaterializedViewAdvisor::analyze(AdvisorContext & context) const
{
    std::vector<SignatureToCandidate> candidates;
    for (const auto & [signature, usage_info] : context.signature_usages)
    {
        std::optional<MaterializedViewCandidateWithBenefit> candidate = buildCandidate(usage_info, context.session_context);

        if (!candidate.has_value())
            continue;

        if (agg_only && !candidate.value().first.containsAggregate())
            continue;

        candidates.emplace_back(std::make_pair(signature, candidate.value()));
    }

    // sort desc by benefit desc
    std::sort(candidates.begin(), candidates.end(),
              [](const auto & lhs, const auto & rhs)
              {
                  auto lhs_benefit = lhs.second.second;
                  auto rhs_benefit = rhs.second.second;
                  if (lhs_benefit == rhs_benefit)
                      return lhs.first > rhs.first;
                  return lhs_benefit > rhs_benefit;
              });
    // collect by table and skip related signatures
    std::unordered_set<PlanSignature> skip_signatures{};
    std::unordered_multimap<QualifiedTableName, MaterializedViewCandidateWithBenefit> candidates_by_table;
    for (auto & [signature, candidate] : candidates)
    {
        if (skip_signatures.contains(signature))
            continue;
        // add descendant and ancestors to skip_signatures
        addSkipSignaturesRecursive(signature, skip_signatures, context.signature_usages);
        candidates_by_table.emplace(candidate.first.getTableName(), std::move(candidate));
    }

    return collectAdvises(std::move(candidates_by_table));
}

namespace
{
class MaterializedViewCandidateBuilder : public PlanNodeVisitor<void, Void>
{
public:
    bool isValid() const { return is_valid; }
    std::optional<MaterializedViewCandidate> build(const SymbolTransformMap & symbol_map)
    {
        if (!is_valid || !table || output_symbols.empty())
            return std::nullopt;

        auto table_name = table.value();
        MaterializedViewCandidate::EqASTBySerializeSet wheres{};
        MaterializedViewCandidate::EqASTBySerializeSet group_bys{};
        MaterializedViewCandidate::EqASTBySerializeSet outputs{};
        if (where_filter)
            wheres.emplace(MaterializedViewCandidate::EqASTBySerialize(TableColumnToIdentifierRewriter::rewrite(symbol_map.inlineReferences(where_filter))));
        for (const auto & symbol : grouping_symbols)
            group_bys.emplace(MaterializedViewCandidate::EqASTBySerialize(TableColumnToIdentifierRewriter::rewrite(symbol_map.inlineReferences(symbol))));
        for (const auto & symbol : output_symbols)
            outputs.emplace(MaterializedViewCandidate::EqASTBySerialize(TableColumnToIdentifierRewriter::rewrite(symbol_map.inlineReferences(symbol))));

        return std::make_optional(MaterializedViewCandidate(
            std::move(table_name), contains_aggregate, std::move(wheres), std::move(group_bys), std::move(outputs)));
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
        if (!is_valid || contains_aggregate)
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
        if (!is_valid || contains_aggregate || agg_step->isGroupingSet())
        {
            invalidate();
            return;
        }

        contains_aggregate = true;
        grouping_symbols = agg_step->getKeys();
        output_symbols.clear();
        for (const auto & out_column : agg_step->getOutputStream().header)
            output_symbols.emplace_back(out_column.name);
    }
    void visitExchangeNode(ExchangeNode & node, Void & context) override { return processChildren(node, context); }

private:
    bool is_valid = true;
    bool contains_aggregate = false;
    std::optional<QualifiedTableName> table = std::nullopt;
    ASTPtr where_filter;
    Names grouping_symbols;
    Names output_symbols;

    void invalidate() { is_valid = false; }
};
}

MaterializedViewCandidate::EqASTBySerialize::EqASTBySerialize(ASTPtr _ast): ast(_ast)
{
    if (!ast)
        return;
    SipHash hasher;
    std::string str = serializeAST(*ast);
    hasher.update(str);
    hash = hasher.get64();
}


std::optional<MaterializedViewCandidate> MaterializedViewCandidate::from(PlanNodePtr plan)
{
    MaterializedViewCandidateBuilder builder;
    Void c{};
    VisitorUtil::accept(plan, builder, c);
    if (!builder.isValid())
        return std::nullopt;

    auto symbol_map = SymbolTransformMap::buildFrom(*plan);
    if (!symbol_map.has_value())
        return std::nullopt;

    return builder.build(symbol_map.value());
}

std::optional<MaterializedViewCandidateWithBenefit> MaterializedViewAdvisor::buildCandidate(const SignatureUsageInfo & usage_info, ContextPtr context) const
{
    // check appeared >=2 times
    size_t frequency = usage_info.getFrequency();
    if (frequency < 2)
        return std::nullopt;

    // check not TableScan
    PlanNodePtr plan = const_pointer_cast<PlanNodeBase>(usage_info.getPlan());
    if (plan->getStep()->getType() == IQueryPlanStep::Type::TableScan)
        return std::nullopt;

    if (auto stats_opt = plan->getStatistics().getStatistics();
        !stats_opt.has_value() || stats_opt.value()->getRowCount() > ADVISE_MAX_MV_SIZE)
        return std::nullopt;

    PlanNodePtr table_scan = plan;
    while (!dynamic_pointer_cast<TableScanNode>(table_scan) && !table_scan->getChildren().empty())
        table_scan = table_scan->getChildren().front();
    if (!dynamic_pointer_cast<TableScanNode>(table_scan))
        return std::nullopt;
    if (auto stats_opt = table_scan->getStatistics().getStatistics();
        !stats_opt.has_value() || stats_opt.value()->getRowCount() < ADVISE_MIN_TABLE_SIZE)
        return std::nullopt;

    std::optional<double> benefit_opt = getMaterializeBenefit(plan, usage_info.getCost(), context);
    if (!benefit_opt.has_value())
        return std::nullopt;

    std::optional<MaterializedViewCandidate> candidate = MaterializedViewCandidate::from(plan);
    if (!candidate.has_value())
        return std::nullopt;

    return std::make_optional(std::make_pair(std::move(candidate.value()), frequency * benefit_opt.value()));
}

std::string MaterializedViewCandidate::toSQL()
{
    auto select = std::make_shared<ASTSelectQuery>();
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    select->tables()->children.emplace_back(toAST(table_name));

    auto output_list = std::make_shared<ASTExpressionList>();
    for (const auto & ast : toVector(std::move(outputs)))
        output_list->children.emplace_back(ast);
    select->setExpression(ASTSelectQuery::Expression::SELECT, output_list);

    // todo: better merging for filters, e.g. combine equals -> in
    if (wheres.size() > 1)
    {
        auto coalesced_predicates = makeASTFunction("or");
        for (const auto & filter : toVector(std::move(wheres)))
            coalesced_predicates->arguments->children.emplace_back(filter);
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(coalesced_predicates));
    }
    else if (wheres.size() == 1)
    {
        ASTPtr where = toVector(std::move(wheres)).front();
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));
    }

    if (!group_bys.empty())
    {
        auto group_by = std::make_shared<ASTExpressionList>();
        for (const auto & ast : toVector(std::move(group_bys)))
            group_by->children.emplace_back(ast);
        select->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by));
    }

    return serializeAST(*select);
}

std::optional<double> MaterializedViewAdvisor::getMaterializeBenefit(std::shared_ptr<const PlanNodeBase> root,
                                                                     std::optional<double> original_cost,
                                                                     ContextPtr context) const
{
    if (!original_cost.has_value())
        return std::nullopt;
    auto node_stats_est = root->getStatistics();
    if (!node_stats_est)
        return std::nullopt;
    auto node_stats = node_stats_est.getStatistics();
    if (!node_stats || !node_stats.value())
        return std::nullopt;
    // see TableScanCost.h
    PlanNodeCost scan_output_cost = PlanNodeCost::cpuCost(node_stats.value()->getRowCount()) * CostModel{*context}.getTableScanCostWeight();
    double scaled_mv_scan_cost = MV_SCAN_COST_SCALE_UP * scan_output_cost.getCost();
    if (original_cost.value() <= scaled_mv_scan_cost)
        return std::nullopt;
    return original_cost.value() - scaled_mv_scan_cost;
}

bool MaterializedViewCandidate::tryMerge(const MaterializedViewCandidate & other)
{
    // cannot merge mv on different tables
    if (table_name != other.table_name)
        return false;
    // cannot merge different wheres
    if (wheres != other.wheres)
        return false;
    // cannot merge mv containing agg vs mv not containing agg
    if (contains_aggregate ^ other.contains_aggregate)
        return false;
    // cannot merge mv with group by vs mv without group by
    if (group_bys.empty() ^ other.group_bys.empty())
        return false;

    for (const auto & group_by : other.group_bys)
        group_bys.emplace(group_by);
    for (const auto & output : other.outputs)
        outputs.emplace(output);
    return true;
}

} // namespace DB

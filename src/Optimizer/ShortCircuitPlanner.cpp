#include <Optimizer/ShortCircuitPlanner.h>

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/PredicateConst.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/TableScanStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>

namespace DB
{

class ShortCircuitPlanner::ShortCircuitPlanVisitor : public DB::PlanNodeVisitor<bool, Void>
{
public:
    explicit ShortCircuitPlanVisitor(ContextPtr context_) : context(context_)
    {
    }

    bool visitPlanNode(PlanNodeBase & plan, Void & c) override
    {
        if (plan.getChildren().size() == 1)
            return VisitorUtil::accept(plan.getChildren()[0], *this, c);
        return false;
    }

    bool visitLimitNode(LimitNode & plan, Void & v) override
    {
        return VisitorUtil::accept(plan.getChildren()[0], *this, v);
    }

    bool visitProjectionNode(ProjectionNode & plan, Void & v) override
    {
        return VisitorUtil::accept(plan.getChildren()[0], *this, v);
    }

    bool visitFilterNode(FilterNode & plan, Void &) override
    {
        if (plan.getChildren()[0]->getType() == IQueryPlanStep::Type::TableScan)
            return checkTableScan(dynamic_cast<TableScanStep &>(*plan.getChildren()[0]->getStep()), plan.getStep()->getFilter());
        return false;
    }

    static bool checkTableScan(TableScanStep & table_scan, ConstASTPtr filter)
    {
        auto constraints = extractConstraints(filter);
        auto metadata = table_scan.getStorage()->getInMemoryMetadataPtr();
        return isPointScan(metadata->getUniqueKey(), constraints);
    }

    static std::unordered_set<String> extractConstraints(ConstASTPtr filter)
    {
        std::unordered_set<String> constraints;
        for (const auto & conjunct : PredicateUtils::extractConjuncts(filter))
        {
            const auto * func = conjunct->as<ASTFunction>();
            if (!func || func->name != "equals")
                continue;
            const auto * column = func->arguments->children[0]->as<ASTIdentifier>();
            if (!column)
                continue;
            if (func->arguments->children[1]->getType() != ASTType::ASTLiteral
                && func->arguments->children[1]->getType() != ASTType::ASTPreparedParameter)
                continue;
            constraints.emplace(column->name());
        }
        return constraints;
    }

    /**
     * Check filter constains all unique.
     */
    static bool isPointScan(const KeyDescription & primary_key, const std::unordered_set<String> & constraints)
    {
        return std::all_of(
            primary_key.column_names.begin(), primary_key.column_names.end(), [&](const auto & key) { return constraints.contains(key); });
    }

private:
    ContextPtr context;
};

bool ShortCircuitPlanner::isShortCircuitPlan(QueryPlan & query_plan, ContextPtr context)
{
    if (!context->getSettingsRef().enable_short_circuit)
        return false;

    ShortCircuitPlanVisitor visitor{context};
    Void v;
    return query_plan.getCTEInfo().empty() && VisitorUtil::accept(query_plan.getPlanNode(), visitor, v);
}

void ShortCircuitPlanner::addExchangeIfNeeded(QueryPlan & query_plan, ContextMutablePtr context)
{
    // todo: analyze optimized cluster
    auto output = query_plan.getPlanNode();
    if (output->getType() != IQueryPlanStep::Type::Projection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "output node is expected a project");
    auto child = output->getChildren()[0];
    auto gather = PlanNodeBase::createPlanNode(
        query_plan.getIdAllocator()->nextId(),
        std::make_unique<ExchangeStep>(
            DataStreams{child->getStep()->getOutputStream()},
            ExchangeMode::GATHER,
            Partitioning(Names{}),
            context->getSettingsRef().enable_shuffle_with_order),
        {child});
    output->replaceChildren({gather});
}
}

#include <memory>
#include <Optimizer/Rule/Rewrite/AddRepartitionColumn.h>

#include <Optimizer/PlanNodeCardinality.h>
#include <Optimizer/ProjectionPlanner.h>
#include <Optimizer/Rule/Rule.h>
#include <Parsers/ASTClusterByElement.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/WindowStep.h>
#include "Parsers/ASTIdentifier.h"

namespace DB
{
TransformResult AddRepartitionColumn::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto * step = dynamic_cast<const ExchangeStep *>(node->getStep().get());
    if (!step)
        return {};


    ProjectionPlanner proj(node->getChildren()[0], context.context);
    auto col = proj.addColumn(step->getSchema().getShuffleExpr());
    auto projection = proj.build();

    auto new_partition = step->getSchema();
    auto bucket_expr = new_partition.getBucketExpr();
    if (bucket_expr)
    {
        if (auto * cluster_by_ast_element = bucket_expr->as<ASTClusterByElement>())
        {
            cluster_by_ast_element->children[0] = std::make_shared<ASTIdentifier>("$0");
            new_partition.setBucketExpr(bucket_expr);
            new_partition.setColumns({col.first});
            return PlanNodeBase::createPlanNode(
                context.context->nextNodeId(),
                std::make_shared<ExchangeStep>(
                    DataStreams{projection->getCurrentDataStream()}, step->getExchangeMode(), new_partition, step->needKeepOrder()),
                {projection});
        }
    }

    return {};
}

}

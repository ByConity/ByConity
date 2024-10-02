
#include <Optimizer/Rewriter/ShareCommonPlanNode.h>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanSymbolReallocator.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <QueryPlan/SymbolMapper.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

using PlanSignatureOutputOrders = std::unordered_map<PlanNodePtr, Block>;

class ShareCommonPlanNode::Rewriter : public SimplePlanRewriter<Void>
{
public:
    Rewriter(
        ContextMutablePtr context_,
        CTEInfo & cte_info_,
        PlanNodeToSignatures & plan_node_to_signature_,
        PlanSignatureOutputOrders & plan_signature_output_orders_)
        : SimplePlanRewriter(context_, cte_info_)
        , cte_info(cte_info_)
        , plan_node_to_signature(plan_node_to_signature_)
        , plan_signature_output_orders(plan_signature_output_orders_)
    {
    }

    PlanNodePtr visitPlanNode(PlanNodeBase & node, Void & c) override
    {
        auto node_ptr = node.shared_from_this();
        if (auto signature = plan_node_to_signature.find(node_ptr); signature != plan_node_to_signature.end())
        {
            if (!plan_signature_to_cte_id.contains(signature->second))
            {
                auto cte_id = cte_info.nextCTEId();
                cte_info.add(cte_id, node_ptr);
                plan_signature_to_cte_id.emplace(signature->second, std::make_pair(cte_id, node_ptr));
                std::unordered_map<String, String> output_columns;
                for (const auto & output : node.getOutputNames())
                    output_columns.emplace(output, output);
                return PlanNodeBase::createPlanNode(
                    context->nextNodeId(), std::make_shared<CTERefStep>(node.getStep()->getOutputStream(), cte_id, output_columns, false));
            }
            else
            {
                auto & cte = plan_signature_to_cte_id.at(signature->second);
                auto cte_id = cte.first;

                auto forward_order = plan_signature_output_orders.at(node_ptr);
                auto reverse_order = plan_signature_output_orders.at(cte.second);

                std::unordered_map<String, String> output_columns;
                for (const auto & output : node.getOutputNames())
                {
                    auto input_column = reverse_order.getByPosition(forward_order.getPositionByName(output)).name;
                    output_columns.emplace(output, input_column);
                }
                return PlanNodeBase::createPlanNode(
                    context->nextNodeId(), std::make_shared<CTERefStep>(node.getStep()->getOutputStream(), cte_id, output_columns, false));
            }
        }
        return SimplePlanRewriter::visitPlanNode(node, c);
    }

private:
    CTEInfo & cte_info;
    PlanNodeToSignatures & plan_node_to_signature;
    PlanSignatureOutputOrders & plan_signature_output_orders;
    std::unordered_map<PlanSignature, std::pair<CTEId, PlanNodePtr>> plan_signature_to_cte_id;
};

PlanNodePtr ShareCommonPlanNode::createCTERefNode(
    CTEId cte_id, const DataStream & output_stream, const DataStream & cte_output_stream, ContextMutablePtr context)
{
    std::unordered_map<String, String> output_columns;
    for (size_t i = 0; i < output_stream.header.columns(); i++)
        output_columns.emplace(output_stream.header.getByPosition(i).name, cte_output_stream.header.getByPosition(i).name);
    return PlanNodeBase::createPlanNode(context->nextNodeId(), std::make_shared<CTERefStep>(output_stream, cte_id, output_columns, false));
}

bool ShareCommonPlanNode::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    PlanSignatureProvider signature{plan.getCTEInfo(), context};
    PlanNodeToSignatures plan_node_to_signatures = signature.computeSignatures(plan.getPlanNode());
    std::unordered_map<PlanSignature, size_t> signature_counts;
    for (const auto & item : plan_node_to_signatures)
    {
        // skip partterns like [Projection] - Tablescan/CTERef/ReadNothing ...
        if (item.first->getChildren().empty())
            continue;
        if (item.first->getType() == IQueryPlanStep::Type::Projection && item.first->getChildren()[0]->getChildren().empty())
            continue;
        signature_counts[item.second]++;
    }

    PlanNodeToSignatures reuseable;
    for (const auto & item : plan_node_to_signatures)
    {
        if (signature_counts[item.second] >= 2)
            reuseable.emplace(item);
    }

    if (reuseable.empty())
        return false;

    PlanSignatureOutputOrders plan_signature_output_orders;
    for (const auto & item : reuseable)
        plan_signature_output_orders.emplace(item.first, signature.computeNormalOutputOrder(item.first));

    Rewriter rewriter{context, plan.getCTEInfo(), reuseable, plan_signature_output_orders};
    Void c;
    auto rewrite = VisitorUtil::accept(plan.getPlanNode(), rewriter, c);
    plan.update(rewrite);
    return true;
}

}

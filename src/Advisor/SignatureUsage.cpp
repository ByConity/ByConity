#include <Advisor/SignatureUsage.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
#include <Optimizer/Signature/PlanSignature.h>

#include <memory>
#include <optional>
#include <unordered_set>

namespace DB
{

SignatureUsages buildSignatureUsages(const WorkloadQueries & queries, ContextPtr context)
{
    SignatureUsages signature_usages;
    for (const auto & query : queries)
    {
        const auto & plan = query->getPlan();
        const auto & costs = query->getCosts();
        PlanSignatureProvider provider(*plan, context);
        auto plan_signatures = provider.computeSignatures();
        for (const auto & [node, sig] : plan_signatures)
        {
            // get cost
            std::optional<double> cost{};
            if (auto it = costs.find(node->getId()); it != costs.end())
                cost = std::make_optional(it->second);
            if (auto it = signature_usages.find(sig); it != signature_usages.end())
                it->second.update(node, cost);
            else
            {
                // get children
                std::unordered_set<PlanSignature> children;
                for (const auto & child : node->getChildren()) {
                    if (plan_signatures.contains(child))
                        children.emplace(plan_signatures[child]);
                }
                signature_usages.emplace(sig, SignatureUsageInfo(node, cost, children));
            }
        }
    }
    return signature_usages;
}

void SignatureUsageInfo::update(std::shared_ptr<const PlanNodeBase> _plan, std::optional<double> _cost)
{
    ++frequency;
    if (!cost.has_value() && _cost.has_value()) {
        // update with plan that have cost
        plan = _plan;
        cost = _cost;
    }
}

}

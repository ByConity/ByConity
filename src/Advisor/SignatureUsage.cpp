#include <Advisor/SignatureUsage.h>

#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>

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
        const auto & plan = query->getPlanBeforeCascades();
        PlanSignatureProvider provider(plan->getCTEInfo(), context);
        auto plan_signatures = provider.computeSignatures(plan->getPlanNode());
        for (const auto & [plan_node, signature] : plan_signatures)
        {
            signature_usages.plan_to_signature_map.emplace(plan_node, signature);
            signature_usages.signature_to_query_map[signature].emplace_back(plan_node, query);
        }
    }
    return signature_usages;
}

}

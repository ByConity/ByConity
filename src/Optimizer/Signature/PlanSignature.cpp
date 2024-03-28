#include <Optimizer/Signature/PlanSignature.h>

#include <Common/SipHash.h>
#include <Optimizer/Signature/PlanNormalizer.h>
#include <QueryPlan/PlanNode.h>

#include <memory>
#include <vector>

namespace DB
{

PlanSignature PlanSignatureProvider::computeSignature(std::shared_ptr<const PlanNodeBase> node)
{
    buffer = PlanNodeToSignatures{};
    PlanNormalizeResult res = PlanNormalizer::normalize(query_plan, context);
    return computeSignatureImpl(node, res, /*write_to_buffer*/false);
}

PlanNodeToSignatures PlanSignatureProvider::computeSignatures()
{
    buffer = PlanNodeToSignatures{};
    PlanNormalizeResult res = PlanNormalizer::normalize(query_plan, context);
    computeSignatureImpl(query_plan.getPlanNode(), res, /*write_to_buffer*/true);
    return std::move(buffer);
}

PlanNodeToSignatures computeSignatures(std::shared_ptr<const PlanNodeBase> root);


PlanSignature PlanSignatureProvider::computeSignatureImpl(std::shared_ptr<const PlanNodeBase> node,
                                                          const PlanNormalizeResult & res,
                                                          bool write_to_buffer)
{
    std::vector<PlanSignature> sigs;
    for (const auto & child : node->getChildren())
        sigs.emplace_back(computeSignatureImpl(child, res, write_to_buffer));
    // special case for cte, because its child is implicit
    if (auto cte_step = dynamic_pointer_cast<const CTERefStep>(node->getStep()))
    {
        auto cte_root = query_plan.getCTEInfo().getCTEs().at(cte_step->getId());
        auto cte_root_signature = buffer.contains(cte_root) ? buffer.at(cte_root)
                                                            : computeSignatureImpl(cte_root, res, /*write_to_buffer*/true);
        sigs.emplace_back(std::move(cte_root_signature));
    }

    sigs.emplace_back(res.getNormalizedHash(node));
    PlanSignature node_sig = combine(sigs);
    if (write_to_buffer)
        buffer.emplace(node, node_sig);
    return node_sig;
}

PlanSignature PlanSignatureProvider::combine(const std::vector<PlanSignature> & sigs)
{
    SipHash hash;
    hash.update(sigs.size());
    for (const auto & sig : sigs)
        hash.update(sig);
    return hash.get64();
}

}

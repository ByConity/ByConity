#include <Optimizer/Signature/PlanSignature.h>

#include <Common/SipHash.h>
#include <Optimizer/Signature/PlanNormalizer.h>
#include <QueryPlan/PlanNode.h>

#include <memory>
#include <vector>

namespace DB
{

size_t PlanSignatureProvider::combine(const std::vector<size_t> & hashes)
{
    SipHash hash;
    hash.update(hashes.size());
    for (const auto & v : hashes)
        hash.update(v);
    return hash.get64();
}

PlanSignature PlanSignatureProvider::computeSignature(PlanNodePtr node)
{
    PlanNodeToSignatures buffer{};
    return computeSignatureImpl(node, /*write_to_buffer*/false, buffer);
}

PlanNodeToSignatures PlanSignatureProvider::computeSignatures(PlanNodePtr node)
{
    PlanNodeToSignatures buffer{};
    computeSignatureImpl(node, /*write_to_buffer*/true, buffer);
    return buffer;
}

PlanSignature PlanSignatureProvider::computeSignatureImpl(PlanNodePtr node, bool write_to_buffer, PlanNodeToSignatures & buffer)
{
    std::vector<PlanSignature> sigs;
    for (const auto & child : node->getChildren())
        sigs.emplace_back(computeSignatureImpl(child, write_to_buffer, buffer));
    // special case for cte, because its child is implicit
    if (auto cte_step = dynamic_pointer_cast<const CTERefStep>(node->getStep()))
    {
        auto cte_root = cte_info.getCTEs().at(cte_step->getId());
        auto cte_root_signature = buffer.contains(cte_root) ? buffer.at(cte_root)
                                                            : computeSignatureImpl(cte_root, /*write_to_buffer*/true, buffer);
        sigs.emplace_back(std::move(cte_root_signature));
    }

    sigs.emplace_back(computeStepHash(node));
    PlanSignature node_sig = combine(sigs);
    if (write_to_buffer)
        buffer.emplace(node, node_sig);
    return node_sig;
}

}

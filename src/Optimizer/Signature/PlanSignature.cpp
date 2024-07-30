#include <Optimizer/Signature/PlanSignature.h>

#include <Common/SipHash.h>
#include <Optimizer/Signature/PlanNormalizer.h>
#include <QueryPlan/PlanNode.h>

#include <memory>
#include <vector>

namespace DB
{
// for test assert, see more 10102_intermediate_result_cache
static const std::unordered_set<std::string> IGNORED_SETTINGS{"max_bytes_to_read", "max_rows_to_read"};

PlanSignature PlanSignatureProvider::combine(const std::vector<PlanSignature> & hashes)
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

PlanSignature PlanSignatureProvider::combineSettings(PlanSignature signature, const SettingsChanges & settings)
{
    SipHash hash;
    hash.update(signature);

    size_t size = 0;
    for (const auto & item : settings)
    {
        if (IGNORED_SETTINGS.contains(item.name))
            continue;
        size += 1;
    }

    hash.update(size);
    for (const auto & item : settings)
    {
        if (IGNORED_SETTINGS.contains(item.name))
            continue;
        hash.update(sipHash64(item.name));
        applyVisitor(FieldVisitorHash(hash), item.value);
    }
    return hash.get64();
}

PlanNodeToSignatures PlanSignatureProvider::computeSignatures(PlanNodePtr node)
{
    PlanNodeToSignatures buffer{};
    computeSignatureImpl(node, /*write_to_buffer*/true, buffer);
    return buffer;
}


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

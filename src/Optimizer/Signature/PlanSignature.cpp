#include <Optimizer/Signature/PlanSignature.h>

#include <Optimizer/Signature/PlanNormalizer.h>
#include <Optimizer/Signature/StepNormalizer.h>
#include <QueryPlan/PlanNode.h>
#include <Common/SipHash.h>

#include <memory>
#include <vector>

namespace DB
{
// for test assert, see more 10102_intermediate_result_cache
static const std::unordered_set<std::string> IGNORED_SETTINGS{
    "max_bytes_to_read", "max_rows_to_read", "load_balancing", "prefer_localhost_replica", "send_logs_level", "max_execution_time"};

#define CHECK_IGNORED_SETTINGS(name) ((name).find("timeout") != std::string::npos || IGNORED_SETTINGS.contains(name))

size_t PlanSignatureProvider::combine(const std::vector<size_t> & hashes)
{
    SipHash hash;
    hash.update(hashes.size());
    for (const auto & v : hashes)
        hash.update(v);
    return hash.get64();
}

PlanSignature PlanSignatureProvider::computeSignature(PlanNodePtr node, PlanNormalizerOptions options)
{
    PlanNodeToSignatures buffer{};
    return computeSignatureImpl(node, /*write_to_buffer*/ false, buffer, options);
}

PlanSignature PlanSignatureProvider::combineSettings(PlanSignature signature, const SettingsChanges & settings)
{
    SipHash hash;
    hash.update(signature);

    size_t size = 0;
    for (const auto & item : settings)
    {
        if (CHECK_IGNORED_SETTINGS(item.name))
            continue;
        size += 1;
    }

    hash.update(size);
    for (const auto & item : settings)
    {
        if (CHECK_IGNORED_SETTINGS(item.name))
            continue;
        hash.update(sipHash64(item.name));
        applyVisitor(FieldVisitorHash(hash), item.value);
    }
    return hash.get64();
}

PlanNodeToSignatures PlanSignatureProvider::computeSignatures(PlanNodePtr node)
{
    PlanNodeToSignatures buffer{};
    computeSignatureImpl(node, /*write_to_buffer*/ true, buffer, {});
    return buffer;
}

PlanSignature PlanSignatureProvider::computeSignatureImpl(
    PlanNodePtr node, bool write_to_buffer, PlanNodeToSignatures & buffer, PlanNormalizerOptions options)
{
    std::vector<PlanSignature> sigs;
    for (const auto & child : node->getChildren())
        sigs.emplace_back(computeSignatureImpl(child, write_to_buffer, buffer, options));
    // special case for cte, because its child is implicit
    if (auto cte_step = dynamic_pointer_cast<const CTERefStep>(node->getStep()))
    {
        auto cte_root = cte_info.getCTEs().at(cte_step->getId());
        auto cte_root_signature
            = buffer.contains(cte_root) ? buffer.at(cte_root) : computeSignatureImpl(cte_root, /*write_to_buffer*/ true, buffer, options);
        sigs.emplace_back(std::move(cte_root_signature));
    }

    sigs.emplace_back(computeStepHash(node));
    PlanSignature node_sig = combine(sigs);
    if (write_to_buffer)
        buffer.emplace(node, node_sig);
    return node_sig;
}
}

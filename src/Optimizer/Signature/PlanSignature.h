#pragma once

#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/PlanNormalizer.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
#pragma once

#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/PlanNormalizer.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
#include <common/types.h>

#include <memory>
#include <unordered_map>
#include <vector>

namespace DB
{

using PlanSignature = UInt64;
using PlanNodeToSignatures = std::unordered_map<PlanNodePtr, PlanSignature>;

/**
 * an abstract class for recursively computing step hashes
 * override computeStepHash() over supported nodes
 */
class PlanSignatureProvider
{
public:
    virtual ~PlanSignatureProvider() = default;
    PlanSignatureProvider(PlanSignatureProvider &&) = default;
    PlanSignatureProvider(const PlanSignatureProvider &) = default;

    explicit PlanSignatureProvider(const CTEInfo & _cte_info, ContextPtr context): normalizer(_cte_info, context), cte_info(_cte_info)
    {
    }
    static PlanSignatureProvider from(const QueryPlan & plan, ContextPtr _context)
    {
        return PlanSignatureProvider(plan.getCTEInfo(), _context);
    }

    PlanSignature computeSignature(PlanNodePtr node);

    static PlanSignature combineSettings(PlanSignature signature, const SettingsChanges & settings);

    PlanNodeToSignatures computeSignatures(PlanNodePtr node);
    Block computeNormalOutputOrder(PlanNodePtr node)
    {
        return normalizer.computeNormalOutputOrder(node);
    }
    PlanNodePtr computeNormalPlan(PlanNodePtr node)
    {
        return normalizer.buildNormalPlan(node);
    }

protected:
    virtual PlanSignature computeStepHash(PlanNodePtr node)
    {
        return normalizer.computeNormalStep(node)->hash();
    }

    static size_t combine(const std::vector<size_t> & hashes);

    PlanNormalizer normalizer;
private:
    const CTEInfo & cte_info;
    PlanSignature computeSignatureImpl(PlanNodePtr root, bool write_to_buffer, PlanNodeToSignatures & buffer);
};

} // DB

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
using PlanNodeToSignatures = std::unordered_map<std::shared_ptr<const PlanNodeBase>, PlanSignature>;

class PlanSignatureProvider
{
public:
    explicit PlanSignatureProvider(const QueryPlan & _query_plan, ContextPtr _context): query_plan(_query_plan), context(_context) {}
    PlanSignature computeSignature(std::shared_ptr<const PlanNodeBase> node);
    PlanNodeToSignatures computeSignatures();
private:
    const QueryPlan & query_plan;
    ContextPtr context;
    // buffer initialized at every call to computeSignature(s). 1) avoid recomputing CTE signatures; 2) reporting all computed signatures
    PlanNodeToSignatures buffer;
    PlanSignature computeSignatureImpl(std::shared_ptr<const PlanNodeBase> root, const PlanNormalizeResult & res, bool write_to_buffer);
    static PlanSignature combine(const std::vector<PlanSignature> & sigs);
};

} // DB

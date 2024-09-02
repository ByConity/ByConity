#pragma once

#include <Advisor/WorkloadQuery.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>

#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>

namespace DB
{
using RelatedQueries = std::pair<PlanNodePtr, WorkloadQueryPtr>;
struct SignatureUsages
{
    PlanNodeToSignatures plan_to_signature_map;
    std::unordered_map<PlanSignature, std::vector<RelatedQueries>> signature_to_query_map;
};
SignatureUsages buildSignatureUsages(const WorkloadQueries & queries, ContextPtr context);
}

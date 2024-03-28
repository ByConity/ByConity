#pragma once

#include <Advisor/WorkloadQuery.h>
#include <Core/Types.h>
#include <Optimizer/CostModel/CostCalculator.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>

namespace DB
{
class SignatureUsageInfo;
using SignatureUsages = std::unordered_map<PlanSignature, SignatureUsageInfo>;
SignatureUsages buildSignatureUsages(const WorkloadQueries & queries, ContextPtr context);

class SignatureUsageInfo
{
public:
    explicit SignatureUsageInfo(std::shared_ptr<const PlanNodeBase> _plan,
                              std::optional<double> _cost,
                              std::unordered_set<PlanSignature> _children)
        : plan(_plan), cost(_cost), frequency(1), children(_children)
    {
    }

    void update(std::shared_ptr<const PlanNodeBase> _plan, std::optional<double> _cost);

    size_t getFrequency() const { return frequency; }
    std::shared_ptr<const PlanNodeBase> getPlan() const { return plan; }
    std::optional<double> getCost() const { return cost; }
    const std::unordered_set<PlanSignature> & getChildren() const { return children; }

private:
    // a sample plan associated with the signature
    std::shared_ptr<const PlanNodeBase> plan;
    // the cost of the sample plan
    std::optional<double> cost;
    // number of times the signature occurs
    size_t frequency;
    // children signatures
    std::unordered_set<PlanSignature> children;
};

}

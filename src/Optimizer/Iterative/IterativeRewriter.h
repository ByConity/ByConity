#pragma once

#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/IQueryPlanStep.h>

#include <unordered_map>
#include <vector>
#include <chrono>

namespace DB
{

// use a linked multi map to make rule application process more reasonable
using RuleIndex = std::unordered_map<IQueryPlanStep::Type, std::vector<RulePtr>>;

struct IterativeRewriterContext
{
    ContextMutablePtr globalContext;
    CTEInfo & cte_info;
    std::chrono::time_point<std::chrono::system_clock> start_time;
    UInt64 optimizer_timeout;
};

/**
 * A IterativeOptimizer will loop to apply `Rule`s recursively until
 * the plan does not change or the optimizer timeout been exhausted.
 */
class IterativeRewriter : public Rewriter
{
public:
    IterativeRewriter(const std::vector<RulePtr> & rules_, std::string name_);
    String name() const override { return names; }
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;

private:
    String names;
    RuleIndex rules;

    bool explorePlan(PlanNodePtr & plan, IterativeRewriterContext & context) const;
    bool exploreNode(PlanNodePtr & node, IterativeRewriterContext & context) const;
    bool exploreChildren(PlanNodePtr & plan, IterativeRewriterContext & context) const;

    static void checkTimeoutNotExhausted(const String & rule_name, const IterativeRewriterContext & context);
};

}

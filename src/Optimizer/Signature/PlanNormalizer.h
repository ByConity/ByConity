#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Signature/StepNormalizer.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/QueryPlan.h>

#include <memory>
#include <unordered_map>
#include <utility>

namespace DB
{

struct PlanNormalizerOptions
{
    bool normalize_literals = false;
    bool normalize_storage = false;
};

class PlanNormalizer
{
public:
    using NormalSteps = std::unordered_map<PlanNodePtr, StepAndOutputOrder>;
    virtual ~PlanNormalizer() = default;
    PlanNormalizer(PlanNormalizer &&) = default;
    PlanNormalizer(const PlanNormalizer &) = default;

    explicit PlanNormalizer(const CTEInfo & _cte_info, ContextPtr _context): cte_info(_cte_info), context(_context)
    {
    }

    static PlanNormalizer from(const QueryPlan & plan, ContextPtr _context) { return PlanNormalizer(plan.getCTEInfo(), _context); }

    QueryPlanStepPtr computeNormalStep(PlanNodePtr node, PlanNormalizerOptions options = {})
    {
        return computeNormalStepImpl(node, options).normal_step;
    }
    Block computeNormalOutputOrder(PlanNodePtr node)
    {
        return computeNormalStepImpl(node, {}).output_order;
    }
    PlanNodePtr buildNormalPlan(PlanNodePtr root, PlanNormalizerOptions options = {})
    {
        return buildNormalPlanImpl(root, options);
    }

protected:
    virtual StepAndOutputOrder computeNormalStepImpl(PlanNodePtr node, PlanNormalizerOptions options);

private:
    const CTEInfo & cte_info;
    ContextPtr context;
    NormalSteps normal_steps;
    PlanNodePtr buildNormalPlanImpl(PlanNodePtr node, PlanNormalizerOptions options);
};


// NormalizeVisitor is actually a ConstVisitor, which normalizes each step in the tree in post-order
class NormalizeVisitor : public PlanNodeVisitor<StepAndOutputOrder, PlanNormalizer::NormalSteps>
{
public:
    static StepAndOutputOrder normalize(
        PlanNodePtr node,
        const CTEInfo & _cte_info,
        ContextPtr _context,
        PlanNormalizer::NormalSteps & normal_steps,
        PlanNormalizerOptions options)
    {
        NormalizeVisitor visitor(_cte_info, _context, options);
        return VisitorUtil::accept(node, visitor, normal_steps);
    }

protected:
    StepAndOutputOrder visitPlanNode(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps) override
    {
        return visitPlanNodeImpl(node, normal_steps);
    }
    StepAndOutputOrder visitCTERefNode(CTERefNode & node, PlanNormalizer::NormalSteps & normal_steps) override
    {
        return visitCTERefNodeImpl(node, normal_steps);
    }
    StepAndOutputOrder visitPlanNodeImpl(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps);
    StepAndOutputOrder visitCTERefNodeImpl(PlanNodeBase & node, PlanNormalizer::NormalSteps & normal_steps);

private:
    const CTEInfo & cte_info;
    StepNormalizer step_normalizer;
    explicit NormalizeVisitor(const CTEInfo & _cte_info, ContextPtr _context, PlanNormalizerOptions options_)
        : cte_info(_cte_info), step_normalizer(_context, options_.normalize_literals, options_.normalize_storage)
    {
    }
};

class NormalizeNodeVisitor : public NodeVisitor<StepAndOutputOrder, std::unordered_map<QueryPlan::Node *, StepAndOutputOrder>>
{
public:
    static StepAndOutputOrder normalize(
        ContextPtr _context,
        QueryPlan::Node * node,
        std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> & results,
        PlanNormalizerOptions options);

protected:
    StepAndOutputOrder visitNode(QueryPlan::Node * node, std::unordered_map<QueryPlan::Node *, StepAndOutputOrder> & results) override;

private:
    StepNormalizer step_normalizer;
    explicit NormalizeNodeVisitor(ContextPtr _context, PlanNormalizerOptions options_)
        : step_normalizer(_context, options_.normalize_literals, options_.normalize_storage)
    {
    }
};
}

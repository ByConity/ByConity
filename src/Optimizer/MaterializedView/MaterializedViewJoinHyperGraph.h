#pragma once

#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/PlanNodeIdAllocator.h>
#include <QueryPlan/PlanVisitor.h>
#include <QueryPlan/QueryPlan.h>

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{
using JoinStepPtr = std::shared_ptr<JoinStep>;
struct JoinHyperGraphContext;

/**
 * JoinHyperGraph is based on "HyperGraph 
 * —— On the Correct and Complete Enumeration of the Core Search Space".
 */
class JoinHyperGraph
{
public:
    // use bitset to optimize. only support 64 tables, that is enough.
    static constexpr size_t MAX_NODE = 64;
    using NodeSet = std::bitset<MAX_NODE>;

    struct Edge
    {
        NodeSet left_all_nodes;
        NodeSet right_all_nodes;
        NodeSet all_nodes;

        // symbols sources
        NodeSet left_conditions_used_nodes;
        NodeSet right_conditions_used_nodes;
        NodeSet conditions_used_nodes;

        // join kind
        JoinStepPtr join_step;

        // join conditions
        ConstASTPtr join_condition;
        std::vector<String> left_conditions_used_symbols;
        std::vector<String> right_conditions_used_symbols;
    };

    struct HyperEdge
    {
        NodeSet left_required_nodes;
        NodeSet right_required_nodes;
    };

    struct ConflictRule
    {
        NodeSet needed_to_activate_rule;
        NodeSet required_nodes;
    };

    // build hyper graph
    static JoinHyperGraph build(
        const PlanNodePtr & plan,
        const SymbolTransformMap & symbol_transform_map,
        ContextPtr context,
        std::unordered_set<IQueryPlanStep::Type> skip_nodes = {});

    JoinHyperGraph(
        PlanNodes plan_nodes_,
        NodeSet nodes_,
        std::vector<Edge> edges_,
        std::vector<HyperEdge> hyper_edges_,
        std::unordered_map<NodeSet, std::vector<ConstASTPtr>> filters_)
        : plan_nodes(std::move(plan_nodes_))
        , nodes(std::move(nodes_))
        , edges(std::move(edges_))
        , hyper_edges(std::move(hyper_edges_))
        , filters(std::move(filters_))
    {
        assert(edges.size() == hyper_edges.size());
    }

    const PlanNodes & getPlanNodes() const { return plan_nodes; }
    const std::vector<Edge> & getEdges() const { return edges; }
    const std::vector<HyperEdge> & getHyperEdges() const { return hyper_edges; }
    const std::unordered_map<NodeSet, std::vector<ConstASTPtr>> & getFilters() const { return filters; }
    std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> getJoinConditions() const;

    bool isEmpty() const { return plan_nodes.empty(); }
    size_t size() const { return plan_nodes.size(); }

    PlanNodes getPlanNodes(const NodeSet & sub_set) const;
    NodeSet getNodeSet(const PlanNodes & sub_plan_nodes) const;
    size_t getNodeSetIndex(const PlanNodePtr & plan) const;
    size_t getNodeSetIndex(const PlanNodeId & plan_node_id) const;

    JoinHyperGraph withJoinGraph(const JoinHyperGraph & other, const JoinStepPtr & join, JoinHyperGraphContext & context) const;
    void withFilter(ConstASTPtr filter);

    String toString() const;
    String toString(const NodeSet & sub_set, bool show_full_table_name = false) const;

private:
    PlanNodes plan_nodes;
    NodeSet nodes;
    std::vector<Edge> edges;
    std::vector<HyperEdge> hyper_edges;
    std::unordered_map<NodeSet, std::vector<ConstASTPtr>> filters;

    static Edge buildEdge(NodeSet left_nodes, NodeSet right_nodes, const JoinStepPtr & join, JoinHyperGraphContext & context);

    static HyperEdge
    buildConflictRulesAndHyperEdge(const Edge & edge, const std::vector<Edge> & left_edges, const std::vector<Edge> & right_edges);

    static NodeSet absorbConflictRulesIntoNodeSets(NodeSet total_eligibility_set, std::vector<ConflictRule> & conflict_rules);
};

struct JoinHyperGraphContext
{
    using NodeSet = JoinHyperGraph::NodeSet;

    NodeSet registerPlanNode(const PlanNodePtr & node);
    NodeSet getSymbolSources(const String & symbol);
    bool isSkiped(IQueryPlanStep::Type type) { return skip_nodes.contains(type); }

    PlanNodes sources;
    std::unordered_map<PlanNodeId, size_t> plan_id_to_index;
    std::unordered_map<PlanNodePtr, size_t> plan_node_to_index;

    const ContextPtr context;
    const SymbolTransformMap & symbol_transform_map;
    const std::unordered_set<IQueryPlanStep::Type> & skip_nodes;
};

class JoinHyperGraphVisitor : public PlanNodeVisitor<JoinHyperGraph, Void>
{
    using NodeSet = JoinHyperGraph::NodeSet;

public:
    explicit JoinHyperGraphVisitor(JoinHyperGraphContext & join_hyper_graph_context_) : join_hyper_graph_context(join_hyper_graph_context_)
    {
    }

    JoinHyperGraph visitPlanNode(PlanNodeBase &, Void &) override;
    JoinHyperGraph visitJoinNode(JoinNode &, Void &) override;
    JoinHyperGraph visitFilterNode(FilterNode &, Void &) override;
    JoinHyperGraph visitProjectionNode(ProjectionNode &, Void &) override;

private:
    JoinHyperGraphContext & join_hyper_graph_context;
};

}

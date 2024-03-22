
#include <Optimizer/MaterializedView/MaterializedViewJoinHyperGraph.h>

#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPipeline.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/QueryPlan.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_JOINS;
}

// bitmap operators
static inline bool overlaps(const JoinHyperGraph::NodeSet & a, const JoinHyperGraph::NodeSet & b)
{
    return (a & b).any();
}

static inline JoinHyperGraph::NodeSet intersectOrTotal(const JoinHyperGraph::NodeSet & a, const JoinHyperGraph::NodeSet & total)
{
    auto intersect = a & total;
    return intersect.any() ? intersect : total;
}

static bool joinsAreAssociative(JoinStepPtr first, JoinStepPtr second)
{
    if (first->isInnerJoin())
        return second->isInnerJoin();
    return false;
}

// Todo: support more join kind
static bool joinsAreLeftAsscom(JoinStepPtr first, JoinStepPtr second)
{
    if (first->isInnerJoin() || first->isLeftOuterJoin())
        return second->isInnerJoin() || second->isLeftOuterJoin();
    return false;
}

static bool joinsAreRightAsscom(JoinStepPtr first, JoinStepPtr second)
{
    if (first->isInnerJoin() || first->isRightOuterJoin())
        return second->isInnerJoin() || second->isRightOuterJoin();
    return false;
}

JoinHyperGraph JoinHyperGraph::build(
    const PlanNodePtr & plan,
    const SymbolTransformMap & symbol_transform_map,
    ContextPtr context,
    std::unordered_set<IQueryPlanStep::Type> skip_nodes)
{
    JoinHyperGraphContext join_hyper_graph_context{
        .context = context, .symbol_transform_map = symbol_transform_map, .skip_nodes = skip_nodes};
    JoinHyperGraphVisitor vistor{join_hyper_graph_context};
    Void c;
    return VisitorUtil::accept(plan, vistor, c);
}

PlanNodes JoinHyperGraph::getPlanNodes(const NodeSet & sub_set) const
{
    PlanNodes res;
    for (size_t i = 0; i < plan_nodes.size(); i++)
        if (sub_set.test(i))
            res.emplace_back(plan_nodes[i]);
    return res;
}

JoinHyperGraph::NodeSet JoinHyperGraph::getNodeSet(const PlanNodes & sub_plan_nodes) const
{
    NodeSet res;
    for (const auto & node : sub_plan_nodes)
        res.set(getNodeSetIndex(node));
    return res;
}

size_t JoinHyperGraph::getNodeSetIndex(const PlanNodePtr & plan) const
{
    return getNodeSetIndex(plan->getId());
}

size_t JoinHyperGraph::getNodeSetIndex(const PlanNodeId & plan_node_id) const
{
    for (size_t i = 0; i < plan_nodes.size(); i++)
        if (plan_node_id == plan_nodes[i]->getId())
            return i;
    throw Exception("unknown plan node", ErrorCodes::LOGICAL_ERROR);
}

String JoinHyperGraph::toString() const
{
    std::stringstream ss;
    ss << "PlanNodes:" << toString(nodes, true) << "\n";

    ss << "Edges:\n";
    for (const auto & edge : edges)
        ss << toString(edge.left_conditions_used_nodes) << " -> " << toString(edge.right_conditions_used_nodes) << "\n";

    ss << "HyperEdges:\n";
    for (const auto & edge : hyper_edges)
        ss << toString(edge.left_required_nodes) << " -> " << toString(edge.right_required_nodes) << "\n";

    ss << "Filters:\n";
    for (const auto & item : filters)
        ss << toString(item.first) << " -> " << queryToString(PredicateUtils::combineConjuncts(item.second)) << "\n";

    ss << "Join Clauses:\n";
    for (const auto & item : getJoinConditions())
        ss << toString(item.first) << " -> " << queryToString(PredicateUtils::combineConjuncts(item.second)) << "\n";
    return ss.str();
}

String JoinHyperGraph::toString(const NodeSet & sub_set, bool show_full_table_name) const
{
    std::vector<String> strings;
    for (const auto & plan_node : getPlanNodes(sub_set))
    {
        if (show_full_table_name && plan_node->getType() == IQueryPlanStep::Type::TableScan)
        {
            auto * table_scan_step = dynamic_cast<TableScanStep *>(plan_node->getStep().get());
            strings.emplace_back(std::to_string(plan_node->getId()) + ":" + table_scan_step->getStorageID().getFullTableName());
        }
        else
        {
            strings.emplace_back(std::to_string(plan_node->getId()));
        }
    }
    return "{" + boost::algorithm::join(strings, ",") + "}";
}

std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> JoinHyperGraph::getJoinConditions() const
{
    std::unordered_map<JoinHyperGraph::NodeSet, std::vector<ConstASTPtr>> join_clauses;
    for (size_t i = 0; i < edges.size(); i++)
    {
        const auto & hyper_edge = hyper_edges.at(i);
        join_clauses[hyper_edge.left_required_nodes | hyper_edge.right_required_nodes].emplace_back(edges.at(i).join_condition);
    }
    return join_clauses;
}

JoinHyperGraph JoinHyperGraph::withJoinGraph(const JoinHyperGraph & other, const JoinStepPtr & join, JoinHyperGraphContext & context) const
{
    // merge plan nodes
    PlanNodes merged_plan_nodes{plan_nodes.begin(), plan_nodes.end()};
    merged_plan_nodes.insert(merged_plan_nodes.end(), other.plan_nodes.begin(), other.plan_nodes.end());

    NodeSet merged_nodes = nodes | other.nodes;

    // merge filters
    std::unordered_map<NodeSet, std::vector<ConstASTPtr>> merged_filters{filters.begin(), filters.end()};
    for (const auto & item : other.filters)
    {
        auto & it = merged_filters[item.first];
        it.insert(it.end(), item.second.begin(), item.second.end());
    }

    // build new edge
    Edge edge = buildEdge(nodes, other.nodes, join, context);

    // build hyper edge
    HyperEdge hyper_edge = buildConflictRulesAndHyperEdge(edge, edges, other.edges);

    // merge edges
    std::vector<Edge> merged_edges{edges.begin(), edges.end()};
    for (const auto & other_edge : other.edges)
        merged_edges.emplace_back(other_edge);
    merged_edges.emplace_back(edge);

    // merge hyper edges
    std::vector<HyperEdge> merged_hyper_edges{hyper_edges.begin(), hyper_edges.end()};
    for (const auto & other_hyper_edge : other.hyper_edges)
        merged_hyper_edges.emplace_back(other_hyper_edge);

    merged_hyper_edges.emplace_back(hyper_edge);

    return JoinHyperGraph{merged_plan_nodes, merged_nodes, merged_edges, merged_hyper_edges, merged_filters};
}

JoinHyperGraph::Edge JoinHyperGraph::buildEdge(NodeSet left_nodes, NodeSet right_nodes, const JoinStepPtr & join, JoinHyperGraphContext & context)
{
    std::vector<String> left_conditions_used_symbols{join->getLeftKeys().begin(), join->getLeftKeys().end()};
    std::vector<String> right_conditions_used_symbols{join->getRightKeys().begin(), join->getRightKeys().end()};
    for (const auto & symbol : SymbolsExtractor::extract(join->getFilter()))
    {
        if (join->getInputStreams()[0].header.has(symbol))
            left_conditions_used_symbols.emplace_back(symbol);
        else
            right_conditions_used_symbols.emplace_back(symbol);
    }

    NodeSet left_conditions_used_nodes;
    NodeSet right_conditions_used_nodes;
    for (const auto & symbol : left_conditions_used_symbols)
        left_conditions_used_nodes |= context.getSymbolSources(symbol);

    for (const auto & symbol : right_conditions_used_symbols)
        right_conditions_used_nodes |= context.getSymbolSources(symbol);

    std::vector<ConstASTPtr> join_filters;
    join_filters.emplace_back(join->getFilter());
    for (size_t i = 0; i < join->getLeftKeys().size(); i++)
        join_filters.emplace_back(makeASTFunction(
            "equals", std::make_shared<ASTIdentifier>(join->getLeftKeys()[i]), std::make_shared<ASTIdentifier>(join->getRightKeys()[i])));

    return Edge{
        left_nodes,
        right_nodes,
        left_nodes | right_nodes,
        left_conditions_used_nodes,
        right_conditions_used_nodes,
        left_conditions_used_nodes | right_conditions_used_nodes,
        join,
        PredicateUtils::combineConjuncts(join_filters),
        left_conditions_used_symbols,
        right_conditions_used_symbols};
}

JoinHyperGraph::HyperEdge JoinHyperGraph::buildConflictRulesAndHyperEdge(
    const Edge & edge, const std::vector<Edge> & left_edges, const std::vector<Edge> & right_edges)
{
    std::vector<ConflictRule> conflict_rules;
    for (const auto & child : left_edges)
    {
        if (!joinsAreAssociative(child.join_step, edge.join_step))
        {
            // Prevent associative rewriting; we cannot apply this operator
            // (rule kicks in as soon as _any_ table from the right side
            // is seen) until we have all nodes mentioned on the left side of
            // the join condition.
            NodeSet left = intersectOrTotal(child.conditions_used_nodes, child.left_all_nodes);
            conflict_rules.emplace_back(ConflictRule{child.right_all_nodes, left});
        }

        if (!joinsAreLeftAsscom(child.join_step, edge.join_step))
        {
            // Prevent l-asscom rewriting; we cannot apply this operator
            // (rule kicks in as soon as _any_ table from the left side
            // is seen) until we have all nodes mentioned on the right side of
            // the join condition.
            NodeSet right = intersectOrTotal(child.conditions_used_nodes, child.right_all_nodes);
            conflict_rules.emplace_back(ConflictRule{child.left_all_nodes, right});
        }
    }

    // Exactly the same as the previous, just mirrored left/right.
    for (const auto & child : right_edges)
    {
        if (!joinsAreAssociative(edge.join_step, child.join_step))
        {
            NodeSet right = intersectOrTotal(child.conditions_used_nodes, child.right_all_nodes);
            conflict_rules.emplace_back(ConflictRule{child.left_all_nodes, right});
        }

        if (!joinsAreRightAsscom(edge.join_step, child.join_step))
        {
            NodeSet left = intersectOrTotal(child.conditions_used_nodes, child.left_all_nodes);
            conflict_rules.emplace_back(ConflictRule{child.right_all_nodes, left});
        }
    }

    // Now go through all the conflict rules and use them to grow the hyper-node
    // TODO: check if there are any conflict rules that we can get rid of.
    NodeSet total_eligibility_set = absorbConflictRulesIntoNodeSets(edge.conditions_used_nodes, conflict_rules);

    // Re-check we cannot have hyper-edges with empty end points.
    if (!overlaps(total_eligibility_set, edge.left_all_nodes))
        total_eligibility_set |= edge.left_all_nodes;
    if (!overlaps(total_eligibility_set, edge.right_all_nodes))
        total_eligibility_set |= edge.right_all_nodes;

    NodeSet left = total_eligibility_set & edge.left_all_nodes;
    NodeSet right = total_eligibility_set & edge.right_all_nodes;
    return HyperEdge{left, right};
}

JoinHyperGraph::NodeSet
JoinHyperGraph::absorbConflictRulesIntoNodeSets(NodeSet total_eligibility_set, std::vector<ConflictRule> & conflict_rules)
{
    for (const ConflictRule & rule : conflict_rules)
    {
        if (overlaps(rule.needed_to_activate_rule, total_eligibility_set))
            total_eligibility_set |= rule.required_nodes;
    }
    return total_eligibility_set;
}

void JoinHyperGraph::withFilter(ConstASTPtr filter)
{
    filters[nodes].emplace_back(filter);
}

JoinHyperGraph JoinHyperGraphVisitor::visitPlanNode(PlanNodeBase & node, Void & c)
{
    if (join_hyper_graph_context.isSkiped(node.getType()))
        return VisitorUtil::accept(node.getChildren()[0], *this, c);

    auto node_ptr = node.shared_from_this();
    return JoinHyperGraph{{node_ptr}, join_hyper_graph_context.registerPlanNode(node_ptr), {}, {}, {}};
}

JoinHyperGraph JoinHyperGraphVisitor::visitJoinNode(JoinNode & node, Void & c)
{
    auto left = VisitorUtil::accept(node.getChildren()[0], *this, c);
    auto right = VisitorUtil::accept(node.getChildren()[1], *this, c);
    return left.withJoinGraph(right, node.getStep(), join_hyper_graph_context);
}

JoinHyperGraph JoinHyperGraphVisitor::visitFilterNode(FilterNode & node, Void & c)
{
    auto child = VisitorUtil::accept(node.getChildren()[0], *this, c);
    child.withFilter(node.getStep()->getFilter());
    return child;
}

JoinHyperGraph JoinHyperGraphVisitor::visitProjectionNode(ProjectionNode & node, Void & c)
{
    return VisitorUtil::accept(node.getChildren()[0], *this, c);
}

JoinHyperGraph::NodeSet JoinHyperGraphContext::getSymbolSources(const String & symbol)
{
    JoinHyperGraph::NodeSet node_set;
    std::function<void(const ConstASTPtr &)> collect = [&](const ConstASTPtr & expr) -> void {
        if (expr->getType() == ASTType::ASTTableColumnReference)
        {
            auto target_plan_node_id = expr->as<ASTTableColumnReference>()->unique_id;
            node_set.set(plan_id_to_index.at(target_plan_node_id), true);
            return;
        }
        for (const auto & child : expr->children)
            collect(child);
    };

    auto lineage = symbol_transform_map.inlineReferences(symbol);
    collect(lineage);
    return node_set;
}

JoinHyperGraph::NodeSet JoinHyperGraphContext::registerPlanNode(const PlanNodePtr & node)
{
    size_t index = sources.size();
    sources.emplace_back(node);
    if (sources.size() > JoinHyperGraph::MAX_NODE)
        throw Exception("max join node size exceeded: " + std::to_string(JoinHyperGraph::MAX_NODE), ErrorCodes::TOO_MANY_JOINS);
    plan_id_to_index.emplace(node->getId(), index);
    plan_node_to_index.emplace(node, index);

    JoinHyperGraph::NodeSet node_set;
    node_set.set(index, true);
    return node_set;
}

}

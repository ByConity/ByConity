#pragma once

#include <Optimizer/Property/Equivalences.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <boost/dynamic_bitset.hpp>

namespace DB
{
using GroupId = UInt32;
using BitSet = boost::dynamic_bitset<>;
class Memo;

class Graph
{
public:
    struct Edge
    {
        Edge(String source_symbol_, String target_symbol_)
            : source_symbol(std::move(source_symbol_)), target_symbol(std::move(target_symbol_))
        {
        }
        String source_symbol;
        String target_symbol;
    };

    struct Partition
    {
        std::unordered_set<GroupId> left;
        std::unordered_set<GroupId> right;
        Partition(const BitSet & left_bit, const BitSet & right_bit)
        {
            auto pos = left_bit.find_first();
            while (pos != BitSet::npos)
            {
                left.insert(pos);
                pos = left_bit.find_next(pos);
            }
            pos = right_bit.find_first();
            while (pos != BitSet::npos)
            {
                right.insert(pos);
                pos = right_bit.find_next(pos);
            }
        }
    };

    static Graph build(const std::vector<GroupId> & groups, const UnionFind<String> & union_find, ASTPtr filter, const Memo & memo);
    std::vector<Partition> cutPartitions() const;

    const std::vector<GroupId> & getNodes() const { return nodes; }
    const std::map<GroupId, std::map<GroupId, std::vector<Edge>>> & getEdges() const { return edges; }


    std::vector<std::pair<String, String>> bridges(const std::vector<GroupId> & left, const std::vector<GroupId> & right) const
    {
        std::vector<std::pair<String, String>> result;
        std::unordered_set<GroupId> right_set;
        right_set.insert(right.begin(), right.end());
        for (auto group_id : left)
        {
            for (const auto & item : edges.at(group_id))
            {
                if (right_set.contains(item.first))
                {
                    for (const auto & edge : item.second)
                    {
                        result.emplace_back(edge.source_symbol, edge.target_symbol);
                    }
                }
            }
        }
        return result;
    }

    std::vector<GroupId> getDFSOrder(const std::unordered_set<GroupId> & sub_nodes) const
    {
        std::vector<GroupId> res;
        std::unordered_set<GroupId> visited;

        // first visit min node
        auto min = *std::min_element(sub_nodes.begin(), sub_nodes.end());
        dfs(res, min, sub_nodes, visited);

        return res;
    }

    void toProto(Protos::Graph & proto) const
    {
        for (auto node : nodes)
            proto.add_node(node);

        for (const auto & [from, map] : edges)
        {
            for (const auto & [to, links] : map)
            {
                for (const auto & edge : links)
                {
                    auto * e = proto.add_edges();
                    e->set_from(from);
                    e->set_to(to);
                    e->set_source(edge.source_symbol);
                    e->set_target(edge.target_symbol);
                }
            }
        }
        serializeASTToProto(filter, *proto.mutable_filter());
    }

    void fillFromProto(const Protos::Graph &) { }

    const UnionFind<String> & getUnionFind() const { return union_find; }
    ASTPtr getFilter() const { return filter; }

private:
    void standardize();

    void
    dfs(std::vector<GroupId> & path,
        GroupId node,
        const std::unordered_set<GroupId> & sub_nodes,
        std::unordered_set<GroupId> & visited) const
    {
        path.emplace_back(node);
        visited.insert(node);

        std::vector<GroupId> need_search;
        for (const auto & item : edges.at(node))
        {
            if (sub_nodes.contains(item.first) && !visited.contains(item.first))
            {
                need_search.emplace_back(item.first);
            }
        }
        std::sort(need_search.begin(), need_search.end());
        for (auto group_id : need_search)
        {
            if (!visited.contains(group_id))
            {
                dfs(path, group_id, sub_nodes, visited);
            }
        }
    }

    std::vector<GroupId> nodes;
    std::map<GroupId, std::map<GroupId, std::vector<Edge>>> edges;
    ASTPtr filter;
    UnionFind<String> union_find;
};

class MinCutBranchAlg
{
public:
    explicit MinCutBranchAlg(const Graph & graph_) : graph(graph_) { }

    void partition();

    const std::vector<Graph::Partition> & getPartitions() const { return partitions; }

private:
    BitSet minCutBranch(const BitSet & s, const BitSet & c, const BitSet & x, const BitSet & l);

    BitSet neighbor(const BitSet & nodes);
    BitSet reachable(const BitSet & c, const BitSet & l);

    const Graph & graph;
    std::vector<Graph::Partition> partitions;
};

}

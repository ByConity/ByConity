#include <Optimizer/Graph.h>

#include <Optimizer/Cascades/Memo.h>
#include <Optimizer/JoinGraph.h>
#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <Optimizer/Signature/ExpressionReorderNormalizer.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/CTEInfo.h>

namespace DB
{
Graph Graph::build(const std::vector<GroupId> & groups, const UnionFind<String> & union_find, ASTPtr filter, const Memo & memo)
{
    Graph graph;

    std::unordered_map<String, GroupId> symbol_to_group_id;
    for (auto group_id : groups)
    {
        for (const auto & symbol : memo.getGroupById(group_id)->getStep()->getOutputStream().header)
        {
            assert(!symbol_to_group_id.contains(symbol.name)); // duplicate symbol
            symbol_to_group_id[symbol.name] = group_id;
        }
        graph.nodes.emplace_back(group_id);
    }

    for (const auto & sets : union_find.getSets())
    {
        for (const auto & source_symbol : sets)
        {
            for (const auto & target_symbol : sets)
            {
                Utils::checkState(symbol_to_group_id.contains(source_symbol));
                Utils::checkState(symbol_to_group_id.contains(target_symbol));
                auto source_id = symbol_to_group_id.at(source_symbol);
                auto target_id = symbol_to_group_id.at(target_symbol);
                if (source_id != target_id)
                {
                    graph.edges[source_id][target_id].emplace_back(source_symbol, target_symbol);
                }
            }
        }
    }
    graph.filter = filter;
    graph.union_find = union_find;
    graph.standardize();

    return graph;
}

void Graph::standardize()
{
    std::sort(nodes.begin(), nodes.end());
    for (auto & pair : edges)
    {
        for (auto & edge_pair : pair.second)
        {
            auto & edge = edge_pair.second;
            std::sort(edge.begin(), edge.end(), [](const Edge & lhs, const Edge & rhs) {
                if (lhs.source_symbol != rhs.source_symbol)
                    return lhs.source_symbol < rhs.source_symbol;
                return lhs.target_symbol < rhs.target_symbol;
            });
        }
    }
    ExpressionReorderNormalizer::reorder(filter);
}


std::vector<Graph::Partition> Graph::cutPartitions() const
{
    MinCutBranchAlg alg(*this);
    alg.partition();
    return alg.getPartitions();
}

void MinCutBranchAlg::partition()
{
    auto min_node = *std::min_element(graph.getNodes().begin(), graph.getNodes().end());
    auto bit_size = *std::max_element(graph.getNodes().begin(), graph.getNodes().end()) + 1;

    BitSet s{bit_size};
    for (const auto & group_id : graph.getNodes())
        s[group_id] = true;

    BitSet c{bit_size};
    c[min_node] = true;
    minCutBranch(s, c, BitSet{s.size()}, c);
}

BitSet MinCutBranchAlg::minCutBranch(const BitSet & s, const BitSet & c, const BitSet & x, const BitSet & l)
{
    BitSet r{s.size()};
    BitSet r_tmp{s.size()};
    BitSet neighbor_l = neighbor(l);
    BitSet n_l = ((neighbor_l & s) - c) - x;
    BitSet n_x = ((neighbor_l & s) - c) & x;
    BitSet n_b = (((neighbor(c) & s) - c) - n_l) - x;

    BitSet x_tmp{s.size()};
    GroupId v;
    while (n_l.any() || n_x.any() || (n_b & r_tmp).any())
    {
        if (((n_b | n_l) & r_tmp).any())
        {
            v = ((n_b | n_l) & r_tmp).find_first();
            BitSet new_c = c;
            new_c[v] = true;
            BitSet new_l{s.size()};
            new_l[v] = true;
            minCutBranch(s, new_c, x_tmp, new_l);
            n_l[v] = false;
            n_b[v] = false;
        }
        else
        {
            x_tmp = x;
            if (n_l.any())
            {
                v = n_l.find_first();

                BitSet new_c = c;
                new_c[v] = true;
                BitSet new_l{s.size()};
                new_l[v] = true;

                r_tmp = minCutBranch(s, new_c, x_tmp, new_l);
                n_l[v] = false;
            }
            else
            {
                v = n_x.find_first();

                BitSet new_c = c;
                new_c[v] = true;
                BitSet new_l{s.size()};
                new_l[v] = true;

                r_tmp = reachable(new_c, new_l);
            }
            n_x = n_x - r_tmp;
            if ((r_tmp & x).any())
            {
                n_x = n_x | (n_l - r_tmp);
                n_l = n_l & r_tmp;
                n_b = n_b & r_tmp;
            }
            if (((s - r_tmp) & x).any())
            {
                n_l = n_l - r_tmp;
                n_b = n_b - r_tmp;
            }
            else
            {
                partitions.emplace_back(s - r_tmp, r_tmp);
            }
            r = r | r_tmp;
        }
        x_tmp[v] = true;
    }

    return r | l;
}

BitSet MinCutBranchAlg::neighbor(const BitSet & nodes)
{
    BitSet res{nodes.size()};
    auto pos = nodes.find_first();
    while (pos != boost::dynamic_bitset<>::npos)
    {
        if (!graph.getEdges().contains(pos))
        {
            throw Exception("Not found edge in graph", DB::ErrorCodes::LOGICAL_ERROR);
        }
        for (const auto & item : graph.getEdges().at(pos))
        {
            res[item.first] = true;
        }
        pos = nodes.find_next(pos);
    }
    return res - nodes;
}

BitSet MinCutBranchAlg::reachable(const BitSet & c, const BitSet & l)
{
    BitSet r = l;

    BitSet n = neighbor(l) - c;
    while (n.any())
    {
        r = r | n;
        n = neighbor(n) - r - c;
    }
    return r;
}


}

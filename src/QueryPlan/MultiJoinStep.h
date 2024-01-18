#pragma once

#include <Optimizer/Graph.h>
#include <Optimizer/Property/Equivalences.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/IQueryPlanStep.h>

namespace DB
{
// class JoinSet;
// class Memo;
// using GroupId = UInt32;

// class Graph
// {
// public:
//     struct Edge
//     {
//         Edge(String source_symbol_, String target_symbol_)
//             : source_symbol(std::move(source_symbol_)), target_symbol(std::move(target_symbol_))
//         {
//         }
//         String source_symbol;
//         String target_symbol;
//     };

//     static Graph createFromJoinSet(JoinSet & join_set, const Memo & memo);


//     std::vector<GroupId> nodes;
//     std::map<GroupId, std::map<GroupId, std::vector<Edge>>> edges;
//     ASTPtr filter;
//     UnionFind<String> union_find;

//     void toProto(Protos::Graph & proto) const
//     {
//         for (auto node : nodes)
//             proto.add_node(node);

//         for (const auto & [from, map] : edges)
//         {
//             for (const auto & [to, edges] : map)
//             {
//                 for (const auto & edge : edges)
//                 {
//                     auto * e = proto.add_edges();
//                     e->set_from(from);
//                     e->set_to(to);
//                     e->set_source(edge.source_symbol);
//                     e->set_target(edge.target_symbol);
//                 }
//             }
//         }
//         serializeASTToProto(filter, *proto.mutable_filter());
//     }

//     void fillFromProto(const Protos::Graph & proto) { }

// private:
//     void standardize();
// };

class MultiJoinStep : public IQueryPlanStep
{
public:
    explicit MultiJoinStep(const DataStream & output_, const Graph & graph_) : graph(graph_) { output_stream = output_; }

    String getName() const override { return "MultiJoin"; }
    Type getType() const override { return Type::MultiJoin; }

    bool isPhysical() const override { return false; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & context) override;

    void toProto(Protos::MultiJoinStep & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<MultiJoinStep> fromProto(const Protos::MultiJoinStep &, ContextPtr)
    {
        throw Exception("UNREACHABLE MultiJoinStep::deserialize()!", ErrorCodes::NOT_IMPLEMENTED);
    }

    static QueryPlanStepPtr deserialize(ReadBuffer & buf, ContextPtr context);

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr context) const override;
    void setInputStreams(const DataStreams & input_streams_) override;

    const Graph & getGraph() const { return graph; }

private:
    Graph graph;
};

}

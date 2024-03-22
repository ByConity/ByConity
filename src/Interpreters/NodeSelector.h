#pragma once

#include <memory>
#include <set>
#include <string_view>
#include <time.h>
#include <Client/Connection.h>
#include <CloudServices/CnchServerResource.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DAGGraph.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/sendPlanSegment.h>
#include <Parsers/queryToString.h>
#include <Processors/Exchange/DataTrans/RpcChannelPool.h>
#include <Processors/Exchange/DataTrans/RpcClient.h>
#include <Protos/plan_segment_manager.pb.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <butil/endpoint.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>

namespace DB
{
enum class NodeSelectorPolicy : int8_t
{
    InvalidPolicy,
    LocalPolicy,
    SourcePolicy,
    ComputePolicy
};

enum class NodeType : uint8_t
{
    Local,
    Remote
};

struct WorkerNode
{
    WorkerNode() = default;
    explicit WorkerNode(const AddressInfo & address_, NodeType type_ = NodeType::Remote, String id_ = "")
        : address(address_), type(type_), id(id_)
    {
    }
    AddressInfo address;
    NodeType type{NodeType::Remote};
    String id;
};

using WorkerNodes = std::vector<WorkerNode>;

struct ClusterNodes
{
    explicit ClusterNodes(ContextPtr & query_context)
    {
        // Pick workers per policy.
        AdaptiveScheduler adaptive_scheduler(query_context);
        rank_worker_ids = query_context->getSettingsRef().enable_adaptive_scheduler ? adaptive_scheduler.getHealthyWorkerRank()
                                                                                    : adaptive_scheduler.getRandomWorkerRank();


        const auto & worker_group = query_context->tryGetCurrentWorkerGroup();
        if (worker_group)
        {
            for (auto i : rank_worker_ids)
            {
                const auto & worker_endpoint = worker_group->getHostWithPortsVec()[i];
                auto worker_address = getRemoteAddress(worker_endpoint, query_context);
                rank_workers.emplace_back(worker_address, NodeType::Remote, worker_endpoint.id);
            }
        }
    }
    std::vector<size_t> rank_worker_ids;
    std::vector<WorkerNode> rank_workers;
};

struct NodeSelectorResult
{
    struct SourceAddressInfo
    {
        SourceAddressInfo(const AddressInfos & addresses_)
            : addresses(addresses_)
        {
        }

        // = 0 when the exchange mode to the source is LOCAL_XX_NEED_REPARTITION. Otherwise keep it empty.
        std::optional<size_t> parallel_index;
        AddressInfos addresses;
        String toString() const
        {
            fmt::memory_buffer buf;
            for (const auto & address : addresses)
            {
                fmt::format_to(buf, "source address : {}\t", address.toString());
            }
            return fmt::to_string(buf);
        }
    };

    WorkerNodes worker_nodes;
    std::vector<size_t> indexes;

    //input plansegment id => source address
    std::unordered_map<size_t, SourceAddressInfo> source_addresses;

    AddressInfos getAddress() const
    {
        AddressInfos ret;
        ret.reserve(worker_nodes.size());
        for (auto & address : worker_nodes)
            ret.emplace_back(address.address);
        return ret;
    }

    String toString() const
    {
        fmt::memory_buffer buf;
        for (const auto & node_info : worker_nodes)
        {
            fmt::format_to(buf, "Target address : {}\t", node_info.address.toString());
        }
        return fmt::to_string(buf);
    }
};

///////////////node selector

template <class DerivedSelector>
class CommonNodeSelector
{
public:
    CommonNodeSelector(const ClusterNodes & cluster_nodes_, Poco::Logger * log_) : cluster_nodes(cluster_nodes_), log(log_) { }

    void checkClusterInfo(PlanSegment * plan_segment_ptr)
    {
        if (plan_segment_ptr->getClusterName().empty())
        {
            throw Exception(
                "Logical error: can't find workgroup in context which named " + plan_segment_ptr->getClusterName(),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    ~CommonNodeSelector() = default;

protected:
    const ClusterNodes & cluster_nodes;
    Poco::Logger * log;
};

class LocalNodeSelector : public CommonNodeSelector<LocalNodeSelector>
{
public:
    LocalNodeSelector(const ClusterNodes & cluster_nodes_, Poco::Logger * log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context);
};

class LocalityNodeSelector : public CommonNodeSelector<LocalityNodeSelector>
{
public:
    LocalityNodeSelector(const ClusterNodes & cluster_nodes_, Poco::Logger * log_) : CommonNodeSelector(cluster_nodes_, log_)
    {
    }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context, DAGGraph * dag_graph_ptr);
};

class SourceNodeSelector : public CommonNodeSelector<SourceNodeSelector>
{
public:
    SourceNodeSelector(const ClusterNodes & cluster_nodes_, Poco::Logger * log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr, ContextPtr query_context);
};

class ComputeNodeSelector : public CommonNodeSelector<ComputeNodeSelector>
{
public:
    ComputeNodeSelector(const ClusterNodes & cluster_nodes_, Poco::Logger * log_) : CommonNodeSelector(cluster_nodes_, log_) { }
    NodeSelectorResult select(PlanSegment * plan_segment_ptr);
};

class NodeSelector
{
public:
    using SelectorResultMap = std::unordered_map<size_t, NodeSelectorResult>;
    NodeSelector(ClusterNodes & cluster_nodes_, ContextPtr query_context_, std::shared_ptr<DAGGraph> dag_graph_ptr_)
        : query_context(query_context_)
        , dag_graph_ptr(dag_graph_ptr_.get())
        , cluster_nodes(cluster_nodes_)
        , log(&Poco::Logger::get("NodeSelector"))
        , local_node_selector(cluster_nodes, log)
        , source_node_selector(cluster_nodes, log)
        , compute_node_selector(cluster_nodes, log)
        , locality_node_selector(cluster_nodes, log)
    {
    }

    NodeSelectorResult select(PlanSegment * plan_segment_ptr, bool is_source);
    static PlanSegmentInputPtr tryGetLocalInput(PlanSegment * plan_segment_ptr);

private:
    ContextPtr query_context;
    DAGGraph * dag_graph_ptr;
    const ClusterNodes & cluster_nodes;
    Poco::Logger * log;
    LocalNodeSelector local_node_selector;
    SourceNodeSelector source_node_selector;
    ComputeNodeSelector compute_node_selector;
    LocalityNodeSelector locality_node_selector;
};

}

#include <set>
#include <time.h>
#include <Client/Connection.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/SegmentScheduler.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/Macros.h>
#include <Common/ProfileEvents.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
}

PlanSegmentsStatusPtr
SegmentScheduler::insertPlanSegments(const String & query_id, PlanSegmentTree * plan_segments_ptr, ContextPtr query_context)
{
    std::shared_ptr<DAGGraph> dag_ptr = std::make_shared<DAGGraph>();
    buildDAGGraph(plan_segments_ptr, dag_ptr);
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        if (query_map.find(query_id) != query_map.end())
        {
            // cancel running query
            if (query_context->getSettingsRef().replace_running_query)
            {
                //TODO dongyifeng kill query
            }
            else
                throw Exception("Query with id = " + query_id + " is already running.", ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING);
        }
        query_map.emplace(std::make_pair(query_id, dag_ptr));
    }
    scheduler(query_id, query_context, dag_ptr);
#if defined(TASK_ASSIGN_DEBUG)
    String res;
    res += "dump statics:" + std::to_string(dag_ptr->exchange_data_assign_node_mappings.size()) + "\n";
    for (auto it = dag_ptr->exchange_data_assign_node_mappings.begin(); it != dag_ptr->exchange_data_assign_node_mappings.end(); it++)
    {
        res += "segment id: " + std::to_string(it->first);
        for (size_t j = 0; j < it->second.size(); j++)
        {
            res += "\n  index:" + std::to_string(it->second[j].first) + " address:" + it->second[j].second.getHostName() + "_"
                + std::to_string(it->second[j].second.getPort());
        }
        res += "\n";
    }
    LOG_DEBUG(log, res);

#endif
    return dag_ptr->plan_segment_status_ptr;
}


CancellationCode
SegmentScheduler::cancelPlanSegmentsFromCoordinator(const String query_id, const String & exception, ContextPtr query_context)
{
    String coordinator_host = query_context->getLocalHost();
    return cancelPlanSegments(query_id, exception, coordinator_host);
}

CancellationCode SegmentScheduler::cancelPlanSegments(
    const String & query_id, const String & exception, const String & origin_host_name, std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    std::shared_ptr<DAGGraph> dag_ptr;

    if (dag_graph_ptr == nullptr) // try to get the dag_graph_ptr
    {
        std::unique_lock<bthread::Mutex> lock(mutex);
        auto query_map_ite = query_map.find(query_id);
        if (query_map_ite == query_map.end())
            return CancellationCode::NotFound;
        dag_ptr = query_map_ite->second;
    }
    else
    {
        dag_ptr = dag_graph_ptr;
    }

    {
        {
            std::unique_lock<bthread::Mutex> lock(dag_ptr->status_mutex);
            LOG_ERROR(
                log,
                "query(" + query_id + ") receive error from host:" + origin_host_name + " with exception:" + exception
                    + " and plan_send_addresses size:" + std::to_string(dag_ptr->plan_send_addresses.size()));

            if (dag_ptr->plan_segment_status_ptr->is_cancel.load(std::memory_order_relaxed))
                return CancellationCode::CancelSent;
            dag_ptr->plan_segment_status_ptr->is_cancel.store(true, std::memory_order_relaxed);
            dag_ptr->plan_segment_status_ptr->exception
                = "query(" + query_id + ") receive exception from host-" + origin_host_name + " with exception:" + exception;
        }

        /// send cancel query rpc request to all executor except exception original executor
        // TODO dongyifeng add logic when get status rpc is merged
    }
    return CancellationCode::CancelSent;
}

bool SegmentScheduler::finishPlanSegments(const String & query_id)
{
    std::unique_lock<bthread::Mutex> lock(mutex);
    auto query_map_ite = query_map.find(query_id);
    if (query_map_ite != query_map.end())
        query_map.erase(query_map_ite);
    return true;
}

AddressInfos SegmentScheduler::getWorkerAddress(const String & query_id, size_t segment_id)
{
    std::unique_lock<bthread::Mutex> lock(mutex);
    auto query_map_ite = query_map.find(query_id);
    if (query_map_ite == query_map.end())
        return {};
    std::shared_ptr<DAGGraph> dag_ptr = query_map_ite->second;
    if (dag_ptr->id_to_address.count(segment_id))
        return dag_ptr->id_to_address[segment_id];
    else
        return {};
}

String SegmentScheduler::getCurrentDispatchStatus(const String & query_id)
{
    std::unique_lock<bthread::Mutex> lock(mutex);
    auto query_map_ite = query_map.find(query_id);
    if (query_map_ite == query_map.end())
        return "query_id-" + query_id + " is not exist in scheduler query map";

    std::shared_ptr<DAGGraph> dag_ptr = query_map_ite->second;
    String status("(segment_id, worker_size): ");
    for (const auto & ip_address : dag_ptr->id_to_address)
        status += "(" + std::to_string(ip_address.first) + "," + std::to_string(ip_address.second.size()) + "), ";
    return status;
}


void SegmentScheduler::buildDAGGraph(PlanSegmentTree * plan_segments_ptr, std::shared_ptr<DAGGraph> graph_ptr)
{
    graph_ptr->plan_segment_status_ptr = std::make_shared<PlanSegmentsStatus>();
    PlanSegmentTree::Nodes & nodes = plan_segments_ptr->getNodes();

    // use to traversal the tree
    std::stack<PlanSegmentTree::Node *> plan_segment_stack;
    std::vector<PlanSegment *> plan_segment_vector;
    std::set<size_t> plan_segment_vector_id_list;
    for (PlanSegmentTree::Node & node : nodes)
    {
        plan_segment_stack.emplace(&node);
        if (plan_segment_vector_id_list.find(node.getPlanSegment()->getPlanSegmentId()) == plan_segment_vector_id_list.end())
        {
            // todo dongyifeng for test, should remove later
//            node.getPlanSegment()->setParallelSize(2);
//            node.getPlanSegment()->getPlanSegmentInputs()[0]->setStorageID({String("default"), String("test_join_local")});

            plan_segment_vector.emplace_back(node.getPlanSegment());
            plan_segment_vector_id_list.emplace(node.getPlanSegment()->getPlanSegmentId());
        }
    }
    while (!plan_segment_stack.empty())
    {
        PlanSegmentTree::Node * node_ptr = plan_segment_stack.top();
        plan_segment_stack.pop();
        for (PlanSegmentTree::Node * node : node_ptr->children)
        {
            plan_segment_stack.emplace(node);
            if (plan_segment_vector_id_list.find(node->getPlanSegment()->getPlanSegmentId()) == plan_segment_vector_id_list.end())
            {
                // todo dongyifeng for test, should remove later
//                node->getPlanSegment()->setParallelSize(2);
//                node->getPlanSegment()->getPlanSegmentInputs()[0]->setStorageID({String("default"), String("test_join_local")});

                plan_segment_vector.emplace_back(node->getPlanSegment());
                plan_segment_vector_id_list.emplace(node->getPlanSegment()->getPlanSegmentId());
            }
        }
    }

    for (PlanSegment * plan_segment_ptr : plan_segment_vector)
    {
        graph_ptr->id_to_segment.emplace(std::make_pair(plan_segment_ptr->getPlanSegmentId(), plan_segment_ptr));
        // source
        if (plan_segment_ptr->getPlanSegmentInputs().size() >= 1)
        {
            bool all_tables = true;
            for (const auto & input : plan_segment_ptr->getPlanSegmentInputs())
            {
                if (input->getPlanSegmentType() != PlanSegmentType::SOURCE)
                {
                    all_tables = false;
                    break;
                }
            }
            if (all_tables)
                graph_ptr->sources.emplace_back(plan_segment_ptr->getPlanSegmentId());
        }
        // final stage
        if (plan_segment_ptr->getPlanSegmentOutput()->getPlanSegmentType() == PlanSegmentType::OUTPUT)
        {
            if (graph_ptr->final != std::numeric_limits<size_t>::max())
            {
                throw Exception("Logical error: PlanSegments should be only one final stage", ErrorCodes::LOGICAL_ERROR);
            }
            else
            {
                graph_ptr->final = plan_segment_ptr->getPlanSegmentId();
            }
        }
        //        graph_ptr->plan_segment_status_ptr->segment_status_map[plan_segment_ptr->getPlanSegmentId()] = {};
    }
    // set exchangeParallelSize for plan inputs
    for (PlanSegment * plan_segment_ptr : plan_segment_vector)
    {
        for (const auto & input : plan_segment_ptr->getPlanSegmentInputs())
        {
            if (input->getPlanSegmentType() == PlanSegmentType::EXCHANGE)
            {
                if (graph_ptr->id_to_segment.find(input->getPlanSegmentId()) == graph_ptr->id_to_segment.end())
                    throw Exception(
                        "Logical error: can't find the segment which id is " + std::to_string(input->getPlanSegmentId()),
                        ErrorCodes::LOGICAL_ERROR);
                PlanSegment * input_plan_segment_ptr = graph_ptr->id_to_segment.find(input->getPlanSegmentId())->second;
                input->setExchangeParallelSize(input_plan_segment_ptr->getExchangeParallelSize());
            }
        }
    }
    // do some check
    // 1. check source or final is empty
    if (graph_ptr->sources.empty())
        throw Exception("Logical error: source is empty", ErrorCodes::LOGICAL_ERROR);
    if (graph_ptr->final == std::numeric_limits<size_t>::max())
        throw Exception("Logical error: final is empty", ErrorCodes::LOGICAL_ERROR);

    // 2. check the parallel size
    for (auto it = graph_ptr->id_to_segment.begin(); it != graph_ptr->id_to_segment.end(); it++)
    {
        if (!it->second->getPlanSegmentInputs().empty())
        {
            for (auto plan_segment_input_ptr : it->second->getPlanSegmentInputs())
            {
                // only check when input is from an another exchange
                if (plan_segment_input_ptr->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
                    continue;
                size_t input_plan_segment_id = plan_segment_input_ptr->getPlanSegmentId();
                if (graph_ptr->id_to_segment.find(input_plan_segment_id) == graph_ptr->id_to_segment.end())
                    throw Exception(
                        "Logical error: can't find the segment which id is " + std::to_string(input_plan_segment_id),
                        ErrorCodes::LOGICAL_ERROR);
                auto & input_plan_segment_ptr = graph_ptr->id_to_segment.find(input_plan_segment_id)->second;
                auto plan_segment_output = input_plan_segment_ptr->getPlanSegmentOutput();
                {
                    // if stage out is write to local:
                    // 1.the left table for broadcast join
                    // 2.the left table or right table for local join
                    // the next stage parallel size must be the same
                    if (plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                        || plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                    {
                        if (input_plan_segment_ptr->getParallelSize() != it->second->getParallelSize()
                            || (graph_ptr->local_exchange_parallel_size != 0
                                && (graph_ptr->local_exchange_parallel_size != input_plan_segment_ptr->getParallelSize())))
                            throw Exception(
                                "Logical error: the parallel size between local stage is different, input id:"
                                    + std::to_string(input_plan_segment_id)
                                    + " current id:" + std::to_string(it->second->getPlanSegmentId()),
                                ErrorCodes::LOGICAL_ERROR);
                        // set output parallel size to 1, no need to shuffle
                        if (graph_ptr->local_exchange_parallel_size == 0)
                            graph_ptr->local_exchange_parallel_size = input_plan_segment_ptr->getParallelSize();
                        plan_segment_output->setParallelSize(1);
                        graph_ptr->local_exchange_ids.emplace(input_plan_segment_id);
                        graph_ptr->local_exchange_ids.emplace(it->second->getPlanSegmentId());
                    }
                    else
                    {
                        // if stage out is shuffle, the output parallel size must be equal to next stage parallel size
                        if (plan_segment_output->getParallelSize() != it->second->getParallelSize())
                        {
                            throw Exception(
                                "Logical error: the parallel size between stage is different, input id:"
                                    + std::to_string(input_plan_segment_id)
                                    + " current id:" + std::to_string(it->second->getPlanSegmentId()),
                                ErrorCodes::LOGICAL_ERROR);
                        }
                    }
                }
            }
        }
    }
}

bool SegmentScheduler::scheduler(const String & query_id, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    try
    {
        // scheduler source
        for (auto segment_id : dag_graph_ptr->sources)
        {
            if (segment_id == dag_graph_ptr->final)
                continue;
            std::unordered_map<size_t, PlanSegment *>::iterator it;
            it = dag_graph_ptr->id_to_segment.find(segment_id);
            if (it == dag_graph_ptr->id_to_segment.end())
                throw Exception("Logical error: source segment can not be found", ErrorCodes::LOGICAL_ERROR);
            AddressInfos address_infos;
            // TODO dongyifeng support send plansegment parallel
            address_infos = sendPlanSegment(it->second, true, query_context, dag_graph_ptr);
            if (dag_graph_ptr->local_exchange_ids.find(segment_id) != dag_graph_ptr->local_exchange_ids.end()
                && !dag_graph_ptr->has_set_local_exchange)
            {
                dag_graph_ptr->has_set_local_exchange = true;
                dag_graph_ptr->first_local_exchange_address = address_infos;
            }
            dag_graph_ptr->id_to_address.emplace(std::make_pair(segment_id, std::move(address_infos)));
            dag_graph_ptr->scheduler_segments.emplace(segment_id);
            std::cerr << "finish handle segment: " << segment_id << std::endl;
        }

        std::unordered_map<size_t, PlanSegment *>::iterator it;
        while (dag_graph_ptr->id_to_address.size() < (dag_graph_ptr->id_to_segment.size() - 1))
        {
            for (it = dag_graph_ptr->id_to_segment.begin(); it != dag_graph_ptr->id_to_segment.end(); it++)
            {
                // final stage should not scheduler
                if (it->first == dag_graph_ptr->final)
                    continue;
                // already scheduled
                if (dag_graph_ptr->scheduler_segments.find(it->first) != dag_graph_ptr->scheduler_segments.end())
                    continue;
                // source
                if (it->second->getPlanSegmentInputs().size() == 1
                    && it->second->getPlanSegmentInputs()[0]->getPlanSegmentType() == PlanSegmentType::SOURCE)
                    throw Exception("Logical error: source segment should be schedule", ErrorCodes::LOGICAL_ERROR);

                bool is_inputs_ready = true;
                for (auto & segment_input : it->second->getPlanSegmentInputs())
                {
                    if (!segment_input)
                    {
                        // segment has more than one input which one is table
                        continue;
                    }
                    if (dag_graph_ptr->scheduler_segments.find(segment_input->getPlanSegmentId())
                        != dag_graph_ptr->scheduler_segments.end())
                    {
                        auto address_it = dag_graph_ptr->id_to_address.find(segment_input->getPlanSegmentId());
                        if (address_it == dag_graph_ptr->id_to_address.end())
                            throw Exception(
                                "Logical error: address of segment " + std::to_string(segment_input->getPlanSegmentId())
                                    + " can not be found",
                                ErrorCodes::LOGICAL_ERROR);
                        if (segment_input->getSourceAddresses().empty())
                            segment_input->insertSourceAddress(address_it->second);
                    }
                    else
                    {
                        is_inputs_ready = false;
                        break;
                    }
                }
                if (is_inputs_ready)
                {
                    AddressInfos address_infos;
                    address_infos = sendPlanSegment(it->second, false, query_context, dag_graph_ptr);
                    // local join/global join is not between two source stages, for example, group by subquery global join source table
                    if (dag_graph_ptr->local_exchange_ids.find(it->first) != dag_graph_ptr->local_exchange_ids.end()
                        && !dag_graph_ptr->has_set_local_exchange)
                    {
                        dag_graph_ptr->has_set_local_exchange = true;
                        dag_graph_ptr->first_local_exchange_address = address_infos;
                    }
                    dag_graph_ptr->id_to_address.emplace(std::make_pair(it->first, std::move(address_infos)));
                    dag_graph_ptr->scheduler_segments.emplace(it->first);
                }
            }
        }
        auto final_it = dag_graph_ptr->id_to_segment.find(dag_graph_ptr->final);
        if (final_it == dag_graph_ptr->id_to_segment.end())
            throw Exception("Logical error: final stage is not found", ErrorCodes::LOGICAL_ERROR);
        std::shared_ptr<Cluster> cluster = query_context->tryGetCluster(final_it->second->getClusterName());
        if (cluster)
        {
            bool set_local_address = false;
            for (const auto & shard_info : cluster->getShardsInfo())
            {
                if (shard_info.isLocal())
                {
                    for (size_t i = 0; i < shard_info.local_addresses.size(); i++)
                    {
                        if (shard_info.local_addresses[i].host_name == query_context->getLocalHost()
                            && shard_info.local_addresses[i].port == query_context->getTCPPort())
                        {
                            final_it->second->setCurrentAddress(AddressInfo(
                                shard_info.local_addresses[i].host_name,
                                shard_info.local_addresses[i].port,
                                shard_info.local_addresses[i].user,
                                shard_info.local_addresses[i].password,
                                shard_info.local_addresses[i].exchange_port,
                                shard_info.local_addresses[i].exchange_status_port));
                            final_it->second->setCoordinatorAddress(AddressInfo(
                                shard_info.local_addresses[i].host_name,
                                shard_info.local_addresses[i].port,
                                shard_info.local_addresses[i].user,
                                shard_info.local_addresses[i].password,
                                shard_info.local_addresses[i].exchange_port,
                                shard_info.local_addresses[i].exchange_status_port));
                            set_local_address = true;
                            break;
                        }
                    }
                }
            }
            if (!set_local_address)
                throw Exception("Logical error: can't find local replica in cluster settings", ErrorCodes::LOGICAL_ERROR);
        }
        else // final table cluster is nullptr
        {
            bool set_local_address = false;
            for (auto & cluster_it : query_context->getClusters()->getContainer())
            {
                for (const auto & shard_info : cluster_it.second->getShardsInfo())
                {
                    if (shard_info.isLocal())
                    {
                        for (size_t i = 0; i < shard_info.local_addresses.size(); i++)
                        {
                            if (shard_info.local_addresses[i].host_name == query_context->getLocalHost()
                                && shard_info.local_addresses[i].port == query_context->getTCPPort())
                            {
                                final_it->second->setCurrentAddress(AddressInfo(
                                    shard_info.local_addresses[i].host_name,
                                    shard_info.local_addresses[i].port,
                                    shard_info.local_addresses[i].user,
                                    shard_info.local_addresses[i].password,
                                    shard_info.local_addresses[i].exchange_port,
                                    shard_info.local_addresses[i].exchange_status_port));
                                final_it->second->setCoordinatorAddress(AddressInfo(
                                    shard_info.local_addresses[i].host_name,
                                    shard_info.local_addresses[i].port,
                                    shard_info.local_addresses[i].user,
                                    shard_info.local_addresses[i].password,
                                    shard_info.local_addresses[i].exchange_port,
                                    shard_info.local_addresses[i].exchange_status_port));
                                set_local_address = true;
                                break;
                            }
                        }
                    }
                }
            }
            if (!set_local_address)
                throw Exception("Logical error: can't find local replica in cluster settings", ErrorCodes::LOGICAL_ERROR);
        }

        for (auto plan_segment_input : final_it->second->getPlanSegmentInputs())
        {
            // segment has more than one input which one is table
            if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
                continue;
            plan_segment_input->setParallelIndex(1);
            if (dag_graph_ptr->scheduler_segments.find(plan_segment_input->getPlanSegmentId()) != dag_graph_ptr->scheduler_segments.end())
            {
                auto address_it = dag_graph_ptr->id_to_address.find(plan_segment_input->getPlanSegmentId());
                if (address_it == dag_graph_ptr->id_to_address.end())
                    throw Exception(
                        "Logical error: address of segment " + std::to_string(plan_segment_input->getPlanSegmentId()) + " can not be found",
                        ErrorCodes::LOGICAL_ERROR);
                if (plan_segment_input->getSourceAddresses().size() == 0)
                    plan_segment_input->insertSourceAddress(address_it->second);
            }
            else
            {
                throw Exception(
                    "Logical error: source of final stage is not ready, id=" + std::to_string(plan_segment_input->getPlanSegmentId()),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }
        dag_graph_ptr->plan_segment_status_ptr->is_final_stage_start = true;
    }
    catch (const Exception & e)
    {
        this->cancelPlanSegments(query_id, "receive exception during scheduler:" + e.message(), "coordinator", dag_graph_ptr);
        e.rethrow();
    }
    catch (...)
    {
        this->cancelPlanSegments(query_id, "receive unknown exception during scheduler", "coordinator", dag_graph_ptr);
        throw;
    }
    return true;
}

AddressInfo getLocalAddress(PlanSegment * plan_segment_ptr, ContextPtr query_context)
{
    std::shared_ptr<Cluster> cluster = query_context->tryGetCluster(plan_segment_ptr->getClusterName());
    if (!cluster)
    {
        auto host = query_context->getLocalHost();
        auto port = query_context->getTCPPort();
        const ClientInfo & info = query_context->getClientInfo();
        return AddressInfo(
            host, port, info.current_user, info.current_password, query_context->getExchangePort(), query_context->getExchangeStatusPort());
    }

    ConnectionTimeouts connection_timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(query_context->getSettingsRef());
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (shard_info.isLocal())
        {
            for (size_t i = 0; i < shard_info.local_addresses.size(); i++)
            {
                if (shard_info.local_addresses[i].host_name == query_context->getLocalHost()
                    && shard_info.local_addresses[i].port == query_context->getTCPPort())
                {
                    return AddressInfo(
                        shard_info.local_addresses[i].host_name,
                        shard_info.local_addresses[i].port,
                        shard_info.local_addresses[i].user,
                        shard_info.local_addresses[i].password,
                        shard_info.local_addresses[i].exchange_port,
                        shard_info.local_addresses[i].exchange_status_port);
                }
            }
        }
    }
    throw Exception("Logical error: can't find local replica in cluster settings", ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<Connection> getQueryLocalAddress(PlanSegment * plan_segment_ptr, ContextPtr query_context)
{
    std::shared_ptr<Cluster> cluster = query_context->tryGetCluster(plan_segment_ptr->getClusterName());
    ConnectionTimeouts connection_timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(query_context->getSettingsRef());
    std::unique_ptr<Connection> connection;
    if (!cluster)
    {
        auto address = getLocalAddress(plan_segment_ptr, query_context);
        connection = std::make_unique<Connection>(
            address.getHostName(),
            address.getPort(),
            "",
            address.getUser(),
            address.getPassword(),
            "",
            "",
            "client",
            Protocol::Compression::Enable,
            Protocol::Secure::Disable,
            Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0),
            address.getExchangePort(),
            address.getExchangeStatusPort());
        return connection;
        //throw Exception("Logical error: can't find cluster in context which named " + plan_segment_ptr->getClusterName(), ErrorCodes::LOGICAL_ERROR);
    }

    for (const auto & shard_info : cluster->getShardsInfo())
    {
        if (shard_info.isLocal())
        {
            for (size_t i = 0; i < shard_info.local_addresses.size(); i++)
            {
                if (shard_info.local_addresses[i].host_name == query_context->getLocalHost()
                    && shard_info.local_addresses[i].port == query_context->getTCPPort())
                {
                    connection = std::make_unique<Connection>(
                        shard_info.local_addresses[i].host_name,
                        shard_info.local_addresses[i].port,
                        shard_info.local_addresses[i].default_database,
                        shard_info.local_addresses[i].user,
                        shard_info.local_addresses[i].password,
                        shard_info.local_addresses[i].cluster,
                        shard_info.local_addresses[i].cluster_secret,
                        "client",
                        Protocol::Compression::Enable,
                        Protocol::Secure::Disable,
                        Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0),
                        shard_info.local_addresses[i].exchange_port,
                        shard_info.local_addresses[i].exchange_status_port);
                    return connection;
                }
            }
        }
    }
    throw Exception("Logical error: can't find local replica in cluster settings", ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<Connection> getConnectionForAddresses(AddressInfo address, ContextPtr /*query_context*/)
{
    return std::make_unique<Connection>(
        address.getHostName(),
        address.getPort(),
        "default",
        address.getUser(),
        address.getPassword(),
        "",
        "",
        "client",
        Protocol::Compression::Enable,
        Protocol::Secure::Disable,
        Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0),
        address.getExchangePort(),
        address.getExchangeStatusPort());
}

void sendPlanSegmentToLocal(PlanSegment * plan_segment_ptr, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    ConnectionTimeouts connection_timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(query_context->getSettingsRef());
    std::unique_ptr<Connection> connection = getQueryLocalAddress(plan_segment_ptr, query_context);
    // std::cout<<"<<--<< will send local segment " << plan_segment_ptr->getPlanSegmentId() << " to " << connection->getHost() << ":" << connection->getPort() << std::endl;
    plan_segment_ptr->setCurrentAddress(AddressInfo(
        connection->getHost(),
        connection->getPort(),
        connection->getUser(),
        connection->getPassword(),
        connection->getExchangePort(),
        connection->getExchangeStatusPort()));
    connection->sendPlanSegment(connection_timeouts, plan_segment_ptr, &query_context->getSettingsRef(), &query_context->getClientInfo());
    connection->poll(1000);
    Packet packet = connection->receivePacket();
    std::cerr << "sendPlanSegmentToLocal finish:" << packet.type << std::endl;
    switch (packet.type)
    {
        case Protocol::Server::Exception:
            throw *packet.exception;
        case Protocol::Server::EndOfStream:
            break;
        case Protocol::Server::Log:
            break;
        default: {
            connection->disconnect();
            throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        }
    }
    connection->disconnect();
    std::cerr << "sendPlanSegmentToLocal disconnect, finish: " << plan_segment_ptr->getPlanSegmentId() << std::endl;

    if (dag_graph_ptr)
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(AddressInfo(
            connection->getHost(),
            connection->getPort(),
            connection->getUser(),
            connection->getPassword(),
            connection->getExchangePort(),
            connection->getExchangeStatusPort()));
    }
}

void sendPlanSegmentToRemote(
    AddressInfo & addressinfo,
    ContextPtr query_context,
    ConnectionTimeouts connection_timeouts,
    PlanSegment * plan_segment_ptr,
    std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    // std::cout<<"<<--<< will send remote segment " << plan_segment_ptr->getPlanSegmentId() << " to " << addressinfo.getHostName() << ":" << addressinfo.getPort() << std::endl;
    plan_segment_ptr->setCurrentAddress(addressinfo);
    auto connection = getConnectionForAddresses(addressinfo, query_context);
    connection->sendPlanSegment(connection_timeouts, plan_segment_ptr, &query_context->getSettingsRef(), &query_context->getClientInfo());
    connection->poll(1000);
    Packet packet = connection->receivePacket();
    switch (packet.type)
    {
        case Protocol::Server::Exception:
            throw *packet.exception;
        case Protocol::Server::EndOfStream:
            break;
        case Protocol::Server::Log:
            break;
        default: {
            connection->disconnect();
            throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        }
    }
    std::cerr << "sendPlanSegmentToRemote finish:" << packet.type << std::endl;
    connection->disconnect();
    std::cerr << "sendPlanSegmentToRemote disconnect, finish: " << plan_segment_ptr->getPlanSegmentId() << std::endl;

    if (dag_graph_ptr)
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(addressinfo);
    }
}

void sendPlanSegmentToRemote(
    ConnectionPool::Entry & connection,
    ContextPtr query_context,
    ConnectionTimeouts connection_timeouts,
    PlanSegment * plan_segment_ptr,
    std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    LOG_TRACE(
        &Poco::Logger::get("SegmentScheduler::sendPlanSegment"),
        "<<--<< will send remote segment {} to {}:{}",
        std::to_string(plan_segment_ptr->getPlanSegmentId()),
        connection->getHost(),
        connection->getPort());
    plan_segment_ptr->setCurrentAddress(AddressInfo(
        connection->getHost(),
        connection->getPort(),
        connection->getUser(),
        connection->getPassword(),
        connection->getExchangePort(),
        connection->getExchangeStatusPort()));
    connection->sendPlanSegment(connection_timeouts, plan_segment_ptr, &query_context->getSettingsRef(), &query_context->getClientInfo());
    connection->poll(1000);
    Packet packet = connection->receivePacket();
    switch (packet.type)
    {
        case Protocol::Server::Exception:
            throw *packet.exception;
        case Protocol::Server::EndOfStream:
            break;
        case Protocol::Server::Log:
            break;
        default: {
            connection->disconnect();
            throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        }
    }
    LOG_TRACE(&Poco::Logger::get("SegmentScheduler::sendPlanSegment"), "sendPlanSegmentToRemote finish: {}", packet.type);
    connection->disconnect();
    if (dag_graph_ptr)
    {
        std::unique_lock<bthread::Mutex> lock(dag_graph_ptr->status_mutex);
        dag_graph_ptr->plan_send_addresses.emplace(AddressInfo(
            connection->getHost(),
            connection->getPort(),
            connection->getUser(),
            connection->getPassword(),
            connection->getExchangePort(),
            connection->getExchangeStatusPort()));
    }
}

AddressInfos SegmentScheduler::sendPlanSegment(
    PlanSegment * plan_segment_ptr, bool is_source, ContextPtr query_context, std::shared_ptr<DAGGraph> dag_graph_ptr)
{
    LOG_TRACE(
        &Poco::Logger::get("SegmentScheduler::sendPlanSegment"),
        "begin sendPlanSegment: " + std::to_string(plan_segment_ptr->getPlanSegmentId()));
    std::unique_ptr<Connection> connection = getQueryLocalAddress(plan_segment_ptr, query_context);
    ConnectionTimeouts connection_timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(query_context->getSettingsRef());
    plan_segment_ptr->setCoordinatorAddress(AddressInfo(
        connection->getHost(),
        connection->getPort(),
        connection->getUser(),
        connection->getPassword(),
        connection->getExchangePort(),
        connection->getExchangeStatusPort()));
    // if stage is relation with local stage
    if (dag_graph_ptr->local_exchange_ids.find(plan_segment_ptr->getPlanSegmentId()) != dag_graph_ptr->local_exchange_ids.end()
        && dag_graph_ptr->has_set_local_exchange)
    {
        size_t parallel_index_id_index = 0;

        for (auto & address : dag_graph_ptr->first_local_exchange_address)
        {
            parallel_index_id_index++;
            for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
            {
                {
                    plan_segment_input->setParallelIndex(parallel_index_id_index);

                    // if input mode is local, set parallel index to 1
                    auto it = dag_graph_ptr->id_to_segment.find(plan_segment_input->getPlanSegmentId());
                    auto plan_segment_output = it->second->getPlanSegmentOutput();
                    {
                        // if data is write to local, so no need to shuffle data
                        if (plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                            || plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                        {
                            plan_segment_input->setParallelIndex(1);
                            plan_segment_input->clearSourceAddresses();
                            plan_segment_input->insertSourceAddress(AddressInfo("localhost", 0, "", ""));
                            //                            std::cerr << plan_segment_ptr->getPlanSegmentId() << " plan_segment_input " << plan_segment_input->getPlanSegmentId() << "  change getSourceAddresses to "<< plan_segment_input->getSourceAddresses().size() << std::endl;
                        }
                    }

                    // collect status, useful for debug
#if defined(TASK_ASSIGN_DEBUG)
                    if (dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input->getPlanSegmentId())
                        == dag_graph_ptr->exchange_data_assign_node_mappings.end())
                    {
                        dag_graph_ptr->exchange_data_assign_node_mappings.emplace(
                            std::make_pair(plan_segment_input->getPlanSegmentId(), std::vector<std::pair<size_t, AddressInfo>>{}));
                    }
                    dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input->getPlanSegmentId())
                        ->second.emplace_back(std::make_pair(plan_segment_input->getParallelIndex(), address));
#endif
                }
            }
            sendPlanSegmentToRemote(address, query_context, connection_timeouts, plan_segment_ptr, dag_graph_ptr);
        }
        return dag_graph_ptr->first_local_exchange_address;
    }

    AddressInfos addresses;
    std::shared_ptr<Cluster> cluster = query_context->tryGetCluster(plan_segment_ptr->getClusterName());
    // getParallelSize equals to 1, then is just to send to local
    if (plan_segment_ptr->getParallelSize() == 1 || !cluster)
    {
        // send to local
        addresses.emplace_back(AddressInfo(
            connection->getHost(),
            connection->getPort(),
            connection->getUser(),
            connection->getPassword(),
            connection->getExchangePort(),
            connection->getExchangeStatusPort()));
        for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
        {
            plan_segment_input->setParallelIndex(1);
#if defined(TASK_ASSIGN_DEBUG)
            if (dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input->getPlanSegmentId())
                == dag_graph_ptr->exchange_data_assign_node_mappings.end())
            {
                dag_graph_ptr->exchange_data_assign_node_mappings.emplace(
                    std::make_pair(plan_segment_input->getPlanSegmentId(), std::vector<std::pair<size_t, AddressInfo>>{}));
            }
            dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input->getPlanSegmentId())
                ->second.emplace_back(std::make_pair(
                    plan_segment_input->getParallelIndex(),
                    AddressInfo(
                        connection->getHost(),
                        connection->getPort(),
                        connection->getUser(),
                        connection->getPassword(),
                        connection->getExchangePort(),
                        connection->getExchangeStatusPort())));
#endif
        }
        sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);
    }
    else
    {
        if (!cluster)
        {
            throw Exception(
                "Logical error: can't find cluster in context which named " + plan_segment_ptr->getClusterName(),
                ErrorCodes::LOGICAL_ERROR);
        }
        if (plan_segment_ptr->getParallelSize() != cluster->getShardCount())
        {
            throw Exception(
                "Logical error: segment id(" + std::to_string(plan_segment_ptr->getPlanSegmentId()) + ") cluster shard count("
                    + std::to_string(cluster->getShardCount()) + ") not equal to parallel size("
                    + std::to_string(plan_segment_ptr->getParallelSize()) + ")",
                ErrorCodes::LOGICAL_ERROR);
        }

        auto get_try_results = [&](const Cluster::ShardInfo & shard_info,
                                   StoragePtr main_table_storage) -> std::vector<ConnectionPoolWithFailover::TryResult> {
            auto current_settings = query_context->getSettingsRef();
            /// set max_parallel_replicas to 1, so up to one connection would return
            current_settings.max_parallel_replicas = 1;

            auto timeouts
                = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings.max_execution_time);
            std::vector<ConnectionPoolWithFailover::TryResult> try_results;

            try
            {
                try_results = shard_info.pool->getManyChecked(
                    timeouts, &current_settings, PoolMode::GET_MANY, main_table_storage->getStorageID().getQualifiedName());
            }
            catch (const Exception & ex)
            {
                if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                    LOG_WARNING(
                        &Poco::Logger::get("SegmentScheduler::sendPlanSegment"),
                        "Connections to remote replicas of shard {}  all failed.",
                        shard_info.shard_num);
                //else
                throw;
            }

            return try_results;
        };

        auto & settings = query_context->getSettingsRef();
        size_t parallel_index_id_index = 0;
        // set ParallelIndexId and source address
        for (const auto & shard_info : cluster->getShardsInfo())
        {
            parallel_index_id_index++;
            for (auto plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
            {
                if (plan_segment_input->getPlanSegmentType() != PlanSegmentType::EXCHANGE)
                    continue;
                plan_segment_input->setParallelIndex(parallel_index_id_index);

                // if input mode is local, set parallel index to 1
                auto it = dag_graph_ptr->id_to_segment.find(plan_segment_input->getPlanSegmentId());
                auto plan_segment_output = it->second->getPlanSegmentOutput();
                {
                    // if data is write to local, so no need to shuffle data
                    if (plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_NO_NEED_REPARTITION
                        || plan_segment_output->getExchangeMode() == ExchangeMode::LOCAL_MAY_NEED_REPARTITION)
                    {
                        plan_segment_input->setParallelIndex(1);
                        plan_segment_input->clearSourceAddresses();
                        plan_segment_input->insertSourceAddress(AddressInfo("localhost", 0, "", ""));
                        //std::cerr << plan_segment_ptr->getPlanSegmentId() << " plan_segment_input " << plan_segment_input->getPlanSegmentId() << "  change getSourceAddresses to "<< plan_segment_input->getSourceAddresses().size() << std::endl;
                    }
                }
            }
            // if is source, should find the most up-to-date node
            if (is_source)
            {
                auto segment_input_table = plan_segment_ptr->getPlanSegmentInputs()[0];
                if (segment_input_table->getPlanSegmentType() != PlanSegmentType::SOURCE)
                    throw Exception(
                        "Logical error: source segment id(" + std::to_string(plan_segment_ptr->getPlanSegmentId()) + ") input is not source",
                        ErrorCodes::LOGICAL_ERROR);
                StoragePtr main_table_storage
                    = DatabaseCatalog::instance().tryGetTable(segment_input_table->getStorageID(), query_context);

                if (settings.prefer_localhost_replica && shard_info.isLocal())
                {
                    if (!main_table_storage) /// Table is absent on a local server.
                    {
                        //ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
                        if (shard_info.hasRemoteConnections())
                        {
                            LOG_WARNING(
                                &Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                                "There is no table {} on local replica of shard {}, will try remote replicas.",
                                segment_input_table->getStorageID().getNameForLogs(),
                                shard_info.shard_num);

                            auto try_results = get_try_results(shard_info, main_table_storage);
                            /// If we didn't get any connections from pool and getMany() did not throw exceptions, this means that
                            /// `skip_unavailable_shards` was set. Then just skip.
                            if (try_results.size() == 1)
                            {
                                addresses.emplace_back(AddressInfo(
                                    try_results[0].entry->getHost(),
                                    try_results[0].entry->getPort(),
                                    try_results[0].entry->getUser(),
                                    try_results[0].entry->getPassword(),
                                    try_results[0].entry->getExchangePort(),
                                    try_results[0].entry->getExchangeStatusPort()));
                                //TODO send to remote
                                sendPlanSegmentToRemote(
                                    try_results[0].entry, query_context, connection_timeouts, plan_segment_ptr, dag_graph_ptr);
                            }
                            else
                            {
                                throw Exception(
                                    "Logical error: can not find available source node for "
                                        + segment_input_table->getStorageID().getNameForLogs(),
                                    ErrorCodes::LOGICAL_ERROR);
                            }
                        }
                        else
                        {
                            throw Exception("Logical error: main table can not be found", ErrorCodes::LOGICAL_ERROR);
                        }
                    }
                    else
                    {
                        const auto * replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());
                        if (!replicated_storage)
                        {
                            // send to local
                            addresses.emplace_back(AddressInfo(
                                connection->getHost(),
                                connection->getPort(),
                                connection->getUser(),
                                connection->getPassword(),
                                connection->getExchangePort(),
                                connection->getExchangeStatusPort()));
                            sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);
                        }
                        else
                        {
                            UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

                            if (!max_allowed_delay)
                            {
                                // send to local
                                addresses.emplace_back(AddressInfo(
                                    connection->getHost(),
                                    connection->getPort(),
                                    connection->getUser(),
                                    connection->getPassword(),
                                    connection->getExchangePort(),
                                    connection->getExchangeStatusPort()));
                                sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);
                            }
                            else
                            {
                                UInt32 local_delay = replicated_storage->getAbsoluteDelay();

                                if (local_delay < max_allowed_delay)
                                {
                                    // send to local
                                    addresses.emplace_back(AddressInfo(
                                        connection->getHost(),
                                        connection->getPort(),
                                        connection->getUser(),
                                        connection->getPassword(),
                                        connection->getExchangePort(),
                                        connection->getExchangeStatusPort()));
                                    sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);

                                    /// If we reached this point, local replica is stale.
                                    /// ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
                                    LOG_DEBUG(
                                        &Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                                        "Local replica of shard {} is stale (delay: {}s.)",
                                        shard_info.shard_num,
                                        local_delay);
                                }
                                else
                                {
                                    if (!settings.fallback_to_stale_replicas_for_distributed_queries)
                                    {
                                        if (shard_info.hasRemoteConnections())
                                        {
                                            /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                                            auto try_results = get_try_results(shard_info, main_table_storage);
                                            if (try_results.size() == 1)
                                            {
                                                addresses.emplace_back(AddressInfo(
                                                    try_results[0].entry->getHost(),
                                                    try_results[0].entry->getPort(),
                                                    try_results[0].entry->getUser(),
                                                    try_results[0].entry->getPassword(),
                                                    try_results[0].entry->getExchangePort(),
                                                    try_results[0].entry->getExchangeStatusPort()));
                                                try_results[0].entry->sendPlanSegment(
                                                    connection_timeouts,
                                                    plan_segment_ptr,
                                                    &query_context->getSettingsRef(),
                                                    &query_context->getClientInfo());
                                            }
                                            else
                                                throw Exception(
                                                    "Local replica of shard " + toString(shard_info.shard_num) + " is stale (delay: "
                                                        + toString(local_delay) + "s.), but no other replica configured",
                                                    ErrorCodes::ALL_REPLICAS_ARE_STALE);
                                        }
                                        else
                                            throw Exception(
                                                "Local replica of shard " + toString(shard_info.shard_num)
                                                    + " is stale (delay: " + toString(local_delay) + "s.), but no other replica configured",
                                                ErrorCodes::ALL_REPLICAS_ARE_STALE);
                                    }
                                    else
                                    {
                                        if (!shard_info.hasRemoteConnections())
                                        {
                                            /// There are no remote replicas but we are allowed to fall back to stale local replica.
                                            // send to local
                                            addresses.emplace_back(AddressInfo(
                                                connection->getHost(),
                                                connection->getPort(),
                                                connection->getUser(),
                                                connection->getPassword(),
                                                connection->getExchangePort(),
                                                connection->getExchangeStatusPort()));
                                            sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);
                                        }
                                        else
                                        {
                                            /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
                                            auto try_results = get_try_results(shard_info, main_table_storage);
                                            UInt32 max_remote_delay = 0;
                                            for (const auto & try_result : try_results)
                                            {
                                                if (!try_result.is_up_to_date)
                                                    max_remote_delay
                                                        = std::max(static_cast<UInt32>(try_result.staleness), max_remote_delay);
                                            }

                                            if (try_results.empty() || local_delay < max_remote_delay)
                                            {
                                                // send to local
                                                addresses.emplace_back(AddressInfo(
                                                    connection->getHost(),
                                                    connection->getPort(),
                                                    connection->getUser(),
                                                    connection->getPassword(),
                                                    connection->getExchangePort(),
                                                    connection->getExchangeStatusPort()));
                                                sendPlanSegmentToLocal(plan_segment_ptr, query_context, dag_graph_ptr);
                                            }
                                            else
                                            {
                                                // send to local
                                                addresses.emplace_back(AddressInfo(
                                                    connection->getHost(),
                                                    connection->getPort(),
                                                    connection->getUser(),
                                                    connection->getPassword(),
                                                    connection->getExchangePort(),
                                                    connection->getExchangeStatusPort()));
                                                sendPlanSegmentToRemote(
                                                    try_results[0].entry,
                                                    query_context,
                                                    connection_timeouts,
                                                    plan_segment_ptr,
                                                    dag_graph_ptr);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    auto try_results = get_try_results(shard_info, main_table_storage);
                    if (try_results.size() == 1)
                    {
                        addresses.emplace_back(AddressInfo(
                            try_results[0].entry->getHost(),
                            try_results[0].entry->getPort(),
                            try_results[0].entry->getUser(),
                            try_results[0].entry->getPassword(),
                            try_results[0].entry->getExchangePort(),
                            try_results[0].entry->getExchangeStatusPort()));
                        sendPlanSegmentToRemote(try_results[0].entry, query_context, connection_timeouts, plan_segment_ptr, dag_graph_ptr);
                    }
                    else
                    {
                        throw Exception(
                            "Logical error: can not find available source node for " + main_table_storage->getStorageID().getNameForLogs(),
                            ErrorCodes::LOGICAL_ERROR);
                    }
                }
            }
            else
            {
                auto current_settings = query_context->getSettingsRef();
                /// set max_parallel_replicas to 1, so up to one connection would return
                current_settings.max_parallel_replicas = 1;

                auto timeouts
                    = ConnectionTimeouts::getTCPTimeoutsWithFailover(current_settings).getSaturated(current_settings.max_execution_time);

                auto entry = shard_info.pool->get(timeouts, &current_settings, true);
                sendPlanSegmentToRemote(entry, query_context, connection_timeouts, plan_segment_ptr, dag_graph_ptr);
                addresses.emplace_back(AddressInfo(
                    entry->getHost(),
                    entry->getPort(),
                    entry->getUser(),
                    entry->getPassword(),
                    entry->getExchangePort(),
                    entry->getExchangeStatusPort()));
            }

#if defined(TASK_ASSIGN_DEBUG)
            for (auto & plan_segment_input : plan_segment_ptr->getPlanSegmentInputs())
            {
                {
                    if (dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input->getPlanSegmentId())
                        == dag_graph_ptr->exchange_data_assign_node_mappings.end())
                    {
                        dag_graph_ptr->exchange_data_assign_node_mappings.emplace(
                            std::make_pair(plan_segment_input->getPlanSegmentId(), std::vector<std::pair<size_t, AddressInfo>>{}));
                    }
                    dag_graph_ptr->exchange_data_assign_node_mappings.find(plan_segment_input->getPlanSegmentId())
                        ->second.emplace_back(std::make_pair(plan_segment_input->getParallelIndex(), addresses[addresses.size() - 1]));
                }
            }
#endif
        }
    }

#if defined(TASK_ASSIGN_DEBUG)
    String res_log = "segment id:" + std::to_string(plan_segment_ptr->getPlanSegmentId()) + " send planSegment address information:\n";
    for (auto address_inf : addresses)
    {
        res_log += "  " + address_inf.toString() + "\n";
    }
    LOG_DEBUG(log, res_log);
#endif

    LOG_TRACE(&Poco::Logger::get("SegmentScheduler::sendPlanSegment"), "end sendPlanSegment: {}", plan_segment_ptr->getPlanSegmentId());
    return addresses;
}

}

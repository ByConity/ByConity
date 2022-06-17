#pragma once

#include <ResourceManagement/CommonData.h>
#include <Common/RWLock.h>
#include <CloudServices/RpcLeaderClientBase.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace Protos
{
    class ResourceManagerService_Stub;
}

namespace ErrorCodes
{
    extern const int BRPC_CONNECT_ERROR;
    extern const int BRPC_EXCEPTION;
    extern const int BRPC_HOST_DOWN;
    extern const int BRPC_TIMEOUT;
    extern const int NO_SUCH_SERVICE;
    extern const int RESOURCE_MANAGER_NO_LEADER_ELECTED;
}

namespace ResourceManagement
{
struct WorkerNode;

String fetchByteJournalLeader(ContextMutablePtr context, String election_ns, String election_point);

class ResourceManagerClient : public RpcLeaderClientBase, protected WithMutableContext
{
    friend class ResourceReporterTask;
public:
    static String getName() { return "ResourceManagerClient"; }

    ResourceManagerClient(ContextMutablePtr global_context_, const String & election_ns_, const String & election_point_);
    ~ResourceManagerClient() override;

    void getVirtualWarehouse(const std::string & name, VirtualWarehouseData & vw_data);
    void createVirtualWarehouse(const std::string & vw_name, const VirtualWarehouseSettings & vw_settings, bool if_not_exists);
    void updateVirtualWarehouse(const std::string & vw_name, const VirtualWarehouseAlterSettings & vw_alter_settings);
    void dropVirtualWarehouse(const std::string & vw_name, const bool if_exists);
    void getAllVirtualWarehouses(std::vector<VirtualWarehouseData> & vw_data_list);

    void createWorkerGroup(const String & group_id, bool if_not_exists, const String & vw_name, const WorkerGroupData & group_data);
    void dropWorkerGroup(const String & group_id, bool if_exists);
    std::vector<WorkerGroupData> getAllWorkerGroups(bool with_metrics = false);

    void getAllWorkers(std::vector<WorkerNodeResourceData> & data);
    void getWorkerGroups(const std::string & vw_name, std::vector<WorkerGroupData> & groups_data);
    bool reportResourceUsage(const WorkerNodeResourceData & data);

    void registerWorker(const WorkerNodeResourceData & data);
    void removeWorker(const String & worker_id, const String & vw_name, const String & group_id);

    WorkerGroupData pickWorkerGroup(const String & vw_name, VWScheduleAlgo vw_schedule_algo, const ResourceRequirement & requirement);
    HostWithPorts pickWorker(const String & vw_name, VWScheduleAlgo vw_schedule_algo, const ResourceRequirement & requirement);

    AggQueryQueueMap syncQueueDetails(VWQueryQueueMap vw_query_queue_map
                                    , std::vector<String> * deleted_vw_list);

private:
    using Stub = Protos::ResourceManagerService_Stub;
    mutable RWLock leader_mutex = RWLockImpl::create();
    std::unique_ptr<Stub> stub;
    String election_ns;
    String election_point;

    String fetchByteJournalLeader() const;

    RWLockImpl::LockHolder getReadLock() const
    {
        return leader_mutex->getLock(RWLockImpl::Read, RWLockImpl::NO_QUERY);
    }

    RWLockImpl::LockHolder getWriteLock() const
    {
        return leader_mutex->getLock(RWLockImpl::Write, RWLockImpl::NO_QUERY);
    }

    /** Overloaded function, where process_response is only executed
      * after the response has been determined to be from a leader
      *
      */
    template <typename RMResponse, typename RpcFunc, typename RespHandler>
    void callToLeaderWrapper(RMResponse & response, RpcFunc & rpc_func, RespHandler & process_response)
    {
        auto is_leader = callToLeaderWrapper(response, rpc_func);
        if (is_leader)
            process_response(response);
    }

    /** Helper function that ensures that RPC calls are sent to the leader node.
      * Updates stub based on RM node's response or via ByteJournal
      * rpc_func should not contain any response processing
      * Depends on is_leader field and leader_host_port of RPC response
      */
    template <typename RMResponse, typename RpcFunc>
    bool callToLeaderWrapper(RMResponse & response, RpcFunc & rpc_func)
    {
        auto & config = getContext()->getConfigRef();
        auto max_retry_count = config.getInt("resource_manager.max_retry_count", 3);

        int retry_count = 0;
        do
        {
            try
            {
                {
                    auto lock = getReadLock();
                    rpc_func(stub);
                }

                if (response.has_is_leader() && response.is_leader())
                {
                    return true;
                }
                else
                {
                    auto lock = getWriteLock();

                    if (!response.has_leader_host_port() || response.leader_host_port() == leader_host_port)
                    {
                        if (response.leader_host_port().empty())
                            LOG_DEBUG(log, "RM response does not contain an elected RM leader");
                        throw Exception("There is currently no alive elected RM leader.", ErrorCodes::RESOURCE_MANAGER_NO_LEADER_ELECTED);
                    }

                    LOG_DEBUG(log, "Updating RM Leader to " + response.leader_host_port() + " based on RMResponse");
                    stub = std::make_unique<Stub>(&updateChannel(response.leader_host_port()));
                }
            }
            catch (const Exception & e)
            {
                if (!(e.code() == ErrorCodes::BRPC_HOST_DOWN || e.code() == ErrorCodes::BRPC_CONNECT_ERROR
                      || e.code() == ErrorCodes::NO_SUCH_SERVICE || e.code() == ErrorCodes::RESOURCE_MANAGER_NO_LEADER_ELECTED
                    || e.code() == ErrorCodes::BRPC_TIMEOUT || e.code() == ErrorCodes::BRPC_EXCEPTION)
                    || retry_count == max_retry_count)
                    throw;

                tryLogDebugCurrentException(__PRETTY_FUNCTION__);
                auto lock = getWriteLock();
                auto new_leader = fetchByteJournalLeader();
                if (new_leader.empty() || new_leader == leader_host_port)
                {
                    LOG_DEBUG(log, "There is no active elected RM leader");
                    throw;
                }
                else
                {
                    LOG_DEBUG(log, "Updating RM Leader to " + new_leader + " based on ByteJournal");
                    stub = std::make_unique<Stub>(&updateChannel(new_leader));
                }
            }
        } while (retry_count++ < max_retry_count);

        return false;
    }
};

}

using ResourceManagerClient = ResourceManagement::ResourceManagerClient;
using ResourceManagerClientPtr = std::shared_ptr<ResourceManagerClient>;
}

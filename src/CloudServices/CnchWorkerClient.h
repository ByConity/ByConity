#pragma once

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/RpcClientBase.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/TxnTimestamp.h>

#include <unordered_set>


namespace DB
{
namespace Protos
{
    class CnchWorkerService_Stub;
}

class MergeTreeMetaBase;
struct StorageID;
struct ManipulationInfo;
struct ManipulationTaskParams;

class CnchWorkerClient : public RpcClientBase
{
public:
    static String getName() { return "CnchWorkerClient"; }

    explicit CnchWorkerClient(String host_port_);
    explicit CnchWorkerClient(HostWithPorts host_ports_);
    ~CnchWorkerClient() override;

    void submitManipulationTask(
        const MergeTreeMetaBase & storage,
        const ManipulationTaskParams & params,
        TxnTimestamp txn_id,
        TxnTimestamp begin_ts);

    void shutdownManipulationTasks(const UUID & table_uuid);
    std::unordered_set<String> touchManipulationTasks(const UUID & table_uuid, const Strings & tasks_id);
    std::vector<ManipulationInfo> getManipulationTasksStatus();

    /// send resource to worker
    void sendCreateQueries(const ContextPtr & context, const std::vector<String> & create_queries);

    void sendQueryDataParts(
        const ContextPtr & context,
        const StoragePtr & storage,
        const String & local_table_name,
        const ServerDataPartsVector & parts,
        const std::set<Int64> & required_bucket_numbers);

    void sendOffloadingInfo(
        const ContextPtr & context,
        const HostWithPortsVec & read_workers,
        const std::vector<std::pair<StorageID, String>> & worker_table_names,
        const std::vector<HostWithPortsVec> & buffer_workers_vec);

    void sendFinishTask(TxnTimestamp txn_id, bool only_clean);

private:
    std::unique_ptr<Protos::CnchWorkerService_Stub> stub;
};

using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
using CnchWorkerClients = std::vector<CnchWorkerClientPtr>;


}

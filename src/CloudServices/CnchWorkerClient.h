/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <CloudServices/RpcClientBase.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/Kafka/KafkaTaskCommand.h>
#include <Transaction/TxnTimestamp.h>
#include <brpc/controller.h>
#include <Common/Exception.h>
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"
#include "Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h"
#include <Storages/DataPart_fwd.h>

#include <unordered_set>

#if USE_MYSQL
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#endif

namespace DB
{
namespace Protos
{
    class CnchWorkerService_Stub;
}

namespace IngestColumnCnch
{
    struct IngestPartitionParam;
}

class MergeTreeMetaBase;
class StorageMaterializedView;
struct MarkRange;
struct StorageID;
struct ManipulationInfo;
struct ManipulationTaskParams;
struct DedupWorkerStatus;
struct AssignedResource;

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
        TxnTimestamp txn_id);

    void shutdownManipulationTasks(const UUID & table_uuid, const Strings & task_ids = Strings{});
    std::unordered_set<String> touchManipulationTasks(const UUID & table_uuid, const Strings & tasks_id);
    std::vector<ManipulationInfo> getManipulationTasksStatus();

    void submitMvRefreshTask(
        const StorageMaterializedView & storage, const ManipulationTaskParams & params, TxnTimestamp txn_id);

    /// send resource to worker async
    void sendCreateQueries(const ContextPtr & context, const std::vector<String> & create_queries, std::set<String> cnch_table_create_queries = {});

    brpc::CallId sendQueryDataParts(
        const ContextPtr & context,
        const StoragePtr & storage,
        const String & local_table_name,
        const ServerDataPartsVector & parts,
        const std::set<Int64> & required_bucket_numbers,
        const ExceptionHandlerWithFailedInfoPtr & handler,
        const WorkerId & worker_id = WorkerId{});

    brpc::CallId preloadDataParts(
        const ContextPtr & context,
        const TxnTimestamp & txn_id,
        const IStorage & storage,
        const String & create_local_table_query,
        const ServerDataPartsVector & parts,
        const ExceptionHandlerPtr & handler,
        bool enable_parts_sync_preload,
        UInt64 parts_preload_level,
        UInt64 submit_ts);

        brpc::CallId dropPartDiskCache(
            const ContextPtr & context,
            const TxnTimestamp & txn_id,
            const IStorage & storage,
            const String & create_local_table_query,
            const ServerDataPartsVector & parts,
            bool sync,
            bool drop_vw_disk_cache);

    brpc::CallId sendOffloadingInfo(
        const ContextPtr & context,
        const HostWithPortsVec & read_workers,
        const std::vector<std::pair<StorageID, String>> & worker_table_names,
        const std::vector<HostWithPortsVec> & buffer_workers_vec,
        const ExceptionHandlerPtr & handler);

    brpc::CallId sendResources(
        const ContextPtr & context,
        const std::vector<AssignedResource> & resources_to_send,
        const ExceptionHandlerWithFailedInfoPtr & handler,
        const WorkerId & worker_id,
        bool with_mutations = false);

    void removeWorkerResource(TxnTimestamp txn_id);

    void createDedupWorker(const StorageID & storage_id, const String & create_table_query, const HostWithPorts & host_ports);
    void dropDedupWorker(const StorageID & storage_id);
    DedupWorkerStatus getDedupWorkerStatus(const StorageID & storage_id);

#if USE_RDKAFKA
    void submitKafkaConsumeTask(const KafkaTaskCommand & command);
    CnchConsumerStatus getConsumerStatus(const StorageID & storage_id);
#endif

#if USE_MYSQL
    void submitMySQLSyncThreadTask(const MySQLSyncThreadCommand & command);
    bool checkMySQLSyncThreadStatus(const String & database_name, const String & sync_thread);
#endif

private:
    std::unique_ptr<Protos::CnchWorkerService_Stub> stub;
};

using CnchWorkerClientPtr = std::shared_ptr<CnchWorkerClient>;
using CnchWorkerClients = std::vector<CnchWorkerClientPtr>;


}

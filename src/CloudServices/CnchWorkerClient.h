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
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/Kafka/KafkaTaskCommand.h>
#include <Transaction/TxnTimestamp.h>
#include <brpc/controller.h>
#include <Common/Exception.h>
#include <Storages/CheckResults.h>
#include <Storages/Hive/HiveFile/IHiveFile_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
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
    class BackupCopyTask;
}

namespace IngestColumnCnch
{
    struct IngestPartitionParam;
}

namespace CnchDedupHelper
{
    struct DedupTask;
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

    CheckResults checkDataParts(
        const ContextPtr & context,
        const IStorage & storage,
        const String & local_table_name,
        const String & create_query,
        const ServerDataPartsVector & parts);

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

    brpc::CallId dropManifestDiskCache(
        const ContextPtr & context,
        const IStorage & storage,
        const String & version,
        const bool sync);

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

    brpc::CallId executeDedupTask(
        const ContextPtr & context,
        const TxnTimestamp & txn_id,
        UInt16 rpc_port,
        const IStorage & storage,
        const CnchDedupHelper::DedupTask & dedup_task,
        const ExceptionHandlerPtr & handler,
        std::function<void(bool)> funcOnCallback);

    brpc::CallId removeWorkerResource(TxnTimestamp txn_id, ExceptionHandlerPtr handler);

    brpc::CallId broadcastManifest(
        const ContextPtr & context,
        const TxnTimestamp & txn_id,
        const WorkerId & worker_id,
        const StoragePtr & table,
        const Protos::DataModelPartVector & parts_vector,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const ExceptionHandlerPtr & handler);

    void createDedupWorker(const StorageID & storage_id, const String & create_table_query, const HostWithPorts & host_ports, const size_t & deduper_index);
    void assignHighPriorityDedupPartition(const StorageID & storage_id, const Names & high_priority_partition);
    void assignRepairGran(const StorageID & storage_id, const String & partition_id, const Int64 & bucket_number, const UInt64 & max_event_time);
    void dropDedupWorker(const StorageID & storage_id);
    DedupWorkerStatus getDedupWorkerStatus(const StorageID & storage_id);

    brpc::CallId sendBackupCopyTask(const ContextPtr & context, const String & backup_id, const std::vector<Protos::BackupCopyTask> & copy_tasks, const ExceptionHandlerPtr & handler);

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

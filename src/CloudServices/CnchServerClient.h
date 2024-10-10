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

#include <CloudServices/RpcClientBase.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Storages/IStorage_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/CnchLock.h>
#include <WorkerTasks/ManipulationType.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Catalog/CatalogUtils.h>
#include <Access/IAccessEntity.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>

namespace DB
{
namespace Protos
{
    class CnchServerService_Stub;
}

class ICnchTransaction;
class CnchServerTransaction;
using CnchServerTransactionPtr = std::shared_ptr<CnchServerTransaction>;
struct PrunedPartitions;
class StorageCloudMergeTree;
struct DumpedData;

class CnchServerClient : public RpcClientBase
{
public:
    static String getName() { return "CnchServerClient"; }

    explicit CnchServerClient(String host_port_);
    explicit CnchServerClient(HostWithPorts host_ports_);

    ~CnchServerClient() override;

    /// Transaction RPCs related. TODO @canh: add implement when baseline rpc implementation is merged
    std::pair<TxnTimestamp, TxnTimestamp> createTransaction(const TxnTimestamp & primary_txn_id, bool read_only);
    std::pair<TxnTimestamp, TxnTimestamp> createTransactionForKafka(const StorageID & storage_id, const size_t consumer_index);
    TxnTimestamp commitTransaction(
        const ICnchTransaction & txn, const StorageID & kafka_storage_id = StorageID::createEmpty(), const size_t consumer_index = 0);
    void precommitTransaction(const ContextPtr & context, const TxnTimestamp & txn_id, const UUID & uuid = UUIDHelpers::Nil);
    TxnTimestamp rollbackTransaction(const TxnTimestamp & txn_id);
    void finishTransaction(const TxnTimestamp & txn_id);

    void commitTransactionViaGlobalCommitter(const TransactionCnchPtr & txn);

    CnchTransactionStatus getTransactionStatus(const TxnTimestamp & txn_id, bool need_search_catalog = false);

    // Statistics
    std::unordered_map<UUID, UInt64> queryUdiCounter();
    void redirectUdiCounter(const std::unordered_map<UUID, UInt64>& data);
    void scheduleDistributeUdiCount();
    void scheduleAutoStatsCollect();
    void redirectAsyncStatsTasks(google::protobuf::RepeatedPtrField<Protos::AutoStats::TaskInfoCore> tasks);

    void removeIntermediateData(const TxnTimestamp & txn_id);

    ServerDataPartsVector fetchDataParts(const String & remote_host, const ConstStoragePtr & table, const Strings & partition_list, const TxnTimestamp & ts, const std::set<Int64> & bucket_numbers);
    DeleteBitmapMetaPtrVector fetchDeleteBitmaps(
        const String & remote_host,
        const ConstStoragePtr & table,
        const Strings & partition_list,
        const TxnTimestamp & ts,
        const std::set<Int64> & bucket_numbers = {});

    PrunedPartitions fetchPartitions(
        const String & remote_host,
        const ConstStoragePtr & table,
        const SelectQueryInfo & query_info,
        const Names & column_names,
        const TxnTimestamp & txn_id,
        const bool & ignore_ttl);

    void redirectCommitParts(
        const StoragePtr & table,
        const Catalog::CommitItems & commit_data,
        const TxnTimestamp & txnID,
        const bool is_merged_parts,
        const bool preallocate_mode);

    void redirectClearParts(const StoragePtr & table, const Catalog::CommitItems & commit_data);

    void redirectSetCommitTime(
        const StoragePtr & table,
        const Catalog::CommitItems & commit_data,
        const TxnTimestamp & commitTs,
        const UInt64 txn_id);

    void redirectAttachDetachedS3Parts(
        const StoragePtr & to_table,
        const UUID & from_table_uuid,
        const UUID & to_table_uuid,
        const IMergeTreeDataPartsVector & commit_parts,
        const IMergeTreeDataPartsVector & commit_staged_parts,
        const Strings & detached_part_names,
        size_t detached_visible_part_size,
        size_t detached_staged_part_size,
        const Strings & detached_bitmap_names,
        const DeleteBitmapMetaPtrVector & detached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps,
        const DB::Protos::DetachAttachType & type,
        const UInt64 & txn_id);

    void redirectDetachAttachedS3Parts(
        const StoragePtr & to_table,
        const UUID & from_table_uuid,
        const UUID & to_table_uuid,
        const IMergeTreeDataPartsVector & attached_parts,
        const IMergeTreeDataPartsVector & attached_staged_parts,
        const IMergeTreeDataPartsVector & commit_parts,
        const Strings & attached_part_names,
        const Strings & attached_bitmap_names,
        const DeleteBitmapMetaPtrVector & attached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps,
        const std::vector<std::pair<String, String>> & detached_part_metas,
        const std::vector<std::pair<String, String>> & detached_bitmap_metas,
        const DB::Protos::DetachAttachType & type,
        const UInt64 & txn_id);

    UInt32 commitParts(
        const TxnTimestamp & txn_id,
        ManipulationType type,
        MergeTreeMetaBase & storage,
        const DumpedData & dumped_data,
        const String & task_id = {},
        const bool from_server = false,
        const String & consumer_group = {},
        const cppkafka::TopicPartitionList & tpl = {},
        const MySQLBinLogInfo & binlog = {},
        const UInt64 peak_memory_usage = 0);

    /**
     * @return UInt32 dedup impl version for unique table, current valid value is 1 or 2. 1 means old impl which will dedup in write suffix stage, 2 means new impl which will dedup in txn commit stage. 
     */
    UInt32 precommitParts(
        ContextPtr context,
        const TxnTimestamp & txn_id,
        ManipulationType type,
        MergeTreeMetaBase & storage,
        const DumpedData & dumped_data,
        const String & task_id = {},
        const bool from_server = false,
        const String & consumer_group = {},
        const cppkafka::TopicPartitionList & tpl = {},
        const MySQLBinLogInfo & binlog = {},
        const UInt64 peak_memory_usage = 0);

    MergeTreeDataPartsCNCHVector fetchCloudTableMeta(
        const StorageCloudMergeTree & storage, const TxnTimestamp & ts, const std::unordered_set<Int64> & bucket_numbers = {});

    google::protobuf::RepeatedPtrField<DB::Protos::DataModelTableInfo>
    getTableInfo(const std::vector<std::shared_ptr<Protos::TableIdentifier>> & tables);
    void controlCnchBGThread(const StorageID & storage_id, CnchBGThreadType type, CnchBGThreadAction action);
    void cleanTransaction(const TransactionRecord & txn_record);
    /**
     * @brief Clean undo buffers with the given txn (only) on target server.
     *
     * @param txn_record The transaction to which the Undo Buffer belongs.
     */
    void cleanUndoBuffers(const TransactionRecord & txn_record);
    std::set<UUID> getDeletingTablesInGlobalGC();
    bool removeMergeMutateTasksOnPartitions(const StorageID &, const std::unordered_set<String> &);

    void acquireLock(const LockInfoPtr & info);
    void releaseLock(const LockInfoPtr & info);
    void assertLockAcquired(const LockInfoPtr & info);
    void reportCnchLockHeartBeat(const TxnTimestamp & txn_id, UInt64 expire_time = 0);

    std::optional<TxnTimestamp> getMinActiveTimestamp(const StorageID & storage_id);

    UInt64 getServerStartTime();
    bool scheduleGlobalGC(const std::vector<Protos::DataModelTable> & tables);
    size_t getNumOfTablesCanSendForGlobalGC();
    google::protobuf::RepeatedPtrField<DB::Protos::BackgroundThreadStatus>
    getBackGroundStatus(const CnchBGThreadType & type);

    brpc::CallId submitPreloadTask(const MergeTreeMetaBase & storage, const MutableMergeTreeDataPartsCNCHVector & parts, UInt64 timeout_ms);

    UInt32 reportDeduperHeartbeat(const StorageID & cnch_storage_id, const String & worker_table_name);

    void handleRefreshTaskOnFinish(StorageID & mv_storage_id, String task_id, Int64 txn_id);

    void executeOptimize(const StorageID & storage_id, const String & partition_id, bool enable_try, bool mutations_sync, UInt64 timeout_ms);
    void notifyAccessEntityChange(IAccessEntity::Type type, const String & name);

    brpc::CallId submitBackupTask(const String & backup_id, const String & backup_command);

    std::optional<String> getRunningBackupTask();

    void removeRunningBackupTask(const String & backup_id);

    UInt32 getDedupImplVersion(const TxnTimestamp & txn_id, const UUID & uuid);

#if USE_MYSQL
    void submitMaterializedMySQLDDLQuery(const String & database_name, const String & sync_thread, const String & query, const MySQLBinLogInfo & binlog);
    void reportHeartBeatForSyncThread(const String & database_name, const String & sync_thread);
    void reportSyncFailedForSyncThread(const String & database_name, const String & sync_thread);
#endif

    void forceRecalculateMetrics(const StorageID & storage_id);
    std::vector<Protos::LastModificationTimeHint> getLastModificationTimeHints(const StorageID & storage_id);
    void notifyTableCreated(const UUID & storage_uuid, int64_t cnch_notify_table_created_rpc_timeout_ms);

    void notifyAccessEntityChange(IAccessEntity::Type type, const String & name, const UUID & uuid);

    void checkDelayInsertOrThrowIfNeeded(UUID storage_uuid);
private:
    std::unique_ptr<Protos::CnchServerService_Stub> stub;
};

using CnchServerClientPtr = std::shared_ptr<CnchServerClient>;

}

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

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Catalog/Catalog.h>
#include <Catalog/CatalogFactory.h>
#include <Catalog/DataModelPartWrapper.h>
#include <Catalog/StringHelper.h>
#include <Catalog/LargeKVHandler.h>
#include <Catalog/CatalogBackgroundTask.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTCreateQuery.h>
#include <Databases/DatabaseCnch.h>
#include <common/logger_useful.h>
// #include <ext/range.h>
#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Catalog/MetastoreCommon.h>
#include <Core/Types.h>
#include <DataTypes/ObjectUtils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>
#include <Transaction/TransactionCommon.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/RpcClientPool.h>
#include <Common/ScanWaitFreeMap.h>
#include <Common/Status.h>
#include <Common/serverLocality.h>
#include "Interpreters/DistributedStages/PlanSegmentInstance.h"
// #include <Access/MaskingPolicyDataModel.h>
// #include <Access/MaskingPolicyCommon.h>
#include <Catalog/CatalogMetricHelper.h>
#include <CloudServices/CnchPartsHelper.h>
#include <CloudServices/CnchServerClient.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Parsers/formatAST.h>
#include <Protos/RPCHelpers.h>
#include <Protos/data_models.pb.h>
#include <Statistics/ExportSymbols.h>
#include <Statistics/StatisticsBase.h>
#include <Storages/CnchStorageCache.h>
#include <Storages/Hive/StorageCnchHive.h>
#include <Storages/MergeTree/CnchAttachProcessor.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/PartCacheManager.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDictionary.h>
#include <Transaction/Actions/S3AttachMetaAction.h>
#include <Transaction/Actions/S3DetachMetaAction.h>
#include <Transaction/TxnTimestamp.h>
#include <Transaction/getCommitted.h>
#include <brpc/server.h>


/// TODO: put all global gflags together in somewhere.
namespace brpc::policy { DECLARE_string(consul_agent_addr); }

namespace brpc { DECLARE_int32(defer_close_second);}

namespace ProfileEvents
{
    extern const int CnchTxnAllTransactionRecord;
    extern const Event CatalogConstructorSuccess;
    extern const Event CatalogConstructorFailed;
    extern const Event UpdateTableStatisticsSuccess;
    extern const Event UpdateTableStatisticsFailed;
    extern const Event GetTableStatisticsSuccess;
    extern const Event GetTableStatisticsFailed;
    extern const Event RemoveTableStatisticsSuccess;
    extern const Event RemoveTableStatisticsFailed;
    extern const Event UpdateColumnStatisticsSuccess;
    extern const Event UpdateColumnStatisticsFailed;
    extern const Event GetColumnStatisticsSuccess;
    extern const Event GetColumnStatisticsFailed;
    extern const Event GetAllColumnStatisticsSuccess;
    extern const Event GetAllColumnStatisticsFailed;
    extern const Event RemoveColumnStatisticsSuccess;
    extern const Event RemoveColumnStatisticsFailed;
    extern const Event RemoveAllColumnStatisticsSuccess;
    extern const Event RemoveAllColumnStatisticsFailed;
    extern const Event UpdateSQLBindingSuccess;
    extern const Event UpdateSQLBindingFailed;
    extern const Event GetSQLBindingSuccess;
    extern const Event GetSQLBindingFailed;
    extern const Event GetSQLBindingsSuccess;
    extern const Event GetSQLBindingsFailed;
    extern const Event RemoveSQLBindingSuccess;
    extern const Event RemoveSQLBindingFailed;
    extern const Event UpdatePreparedStatementSuccess;
    extern const Event UpdatePreparedStatementFailed;
    extern const Event GetPreparedStatementSuccess;
    extern const Event GetPreparedStatementFailed;
    extern const Event GetPreparedStatementsSuccess;
    extern const Event GetPreparedStatementsFailed;
    extern const Event RemovePreparedStatementSuccess;
    extern const Event RemovePreparedStatementFailed;
    extern const Event CreateDatabaseSuccess;
    extern const Event CreateDatabaseFailed;
    extern const Event GetDatabaseSuccess;
    extern const Event GetDatabaseFailed;
    extern const Event IsDatabaseExistsSuccess;
    extern const Event IsDatabaseExistsFailed;
    extern const Event DropDatabaseSuccess;
    extern const Event DropDatabaseFailed;
    extern const Event RenameDatabaseSuccess;
    extern const Event RenameDatabaseFailed;
    extern const Event AlterDatabaseSuccess;
    extern const Event AlterDatabaseFailed;
    extern const Event CreateSnapshotSuccess;
    extern const Event CreateSnapshotFailed;
    extern const Event RemoveSnapshotSuccess;
    extern const Event RemoveSnapshotFailed;
    extern const Event TryGetSnapshotSuccess;
    extern const Event TryGetSnapshotFailed;
    extern const Event GetAllSnapshotsSuccess;
    extern const Event GetAllSnapshotsFailed;
    extern const Event CreateTableSuccess;
    extern const Event CreateTableFailed;
    extern const Event DropTableSuccess;
    extern const Event DropTableFailed;
    extern const Event CreateUDFSuccess;
    extern const Event CreateUDFFailed;
    extern const Event DropUDFSuccess;
    extern const Event DropUDFFailed;
    extern const Event DetachTableSuccess;
    extern const Event DetachTableFailed;
    extern const Event AttachTableSuccess;
    extern const Event AttachTableFailed;
    extern const Event IsTableExistsSuccess;
    extern const Event IsTableExistsFailed;
    extern const Event AlterTableSuccess;
    extern const Event AlterTableFailed;
    extern const Event RenameTableSuccess;
    extern const Event RenameTableFailed;
    extern const Event SetWorkerGroupForTableSuccess;
    extern const Event SetWorkerGroupForTableFailed;
    extern const Event GetTableSuccess;
    extern const Event GetTableFailed;
    extern const Event TryGetTableSuccess;
    extern const Event TryGetTableFailed;
    extern const Event TryGetTableByUUIDSuccess;
    extern const Event TryGetTableByUUIDFailed;
    extern const Event GetTableByUUIDSuccess;
    extern const Event GetTableByUUIDFailed;
    extern const Event GetTablesInDBSuccess;
    extern const Event GetTablesInDBFailed;
    extern const Event GetAllViewsOnSuccess;
    extern const Event GetAllViewsOnFailed;
    extern const Event SetTableActivenessSuccess;
    extern const Event SetTableActivenessFailed;
    extern const Event GetTableActivenessSuccess;
    extern const Event GetTableActivenessFailed;
    extern const Event GetServerDataPartsInPartitionsSuccess;
    extern const Event GetServerDataPartsInPartitionsFailed;
    extern const Event GetAllServerDataPartsSuccess;
    extern const Event GetAllServerDataPartsFailed;
    extern const Event GetAllServerDataPartsWithDBMSuccess;
    extern const Event GetAllServerDataPartsWithDBMFailed;
    extern const Event GetDataPartsByNamesSuccess;
    extern const Event GetDataPartsByNamesFailed;
    extern const Event GetStagedDataPartsByNamesSuccess;
    extern const Event GetStagedDataPartsByNamesFailed;
    extern const Event GetAllDeleteBitmapsSuccess;
    extern const Event GetAllDeleteBitmapsFailed;
    extern const Event GetStagedPartsSuccess;
    extern const Event GetStagedPartsFailed;
    extern const Event GetDeleteBitmapsInPartitionsSuccess;
    extern const Event GetDeleteBitmapsInPartitionsFailed;
    extern const Event GetDeleteBitmapsFromCacheInPartitionsSuccess;
    extern const Event GetDeleteBitmapsFromCacheInPartitionsFailed;
    extern const Event GetDeleteBitmapByKeysSuccess;
    extern const Event GetDeleteBitmapByKeysFailed;
    extern const Event AddDeleteBitmapsSuccess;
    extern const Event AddDeleteBitmapsFailed;
    extern const Event RemoveDeleteBitmapsSuccess;
    extern const Event RemoveDeleteBitmapsFailed;
    extern const Event FinishCommitSuccess;
    extern const Event FinishCommitFailed;
    extern const Event GetKafkaOffsetsVoidSuccess;
    extern const Event GetKafkaOffsetsVoidFailed;
    extern const Event GetKafkaOffsetsTopicPartitionListSuccess;
    extern const Event GetKafkaOffsetsTopicPartitionListFailed;
    extern const Event ClearOffsetsForWholeTopicSuccess;
    extern const Event ClearOffsetsForWholeTopicFailed;
    extern const Event SetTransactionForKafkaConsumerSuccess;
    extern const Event SetTransactionForKafkaConsumerFailed;
    extern const Event GetTransactionForKafkaConsumerSuccess;
    extern const Event GetTransactionForKafkaConsumerFailed;
    extern const Event ClearKafkaTransactionsForTableSuccess;
    extern const Event ClearKafkaTransactionsForTableFailed;
    extern const Event DropAllPartSuccess;
    extern const Event DropAllPartFailed;
    extern const Event GetPartitionListSuccess;
    extern const Event GetPartitionListFailed;
    extern const Event GetPartitionsFromMetastoreSuccess;
    extern const Event GetPartitionsFromMetastoreFailed;
    extern const Event GetPartitionIDsSuccess;
    extern const Event GetPartitionIDsFailed;
    extern const Event CreateDictionarySuccess;
    extern const Event CreateDictionaryFailed;
    extern const Event GetCreateDictionarySuccess;
    extern const Event GetCreateDictionaryFailed;
    extern const Event DropDictionarySuccess;
    extern const Event DropDictionaryFailed;
    extern const Event DetachDictionarySuccess;
    extern const Event DetachDictionaryFailed;
    extern const Event AttachDictionarySuccess;
    extern const Event AttachDictionaryFailed;
    extern const Event GetDictionariesInDBSuccess;
    extern const Event GetDictionariesInDBFailed;
    extern const Event GetDictionarySuccess;
    extern const Event GetDictionaryFailed;
    extern const Event IsDictionaryExistsSuccess;
    extern const Event IsDictionaryExistsFailed;
    extern const Event CreateTransactionRecordSuccess;
    extern const Event CreateTransactionRecordFailed;
    extern const Event RemoveTransactionRecordSuccess;
    extern const Event RemoveTransactionRecordFailed;
    extern const Event RemoveTransactionRecordsSuccess;
    extern const Event RemoveTransactionRecordsFailed;
    extern const Event GetTransactionRecordSuccess;
    extern const Event GetTransactionRecordFailed;
    extern const Event TryGetTransactionRecordSuccess;
    extern const Event TryGetTransactionRecordFailed;
    extern const Event SetTransactionRecordSuccess;
    extern const Event SetTransactionRecordFailed;
    extern const Event SetTransactionRecordWithRequestsSuccess;
    extern const Event SetTransactionRecordWithRequestsFailed;
    extern const Event SetTransactionRecordCleanTimeSuccess;
    extern const Event SetTransactionRecordCleanTimeFailed;
    extern const Event SetTransactionRecordStatusWithOffsetsSuccess;
    extern const Event SetTransactionRecordStatusWithOffsetsFailed;
    extern const Event RollbackTransactionSuccess;
    extern const Event RollbackTransactionFailed;
    extern const Event WriteIntentsSuccess;
    extern const Event WriteIntentsFailed;
    extern const Event TryResetIntentsIntentsToResetSuccess;
    extern const Event TryResetIntentsIntentsToResetFailed;
    extern const Event TryResetIntentsOldIntentsSuccess;
    extern const Event TryResetIntentsOldIntentsFailed;
    extern const Event ClearIntentsSuccess;
    extern const Event ClearIntentsFailed;
    extern const Event WritePartsSuccess;
    extern const Event WritePartsFailed;
    extern const Event SetCommitTimeSuccess;
    extern const Event SetCommitTimeFailed;
    extern const Event ClearPartsSuccess;
    extern const Event ClearPartsFailed;
    extern const Event WriteUndoBufferConstResourceSuccess;
    extern const Event WriteUndoBufferConstResourceFailed;
    extern const Event WriteUndoBufferNoConstResourceSuccess;
    extern const Event WriteUndoBufferNoConstResourceFailed;
    extern const Event ClearUndoBufferSuccess;
    extern const Event ClearUndoBufferFailed;
    extern const Event GetUndoBufferSuccess;
    extern const Event GetUndoBufferFailed;
    extern const Event GetAllUndoBufferSuccess;
    extern const Event GetAllUndoBufferFailed;
    extern const Event GetUndoBufferIteratorSuccess;
    extern const Event GetUndoBufferIteratorFailed;
    extern const Event GetTransactionRecordsSuccess;
    extern const Event GetTransactionRecordsFailed;
    extern const Event GetTransactionRecordsTxnIdsSuccess;
    extern const Event GetTransactionRecordsTxnIdsFailed;
    extern const Event GetTransactionRecordsForGCSuccess;
    extern const Event GetTransactionRecordsForGCFailed;
    extern const Event ClearZombieIntentSuccess;
    extern const Event ClearZombieIntentFailed;
    extern const Event WriteFilesysLockSuccess;
    extern const Event WriteFilesysLockFailed;
    extern const Event GetFilesysLockSuccess;
    extern const Event GetFilesysLockFailed;
    extern const Event ClearFilesysLockDirSuccess;
    extern const Event ClearFilesysLockDirFailed;
    extern const Event ClearFilesysLockTxnIdSuccess;
    extern const Event ClearFilesysLockTxnIdFailed;
    extern const Event GetAllFilesysLockSuccess;
    extern const Event GetAllFilesysLockFailed;
    extern const Event InsertTransactionSuccess;
    extern const Event InsertTransactionFailed;
    extern const Event RemoveTransactionSuccess;
    extern const Event RemoveTransactionFailed;
    extern const Event GetActiveTransactionsSuccess;
    extern const Event GetActiveTransactionsFailed;
    extern const Event UpdateServerWorkerGroupSuccess;
    extern const Event UpdateServerWorkerGroupFailed;
    extern const Event GetWorkersInWorkerGroupSuccess;
    extern const Event GetWorkersInWorkerGroupFailed;
    extern const Event GetTableHistoriesSuccess;
    extern const Event GetTableHistoriesFailed;
    extern const Event GetTablesByIDSuccess;
    extern const Event GetTablesByIDFailed;
    extern const Event GetAllDataBasesSuccess;
    extern const Event GetAllDataBasesFailed;
    extern const Event GetAllTablesSuccess;
    extern const Event GetAllTablesFailed;
    extern const Event GetTrashTableIDIteratorSuccess;
    extern const Event GetTrashTableIDIteratorFailed;
    extern const Event GetAllUDFsSuccess;
    extern const Event GetAllUDFsFailed;
    extern const Event GetUDFByNameSuccess;
    extern const Event GetUDFByNameFailed;
    extern const Event GetTrashTableIDSuccess;
    extern const Event GetTrashTableIDFailed;
    extern const Event GetTablesInTrashSuccess;
    extern const Event GetTablesInTrashFailed;
    extern const Event GetDatabaseInTrashSuccess;
    extern const Event GetDatabaseInTrashFailed;
    extern const Event GetAllTablesIDSuccess;
    extern const Event GetAllTablesIDFailed;
    extern const Event GetTablesIDByTenantSuccess;
    extern const Event GetTablesIDByTenantFailed;
    extern const Event GetTableIDByNameSuccess;
    extern const Event GetTableIDByNameFailed;
    extern const Event GetTableIDsByNamesSuccess;
    extern const Event GetTableIDsByNamesFailed;
    extern const Event GetAllWorkerGroupsSuccess;
    extern const Event GetAllWorkerGroupsFailed;
    extern const Event GetAllDictionariesSuccess;
    extern const Event GetAllDictionariesFailed;
    extern const Event ClearDatabaseMetaSuccess;
    extern const Event ClearDatabaseMetaFailed;
    extern const Event ClearTableMetaForGCSuccess;
    extern const Event ClearTableMetaForGCFailed;
    extern const Event ClearDataPartsMetaSuccess;
    extern const Event ClearDataPartsMetaFailed;
    extern const Event ClearStagePartsMetaSuccess;
    extern const Event ClearStagePartsMetaFailed;
    extern const Event ClearDataPartsMetaForTableSuccess;
    extern const Event ClearDataPartsMetaForTableFailed;
    extern const Event ClearDeleteBitmapsMetaForTableSuccess;
    extern const Event ClearDeleteBitmapsMetaForTableFailed;
    extern const Event GetSyncListSuccess;
    extern const Event GetSyncListFailed;
    extern const Event ClearSyncListSuccess;
    extern const Event ClearSyncListFailed;
    extern const Event GetServerPartsByCommitTimeSuccess;
    extern const Event GetServerPartsByCommitTimeFailed;
    extern const Event CreateRootPathSuccess;
    extern const Event CreateRootPathFailed;
    extern const Event DeleteRootPathSuccess;
    extern const Event DeleteRootPathFailed;
    extern const Event GetAllRootPathSuccess;
    extern const Event GetAllRootPathFailed;
    extern const Event CreateMutationSuccess;
    extern const Event CreateMutationFailed;
    extern const Event RemoveMutationSuccess;
    extern const Event RemoveMutationFailed;
    extern const Event GetAllMutationsStorageIdSuccess;
    extern const Event GetAllMutationsStorageIdFailed;
    extern const Event GetAllMutationsSuccess;
    extern const Event GetAllMutationsFailed;
    extern const Event SetTableClusterStatusSuccess;
    extern const Event SetTableClusterStatusFailed;
    extern const Event GetTableClusterStatusSuccess;
    extern const Event GetTableClusterStatusFailed;
    extern const Event IsTableClusteredSuccess;
    extern const Event IsTableClusteredFailed;
    extern const Event SetTablePreallocateVWSuccess;
    extern const Event SetTablePreallocateVWFailed;
    extern const Event GetTablePreallocateVWSuccess;
    extern const Event GetTablePreallocateVWFailed;
    extern const Event GetTrashItemsInfoMetricsSuccess;
    extern const Event GetTrashItemsInfoMetricsFailed;
    extern const Event GetPartsInfoMetricsSuccess;
    extern const Event GetPartsInfoMetricsFailed;
    extern const Event GetMetricsFromMetastoreSuccess;
    extern const Event GetPartitionMetricsFromMetastoreFailed;
    extern const Event GetPartitionMetricsFromMetastoreSuccess;
    extern const Event GetTableTrashItemsMetricsDataFromMetastoreFailed;
    extern const Event GetTableTrashItemsMetricsDataFromMetastoreSuccess;
    extern const Event UpdateTopologiesSuccess;
    extern const Event UpdateTopologiesFailed;
    extern const Event GetTopologiesSuccess;
    extern const Event GetTopologiesFailed;
    extern const Event GetTrashDBVersionsSuccess;
    extern const Event GetTrashDBVersionsFailed;
    extern const Event UndropDatabaseSuccess;
    extern const Event UndropDatabaseFailed;
    extern const Event GetTrashTableVersionsSuccess;
    extern const Event GetTrashTableVersionsFailed;
    extern const Event UndropTableSuccess;
    extern const Event UndropTableFailed;
    extern const Event GetInsertionLabelKeySuccess;
    extern const Event GetInsertionLabelKeyFailed;
    extern const Event PrecommitInsertionLabelSuccess;
    extern const Event PrecommitInsertionLabelFailed;
    extern const Event CommitInsertionLabelSuccess;
    extern const Event CommitInsertionLabelFailed;
    extern const Event TryCommitInsertionLabelSuccess;
    extern const Event TryCommitInsertionLabelFailed;
    extern const Event AbortInsertionLabelSuccess;
    extern const Event AbortInsertionLabelFailed;
    extern const Event GetInsertionLabelSuccess;
    extern const Event GetInsertionLabelFailed;
    extern const Event RemoveInsertionLabelSuccess;
    extern const Event RemoveInsertionLabelFailed;
    extern const Event RemoveInsertionLabelsSuccess;
    extern const Event RemoveInsertionLabelsFailed;
    extern const Event ScanInsertionLabelsSuccess;
    extern const Event ScanInsertionLabelsFailed;
    extern const Event ClearInsertionLabelsSuccess;
    extern const Event ClearInsertionLabelsFailed;
    extern const Event CreateVirtualWarehouseSuccess;
    extern const Event CreateVirtualWarehouseFailed;
    extern const Event AlterVirtualWarehouseSuccess;
    extern const Event AlterVirtualWarehouseFailed;
    extern const Event TryGetVirtualWarehouseSuccess;
    extern const Event TryGetVirtualWarehouseFailed;
    extern const Event ScanVirtualWarehousesSuccess;
    extern const Event ScanVirtualWarehousesFailed;
    extern const Event DropVirtualWarehouseSuccess;
    extern const Event DropVirtualWarehouseFailed;
    extern const Event CreateWorkerGroupSuccess;
    extern const Event CreateWorkerGroupFailed;
    extern const Event UpdateWorkerGroupSuccess;
    extern const Event UpdateWorkerGroupFailed;
    extern const Event TryGetWorkerGroupSuccess;
    extern const Event TryGetWorkerGroupFailed;
    extern const Event ScanWorkerGroupsSuccess;
    extern const Event ScanWorkerGroupsFailed;
    extern const Event DropWorkerGroupSuccess;
    extern const Event DropWorkerGroupFailed;
    extern const Event GetNonHostUpdateTimestampFromByteKVSuccess;
    extern const Event GetNonHostUpdateTimestampFromByteKVFailed;
    extern const Event MaskingPolicyExistsSuccess;
    extern const Event MaskingPolicyExistsFailed;
    extern const Event GetMaskingPoliciesSuccess;
    extern const Event GetMaskingPoliciesFailed;
    extern const Event PutMaskingPolicySuccess;
    extern const Event PutMaskingPolicyFailed;
    extern const Event TryGetMaskingPolicySuccess;
    extern const Event TryGetMaskingPolicyFailed;
    extern const Event GetMaskingPolicySuccess;
    extern const Event GetMaskingPolicyFailed;
    extern const Event GetAllMaskingPolicySuccess;
    extern const Event GetAllMaskingPolicyFailed;
    extern const Event GetMaskingPolicyAppliedTablesSuccess;
    extern const Event GetMaskingPolicyAppliedTablesFailed;
    extern const Event GetAllMaskingPolicyAppliedTablesSuccess;
    extern const Event GetAllMaskingPolicyAppliedTablesFailed;
    extern const Event DropMaskingPoliciesSuccess;
    extern const Event DropMaskingPoliciesFailed;
    extern const Event SetBGJobStatusSuccess;
    extern const Event SetBGJobStatusFailed;
    extern const Event GetBGJobStatusSuccess;
    extern const Event GetBGJobStatusFailed;
    extern const Event GetBGJobStatusesSuccess;
    extern const Event GetBGJobStatusesFailed;
    extern const Event DropBGJobStatusSuccess;
    extern const Event DropBGJobStatusFailed;
    extern const Event PutSensitiveResourceSuccess;
    extern const Event PutSensitiveResourceFailed;
    extern const Event GetSensitiveResourceSuccess;
    extern const Event GetSensitiveResourceFailed;
    extern const Event TryGetAccessEntitySuccess;
    extern const Event TryGetAccessEntityFailed;
    extern const Event GetAllAccessEntitySuccess;
    extern const Event GetAllAccessEntityFailed;
    extern const Event TryGetAccessEntityNameSuccess;
    extern const Event TryGetAccessEntityNameFailed;
    extern const Event PutAccessEntitySuccess;
    extern const Event PutAccessEntityFailed;
    extern const Event DropAccessEntitySuccess;
    extern const Event DropAccessEntityFailed;
    extern const Event GetByKeySuccess;
    extern const Event GetByKeyFailed;
    extern const Event GetMvBaseTableIDSuccess;
    extern const Event GetMvBaseTableIDFailed;
    extern const Event GetMvBaseTableVersionSuccess;
    extern const Event GetMvBaseTableVersionFailed;
    extern const Event UpdateMvMetaIDSuccess;
    extern const Event UpdateMvMetaIDFailed;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int UNKNOWN_DATABASE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int CATALOG_COMMIT_PART_ERROR;
    extern const int CATALOG_COMMIT_NHUT_ERROR;
    extern const int CATALOG_LOCK_PARTS_FAILURE;
    extern const int CATALOG_ALTER_TABLE_FAILURE;
    extern const int CATALOG_SERVICE_INTERNAL_ERROR;
    extern const int CATALOG_TRANSACTION_RECORD_NOT_FOUND;
    extern const int CNCH_TOPOLOGY_NOT_MATCH_ERROR;
    extern const int DICTIONARY_NOT_EXIST;
    extern const int UNKNOWN_MASKING_POLICY_NAME;
    extern const int BUCKET_TABLE_ENGINE_MISMATCH;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
}

namespace Catalog
{
    static ASTPtr parseCreateQuery(const String & create_query)
    {
        Strings res;
        const char *begin = create_query.data();
        const char *end = begin + create_query.size();
        ParserQuery parser(end);
        return parseQuery(parser, begin, end, "", 0, 0);
    }

    void Catalog::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
    {
        if (!config.has(config_elem))
            return;
        settings.loadFromConfig(config_elem + ".settings", config);
    }

    Catalog::Catalog(Context & _context, const MetastoreConfig & config, String _name_space, bool writable) : context(_context), name_space(_name_space)
    {
        runWithMetricSupport(
            [&] {
                /// Set defer_close_second to 30s to reuse connections. Normally, the connection is closed when a channle is destroyed after completion of the job.
                /// In the scan operation of bytekv, a new channel is created each time and destroyed when scan finished. In this case, connections are close
                /// and re-open again frequently. This will cause a lot of TIME-WAIT connections and may lead to unexpected exceptions.
                brpc::FLAGS_defer_close_second = 30;

                const char * consul_http_host = getConsulIPFromEnv();
                const char * consul_http_port = getenv("CONSUL_HTTP_PORT");
                if (consul_http_host != nullptr && consul_http_port != nullptr)
                {
                    brpc::policy::FLAGS_consul_agent_addr = "http://" + createHostPortString(consul_http_host, consul_http_port);
                    LOG_DEBUG(log, "Using consul agent: {}", brpc::policy::FLAGS_consul_agent_addr);
                }

                meta_proxy = std::make_shared<MetastoreProxy>(config, writable);
                /// Support set a custom topology key
                if (config.topology_key.empty())
                    topology_key = name_space;
                else
                    topology_key = name_space + "_" + config.topology_key;

                // Add background task to do some GC job for Catalog.
                bg_task = std::make_shared<CatalogBackgroundTask>(Context::createCopy(context.shared_from_this()), meta_proxy->getMetastore(), name_space);
            },
            ProfileEvents::CatalogConstructorSuccess,
            ProfileEvents::CatalogConstructorFailed);
    }

    MetastoreProxy::MetastorePtr Catalog::getMetastore()
    {
        return meta_proxy->getMetastore();
    }

    void Catalog::createDatabase(const String & database, const UUID & uuid, const TxnTimestamp & txnID, const TxnTimestamp & ts,
                                 const String & create_query, const String & engine_name, enum TextCaseOption text_case_option)
    {
        runWithMetricSupport(
            [&] {
                if (uuid == UUIDHelpers::Nil)
                {
                    throw Exception("Missing uuid for createDatabase", ErrorCodes::LOGICAL_ERROR);
                }
                if (tryGetDatabaseFromMetastore(database, ts.toUInt64()))
                {
                    throw Exception("Database already exits.", ErrorCodes::DATABASE_ALREADY_EXISTS);
                }
                {
                    DB::Protos::DataModelDB db_data;
                    db_data.set_name(database);
                    db_data.set_previous_version(0);
                    db_data.set_txnid(txnID.toUInt64());
                    db_data.set_commit_time(ts.toUInt64());
                    db_data.set_status(0);
                    RPCHelpers::fillUUID(uuid, *(db_data.mutable_uuid()));
                    if (engine_name == "CnchMaterializedMySQL")
                    {
                        if (create_query.empty())
                            throw Exception("create-query is required while create CnchMaterializedMySQL in catalog", ErrorCodes::LOGICAL_ERROR);

                        db_data.set_definition(create_query);
                        db_data.set_type(DB::Protos::CnchDatabaseType::MaterializedMySQL);
                    }

                    switch (text_case_option)
                    {
                        case TextCaseOption::MIXED:
                            db_data.set_text_case_option(DB::Protos::CnchTextCaseOption::Mixed);
                            break;
                        case TextCaseOption::LOWERCASE:
                            db_data.set_text_case_option(DB::Protos::CnchTextCaseOption::LowerCase);
                            break;
                        case TextCaseOption::UPPERCASE:
                            db_data.set_text_case_option(DB::Protos::CnchTextCaseOption::UpperCase);
                            break;
                        default:
                            break;
                    }

                    meta_proxy->addDatabase(name_space, db_data);
                }
            },
            ProfileEvents::CreateDatabaseSuccess,
            ProfileEvents::CreateDatabaseFailed);
    }

    DatabasePtr Catalog::getDatabase(const String & database, const ContextPtr & context, const TxnTimestamp & ts)
    {
        DatabasePtr res = nullptr;
        runWithMetricSupport(
            [&] {
                auto database_model = tryGetDatabaseFromMetastore(database, ts.toUInt64());
                if (database_model)
                {
                    DatabasePtr db = CatalogFactory::getDatabaseByDataModel(*database_model, context);

                    if (database_model->has_commit_time())
                    {
                        if (!database_model->has_type() || database_model->type() == DB::Protos::CnchDatabaseType::Cnch)
                            dynamic_cast<DatabaseCnch &>(*db).commit_time = TxnTimestamp{database_model->commit_time()};
                    }
                    res = db;
                }
                else
                {
                    res = nullptr;
                }
            },
            ProfileEvents::GetDatabaseSuccess,
            ProfileEvents::GetDatabaseFailed);
        return res;
    }

    bool Catalog::isDatabaseExists(const String & database, const TxnTimestamp & ts)
    {
        bool res = false;
        runWithMetricSupport(
            [&] {
                if (tryGetDatabaseFromMetastore(database, ts.toUInt64()))
                    res = true;
            },
            ProfileEvents::IsDatabaseExistsSuccess,
            ProfileEvents::IsDatabaseExistsFailed);
        return res;
    }

    void Catalog::dropDatabase(
        const String & database, const TxnTimestamp & previous_version, const TxnTimestamp & txnID, const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                auto db = tryGetDatabaseFromMetastore(database, ts.toUInt64());
                if (db)
                {
                    BatchCommitRequest batch_writes;

                    DB::Protos::DataModelDB db_data;
                    db_data.CopyFrom(*db);
                    db_data.set_previous_version(previous_version.toUInt64());
                    db_data.set_txnid(txnID.toUInt64());
                    db_data.set_commit_time(ts.toUInt64());
                    db_data.set_status(Status::setDelete(db->status()));

                    String db_meta;
                    db_data.SerializeToString(&db_meta);

                    // add a record into trash as well as a new version(mark as deleted) of db meta
                    batch_writes.AddPut(SinglePutRequest(MetastoreProxy::dbTrashKey(name_space, database, ts.toUInt64()), db_meta));
                    batch_writes.AddPut(SinglePutRequest(MetastoreProxy::dbKey(name_space, database, ts.toUInt64()), db_meta));

                    /// move all tables and dictionaries under this database into trash
                    auto table_id_ptrs = meta_proxy->getAllTablesId(name_space, database);
                    auto dic_ptrs = meta_proxy->getDictionariesInDB(name_space, database);

                    String trashBD_name = database + "_" + std::to_string(ts.toUInt64());
                    LOG_DEBUG(log, "Drop database {} with {} tables and {} dictionaries in it.", database, table_id_ptrs.size(), dic_ptrs.size());
                    for (auto & table_id_ptr : table_id_ptrs)
                    {
                        checkCanbeDropped(*table_id_ptr, true);
                        /// reset database name of table_id in trash because we need to differentiate multiple versions of dropped table which may have same name.
                        table_id_ptr->set_database(trashBD_name);
                        auto table = tryGetTableFromMetastore(table_id_ptr->uuid(), UINT64_MAX);
                        if (table)
                            moveTableIntoTrash(*table, *table_id_ptr, txnID, ts, batch_writes);
                    }

                    for (auto & dic_ptr : dic_ptrs)
                    {
                        batch_writes.AddPut(
                            SinglePutRequest(MetastoreProxy::dictionaryTrashKey(name_space, trashBD_name, dic_ptr->name()), dic_ptr->SerializeAsString()));
                        batch_writes.AddDelete(MetastoreProxy::dictionaryStoreKey(name_space, database, dic_ptr->name()));
                    }

                    BatchCommitResponse resp;
                    ///TODO: resolve risk of exceeding max batch size;
                    meta_proxy->batchWrite(batch_writes, resp);
                }
                else
                {
                    throw Exception("Database not found.", ErrorCodes::UNKNOWN_DATABASE);
                }
            },
            ProfileEvents::DropDatabaseSuccess,
            ProfileEvents::DropDatabaseFailed);
    }

    void Catalog::renameDatabase(
        const UUID & uuid, const String & from_database, const String & to_database, const TxnTimestamp & txnID, const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                auto database = tryGetDatabaseFromMetastore(from_database, ts.toUInt64());
                if (database)
                {
                    /// TODO: if there are too many tables in the database, the batch commit may fail because it will exceed the max batch size that bytekv allowed.
                    /// Then we should split rename tables task in sub transactions.

                    BatchCommitRequest batch_writes;
                    /// rename all tables in current database;
                    auto table_id_ptrs = meta_proxy->getAllTablesId(name_space, from_database);
                    for (auto & table_id_ptr : table_id_ptrs)
                    {
                        String table_uuid = table_id_ptr->uuid();
                        auto table = tryGetTableFromMetastore(table_uuid, UINT64_MAX);
                        if (!table)
                            throw Exception(ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR,
                                "Cannot get metadata of table {}.{} by UUID {}",
                                table_id_ptr->database(),
                                table_id_ptr->name(),
                                table_uuid);

                        if (table->commit_time() >= ts.toUInt64())
                            throw Exception("Cannot rename table with an earlier timestamp", ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

                        replace_definition(*table, to_database, table_id_ptr->name());
                        table->set_txnid(txnID.toUInt64());
                        table->set_commit_time(ts.toUInt64());
                        meta_proxy->prepareRenameTable(name_space, table_uuid, from_database, table_id_ptr->name(), uuid, *table, batch_writes);

                        if (auto cache_manager = context.getPartCacheManager(); cache_manager)
                            cache_manager->removeStorageCache(from_database, table->name());
                    }

                    /// remove old database record;
                    batch_writes.AddDelete(MetastoreProxy::dbKey(name_space, from_database, database->commit_time()));
                    /// create new database record;
                    database->set_name(to_database);
                    database->set_previous_version(0);
                    database->set_txnid(txnID.toUInt64());
                    database->set_commit_time(ts.toUInt64());
                    batch_writes.AddPut(SinglePutRequest(MetastoreProxy::dbKey(name_space, to_database, ts.toUInt64()), database->SerializeAsString()));

                    BatchCommitResponse resp;
                    ///TODO: resolve risk of exceeding max batch size;
                    meta_proxy->batchWrite(batch_writes, resp);
                }
                else
                {
                    throw Exception("Database not found.", ErrorCodes::UNKNOWN_DATABASE);
                }
            },
            ProfileEvents::RenameDatabaseSuccess,
            ProfileEvents::RenameDatabaseFailed);
    }

    void Catalog::alterDatabase(const String & alter_database, const TxnTimestamp & txnID, const TxnTimestamp & ts,
                                const String & create_query, const String & engine_name)
    {
        runWithMetricSupport(
            [&] {
                if (engine_name != "CnchMaterializedMySQL")
                    throw Exception("alter query is only available for CnchMaterializedMySQL", ErrorCodes::LOGICAL_ERROR);

                if (create_query.empty())
                    throw Exception("create_query field is required while alter CnchMaterializedMySQL in catalog", ErrorCodes::LOGICAL_ERROR);

                auto database = tryGetDatabaseFromMetastore(alter_database, ts.toUInt64());
                if (database)
                {
                    BatchCommitRequest batch_writes;

                    /// remove old database record;
                    batch_writes.AddDelete(MetastoreProxy::dbKey(name_space, alter_database, database->commit_time()));
                    /// create new database record;
                    database->set_name(alter_database);
                    database->set_previous_version(0);
                    database->set_txnid(txnID.toUInt64());
                    database->set_commit_time(ts.toUInt64());
                    database->set_definition(create_query);
                    database->set_type(DB::Protos::CnchDatabaseType::MaterializedMySQL);
                    batch_writes.AddPut(SinglePutRequest(MetastoreProxy::dbKey(name_space, alter_database, ts.toUInt64()), database->SerializeAsString()));

                    BatchCommitResponse resp;
                    meta_proxy->batchWrite(batch_writes, resp);
                }
                else
                {
                    throw Exception("Database not found.", ErrorCodes::UNKNOWN_DATABASE);
                }
            },
            ProfileEvents::AlterDatabaseSuccess,
            ProfileEvents::AlterDatabaseFailed);
    }

    void Catalog::createSnapshot(
        const UUID & db,
        const String & snapshot_name,
        const TxnTimestamp & ts,
        int ttl_in_days,
        UUID bind_table)
    {
        runWithMetricSupport(
            [&] {
                if (db == UUIDHelpers::Nil)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid UUID for DB");
                Protos::DataModelSnapshot model;
                model.set_name(snapshot_name);
                model.set_commit_time(ts.toUInt64());
                model.set_ttl_in_days(ttl_in_days);
                if (bind_table != UUIDHelpers::Nil)
                    RPCHelpers::fillUUID(bind_table, *model.mutable_table_uuid());

                String db_uuid_str = UUIDHelpers::UUIDToString(db);
                meta_proxy->createSnapshot(name_space, db_uuid_str, model);
                LOG_INFO(log, "Created snapshot {} in DB {} at {}", snapshot_name, db_uuid_str, ts.toString());
            },
            ProfileEvents::CreateSnapshotSuccess,
            ProfileEvents::CreateSnapshotFailed);
    }

    void Catalog::removeSnapshot(const UUID & db, const String & snapshot_name)
    {
        runWithMetricSupport(
            [&] {
                if (db == UUIDHelpers::Nil)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid UUID for DB");
                auto db_uuid_str = UUIDHelpers::UUIDToString(db);
                meta_proxy->removeSnapshot(name_space, db_uuid_str, snapshot_name);
                LOG_INFO(log, "Removed snapshot {} from DB {}", snapshot_name, db_uuid_str);
            },
            ProfileEvents::RemoveSnapshotSuccess,
            ProfileEvents::RemoveSnapshotFailed);
    }

    /// Return snapshot if exist, nullptr otherwise.
    SnapshotPtr Catalog::tryGetSnapshot(const UUID & db, const String & snapshot_name)
    {
        SnapshotPtr res;
        runWithMetricSupport(
            [&] {
                if (db == UUIDHelpers::Nil)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid UUID for DB");
                res = meta_proxy->tryGetSnapshot(name_space, UUIDHelpers::UUIDToString(db), snapshot_name);
            },
            ProfileEvents::TryGetSnapshotSuccess,
            ProfileEvents::TryGetSnapshotFailed);
        return res;
    }

    Snapshots Catalog::getAllSnapshots(const UUID & db, UUID * table_filter)
    {
        Snapshots res;
        runWithMetricSupport(
            [&] {
                if (db == UUIDHelpers::Nil)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid UUID for DB");

                res = meta_proxy->getAllSnapshots(name_space, UUIDHelpers::UUIDToString(db));
                if (table_filter)
                {
                    std::erase_if(res, [&](SnapshotPtr & item) {
                        return item->has_table_uuid() && RPCHelpers::createUUID(item->table_uuid()) != *table_filter;
                    });
                }
            },
            ProfileEvents::GetAllSnapshotsSuccess,
            ProfileEvents::GetAllSnapshotsFailed);
        return res;
    }

    void Catalog::createTable(
        const UUID & db_uuid,
        const StorageID & storage_id,
        const String & create_query,
        const String & virtual_warehouse,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                String uuid_str = UUIDHelpers::UUIDToString(storage_id.uuid);
                LOG_INFO(log, "start createTable namespace {} table_name {}, uuid {}", name_space, storage_id.getFullTableName(), uuid_str);
                Protos::DataModelTable tb_data;
                tb_data.set_database(storage_id.getDatabaseName());
                tb_data.set_name(storage_id.getTableName());
                tb_data.set_definition(create_query);
                tb_data.set_txnid(txnID.toUInt64());
                tb_data.set_commit_time(ts.toUInt64());
                RPCHelpers::fillUUID(storage_id.uuid, *(tb_data.mutable_uuid()));
                HostWithPorts host_port = context.getCnchTopologyMaster()->getTargetServer(uuid_str, storage_id.server_vw_name, true);
                if (storage_id.server_vw_name != DEFAULT_SERVER_VW_NAME)
                {
                    if (host_port.empty())
                        throw Exception("Cannot create table because no server in vw: " + storage_id.server_vw_name, ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
                    tb_data.set_server_vw_name(storage_id.server_vw_name);
                }

                if (!virtual_warehouse.empty())
                    tb_data.set_vw_name(virtual_warehouse);

                ASTPtr ast = parseCreateQuery(create_query);
                Strings dependencies = tryGetDependency(ast);
                ///FIXME: if masking policy is ready.
                // Strings masking_policy_names = getMaskingPolicyNames(ast);
                Strings masking_policy_names = {};
                meta_proxy->createTable(name_space, db_uuid, tb_data, dependencies, masking_policy_names);
                if (auto query_context = CurrentThread::getGroup()->query_context.lock())
                {
                    meta_proxy->setTableClusterStatus(name_space, uuid_str, true, createTableFromDataModel(*query_context, tb_data)->getTableHashForClusterBy().getDeterminHash());
                }
                else
                {
                    meta_proxy->setTableClusterStatus(name_space, uuid_str, true, createTableFromDataModel(context, tb_data)->getTableHashForClusterBy().getDeterminHash());
                }

                LOG_INFO(log, "finish createTable namespace {} table_name {}, uuid {}", name_space, storage_id.getFullTableName(), uuid_str);
                notifyTableCreated(storage_id.uuid, host_port);
                LOG_DEBUG(log, "finish create table meta for {}", uuid_str);
            },
            ProfileEvents::CreateTableSuccess,
            ProfileEvents::CreateTableFailed);
    }

    void Catalog::notifyTableCreated(const UUID & uuid, const HostWithPorts & host_port) noexcept
    {
        try
        {
            if (!host_port.empty() && !isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())))
            {
                context.getCnchServerClientPool().get(host_port)->notifyTableCreated(
                    uuid, context.getSettings().cnch_notify_table_created_rpc_timeout_ms);
                return;
            }

            StoragePtr storage;
            if (auto query_context = CurrentThread::getGroup()->query_context.lock())
                storage = tryGetTableByUUID(*query_context, UUIDHelpers::UUIDToString(uuid), TxnTimestamp::maxTS());
            else
                storage = tryGetTableByUUID(context, UUIDHelpers::UUIDToString(uuid), TxnTimestamp::maxTS());

            if (auto pcm = context.getPartCacheManager(); pcm && storage)
            {
                pcm->mayUpdateTableMeta(*storage, host_port.topology_version, true);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }

    void Catalog::createUDF(const String & prefix_name, const String & name, const String & create_query)
    {
        runWithMetricSupport(
            [&] {
                Protos::DataModelUDF udf_data;

                LOG_DEBUG(log, "start createUDF {}: {}.{}", name_space, prefix_name, name);
                //:ToDo add version and other info separately.
                udf_data.set_prefix_name(prefix_name);
                udf_data.set_function_name(name);
                udf_data.set_function_definition(create_query);
                meta_proxy->createUDF(name_space, udf_data);
            },
            ProfileEvents::CreateUDFSuccess,
            ProfileEvents::CreateUDFFailed);
    }

    void Catalog::dropUDF(const String & resolved_name)
    {
        runWithMetricSupport(
            [&] { meta_proxy->dropUDF(name_space, resolved_name); }, ProfileEvents::DropUDFSuccess, ProfileEvents::DropUDFFailed);
    }

    void Catalog::dropTable(
        const UUID & db_uuid,
        const StoragePtr & storage,
        const TxnTimestamp & /*previous_version*/,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                String table_uuid = UUIDHelpers::UUIDToString(storage->getStorageID().uuid);
                if (table_uuid.empty())
                    throw Exception("Table not found.", ErrorCodes::UNKNOWN_TABLE);

                /// get latest table version.
                auto table = tryGetTableFromMetastore(table_uuid, UINT64_MAX);

                if (table)
                {
                    const auto & db = table->database();
                    const auto & name = table->name();
                    BatchCommitRequest batch_writes;

                    Protos::TableIdentifier identifier;
                    identifier.set_database(db);
                    identifier.set_name(name);
                    identifier.set_uuid(table_uuid);
                    if (db_uuid != UUIDHelpers::Nil)
                        RPCHelpers::fillUUID(db_uuid, *identifier.mutable_db_uuid());

                    checkCanbeDropped(identifier, false);
                    moveTableIntoTrash(*table, identifier, txnID, ts, batch_writes);
                    ///FIXME: if masking policy is ready
                    // for (const auto & mask : storage->getColumns().getAllMaskingPolicy())
                    //     batch_writes.AddDelete(MetastoreProxy::maskingPolicyTableMappingKey(name_space, mask, table_uuid));

                    BatchCommitResponse resp;
                    meta_proxy->batchWrite(batch_writes, resp);

                    if (auto cache_manager = context.getPartCacheManager(); cache_manager)
                    {
                        cache_manager->invalidPartAndDeleteBitmapCache(storage->getStorageUUID());
                        cache_manager->removeStorageCache(db, name);
                    }
                }
            },
            ProfileEvents::DropTableSuccess,
            ProfileEvents::DropTableFailed);
    }

    void Catalog::detachTable(const String & db, const String & name, const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                detachOrAttachTable(db, name, ts, true);
            },
            ProfileEvents::DetachTableSuccess,
            ProfileEvents::DetachTableFailed);
    }

    void Catalog::attachTable(const String & db, const String & name, const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] { detachOrAttachTable(db, name, ts, false); }, ProfileEvents::AttachTableSuccess, ProfileEvents::AttachTableFailed);
    }

    bool Catalog::isTableExists(const String & db, const String & name, const TxnTimestamp & ts)
    {
        bool res = false;
        std::shared_ptr<Protos::DataModelTable> table;
        runWithMetricSupport(
            [&] {
                String table_uuid = meta_proxy->getTableUUID(name_space, db, name);

                if (!table_uuid.empty() && (table = tryGetTableFromMetastore(table_uuid, ts.toUInt64())) && !Status::isDetached(table->status()) && !Status::isDeleted(table->status()))
                    res = true;
            },
            ProfileEvents::IsTableExistsSuccess,
            ProfileEvents::IsTableExistsFailed);
        return res;
    }

    void Catalog::alterTable(
        const Context & query_context,
        const Settings & query_settings,
        const StoragePtr & storage,
        const String & new_create,
        const TxnTimestamp & previous_version,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts,
        const bool is_modify_cluster_by)
    {
        runWithMetricSupport(
            [&] {
                String table_uuid = meta_proxy->getTableUUID(name_space, storage->getDatabaseName(), storage->getTableName());

                if (table_uuid.empty())
                    throw Exception("Table not found.", ErrorCodes::UNKNOWN_TABLE);

                /// get latest version of the table.
                auto table = tryGetTableFromMetastore(table_uuid, UINT64_MAX);

                if (!table)
                    throw Exception(
                        "Cannot get metadata of table " + storage->getDatabaseName() + "." + storage->getTableName()
                            + " by UUID : " + table_uuid,
                        ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

                if (table->commit_time() >= ts.toUInt64())
                    throw Exception("Cannot alter table with an earlier timestamp", ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

                HostWithPorts host_port;
                bool is_local_server = false;
                if (!query_settings.force_execute_alter)
                {
                    host_port = context.getCnchTopologyMaster()
                                        ->getTargetServer(table_uuid, storage->getServerVwName(), false);
                    is_local_server = isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort()));
                    if (!is_local_server)
                        throw Exception(
                            "Cannot alter table because of choosing wrong server according to current topology, chosen server: " + host_port.toDebugString(),
                            ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
                }

                table->set_definition(new_create);
                table->set_txnid(txnID.toUInt64());
                table->set_commit_time(ts.toUInt64());
                table->set_previous_version(previous_version.toUInt64());

                StoragePtr new_table = CatalogFactory::getTableByDefinition(const_cast<Context&>(query_context).shared_from_this(), storage->getDatabaseName(), storage->getTableName(), new_create);
                bool server_vw_changed = false;
                if (new_table->getServerVwName() != getServerVwNameFrom(*table))
                {
                    if (!query_settings.force_execute_alter && context.getCnchTopologyMaster()->getTargetServer(table_uuid, new_table->getServerVwName(), true).empty())
                        throw Exception("Cannot alter table because no server in vw: " + new_table->getServerVwName(), ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);

                    if (new_table->getServerVwName() == DEFAULT_SERVER_VW_NAME)
                        table->clear_server_vw_name();
                    else
                        table->set_server_vw_name(new_table->getServerVwName());
                    server_vw_changed = true;
                }

                // auto res = meta_proxy->alterTable(
                //     name_space, *table, storage->getOutDatedMaskingPolicy(), storage->getColumns().getAllMaskingPolicy());
                ///FIXME: if masking policy is ready @guanzhe.andy
                auto res = meta_proxy->alterTable(
                    name_space, *table, {}, {});
                if (!res)
                    throw Exception("Alter table failed.", ErrorCodes::CATALOG_ALTER_TABLE_FAILURE);

                // Set cluster status after Alter table is successful to update PartCacheManager with new table metadata
                if (is_modify_cluster_by)
                    setTableClusterStatus(storage->getStorageUUID(), false, new_table->getTableHashForClusterBy());

                if (auto cache_manager = context.getPartCacheManager(); cache_manager)
                {
                    if (server_vw_changed)
                    {
                        /// Invalidate part cache since this server is no longer table's host server
                        cache_manager->invalidPartAndDeleteBitmapCache(storage->getStorageUUID());
                        cache_manager->removeStorageCache(storage->getDatabaseName(), storage->getTableName());
                    }
                    else if (is_local_server)
                    {
                        // update cache with nullptr and latest table commit_time to prevent an old version be inserted into cache.
                        // the cache will be reloaded in following getTable
                        cache_manager->insertStorageCache(storage->getStorageID(), nullptr, table->commit_time(), host_port.topology_version);
                    }
                }
            },
            ProfileEvents::AlterTableSuccess,
            ProfileEvents::AlterTableFailed);
    }

    void Catalog::checkCanbeDropped(Protos::TableIdentifier & table_id, bool is_dropping_db)
    {
        // Get uuids of all tables which rely on current table.
        auto tables_rely_on = meta_proxy->getAllDependence(name_space, table_id.uuid());

        if (!tables_rely_on.empty())
        {
            Strings table_names;
            for (auto & uuid : tables_rely_on)
            {
                StoragePtr storage = tryGetTableByUUID(context, uuid, TxnTimestamp::maxTS(), false);
                if (storage)
                {
                    if (!is_dropping_db || storage->getDatabaseName() != table_id.database())
                    {
                        table_names.emplace_back(storage->getDatabaseName() + "." + storage->getTableName());
                    }
                }
            }
            if (!table_names.empty())
            {
                auto joiner = [](const Strings & vec) -> String {
                    String res;
                    for (auto & e : vec)
                        res += "'" + e + "'" + ", ";
                    return res;
                };
                throw Exception(
                    "Cannot drop table " + table_id.database() + "." + table_id.name() + " because table [" + joiner(table_names)
                        + "] may rely on it. Please drop these tables firstly and try again",
                    ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);
            }
        }
    }

    void Catalog::renameTable(
        const Settings & query_settings,
        const String & from_database,
        const String & from_table,
        const String & to_database,
        const String & to_table,
        const UUID & to_db_uuid,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                auto table_id = meta_proxy->getTableID(name_space, from_database, from_table);

                if (!table_id)
                    throw Exception("Table not found.", ErrorCodes::UNKNOWN_TABLE);

                String table_uuid = table_id->uuid();
                /// get latest version of the table
                auto table = tryGetTableFromMetastore(table_uuid, UINT64_MAX);

                if (!table)
                    throw Exception(
                        "Cannot get metadata of table " + from_database + "." + from_table + " by UUID : " + table_uuid,
                        ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

                if (table->commit_time() >= ts.toUInt64())
                    throw Exception("Cannot rename table with an earlier timestamp", ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

                HostWithPorts host_port;
                bool is_local_server = false;
                if (!query_settings.force_execute_alter)
                {
                    host_port = context.getCnchTopologyMaster()
                                        ->getTargetServer(table_uuid, table_id->has_server_vw_name() ? table_id->server_vw_name() : DEFAULT_SERVER_VW_NAME, false);
                    is_local_server = isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort()));
                    if (!is_local_server)
                        throw Exception(
                            "Cannot rename table because of choosing wrong server according to current topology, targert server: " + host_port.toDebugString(),
                            ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
                }

                BatchCommitRequest batch_writes;
                replace_definition(*table, to_database, to_table);
                table->set_txnid(txnID.toUInt64());
                table->set_commit_time(ts.toUInt64());
                meta_proxy->prepareRenameTable(name_space, table_uuid, from_database, from_table, to_db_uuid, *table, batch_writes);
                BatchCommitResponse resp;
                meta_proxy->batchWrite(batch_writes, resp);

                /// update table name in table meta entry so that we can get table part metrics correctly.
                if (auto cache_manager = context.getPartCacheManager(); cache_manager && is_local_server)
                {
                    cache_manager->insertStorageCache(StorageID{from_database, from_table, UUIDHelpers::toUUID(table_uuid)}, nullptr, ts, host_port.topology_version);
                    cache_manager->updateTableNameInMetaEntry(table_uuid, to_database, to_table);
                }

            },
            ProfileEvents::RenameTableSuccess,
            ProfileEvents::RenameTableFailed);
    }

    void Catalog::setWorkerGroupForTable(const String & db, const String & name, const String & worker_group, UInt64 worker_topology_hash)
    {
        runWithMetricSupport(
            [&] {
                String table_uuid = meta_proxy->getTableUUID(name_space, db, name);
                if (table_uuid.empty())
                    throw Exception("Table not found.", ErrorCodes::UNKNOWN_TABLE);

                auto table = tryGetTableFromMetastore(table_uuid, UINT64_MAX);

                if (table)
                {
                    if (worker_group.empty())
                        table->clear_vw_name();
                    else
                        table->set_vw_name(worker_group);

                    if (worker_topology_hash == 0)
                        table->clear_worker_topology_hash();
                    else
                        table->set_worker_topology_hash(worker_topology_hash);

                    meta_proxy->updateTable(name_space, table_uuid, table->SerializeAsString(), table->commit_time());
                }
                else
                {
                    throw Exception(
                        "Cannot get metadata of table " + db + "." + name + " by UUID : " + table_uuid,
                        ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);
                }
            },
            ProfileEvents::SetWorkerGroupForTableSuccess,
            ProfileEvents::SetWorkerGroupForTableFailed);
    }

    void Catalog::initStorageObjectSchema(StoragePtr & res)
    {
        // Load dynamic object column schema
        if (res && res->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
        {
            auto cnch_table = std::dynamic_pointer_cast<StorageCnchMergeTree>(res);

            if (cnch_table)
            {
                auto assembled_schema = tryGetTableObjectAssembledSchema(res->getStorageUUID());
                auto partial_schemas = tryGetTableObjectPartialSchemas(res->getStorageUUID());
                cnch_table->resetObjectSchemas(assembled_schema, partial_schemas);
            }
        }
    }

    StoragePtr Catalog::getTable(const Context & query_context, const String & database, const String & name, const TxnTimestamp & ts)
    {
        StoragePtr res = nullptr;
        runWithMetricSupport(
            [&] {
                auto table_id = meta_proxy->getTableID(name_space, database, name);

                if (!table_id)
                {
                    throw Exception("Table not found: " + database + "." + name, ErrorCodes::UNKNOWN_TABLE);
                }

                auto cache_manager = context.getPartCacheManager();
                bool is_host_server = false;
                const auto host_server = context.getCnchTopologyMaster()->getTargetServer(table_id->uuid(), getServerVwNameFrom(*table_id), true);

                if (!host_server.empty())
                    is_host_server = isLocalServer(host_server.getRPCAddress(), std::to_string(context.getRPCPort()));

                if (is_host_server && cache_manager && !query_context.hasSessionTimeZone())
                {
                    auto cached_storage = cache_manager->getStorageFromCache(UUIDHelpers::toUUID(table_id->uuid()), host_server.topology_version);
                    if (cached_storage && cached_storage->commit_time <= ts && cached_storage->getStorageID().database_name == database && cached_storage->getStorageID().table_name == name)
                    {
                        res = cached_storage;
                        //TODO:(@lianwenlong) force fetch global object schema from catalog
                        initStorageObjectSchema(res);
                        return;
                    }
                }

                auto table = tryGetTableFromMetastore(table_id->uuid(), ts.toUInt64(), true);

                if (!table)
                {
                    throw Exception(
                        "Cannot get metadata of table " + database + "." + name + " by UUID : " + table_id->uuid(),
                        ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);
                }

                res = createTableFromDataModel(query_context, *table);

                initStorageObjectSchema(res);
                /// TODO: (zuochuang.zema, guanzhe.andy) handle TimeTravel
                if (auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(res.get()))
                {
                    cnch_merge_tree->loadMutations();
                }

                /// Try insert the storage into cache.
                if (res && is_host_server && cache_manager)
                    cache_manager->insertStorageCache(res->getStorageID(), res, table->commit_time(), host_server.topology_version);
            },
            ProfileEvents::GetTableSuccess,
            ProfileEvents::GetTableFailed);
        return res;
    }

    StoragePtr Catalog::tryGetTable(const Context & query_context, const String & database, const String & name, const TxnTimestamp & ts)
    {
        StoragePtr res = nullptr;
        runWithMetricSupport(
            [&] {
                try
                {
                    res = getTable(query_context, database, name, ts);
                }
                catch (Exception & e)
                {
                    /// do not need to log exception if table not found.
                    if (e.code() != ErrorCodes::UNKNOWN_TABLE)
                    {
                        tryLogDebugCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            },
            ProfileEvents::TryGetTableSuccess,
            ProfileEvents::TryGetTableFailed);
        return res;
    }

    StoragePtr Catalog::tryGetTableByUUID(const Context & query_context, const String & uuid, const TxnTimestamp & ts, bool with_delete)
    {
        StoragePtr res = nullptr;
        runWithMetricSupport(
            [&] {
                auto cache_manager = context.getPartCacheManager();
                auto [current_topology_version, current_topology] = context.getCnchTopologyMaster()->getCurrentTopologyVersion();

                if (cache_manager)
                {
                    if (current_topology_version != PairInt64(0, 0))
                    {
                        auto cached_storage = cache_manager->getStorageFromCache(UUIDHelpers::toUUID(uuid), current_topology_version);
                        if (cached_storage && cached_storage->commit_time <= ts)
                        {
                            auto host_server = current_topology.getTargetServer(uuid, cached_storage->getServerVwName());
                            if (isLocalServer(host_server.getRPCAddress(), std::to_string(context.getRPCPort())))
                            {
                                res = cached_storage;
                                return;
                            }
                        }
                    }
                }

                auto table = tryGetTableFromMetastore(uuid, ts.toUInt64(), true, with_delete);
                if (!table)
                    return;
                res = createTableFromDataModel(query_context, *table);

                initStorageObjectSchema(res);

                /// TODO: (zuochuang.zema, guanzhe.andy) handle TimeTravel
                if (auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(res.get()))
                {
                    cnch_merge_tree->loadMutations();
                }

                /// Try insert the storage into cache.
                if (res && cache_manager)
                {
                    auto host_server = current_topology.getTargetServer(uuid, res->getServerVwName());
                    if (!host_server.empty() && isLocalServer(host_server.getRPCAddress(), std::to_string(context.getRPCPort())))
                        cache_manager->insertStorageCache(res->getStorageID(), res, table->commit_time(), current_topology_version);
                }
            },
            ProfileEvents::TryGetTableByUUIDSuccess,
            ProfileEvents::TryGetTableByUUIDFailed);
        return res;
    }

    StoragePtr Catalog::getTableByUUID(const Context & query_context, const String & uuid, const TxnTimestamp & ts, bool with_delete)
    {
        StoragePtr res = nullptr;
        runWithMetricSupport(
            [&] {
                auto table = tryGetTableByUUID(query_context, uuid, ts, with_delete);
                if (!table)
                    throw Exception("Cannot get table by UUID : " + uuid, ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);
                res = table;
            },
            ProfileEvents::GetTableByUUIDSuccess,
            ProfileEvents::GetTableByUUIDFailed);
        return res;
    }

    Strings Catalog::getTablesInDB(const String & database)
    {
        Strings res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllTablesInDB(name_space, database); },
            ProfileEvents::GetTablesInDBSuccess,
            ProfileEvents::GetTablesInDBFailed);
        return res;
    }

    std::vector<StoragePtr> Catalog::getAllViewsOn(const Context & session_context, const StoragePtr & storage, const TxnTimestamp & ts)
    {
        std::vector<StoragePtr> res;
        runWithMetricSupport(
            [&] {
                Strings dependencies = meta_proxy->getAllDependence(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid));
                for (auto & dependence : dependencies)
                {
                    auto table = tryGetTableFromMetastore(dependence, ts, true);
                    if (table)
                    {
                        StoragePtr view = createTableFromDataModel(session_context, *table);
                        res.push_back(view);
                    }
                }
            },
            ProfileEvents::GetAllViewsOnSuccess,
            ProfileEvents::GetAllViewsOnFailed);
        return res;
    }

    void Catalog::setTableActiveness(const StoragePtr & storage, const bool is_active, const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                /// get latest table version.
                String uuid = UUIDHelpers::UUIDToString(storage->getStorageID().uuid);
                auto table = tryGetTableFromMetastore(uuid, ts.toUInt64());

                if (!table)
                    throw Exception("Cannot get table by UUID : " + uuid, ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

                LOG_DEBUG(log, "Modify table activeness to {} ", (is_active ? "active" : "inactive"));
                table->set_status(Status::setInActive(table->status(), is_active));
                /// directly rewrite the old table metadata rather than adding a new version
                meta_proxy->updateTable(name_space, uuid, table->SerializeAsString(), table->commit_time());
            },
            ProfileEvents::SetTableActivenessSuccess,
            ProfileEvents::SetTableActivenessFailed);
    }

    bool Catalog::getTableActiveness(const StoragePtr & storage, const TxnTimestamp & ts)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                String uuid = UUIDHelpers::UUIDToString(storage->getStorageID().uuid);
                /// get latest table version.
                auto table = tryGetTableFromMetastore(uuid, ts.toUInt64());
                if (table)
                {
                    res = !Status::isInActive(table->status());
                }
                else
                {
                    throw Exception("Cannot get table metadata by UUID : " + uuid, ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);
                }
            },
            ProfileEvents::GetTableActivenessSuccess,
            ProfileEvents::GetTableActivenessFailed);
        return res;
    }

    DataPartsVector Catalog::getStagedParts(const ConstStoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions)
    {
        const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(table.get());
        if (!storage)
            throw Exception("Table is not a merge tree", ErrorCodes::BAD_ARGUMENTS);

        auto staged_server_parts = getStagedServerDataParts(table, ts, partitions);
        return CnchPartsHelper::toMergeTreeDataPartsCNCHVector(createPartVectorFromServerParts(*storage, staged_server_parts));
    }

    ServerDataPartsWithDBM Catalog::getStagedServerDataPartsWithDBM(const ConstStoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions)
    {
        ServerDataPartsWithDBM res;
        res.first = getStagedServerDataParts(table, ts, partitions, VisibilityLevel::All);

        if (res.first.empty())
            return res;

        bool is_unique_table = table->getInMemoryMetadataPtr()->hasUniqueKey();
        if (is_unique_table)
        {
            std::set<Int64> bucket_numbers;
            if (table->isBucketTable())
            {
                for (const auto & part : res.first)
                    bucket_numbers.insert(part->part_model().bucket_number());
            }

            res.second = getDeleteBitmapsInPartitions(
                table,
                {partitions->begin(), partitions->end()},
                ts,
                /*session_context=*/nullptr,
                /*visibility=*/VisibilityLevel::All,
                bucket_numbers);
        }

        if (ts)
        {
            LOG_DEBUG(
                log,
                "{} Start handle intermediate parts and delete bitmap metas. Total number of parts is {}, total number of delete bitmap "
                "metas is {}, timestamp: {}",
                table->getStorageID().getNameForLogs(),
                res.first.size(),
                res.second.size(),
                ts.toString());

            auto * txn_record_cache =
                context.getServerType() == ServerType::cnch_server ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache() : nullptr;
            /// Make sure they use the same records of transactions list.
            auto txn_records = getTransactionRecords(res.first, res.second);
            getVisibleServerDataParts(res.first, ts, this, &txn_records, txn_record_cache);
            if (is_unique_table)
                getVisibleBitmaps(res.second, ts, this, &txn_records, txn_record_cache);

            LOG_DEBUG(
                log,
                "{} Finish handle intermediate parts and delete bitmap metas. Total number of parts is {}, total number of delete bitmap "
                "metas of {}, timestamp: {}",
                table->getStorageID().getNameForLogs(),
                res.first.size(),
                res.second.size(),
                ts.toString());
        }

        return res;
    }

    ServerDataPartsVector Catalog::getStagedServerDataParts(const ConstStoragePtr & table, const TxnTimestamp & ts, const NameSet * partitions, VisibilityLevel visibility)
    {
        ServerDataPartsVector res;
        runWithMetricSupport(
            [&] {
                Stopwatch watch;
                const auto * storage = dynamic_cast<const MergeTreeMetaBase *>(table.get());
                if (!storage)
                    throw Exception("Table is not a merge tree", ErrorCodes::BAD_ARGUMENTS);
                String table_uuid = UUIDHelpers::UUIDToString(table->getStorageUUID());

                IMetaStore::IteratorPtr m_it;
                bool need_partitions_check = false;
                if (partitions && partitions->size() == 1)
                {
                    m_it = meta_proxy->getStagedPartsInPartition(name_space, table_uuid, *(partitions->begin()));
                }
                else
                {
                    /// TODO: find out more efficient way to scan staged parts only in the requested partitions.
                    /// currently seems not a bottleneck because the total number of staged parts won't be very large
                    m_it = meta_proxy->getStagedParts(name_space, table_uuid);
                    need_partitions_check = (partitions != nullptr);
                }

                while (m_it->next())
                {
                    /// if ts is set, exclude model whose txn id > ts
                    if (ts.toUInt64())
                    {
                        const auto & key = m_it->key();
                        auto pos = key.find_last_of('_');
                        if (pos != String::npos)
                        {
                            UInt64 txn_id = std::stoull(key.substr(pos + 1, String::npos), nullptr);
                            if (txn_id > ts)
                                continue;
                        }
                    }

                    DataModelPartPtr model = std::make_shared<Protos::DataModelPart>();
                    model->ParseFromString(m_it->value());
                    /// exclude model whose commit ts > ts
                    if (ts.toUInt64() && model->has_commit_time() && model->commit_time() > ts.toUInt64())
                        continue;
                    /// exclude model not belong to the given partitions
                    if (need_partitions_check && !partitions->count(model->part_info().partition_id()))
                        continue;

                    res.push_back(std::make_shared<ServerDataPart>(createPartWrapperFromModel(*storage, std::move(*model))));
                }

                size_t size_before = res.size();
                bool execute_filter = false;

                if (ts && visibility != VisibilityLevel::All)
                {
                    execute_filter = true;
                    LOG_TRACE(
                        log,
                        "{} Start handle intermediate staged data parts. Total number of staged parts is {}, timestamp: {}",
                        storage->getStorageID().getNameForLogs(),
                        res.size(),
                        ts.toString());

                    auto * txn_record_cache = context.getServerType() == ServerType::cnch_server
                        ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache()
                        : nullptr;
                    getVisibleServerDataParts(res, ts, this, nullptr, txn_record_cache);

                    LOG_TRACE(
                        log,
                        "{} Finish handle intermediate staged data parts. Total number of staged parts is {}, timestamp: {}",
                        storage->getStorageID().getNameForLogs(),
                        res.size(),
                        ts.toString());
                }

                LOG_DEBUG(
                    log,
                    "Elapsed {}ms to get {}/{} staged data parts for table : {}, ts : {}, execute_filter: {}"
                    ,watch.elapsedMilliseconds()
                    ,res.size()
                    ,size_before
                    ,storage->getStorageID().getNameForLogs()
                    ,ts.toString()
                    ,execute_filter);
            },
            ProfileEvents::GetStagedPartsSuccess,
            ProfileEvents::GetStagedPartsFailed);
        return res;
    }

    DB::ServerDataPartsWithDBM Catalog::getServerDataPartsInPartitionsWithDBM(
        const ConstStoragePtr & storage,
        const Strings & partitions,
        const TxnTimestamp & ts,
        const Context * session_context,
        const VisibilityLevel visibility,
        const std::set<Int64> & bucket_numbers)
    {
        ServerDataPartsWithDBM res;
        res.first = getServerDataPartsInPartitions(storage, partitions, ts, session_context, VisibilityLevel::All, bucket_numbers);

        if (res.first.empty())
            return res;

        bool is_unique_table = storage->getInMemoryMetadataPtr()->hasUniqueKey();
        if (is_unique_table)
            res.second = getDeleteBitmapsInPartitions(
                storage, {partitions.begin(), partitions.end()}, ts, /*session_context=*/nullptr, VisibilityLevel::All, bucket_numbers);

        /// Make sure they use the same records of transactions list.
        if (ts && visibility != VisibilityLevel::All)
        {
            LOG_DEBUG(
                log,
                "{} Start handle intermediate parts and delete bitmap metas. Total number of parts is {}, total number of delete bitmap "
                "metas is {}, timestamp: {}",
                storage->getStorageID().getNameForLogs(),
                res.first.size(),
                res.second.size(),
                ts.toString());

            auto * txn_record_cache =
                context.getServerType() == ServerType::cnch_server ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache() : nullptr;

            auto txn_records = getTransactionRecords(res.first, res.second);
            if (visibility == VisibilityLevel::Visible)
            {
                getVisibleServerDataParts(res.first, ts, this, &txn_records, txn_record_cache);
                if (is_unique_table)
                    getVisibleBitmaps(res.second, ts, this, &txn_records, txn_record_cache);
            }
            else
            {
                getCommittedServerDataParts(res.first, ts, this, &txn_records, txn_record_cache);
                if (is_unique_table)
                    getCommittedBitmaps(res.second, ts, this, &txn_records, txn_record_cache);
            }

            LOG_DEBUG(
                log,
                "{} Finish handle intermediate parts and delete bitmap metas. Total number of parts is {}, total number of delete bitmap "
                "metas of {}, timestamp: {}",
                storage->getStorageID().getNameForLogs(),
                res.first.size(),
                res.second.size(),
                ts.toString());
        }
        return res;
    }

    DB::ServerDataPartsVector Catalog::getServerDataPartsInPartitions(
        const ConstStoragePtr & storage,
        const Strings & partitions,
        const TxnTimestamp & ts,
        const Context * session_context,
        const VisibilityLevel visibility,
        const std::set<Int64> & bucket_numbers)
    {
        ServerDataPartsVector res;
        String source;
        runWithMetricSupport(
            [&] {
                Stopwatch watch;
                bool filter_bucket_numbers = false;
                auto fall_back = [&]() {
                    ServerDataPartsVector tmp_res;
                    const auto & merge_tree_storage = dynamic_cast<const MergeTreeMetaBase &>(*storage);
                    Strings all_partitions = getPartitionIDsFromMetastore(storage);
                    auto parts_model = getDataPartsMetaFromMetastore(storage, partitions, all_partitions, ts, /*from_trash=*/ false);
                    for (auto & ele : parts_model)
                    {
                        auto part_model_wrapper = createPartWrapperFromModel(merge_tree_storage, std::move(*(ele->model)), std::move(ele->name));
                        tmp_res.push_back(std::make_shared<ServerDataPart>(std::move(part_model_wrapper)));
                    }
                    return tmp_res;
                };

                if (!dynamic_cast<const MergeTreeMetaBase *>(storage.get()))
                {
                    return;
                }
                auto host_port
                    = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageID().uuid), storage->getServerVwName(), true);
                auto host_with_rpc = host_port.getRPCAddress();

                if (host_port.empty())
                {
                    /// if host not found, fall back to fetch part from metastore.
                    LOG_DEBUG(log, "Fall back to get from metastore because no available server found in current topology.");
                    res = fall_back();
                    source = "KV(target server not found)";
                }
                else if (
                    context.getServerType() == ServerType::cnch_server
                    && isLocalServer(host_with_rpc, std::to_string(context.getRPCPort())))
                {
                    bool can_use_cache = canUseCache(storage, session_context);

                    if (!can_use_cache)
                    {
                        res = fall_back();
                        source = "KV(can not laverage cache)";
                    }
                    else
                    {
                        source = "PartCache";
                        std::atomic_bool miss_cache{false};
                        res = context.getPartCacheManager()->getOrSetServerDataPartsInPartitions(
                            *storage,
                            partitions,
                            [&](const Strings & required_partitions, const Strings & full_partitions) -> DataModelPartWrapperVector {
                                miss_cache = true;
                                DataModelPartWithNameVector fetched = getDataPartsMetaFromMetastore(storage, required_partitions, full_partitions, TxnTimestamp{0}, /*from_trash=*/ false);
                                DataModelPartWrapperVector ret;
                                ret.reserve(fetched.size());

                                const auto & merge_tree = dynamic_cast<const MergeTreeMetaBase &>(*storage);
                                for (const auto & part : fetched)
                                {
                                    ret.push_back(createPartWrapperFromModel(merge_tree, std::move(*(part->model)), std::move(part->name)));
                                }
                                return ret;
                            },
                            ts.toUInt64(),
                            host_port.topology_version);
                        if (miss_cache)
                            source = "KV(miss cache)";
                    }
                }
                else
                {
                    try
                    {
                        res = context.getCnchServerClientPool().get(host_with_rpc)->fetchDataParts(host_with_rpc, storage, partitions, ts, bucket_numbers);
                        source = "TargetServer(" + host_with_rpc + ")";
                        filter_bucket_numbers = true;
                    }
                    catch (...)
                    {
                        LOG_DEBUG(log, "Fall back to get from metastore because fail to fetch part from remote server.");
                        res = fall_back();
                        source = "KV(cannot reach target server)";
                    }
                }
                size_t visibility_filtered = 0;
                if (ts && visibility != VisibilityLevel::All)
                {
                    visibility_filtered = res.size();
                    LOG_TRACE(
                        log,
                        "{} Start handle intermediate parts. Total number of parts is {}, timestamp: {}"
                        ,storage->getStorageID().getNameForLogs()
                        ,res.size()
                        ,ts.toString());

                    auto * txn_record_cache =
                        context.getServerType() == ServerType::cnch_server ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache() : nullptr;

                    if (visibility == VisibilityLevel::Visible)
                        getVisibleServerDataParts(res, ts, this, nullptr, txn_record_cache);
                    else
                        getCommittedServerDataParts(res, ts, this, nullptr, txn_record_cache);

                    LOG_TRACE(
                        log,
                        "{} Finish handle intermediate parts. Total number of parts is {}, timestamp: {}"
                        ,storage->getStorageID().getNameForLogs()
                        ,res.size()
                        ,ts.toString());
                    visibility_filtered -= res.size();
                }

                size_t bucket_filtered = 0;
                /// Filter parts by bucket_numbers if it is bucket table and cluster ready
                if (!res.empty() && !filter_bucket_numbers && !bucket_numbers.empty() && storage->isBucketTable() && isTableClustered(storage->getStorageUUID()))
                {
                    auto old_part_size = res.size();
                    std::erase_if(res, [&bucket_numbers](const ServerDataPartPtr & part) {
                        return part->part_model().bucket_number() >= 0 && bucket_numbers.count(part->part_model().bucket_number()) == 0;
                    });
                    LOG_TRACE(log, "{} filter parts by {} buckets from {} parts to {} parts."
                        ,storage->getStorageID().getNameForLogs()
                        ,bucket_numbers.size()
                        ,old_part_size
                        ,res.size());
                    bucket_filtered = old_part_size - res.size();
                }

                LOG_DEBUG(
                    log,
                    "Elapsed {}ms to get {} parts in {} partitions for table : {} , source : {}, ts : {}, visibility filtered {} parts, bucket filtered {} parts."
                    ,watch.elapsedMilliseconds()
                    ,res.size()
                    ,partitions.size()
                    ,storage->getStorageID().getNameForLogs()
                    ,source
                    ,ts.toString()
                    ,visibility_filtered
                    ,bucket_filtered);
            },
            ProfileEvents::GetServerDataPartsInPartitionsSuccess,
            ProfileEvents::GetServerDataPartsInPartitionsFailed);
        if (source.starts_with("KV"))
            std::sort(res.begin(), res.end(), CnchPartsHelper::PartComparator<ServerDataPartPtr>{});
        return res;
    }

    DeleteBitmapMetaPtrVector Catalog::getDeleteBitmapsInPartitions(
        const ConstStoragePtr & storage,
        const Strings & partitions,
        const TxnTimestamp & ts,
        const Context * session_context,
        const VisibilityLevel visibility,
        const std::set<Int64> & bucket_numbers)
    {
        DeleteBitmapMetaPtrVector res;
        String source;
        runWithMetricSupport(
            [&] {
                Stopwatch watch;
                bool filtered_with_bucket_numbers = false;
                auto fall_back = [&]() {
                    DeleteBitmapMetaPtrVector res;
                    const auto & merge_tree_storage = dynamic_cast<const MergeTreeMetaBase &>(*storage);
                    Strings all_partitions = getPartitionIDsFromMetastore(storage);
                    DataModelDeleteBitmapPtrVector bitmaps = getDeleteBitmapsInPartitionsImpl(storage, partitions, all_partitions, ts);
                    for (auto & ele : bitmaps)
                    {
                        res.emplace_back(std::make_shared<DeleteBitmapMeta>(merge_tree_storage, ele));
                    }
                    return res;
                };

                if (!dynamic_cast<const MergeTreeMetaBase *>(storage.get()))
                {
                    return;
                }
                if (partitions.empty())
                {
                    return;
                }
                auto host_port
                    = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageID().uuid), storage->getServerVwName(), true);
                auto host_with_rpc = host_port.getRPCAddress();

                if (host_port.empty())
                {
                    /// if host not found, fall back to fetch part from metastore.
                    LOG_DEBUG(log, "Fall back to get from metastore because no available server found in current topology.");
                    res = fall_back();
                    source = "KV(target server not found)";
                }
                else if (
                    context.getServerType() == ServerType::cnch_server
                    && isLocalServer(host_with_rpc, std::to_string(context.getRPCPort())))
                {
                    bool can_use_cache = canUseCache(storage, session_context);
                    can_use_cache &= !context.getConfigRef().getBool("disable_delete_bitmap_cache", false);

                    if (!can_use_cache)
                    {
                        res = fall_back();
                        source = "KV(can not laverage cache)";
                    }
                    else
                    {
                        source = "DeleteBitmapCache";
                        std::atomic_bool miss_cache{false};
                        res = context.getPartCacheManager()->getOrSetDeleteBitmapInPartitions(
                            *storage,
                            partitions,
                            [&](const Strings & required_partitions, const Strings & full_partitions) -> DataModelDeleteBitmapPtrVector {
                                miss_cache = true;
                                return getDeleteBitmapsInPartitionsImpl(storage, required_partitions, full_partitions, TxnTimestamp::maxTS());
                            },
                            ts.toUInt64(),
                            host_port.topology_version);
                        if (miss_cache)
                            source = "KV(miss cache)";
                    }
                }
                else
                {
                    try
                    {
                        res = context.getCnchServerClientPool()
                                  .get(host_with_rpc)
                                  ->fetchDeleteBitmaps(host_with_rpc, storage, partitions, ts, bucket_numbers);
                        source = "TargetServer(" + host_with_rpc + ")";
                        filtered_with_bucket_numbers = true;
                    }
                    catch (...)
                    {
                        LOG_DEBUG(log, "Fall back to get from metastore because fail to fetch delete bitmaps from remote server.");
                        res = fall_back();
                        source = "KV(cannot reach target server)";
                    }
                }

                size_t size_before = res.size();

                /// filter out invisible bitmaps (uncommitted or invisible to current txn)
                size_t visibility_filtered = 0;
                bool filter_executed = false;
                if (visibility != VisibilityLevel::All)
                {
                    visibility_filtered = res.size();
                    filter_executed = true;
                    auto * txn_record_cache = context.getServerType() == ServerType::cnch_server
                        ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache()
                        : nullptr;
                    getVisibleBitmaps(res, ts, this, nullptr, txn_record_cache);
                    visibility_filtered -= res.size();
                }

                size_t bucket_filtered = 0;
                if (!res.empty() && !filtered_with_bucket_numbers && !bucket_numbers.empty() && storage->isBucketTable()
                    && isTableClustered(storage->getStorageUUID()))
                {
                    auto old_delete_bitmap_size = res.size();
                    std::erase_if(res, [&bucket_numbers](const DeleteBitmapMetaPtr & bitmap) {
                        return bitmap->getModel()->has_bucket_number() && bitmap->getModel()->bucket_number() >= 0
                            && bucket_numbers.count(bitmap->getModel()->bucket_number()) == 0;
                    });
                    LOG_TRACE(
                        log,
                        "{} filter delete bitmaps by {} buckets from {} bitmaps to {} bitmaps.",
                        storage->getStorageID().getNameForLogs(),
                        bucket_numbers.size(),
                        old_delete_bitmap_size,
                        res.size());
                    bucket_filtered = old_delete_bitmap_size - res.size();
                }

                LOG_DEBUG(
                    log,
                    "Elapsed {}ms to get {}/{} delete bitmaps in {} partitions for table: {} , source: {}, ts: {}, visibility filtered "
                    "{} bitmaps (executed: {}), bucket filtered {} bitmaps.",
                    watch.elapsedMilliseconds(),
                    res.size(),
                    size_before,
                    partitions.size(),
                    storage->getStorageID().getNameForLogs(),
                    source,
                    ts.toString(),
                    visibility_filtered,
                    filter_executed,
                    bucket_filtered);
            },
            ProfileEvents::GetDeleteBitmapsFromCacheInPartitionsSuccess,
            ProfileEvents::GetDeleteBitmapsFromCacheInPartitionsFailed);
        return res;
    }

    ServerDataPartsWithDBM Catalog::getTrashedPartsInPartitionsWithDBM(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts)
    {
        ServerDataPartsWithDBM res;
        res.first = getTrashedPartsInPartitions(storage, partitions, ts, VisibilityLevel::All);

        if (res.first.empty())
            return res;

        bool is_unique_table = storage->getInMemoryMetadataPtr()->hasUniqueKey();
        if (is_unique_table)
            res.second = getTrashedDeleteBitmapsInPartitions(storage, partitions, ts, VisibilityLevel::All);

        if (ts)
        {
            LOG_DEBUG(
                log,
                "{} Start handle intermediate parts and delete bitmap metas. Total number of parts is {}, total number of delete bitmap "
                "metas is {}, timestamp: {}",
                storage->getStorageID().getNameForLogs(),
                res.first.size(),
                res.second.size(),
                ts.toString());

            auto * txn_record_cache =
                context.getServerType() == ServerType::cnch_server ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache() : nullptr;
            /// Make sure they use the same records of transactions list.
            auto txn_records = getTransactionRecords(res.first, res.second);
            getVisibleServerDataParts(res.first, ts, this, &txn_records, txn_record_cache);
            if (is_unique_table)
                getVisibleBitmaps(res.second, ts, this, &txn_records, txn_record_cache);

            LOG_DEBUG(
                log,
                "{} Finish handle intermediate parts and delete bitmap metas. Total number of parts is {}, total number of delete bitmap "
                "metas of {}, timestamp: {}",
                storage->getStorageID().getNameForLogs(),
                res.first.size(),
                res.second.size(),
                ts.toString());
        }
        return res;
    }

    ServerDataPartsVector Catalog::getTrashedPartsInPartitions(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, VisibilityLevel visibility)
    {
        ServerDataPartsVector res;
        runWithMetricSupport(
            [&] {
                const auto * merge_tree = dynamic_cast<const MergeTreeMetaBase *>(storage.get());
                if (!merge_tree)
                    return;

                Stopwatch watch;
                Strings all_partitions = getPartitionIDsFromMetastore(storage);
                auto parts_model = getDataPartsMetaFromMetastore(storage, partitions, all_partitions, ts, /*from_trash=*/ true);
                for (auto & ele : parts_model)
                {
                    auto part_model_wrapper = createPartWrapperFromModel(*merge_tree, std::move(*(ele->model)), std::move(ele->name));
                    res.push_back(std::make_shared<ServerDataPart>(std::move(part_model_wrapper)));
                }

                size_t size_before = res.size();

                // TODO: should we remove the visibility check logic for trashed parts?
                if (ts && visibility != VisibilityLevel::All)
                {
                    auto * txn_record_cache = context.getServerType() == ServerType::cnch_server
                        ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache()
                        : nullptr;
                    getVisibleServerDataParts(res, ts, this, nullptr, txn_record_cache);
                }

                LOG_DEBUG(
                    log,
                    "Get {}/{} visible parts in trash for table {} in {} ms, ts={}",
                    res.size(),
                    size_before,
                    storage->getStorageID().getNameForLogs(),
                    watch.elapsedMilliseconds(),
                    ts.toString());
            },
            ProfileEvents::GetServerDataPartsInPartitionsSuccess,
            ProfileEvents::GetServerDataPartsInPartitionsFailed);

        return res;
    }

    bool Catalog::hasTrashedPartsInPartition(const ConstStoragePtr & storage, const String & partition)
    {
        String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
        String trash_part_prefix = MetastoreProxy::trashItemsPrefix(name_space, uuid) + PART_STORE_PREFIX + partition;

        auto it = meta_proxy->getByPrefix(trash_part_prefix, 10);

        return it->next();
    }

    ServerDataPartsWithDBM Catalog::getAllServerDataPartsWithDBM(
        const ConstStoragePtr & storage, const TxnTimestamp & ts, const Context * session_context, const VisibilityLevel visibility)
    {
        ServerDataPartsWithDBM res;
        runWithMetricSupport(
            [&] {
                if (!dynamic_cast<const MergeTreeMetaBase *>(storage.get()))
                    return;

                res = getServerDataPartsInPartitionsWithDBM(storage, getPartitionIDs(storage, session_context), ts, session_context, visibility);
            },
            ProfileEvents::GetAllServerDataPartsWithDBMSuccess,
            ProfileEvents::GetAllServerDataPartsWithDBMFailed);
        return res;
    }

    ServerDataPartsVector Catalog::getAllServerDataParts(
        const ConstStoragePtr & storage, const TxnTimestamp & ts, const Context * session_context, const VisibilityLevel visibility)
    {
        ServerDataPartsVector res;
        runWithMetricSupport(
            [&] {
                if (!dynamic_cast<const MergeTreeMetaBase *>(storage.get()))
                    return;

                res = getServerDataPartsInPartitions(storage, getPartitionIDs(storage, session_context), ts, session_context, visibility);
            },
            ProfileEvents::GetAllServerDataPartsSuccess,
            ProfileEvents::GetAllServerDataPartsFailed);
        return res;
    }

    DataPartsVector Catalog::getDataPartsByNames(const NameSet & names, const StoragePtr & table, const TxnTimestamp & ts)
    {
        DataPartsVector res;
        runWithMetricSupport(
            [&] {
                auto * storage = dynamic_cast<MergeTreeMetaBase *>(table.get());
                if (!storage)
                {
                    return;
                }
                std::unordered_map<String, NameSet> partition_names;
                for (auto & name : names)
                {
                    String partition = MergeTreePartInfo::fromPartName(name, storage->format_version).partition_id;
                    if (auto it = partition_names.find(partition); it != partition_names.end())
                        it->second.insert(name);
                    else
                        partition_names[partition] = {name};
                }

                /// get parts per partition,
                /// because some dirty insert txn will include too many partitions' parts,
                /// which cause too much memory usage when get all related partitions.
                /// find the wanted parts partition by partition could help reduce memory usage
                for (const auto & [partition, p_names] : partition_names)
                {
                    auto parts_from_partitions = getServerDataPartsInPartitions(table, {partition}, ts, nullptr);
                    for (const auto & part : parts_from_partitions)
                    {
                        if (p_names.count(part->info().getPartNameWithHintMutation()))
                            res.push_back(part->toCNCHDataPart(*storage));
                    }
                }
            },
            ProfileEvents::GetDataPartsByNamesSuccess,
            ProfileEvents::GetDataPartsByNamesFailed);
        return res;
    }

    DataPartsVector
    Catalog::getStagedDataPartsByNames(const NameSet & names, const StoragePtr & table, const TxnTimestamp & ts)
    {
        DataPartsVector res;
        runWithMetricSupport(
            [&] {
                auto * storage = dynamic_cast<MergeTreeMetaBase *>(table.get());
                if (!storage)
                {
                    return;
                }
                std::unordered_map<String, NameSet> partition_names;
                for (auto & name : names)
                {
                    auto info = MergeTreePartInfo::fromPartName(name, storage->format_version);
                    if (auto it = partition_names.find(info.partition_id); it != partition_names.end())
                        it->second.insert(name);
                    else
                        partition_names[info.partition_id] = {name};
                }

                /// get parts per partition, same with getDataPartsByNames
                for (const auto & [partition, p_names] : partition_names)
                {
                    NameSet singlePartition{partition};
                    DataPartsVector parts = getStagedParts(table, ts, &singlePartition);
                    for (auto & part : parts)
                    {
                        auto name = part->info.getPartNameWithHintMutation();
                        if (p_names.count(name))
                            res.push_back(part);
                    }
                }
            },
            ProfileEvents::GetStagedDataPartsByNamesSuccess,
            ProfileEvents::GetStagedDataPartsByNamesFailed);
        return res;
    }

    DeleteBitmapMetaPtrVector Catalog::getAllDeleteBitmaps(const MergeTreeMetaBase & storage)
    {
        DeleteBitmapMetaPtrVector res;
        runWithMetricSupport(
            [&] {
                IMetaStore::IteratorPtr iter
                    = meta_proxy->getAllDeleteBitmaps(name_space, UUIDHelpers::UUIDToString(storage.getStorageID().uuid));

                while (iter->next())
                {
                    DataModelDeleteBitmapPtr delete_bitmap_model = std::make_shared<Protos::DataModelDeleteBitmap>();
                    delete_bitmap_model->ParseFromString(iter->value());
                    res.push_back(std::make_shared<DeleteBitmapMeta>(storage, delete_bitmap_model));
                }
            },
            ProfileEvents::GetAllDeleteBitmapsSuccess,
            ProfileEvents::GetAllDeleteBitmapsFailed);
        return res;
    }

    void Catalog::assertLocalServerThrowIfNot(const StoragePtr & storage) const
    {
        auto host_port = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageID().uuid), storage->getServerVwName(), false);

        if (!isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())))
            throw Exception(
                "Cannot commit parts because of choosing wrong server according to current topology, chosen server: "
                    + host_port.getTCPAddress(),
                ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR); /// Gateway client will use this target server with tcp port to do retry
    }

    UInt64 Catalog::getNonHostUpdateTimestampFromByteKV(const UUID & uuid)
    {
        return meta_proxy->getNonHostUpdateTimeStamp(name_space, UUIDHelpers::UUIDToString(uuid));
    }

    bool Catalog::isHostServer(const ConstStoragePtr & storage) const
    {
        bool res;

        const auto host_port
            = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageID().uuid), storage->getServerVwName(), true);
        if (host_port.empty())
            res = false;
        else
            res = isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort()));

        return res;
    }

    std::pair<bool, HostWithPorts> Catalog::checkIfHostServer(const StoragePtr & storage) const
    {
        const auto host_port
            = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
        if (host_port.empty())
            return {false, host_port};
        return {isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())), host_port};
    }

    void Catalog::finishCommit(
        const StoragePtr & storage,
        const TxnTimestamp & txnID,
        [[maybe_unused]] const TxnTimestamp & commit_ts,
        const DataPartsVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const bool is_merged_parts,
        const bool preallocate_mode,
        const bool write_manifest)
    {
        runWithMetricSupport(
            [&] {
                // In some cases the parts and delete_bitmap are empty, eg: truncate an empty table. We just directly return from here without throwing any exception.
                if (parts.empty() && delete_bitmaps.empty())
                {
                    return;
                }
                // If target table is a bucket table, ensure that the source part is not a bucket part or if the source part is a bucket part
                // ensure that the table_definition_hash is the same before committing
                // If allow_attach_parts_with_different_table_definition_hash is set to true AND the table is NOT clustered by user defined expression, skip this check
                // TODO: Implement rollback for this to work properly
                bool skip_table_definition_hash_check = context.getSettings().allow_attach_parts_with_different_table_definition_hash
                                                        && !storage->getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey();
                if (storage->isBucketTable() && !skip_table_definition_hash_check)
                {
                    auto table_definition_hash = storage->getTableHashForClusterBy();
                    for (auto & part : parts)
                    {
                        if (!part->deleted
                            && (part->bucket_number < 0 || !table_definition_hash.match(part->table_definition_hash)))
                        {
                            LOG_DEBUG(
                                log,
                                "Part's table_definition_hash {} is different from target table's table_definition_hash {}"
                                ,part->table_definition_hash
                                ,table_definition_hash.toString());

                            throw Exception(
                                "Source table is not a bucket table or has a different CLUSTER BY definition from the target table. ",
                                ErrorCodes::BUCKET_TABLE_ENGINE_MISMATCH);
                        }
                    }
                }

                // If target table is a bucket table, table_definition_hash check was skipped and is initially fully clustered
                // Check if any of the parts that will be added has a different table_definition_hash. If yes, set cluster status to false
                if (storage->isBucketTable()
                    && skip_table_definition_hash_check
                    && isTableClustered(storage->getStorageUUID()))
                {
                    auto table_definition_hash = storage->getTableHashForClusterBy();
                    for (auto & part : parts)
                    {
                        if (!part->deleted && !table_definition_hash.match(part->table_definition_hash))
                        {
                            setTableClusterStatus(storage->getStorageUUID(), false, table_definition_hash);
                            break;
                        }
                    }
                }

                if (!context.getSettingsRef().server_write_ha)
                    assertLocalServerThrowIfNot(storage);

                /// add data parts
                Protos::DataModelPartVector commit_parts;
                fillPartsModel(*storage, parts, *commit_parts.mutable_parts(), txnID.toUInt64());

                finishCommitInternal(
                    storage,
                    commit_parts.parts(),
                    delete_bitmaps,
                    /*staged_parts*/ {},
                    txnID.toUInt64(),
                    preallocate_mode,
                    write_manifest,
                    std::vector<String>(commit_parts.parts().size()),
                    std::vector<String>(delete_bitmaps.size()),
                    /*expected_staged_parts*/ {});

                /// insert new added parts into cache manager
                if (context.getPartCacheManager())
                {
                    context.getPartCacheManager()->insertDataPartsIntoCache(
                        *storage,
                        commit_parts.parts(),
                        is_merged_parts,
                        false,
                        PairInt64{0, 0}); // Not be used anymore. Its ok set empty cache version

                    if (!delete_bitmaps.empty())
                    {
                        context.getPartCacheManager()->insertDeleteBitmapsIntoCache(*storage, delete_bitmaps, PairInt64{0, 0}, commit_parts);
                    }
                }
            },
            ProfileEvents::FinishCommitSuccess,
            ProfileEvents::FinishCommitFailed);
    }

    void Catalog::finishCommitInBatch(
        const StoragePtr & storage,
        const TxnTimestamp & txnID,
        const Protos::DataModelPartVector & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const Protos::DataModelPartVector & staged_parts,
        const bool preallocate_mode,
        const bool write_manifest,
        const std::vector<String> & expected_parts,
        const std::vector<String> & expected_bitmaps,
        const std::vector<String> & expected_staged_parts)
    {
        size_t total_parts_number = parts.parts().size();
        size_t total_deleted_bitmaps_number = delete_bitmaps.size();
        size_t total_staged_parts_num = staged_parts.parts().size();
        size_t total_expected_parts_num = expected_parts.size();
        size_t total_expected_bitmaps_num = expected_bitmaps.size();
        size_t total_expected_staged_parts_num = expected_staged_parts.size();

        if (total_expected_parts_num != total_parts_number || total_expected_bitmaps_num != total_deleted_bitmaps_number
            || total_staged_parts_num != total_expected_staged_parts_num)
            throw Exception("Expected parts or bitmaps number does not match the actual number", ErrorCodes::LOGICAL_ERROR);

        auto commit_in_batch = [&](BatchedCommitIndex commit_index) {
            // reuse finishCommitInternal for parts write.
            finishCommitInternal(
                storage,
                google::protobuf::RepeatedPtrField<Protos::DataModelPart>{
                    parts.parts().begin() + commit_index.parts_begin, parts.parts().begin() + commit_index.parts_end},
                DeleteBitmapMetaPtrVector{
                    delete_bitmaps.begin() + commit_index.bitmap_begin, delete_bitmaps.begin() + commit_index.bitmap_end},
                google::protobuf::RepeatedPtrField<Protos::DataModelPart>{
                    staged_parts.parts().begin() + commit_index.staged_begin, staged_parts.parts().begin() + commit_index.staged_end},
                txnID.toUInt64(),
                preallocate_mode,
                write_manifest,
                std::vector<String>{
                    expected_parts.begin() + commit_index.expected_parts_begin, expected_parts.begin() + commit_index.expected_parts_end},
                std::vector<String>{
                    expected_bitmaps.begin() + commit_index.expected_bitmap_begin,
                    expected_bitmaps.begin() + commit_index.expected_bitmap_end},
                std::vector<String>{
                    expected_staged_parts.begin() + commit_index.expected_staged_begin,
                    expected_staged_parts.begin() + commit_index.expected_staged_end});
        };

        if (!context.getSettingsRef().server_write_ha)
            assertLocalServerThrowIfNot(storage);

        // commit parts and delete bitmaps in one batch if the size is small.
        if (total_parts_number + delete_bitmaps.size() + total_staged_parts_num < settings.max_commit_size_one_batch)
        {
            commit_in_batch(
                {0,
                 total_parts_number,
                 0,
                 total_deleted_bitmaps_number,
                 0,
                 total_staged_parts_num,
                 0,
                 total_parts_number,
                 0,
                 total_deleted_bitmaps_number,
                 0,
                 total_expected_staged_parts_num});
        }
        else
        {
            // commit data parts first
            size_t batch_count{0};
            while (batch_count + settings.max_commit_size_one_batch < total_parts_number)
            {
                commit_in_batch(
                    {batch_count,
                     batch_count + settings.max_commit_size_one_batch,
                     0,
                     0,
                     0,
                     0,
                     batch_count,
                     batch_count + settings.max_commit_size_one_batch,
                     0,
                     0,
                     0,
                     0});
                batch_count += settings.max_commit_size_one_batch;
            }
            commit_in_batch({batch_count, total_parts_number, 0, 0, 0, 0, batch_count, total_parts_number, 0, 0, 0, 0});

            // then commit delete bitmap
            batch_count = 0;
            while (batch_count + settings.max_commit_size_one_batch < delete_bitmaps.size())
            {
                commit_in_batch(
                    {0,
                     0,
                     batch_count,
                     batch_count + settings.max_commit_size_one_batch,
                     0,
                     0,
                     0,
                     0,
                     batch_count,
                     batch_count + settings.max_commit_size_one_batch,
                     0,
                     0});
                batch_count += settings.max_commit_size_one_batch;
            }
            commit_in_batch({0, 0, batch_count, total_deleted_bitmaps_number, 0, 0, 0, 0, batch_count, total_deleted_bitmaps_number, 0, 0});

            // then commit staged parts
            batch_count = 0;
            while (batch_count + settings.max_commit_size_one_batch < total_staged_parts_num)
            {
                commit_in_batch(
                    {0,
                     0,
                     0,
                     0,
                     batch_count,
                     batch_count + settings.max_commit_size_one_batch,
                     0,
                     0,
                     0,
                     0,
                     batch_count,
                     batch_count + settings.max_commit_size_one_batch});
                batch_count += settings.max_commit_size_one_batch;
            }
            commit_in_batch({0, 0, 0, 0, batch_count, total_staged_parts_num, 0, 0, 0, 0, batch_count, total_staged_parts_num});
        }
    }

    void Catalog::mayUpdateUHUT(const StoragePtr & storage)
    {
        UInt64 current_pts = context.getTimestamp() >> 18;
        auto cache_manager = context.getPartCacheManager();

        if (cache_manager && cache_manager->trySetCachedNHUTForUpdate(storage->getStorageID().uuid, current_pts))
            meta_proxy->setNonHostUpdateTimeStamp(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid), current_pts);
    }

    bool Catalog::canUseCache(const ConstStoragePtr & storage, const Context * session_context)
    {
        if (!context.getPartCacheManager())
            return false;
        if (context.getSettingsRef().server_write_ha)
        {
            UInt64 latest_nhut;
            if (session_context)
                latest_nhut = const_cast<Context *>(session_context )->getNonHostUpdateTime(storage->getStorageID().uuid);
            else
                latest_nhut = getNonHostUpdateTimestampFromByteKV(storage->getStorageID().uuid);
            return context.getPartCacheManager()->checkIfCacheValidWithNHUT(storage->getStorageID().uuid, latest_nhut);
        }
        return true;
    }

    void Catalog::finishCommitInternal(
        const StoragePtr & storage,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts,
        const DeleteBitmapMetaPtrVector & delete_bitmaps,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & staged_parts,
        const UInt64 & txnid,
        const bool preallocate_mode,
        const bool write_manifest,
        const std::vector<String> & expected_parts,
        const std::vector<String> & expected_bitmaps,
        const std::vector<String> & expected_staged_parts)
    {
        BatchCommitRequest batch_writes(false);
        batch_writes.SetTimeout(3000);

        if (!parts.empty())
        {
            Strings current_partitions = getPartitionIDs(storage, nullptr);
            std::unordered_set<String> deleting_partitions;
            if (context.getPartCacheManager())
                deleting_partitions = context.getPartCacheManager()->getDeletingPartitions(storage);
            meta_proxy->prepareAddDataParts(
                name_space,
                UUIDHelpers::UUIDToString(storage->getStorageID().uuid),
                current_partitions,
                deleting_partitions,
                parts,
                batch_writes,
                expected_parts,
                txnid,
                preallocate_mode,
                write_manifest);

            // reset partition GC status if new data inserted into the deleting partition
            if (!deleting_partitions.empty())
            {
                Strings partition_to_reset{deleting_partitions.begin(), deleting_partitions.end()};
                context.getPartCacheManager()->updatePartitionGCTime(storage, partition_to_reset, 0);
            }
        }
        meta_proxy->prepareAddDeleteBitmaps(
            name_space,
            UUIDHelpers::UUIDToString(storage->getStorageID().uuid),
            delete_bitmaps,
            batch_writes,
            txnid,
            expected_bitmaps,
            write_manifest);
        if (!staged_parts.empty())
        {
            Strings current_partitions = getPartitionIDs(storage, nullptr);
            meta_proxy->prepareAddStagedParts(
                name_space,
                UUIDHelpers::UUIDToString(storage->getStorageID().uuid),
                current_partitions,
                staged_parts,
                batch_writes,
                expected_staged_parts);
        }

        if (batch_writes.isEmpty())
        {
            LOG_DEBUG(
                log,
                "Nothing to do while committing: {} with {} parts, {} delete bitmaps, {} staged parts."
                ,txnid
                ,parts.size()
                ,delete_bitmaps.size()
                ,staged_parts.size());
            return;
        }

        BatchCommitResponse resp;
        meta_proxy->batchWrite(batch_writes, resp);
        if (resp.puts.size())
        {
            throw Exception("Commit parts fail with conflicts. First conflict key is : " + batch_writes.puts[resp.puts.begin()->first].key +
                ", total: " + toString(resp.puts.size()), ErrorCodes::CATALOG_COMMIT_PART_ERROR);
        }
    }

    void Catalog::dropAllPart(const StoragePtr & storage, const TxnTimestamp & txnID, const TxnTimestamp & ts)
    {
        runWithMetricSupport(
            [&] {
                String table_uuid = UUIDHelpers::UUIDToString(storage->getStorageID().uuid);
                BatchCommitRequest batch_writes;

                std::unordered_set<String> parts_set;
                String part_prefix = MetastoreProxy::dataPartPrefix(name_space, table_uuid);
                IMetaStore::IteratorPtr it = meta_proxy->getPartsInRange(name_space, table_uuid, "");

                DB::Protos::DataModelPart part_data;
                while (it->next())
                {
                    part_data.ParseFromString(it->value());
                    auto part_info = part_data.mutable_part_info();

                    String part_name = createPartInfoFromModel(*part_info)->getPartName();
                    auto res_p = parts_set.insert(part_name);
                    if (res_p.second)
                    {
                        /// TODO: replace commit time to txnid for mutation?
                        part_info->set_hint_mutation(part_info->mutation());
                        part_info->set_mutation(ts.toUInt64());
                        part_data.set_deleted(true);
                        part_data.set_txnid(txnID.toUInt64());
                        batch_writes.AddPut(SinglePutRequest(part_prefix + part_name, part_data.SerializeAsString()));
                    }
                }
                BatchCommitResponse resp;
                meta_proxy->batchWrite(batch_writes, resp);
            },
            ProfileEvents::DropAllPartSuccess,
            ProfileEvents::DropAllPartFailed);
    }

    std::vector<std::shared_ptr<MergeTreePartition>> Catalog::getPartitionList(const ConstStoragePtr & storage, const Context * /*session_context*/)
    {
        std::vector<std::shared_ptr<MergeTreePartition>> partition_list;
        runWithMetricSupport(
            [&] {
                PartitionMap partitions;
                if (auto * cnch_table = dynamic_cast<const MergeTreeMetaBase *>(storage.get()))
                {
                    if (auto cache_manager = context.getPartCacheManager(); cache_manager)
                    {
                        const auto host_port = context.getCnchTopologyMaster()->getTargetServer(
                            UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
                        if (!host_port.empty() && isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort()))
                            && cache_manager->getPartitionList(*cnch_table, partition_list, host_port.topology_version))
                        {
                            return;
                        }
                    }
                    getPartitionsFromMetastore(*cnch_table, partitions);
                }

                for (auto it = partitions.begin(); it != partitions.end(); it++)
                    partition_list.push_back(it->second->partition_ptr);
            },
            ProfileEvents::GetPartitionListSuccess,
            ProfileEvents::GetPartitionListFailed);
        return partition_list;
    }

    PartitionWithGCStatus Catalog::getPartitionsWithGCStatus(const StoragePtr & storage, const Strings & required_partitions)
    {
        PartitionWithGCStatus res;
        if (required_partitions.empty())
            return res;
        if (auto * cnch_table = dynamic_cast<MergeTreeMetaBase *>(storage.get()))
        {
            PartitionMap partitions;
            if (auto cache_manager = context.getPartCacheManager(); cache_manager)
            {
                const auto host_port = context.getCnchTopologyMaster()->getTargetServer(
                    UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
                if (!host_port.empty() && isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort()))
                    && cache_manager->getPartitionInfo(*cnch_table, partitions, host_port.topology_version, required_partitions))
                {
                    for (const auto & p : partitions)
                    {
                        if (!p.second->part_cache_status.isLoaded() || p.second->metrics_ptr->read().total_parts_number!=0)
                            continue;
                        res.emplace(p.first, p.second->gctime);
                    }
                }
            }
        }
        return res;
    }

    Strings Catalog::getPartitionIDs(const ConstStoragePtr & storage, const Context * /*session_context*/)
    {
        Strings partition_ids;
        runWithMetricSupport(
            [&] {
                if (auto cache_manager = context.getPartCacheManager(); cache_manager)
                {
                    const auto host_port = context.getCnchTopologyMaster()->getTargetServer(
                        UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);
                    if (!host_port.empty() && isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort()))
                        && cache_manager->getPartitionIDs(*storage, partition_ids, host_port.topology_version))
                    {
                        return;
                    }
                }
                partition_ids = getPartitionIDsFromMetastore(storage);
            },
            ProfileEvents::GetPartitionIDsSuccess,
            ProfileEvents::GetPartitionIDsFailed);
        return partition_ids;
    }

    PrunedPartitions Catalog::getPartitionsByPredicate(ContextPtr session_context, const ConstStoragePtr & storage, const SelectQueryInfo & query_info, const Names & column_names_to_return)
    {
        PrunedPartitions pruned_partitions;
        auto getPartitionsLocally = [&]()
        {
            auto * cnch_mergetree = dynamic_cast<const StorageCnchMergeTree *>(storage.get());
            if (!cnch_mergetree)
                return;
            auto all_partitions = getPartitionList(storage, nullptr);
            pruned_partitions.total_partition_number = all_partitions.size();
            pruned_partitions.partitions = cnch_mergetree->selectPartitionsByPredicate(query_info, all_partitions, column_names_to_return, session_context);
        };
        const auto host_port = context.getCnchTopologyMaster()->getTargetServer(
            UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), true);

        if (!host_port.empty() && !isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())))
        {
            // redirect request to host server
            try
            {
                auto host_with_rpc = host_port.getRPCAddress();
                pruned_partitions = context.getCnchServerClientPool().get(host_with_rpc)->fetchPartitions(host_with_rpc, storage, query_info, column_names_to_return, session_context->getCurrentTransactionID());
                LOG_TRACE(log, "Fetched {}/{} partitions from remote host {}", pruned_partitions.partitions.size(), pruned_partitions.total_partition_number, host_port.toDebugString());
            }
            catch (...)
            {
                LOG_DEBUG(log, "Failed to fetch request partitions from remote server. Fall back to get partitions locally.");
                getPartitionsLocally();
            }
        }
        else
            getPartitionsLocally();

        return pruned_partitions;
    }


    template<typename Map>
    void Catalog::getPartitionsFromMetastore(const MergeTreeMetaBase & table, Map & partition_list)
    {
        runWithMetricSupport(
            [&] {
                partition_list.clear();

                auto table_uuid = UUIDHelpers::UUIDToString(table.getStorageUUID());
                IMetaStore::IteratorPtr it = meta_proxy->getPartitionList(name_space, table_uuid);
                while (it->next())
                {
                    Protos::PartitionMeta partition_meta;
                    partition_meta.ParseFromString(it->value());
                    auto partition_ptr = createPartitionFromMetaModel(table, partition_meta);
                    auto partition_info = std::make_shared<CnchPartitionInfo>(table_uuid, partition_ptr, partition_meta.id());
                    if (partition_meta.has_gctime())
                        partition_info->gctime = partition_meta.gctime();
                    partition_list.emplace(partition_meta.id(), std::move(partition_info));
                }
            },
            ProfileEvents::GetPartitionsFromMetastoreSuccess,
            ProfileEvents::GetPartitionsFromMetastoreFailed);
    }

    template void Catalog::getPartitionsFromMetastore<PartitionMap>(const MergeTreeMetaBase &, PartitionMap &);
    template void Catalog::getPartitionsFromMetastore<ScanWaitFreeMap<String, PartitionInfoPtr>>(const MergeTreeMetaBase &, ScanWaitFreeMap<String, PartitionInfoPtr> &);

    Strings Catalog::getPartitionIDsFromMetastore(const ConstStoragePtr & storage)
    {
        Strings partitions_id;
        IMetaStore::IteratorPtr it = meta_proxy->getPartitionList(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid));
        while (it->next())
        {
            Protos::PartitionMeta partition_meta;
            partition_meta.ParseFromString(it->value());
            partitions_id.emplace_back(partition_meta.id());
        }
        return partitions_id;
    }

    void Catalog::createDictionary(const StorageID & storage_id, const String & create_query)
    {
        runWithMetricSupport(
            [&] {
                Protos::DataModelDictionary dic_model;

                dic_model.set_database(storage_id.getDatabaseName());
                dic_model.set_name(storage_id.getTableName());

                RPCHelpers::fillUUID(storage_id.uuid, *(dic_model.mutable_uuid()));
                dic_model.set_definition(create_query);
                dic_model.set_last_modification_time(Poco::Timestamp().raw());

                meta_proxy->createDictionary(name_space, storage_id.getDatabaseName(), storage_id.getTableName(), dic_model.SerializeAsString());
            },
            ProfileEvents::CreateDictionarySuccess,
            ProfileEvents::CreateDictionaryFailed);
    }

    ASTPtr Catalog::getCreateDictionary(const String & database, const String & name)
    {
        ASTPtr res;
        runWithMetricSupport(
            [&] {
                String dic_meta;
                meta_proxy->getDictionary(name_space, database, name, dic_meta);

                if (dic_meta.empty())
                    throw Exception("Dictionary " + database + "." + name + " doesn't exists.", ErrorCodes::DICTIONARY_NOT_EXIST);

                Protos::DataModelDictionary dic_model;
                dic_model.ParseFromString(dic_meta);
                res = CatalogFactory::getCreateDictionaryByDataModel(dic_model);
            },
            ProfileEvents::GetCreateDictionarySuccess,
            ProfileEvents::GetCreateDictionaryFailed);

        return res;
    }

    void Catalog::dropDictionary(const String & database, const String & name)
    {
        runWithMetricSupport(
            [&] {
                String dic_meta;
                meta_proxy->getDictionary(name_space, database, name, dic_meta);
                if (dic_meta.empty())
                    throw Exception("Dictionary " + database + "." + name + " doesn't  exists.", ErrorCodes::DICTIONARY_NOT_EXIST);

                meta_proxy->dropDictionary(name_space, database, name);
            },
            ProfileEvents::DropDictionarySuccess,
            ProfileEvents::DropDictionaryFailed);
    }

    void Catalog::attachDictionary(const String & database, const String & name)
    {
        runWithMetricSupport(
            [&] { detachOrAttachDictionary(database, name, false); },
            ProfileEvents::AttachDictionarySuccess,
            ProfileEvents::AttachDictionaryFailed);
    }

    void Catalog::detachDictionary(const String & database, const String & name)
    {
        runWithMetricSupport(
            [&] { detachOrAttachDictionary(database, name, true); },
            ProfileEvents::DetachDictionarySuccess,
            ProfileEvents::DetachDictionaryFailed);
    }

    void Catalog::fixDictionary(const String & database, const String & name)
    {
        String dic_meta;
        meta_proxy->getDictionary(name_space, database, name, dic_meta);
        DB::Protos::DataModelDictionary dic_model;
        dic_model.ParseFromString(dic_meta);
        fillUUIDForDictionary(dic_model);
        meta_proxy->createDictionary(name_space, database, name, dic_model.SerializeAsString());
    }

    Strings Catalog::getDictionariesInDB(const String & database)
    {
        Strings res;
        runWithMetricSupport(
            [&] {
                auto dic_ptrs = meta_proxy->getDictionariesInDB(name_space, database);
                res.reserve(dic_ptrs.size());
                for (auto & dic_ptr : dic_ptrs)
                    res.push_back(dic_ptr->name());
            },
            ProfileEvents::GetDictionariesInDBSuccess,
            ProfileEvents::GetDictionariesInDBFailed);
        return res;
    }

    Protos::DataModelDictionary Catalog::getDictionary(const String & database, const String & name)
    {
        Protos::DataModelDictionary res;
        runWithMetricSupport(
            [&] {
                String dic_meta;
                meta_proxy->getDictionary(name_space, database, name, dic_meta);

                DB::Protos::DataModelDictionary dict_data;
                dict_data.ParseFromString(dic_meta);
                res = dict_data;
            },
            ProfileEvents::GetDictionarySuccess,
            ProfileEvents::GetDictionaryFailed);
        return res;
    }

    StoragePtr Catalog::tryGetDictionary(const String & database, const String & name, ContextPtr local_context)
    {
        StoragePtr res;
        runWithMetricSupport(
            [&] {
                String dic_meta;
                meta_proxy->getDictionary(name_space, database, name, dic_meta);
                if (dic_meta.empty())
                    return;
                DB::Protos::DataModelDictionary dict_data;
                dict_data.ParseFromString(dic_meta);

                const UInt64 & status = dict_data.status();
                if (Status::isDeleted(status) || Status::isDetached(status))
                    return;
                ASTPtr ast = CatalogFactory::getCreateDictionaryByDataModel(dict_data);
                const ASTCreateQuery & create_query = ast->as<ASTCreateQuery &>();
                DictionaryConfigurationPtr abstract_dictionary_configuration =
                    getDictionaryConfigurationFromAST(create_query, local_context, dict_data.database());
                abstract_dictionary_configuration->setBool("is_cnch_dictionary", true);
                StorageID storage_id{database, name, RPCHelpers::createUUID(dict_data.uuid())};
                res = StorageDictionary::create(
                    storage_id,
                    std::move(abstract_dictionary_configuration),
                    local_context,
                    true);
            },
            ProfileEvents::GetDictionarySuccess,
            ProfileEvents::GetDictionaryFailed);
        return res;
    }

    bool Catalog::isDictionaryExists(const String & database, const String & name)
    {
        bool res = false;
        runWithMetricSupport(
            [&] {
                String dic_meta;
                meta_proxy->getDictionary(name_space, database, name, dic_meta);

                if (dic_meta.empty())
                {
                    res = false;
                    return;
                }

                DB::Protos::DataModelDictionary dict_data;
                dict_data.ParseFromString(dic_meta);
                const UInt64 & status = dict_data.status();
                if (Status::isDeleted(status) || Status::isDetached(status))
                    res = false;
                else
                    res = true;
            },
            ProfileEvents::IsDictionaryExistsSuccess,
            ProfileEvents::IsDictionaryExistsFailed);
        return res;
    }

    void Catalog::createTransactionRecord(const TransactionRecord & record)
    {
        runWithMetricSupport(
            [&] {
                meta_proxy->createTransactionRecord(name_space, record.txnID().toUInt64(), record.serialize());
                ProfileEvents::increment(ProfileEvents::CnchTxnAllTransactionRecord);
            },
            ProfileEvents::CreateTransactionRecordSuccess,
            ProfileEvents::CreateTransactionRecordFailed);
    }

    void Catalog::removeTransactionRecord(const TransactionRecord & record)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeTransactionRecord(name_space, record.txnID()); },
            ProfileEvents::RemoveTransactionRecordSuccess,
            ProfileEvents::RemoveTransactionRecordFailed);
    }

    void Catalog::removeTransactionRecords(const std::vector<TxnTimestamp> & txn_ids)
    {
        runWithMetricSupport(
            [&] {
                if (txn_ids.empty())
                {
                    return;
                }
                meta_proxy->removeTransactionRecords(name_space, txn_ids);
            },
            ProfileEvents::RemoveTransactionRecordsSuccess,
            ProfileEvents::RemoveTransactionRecordsFailed);
    }

    TransactionRecord Catalog::getTransactionRecord(const TxnTimestamp & txnID)
    {
        TransactionRecord res;
        runWithMetricSupport(
            [&] {
                String txn_data = meta_proxy->getTransactionRecord(name_space, txnID);
                if (txn_data.empty())
                    throw Exception(
                        "No transaction record found with txn_id : " + std::to_string(txnID),
                        ErrorCodes::CATALOG_TRANSACTION_RECORD_NOT_FOUND);
                res = TransactionRecord::deserialize(txn_data);
            },
            ProfileEvents::GetTransactionRecordSuccess,
            ProfileEvents::GetTransactionRecordFailed);
        return res;
    }

    std::optional<TransactionRecord> Catalog::tryGetTransactionRecord(const TxnTimestamp & txnID)
    {
        std::optional<TransactionRecord> res;
        runWithMetricSupport(
            [&] {
                String txn_data = meta_proxy->getTransactionRecord(name_space, txnID);
                if (txn_data.empty())
                    return;
                res = TransactionRecord::deserialize(txn_data);
            },
            ProfileEvents::TryGetTransactionRecordSuccess,
            ProfileEvents::TryGetTransactionRecordFailed);
        return res;
    }

    bool Catalog::setTransactionRecordStatusWithOffsets(
        const TransactionRecord & expected_record,
        TransactionRecord & target_record,
        const String & consumer_group,
        const cppkafka::TopicPartitionList & tpl)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                res = meta_proxy->updateTransactionRecordWithOffsets(
                    name_space,
                    expected_record.txnID().toUInt64(),
                    expected_record.serialize(),
                    target_record.serialize(),
                    consumer_group,
                    tpl);
            },
            ProfileEvents::SetTransactionRecordStatusWithOffsetsSuccess,
            ProfileEvents::SetTransactionRecordStatusWithOffsetsFailed);
        return res;
    }

    bool Catalog::setTransactionRecordStatusWithBinlog(const TransactionRecord & expected_record,
                                                       TransactionRecord & target_record,
                                                       const String & binlog_name,
                                                       const std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> & binlog)
    {
        bool outRes;
        runWithMetricSupport(
            [&] {
                auto res = meta_proxy->updateTransactionRecordWithBinlog(
                    name_space,
                    expected_record.txnID().toUInt64(),
                    expected_record.serialize(),
                    target_record.serialize(),
                    binlog_name,
                    binlog);
                outRes = res;
            },
            ProfileEvents::SetTransactionRecordStatusWithOffsetsSuccess,
            ProfileEvents::SetTransactionRecordStatusWithOffsetsFailed);
        return outRes;
    }

    /// commit and abort can reuse this API. set record status to targetStatus if current record.status is Running
    bool Catalog::setTransactionRecord(const TransactionRecord & expected_record, TransactionRecord & target_record)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                // TODO: topology check and solving the gap between checking and write
                auto [success, txn_data] = meta_proxy->updateTransactionRecord(
                    name_space, expected_record.txnID().toUInt64(), expected_record.serialize(), target_record.serialize());
                if (success)
                {
                    res = true;
                }
                else
                {
                    if (txn_data.empty())
                    {
                        LOG_DEBUG(log, "UpdateTransactionRecord fails. Expected record {} not exist.", expected_record.toString());
                        target_record = expected_record;
                        target_record.setStatus(CnchTransactionStatus::Unknown);
                    }
                    else
                    {
                        target_record = TransactionRecord::deserialize(txn_data);
                    }
                    res = false;
                }
            },
            ProfileEvents::SetTransactionRecordSuccess,
            ProfileEvents::SetTransactionRecordFailed);
        return res;
    }

    bool Catalog::commitTransactionWithNewTableVersion(const TransactionCnchPtr & txn, const UInt64 & table_version)
    {
        TransactionRecord target_record = txn->getTransactionRecord();
        target_record.setStatus(CnchTransactionStatus::Finished)
            .setCommitTs(txn->commit_time)
            .setMainTableUUID(txn->getMainTableUUID());

        BatchCommitRequest batch_write;
        /// build extra metadata that need to be commit with transaction
        // commit kafka offsets
        if (!txn->consumer_group.empty())
        {
            for (const auto & tp : txn->tpl)
                batch_write.AddPut(SinglePutRequest(MetastoreProxy::kafkaOffsetsKey(name_space, txn->consumer_group, tp.get_topic(), tp.get_partition()), std::to_string(tp.get_offset())));
        }
        // commit binlog meta
        if (!txn->binlog.binlog_file.empty())
        {
            /// TODO: support materialize mysql
            throw Exception("Commit transaction with new table version does not support MaterializeMysql now.", ErrorCodes::LOGICAL_ERROR);
        }

        /// add new transaction record. CAS operation
        batch_write.AddPut(SinglePutRequest(MetastoreProxy::transactionRecordKey(name_space, target_record.txnID().toUInt64()),
            target_record.serialize(), txn->getTransactionRecord().serialize()));

        /// build manifest list
        Protos::ManifestListModel manifest_list;
        manifest_list.set_version(table_version);
        manifest_list.mutable_txn_ids()->Add(target_record.txnID().toUInt64());
        batch_write.AddPut(SinglePutRequest(MetastoreProxy::manifestListKey(name_space, UUIDHelpers::UUIDToString(txn->getMainTableUUID()), table_version), manifest_list.SerializeAsString()));

        BatchCommitResponse resp;
        ///TODO: how to deal with commit failure. Should we directly throw exception here?
        meta_proxy->batchWrite(batch_write, resp);

        return true;
    }

    bool Catalog::setTransactionRecordWithRequests(
        const TransactionRecord & expected_record, TransactionRecord & target_record, BatchCommitRequest & additional_requests, BatchCommitResponse & response)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                SinglePutRequest txn_request(MetastoreProxy::transactionRecordKey(name_space, expected_record.txnID().toUInt64()), target_record.serialize(), expected_record.serialize());

                auto [success, txn_data] = meta_proxy->updateTransactionRecordWithRequests(txn_request, additional_requests, response);
                if (success)
                {
                    res = true;
                    return;
                }

                if (txn_data.empty())
                {
                    LOG_DEBUG(log, "UpdateTransactionRecord fails. Expected record {} not exist.", expected_record.toString());
                    target_record = expected_record;
                    target_record.setStatus(CnchTransactionStatus::Unknown);
                }
                else
                {
                    for (auto it=response.puts.begin(); it!=response.puts.end(); it++)
                    {
                        if (additional_requests.puts[it->first].callback)
                            additional_requests.puts[it->first].callback(CAS_FAILED, "");
                    }
                    target_record = TransactionRecord::deserialize(txn_data);
                }
                res = false;
            },
            ProfileEvents::SetTransactionRecordWithRequestsSuccess,
            ProfileEvents::SetTransactionRecordWithRequestsFailed);
        return res;
    }

    void Catalog::setTransactionRecordCleanTime(TransactionRecord record, const TxnTimestamp & clean_ts, UInt64 ttl)
    {
        runWithMetricSupport(
            [&] {
                if (record.status() != CnchTransactionStatus::Finished)
                    throw Exception("Unable to set clean time for uncommitted transaction", ErrorCodes::LOGICAL_ERROR);

                record.setCleanTs(clean_ts);
                meta_proxy->setTransactionRecord(name_space, record.txnID().toUInt64(), record.serialize(), ttl);
            },
            ProfileEvents::SetTransactionRecordCleanTimeSuccess,
            ProfileEvents::SetTransactionRecordCleanTimeFailed);
    }

    void Catalog::rollbackTransaction(TransactionRecord record)
    {
        runWithMetricSupport(
            [&] {
                record.setStatus(CnchTransactionStatus::Aborted);
                meta_proxy->setTransactionRecord(name_space, record.txnID().toUInt64(), record.serialize());
            },
            ProfileEvents::RollbackTransactionSuccess,
            ProfileEvents::RollbackTransactionFailed);
    }

    /// add lock implementation for Pessimistic mode.
    /// replace previous tryLockPartInKV implementation.
    /// key is WriteIntent.intent, value is WriteIntent.txn_id and location.
    /// The key format is use `intent` as prefix like intent_partname.
    /// ConflictIntents map: key is <txnid, location> pair, value is intent names.
    /// (*CAS* operation) put if not exist .
    bool Catalog::writeIntents(
        const String & intent_prefix,
        const std::vector<WriteIntent> & intents,
        std::map<std::pair<TxnTimestamp, String>, std::vector<String>> & conflictIntents)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                std::vector<String> cas_failed_list;

                res = meta_proxy->writeIntent(name_space, intent_prefix, intents, cas_failed_list);

                if (!res)
                {
                    for (auto & intent_data : cas_failed_list)
                    {
                        WriteIntent write_intent = WriteIntent::deserialize(intent_data);
                        std::pair<TxnTimestamp, String> key{write_intent.txnId(), write_intent.location()};
                        auto it = conflictIntents.find(key);
                        if (it == conflictIntents.end())
                        {
                            std::vector<String> intent_names{write_intent.intent()};
                            conflictIntents.emplace(key, intent_names);
                        }
                        else
                        {
                            it->second.push_back(write_intent.intent());
                        }
                    }
                }
            },
            ProfileEvents::WriteIntentsSuccess,
            ProfileEvents::WriteIntentsFailed);
        return res;
    }

    /// used in the following 2 cases.
    /// 1. when meeting with some intents belongs to some zombie transaction.
    /// 2. try to preempt some intents belongs to low-priority running transaction.
    // replace previous tryResetAndLockConflictPartsInKV implementation.
    // intentsToReset map: key is <txnID, location> pair, value is the intent names.
    // (*CAS* operation): set to <newTxnID, newLocation> if value equals <txnID, location>.
    bool Catalog::tryResetIntents(
        const String & intent_prefix,
        const std::map<std::pair<TxnTimestamp, String>, std::vector<String>> & intentsToReset,
        const TxnTimestamp & newTxnID,
        const String & newLocation)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                std::vector<WriteIntent> intent_vector;
                for (auto it = intentsToReset.begin(); it != intentsToReset.end(); it++)
                {
                    const TxnTimestamp & txn_id = it->first.first;
                    const String & location = it->first.second;
                    for (auto & intent_name : it->second)
                        intent_vector.emplace_back(txn_id, location, intent_name);
                }

                res = meta_proxy->resetIntent(name_space, intent_prefix, intent_vector, newTxnID.toUInt64(), newLocation);
            },
            ProfileEvents::TryResetIntentsIntentsToResetSuccess,
            ProfileEvents::TryResetIntentsIntentsToResetFailed);
        return res;
    }

    bool Catalog::tryResetIntents(
        const String & intent_prefix,
        const std::vector<WriteIntent> & oldIntents,
        const TxnTimestamp & newTxnID,
        const String & newLocation)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                res = meta_proxy->resetIntent(name_space, intent_prefix, oldIntents, newTxnID.toUInt64(), newLocation);
            },
            ProfileEvents::TryResetIntentsOldIntentsSuccess,
            ProfileEvents::TryResetIntentsOldIntentsFailed);
        return res;
    }

    /// used when current transaction is committed or aborted,
    /// to clear intents in batch.
    // replace previous unLockPartInKV implementation.
    // after txn record is committed, clear the intent asynchronously.
    // (*CAS* operation): delete intents by keys if value equals <txnID, location>
    // clear may fail due to preemption by others. so we need another clearIntent part to clear part by part.
    void Catalog::clearIntents(const String & intent_prefix, const std::vector<WriteIntent> & intents)
    {
        runWithMetricSupport(
            [&] { meta_proxy->clearIntents(name_space, intent_prefix, intents); },
            ProfileEvents::ClearIntentsSuccess,
            ProfileEvents::ClearIntentsFailed);
    }

    /// write part into kvstore, replace commitParts
    void Catalog::writeParts(
        const StoragePtr & table,
        const TxnTimestamp & txnID,
        const CommitItems & commit_data,
        const bool is_merged_parts,
        const bool preallocate_mode,
        const bool write_manifest)
    {
        runWithMetricSupport(
            [&] {
                Stopwatch watch;

                if (commit_data.empty())
                    return;

                LOG_DEBUG(
                    log,
                    "Start write {} parts and {} delete_bitmaps and {} staged parts to kvstore for txn {}, table: {}",
                    commit_data.data_parts.size(),
                    commit_data.delete_bitmaps.size(),
                    commit_data.staged_parts.size(),
                    txnID,
                    UUIDHelpers::UUIDToString(table->getStorageUUID()));

                const auto host_port
                    = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), false);

                if (!isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())))
                {
                    if (context.getSettingsRef().enable_write_non_host_server)
                    {
                        try
                        {
                            LOG_DEBUG(
                                log,
                                "Redirect writeParts request to remote host : {} for table {}, txn id : {}"
                                    , host_port.toDebugString(), table->getStorageID().getNameForLogs(), txnID);
                            context.getCnchServerClientPool().get(host_port)->redirectCommitParts(
                                table, commit_data, txnID, is_merged_parts, preallocate_mode);
                            return;
                        }
                        catch (Exception & e)
                        {
                            /// if remote quest got exception and cannot fallback to commit to current node, throw exception directly
                            throw Exception(
                                "Fail to redirect writeParts request to remote host : " + host_port.toDebugString()
                                    + ". Error message : " + e.what(),
                                ErrorCodes::CATALOG_COMMIT_PART_ERROR);
                        }
                    }
                    else
                    {
                        throw Exception("Cannot commit part to non-host server.", ErrorCodes::CATALOG_COMMIT_PART_ERROR);
                    }
                }

                // If target table is a bucket table, ensure that the source part is not a bucket part or if the source part is a bucket part
                // ensure that the table_definition_hash is the same before committing
                // If allow_attach_parts_with_different_table_definition_hash is set to true AND the table is NOT clustered by user defined expression, skip this check
                // TODO: Implement rollback for this to work properly
                bool skip_table_definition_hash_check = context.getSettings().allow_attach_parts_with_different_table_definition_hash
                                                        && !table->getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey();
                if (table->isBucketTable() && !skip_table_definition_hash_check)
                {
                    auto table_definition_hash = table->getTableHashForClusterBy();
                    for (const auto & part : commit_data.data_parts)
                    {
                        if (!part->deleted && (part->bucket_number < 0 || !table_definition_hash.match(part->table_definition_hash)))
                            throw Exception(
                                "Part's table_definition_hash [" + std::to_string(part->table_definition_hash)
                                    + "] is different from target table's table_definition_hash  ["
                                    + table_definition_hash.toString() + "]",
                                ErrorCodes::BUCKET_TABLE_ENGINE_MISMATCH);
                    }
                }

                // If target table is a bucket table, table_definition_hash check was skipped and is initially fully clustered
                // Check if any of the parts that will be added has a different table_definition_hash. If yes, set cluster status to false
                if (table->isBucketTable()
                    && skip_table_definition_hash_check
                    && isTableClustered(table->getStorageUUID()))
                {
                    auto table_definition_hash = table->getTableHashForClusterBy();
                    for (const auto & part : commit_data.data_parts)
                    {
                        if (!part->deleted && !table_definition_hash.match(part->table_definition_hash))
                        {
                            setTableClusterStatus(table->getStorageUUID(), false, table_definition_hash);
                            break;
                        }
                    }
                }

                Protos::DataModelPartVector part_models;
                fillPartsModel(*table, commit_data.data_parts, *part_models.mutable_parts(), txnID.toUInt64());

                Protos::DataModelPartVector staged_part_models;
                fillPartsModel(*table, commit_data.staged_parts, *staged_part_models.mutable_parts());

                finishCommitInBatch(
                    table,
                    txnID,
                    part_models,
                    commit_data.delete_bitmaps,
                    staged_part_models,
                    preallocate_mode,
                    write_manifest,
                    std::vector<String>(part_models.parts().size()),
                    std::vector<String>(commit_data.delete_bitmaps.size()),
                    std::vector<String>(staged_part_models.parts().size()));
                LOG_DEBUG(
                    log,
                    "Finish write parts ({}) and delete bitmaps ({}) to kvstore for txn {}, elapsed {}ms, start write cache.",
                    part_models.parts().size(),
                    commit_data.delete_bitmaps.size(),
                    txnID,
                    watch.elapsedMilliseconds());

                /// insert new added parts into cache manager
                if (context.getPartCacheManager())
                {
                    if (!part_models.parts().empty())
                        context.getPartCacheManager()->insertDataPartsIntoCache(
                            *table, part_models.parts(), is_merged_parts, false, host_port.topology_version);
                    if (!commit_data.delete_bitmaps.empty())
                    {
                        context.getPartCacheManager()->insertDeleteBitmapsIntoCache(
                            *table, commit_data.delete_bitmaps, host_port.topology_version, part_models, &staged_part_models);
                    }
                }

                LOG_DEBUG(log, "Finish write part for txn {}, elapsed {} ms.", txnID, watch.elapsedMilliseconds());
            },
            ProfileEvents::WritePartsSuccess,
            ProfileEvents::WritePartsFailed);
    }

    /// set commit time for parts
    void Catalog::setCommitTime(const StoragePtr & table, const CommitItems & commit_data, const TxnTimestamp & ts, const UInt64 txn_id)
    {
        runWithMetricSupport(
            [&] {
                Stopwatch watch;

                if (commit_data.empty())
                {
                    return;
                }
                LOG_DEBUG(
                    log,
                    "Start set commit time of {} parts and {} delete_bitmaps and {} staged parts for txn {}."
                    ,commit_data.data_parts.size()
                    ,commit_data.delete_bitmaps.size()
                    ,commit_data.staged_parts.size()
                    ,txn_id);

                const auto host_port
                    = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), false);

                if (!isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())))
                {
                    if (context.getSettingsRef().enable_write_non_host_server)
                    {
                        try
                        {
                            LOG_DEBUG(
                                log,
                                "Redirect setCommitTime request to remote host : {}  for table {}, txn id : {}"
                                    ,host_port.toDebugString(), table->getStorageID().getNameForLogs(), txn_id);
                            context.getCnchServerClientPool().get(host_port)->redirectSetCommitTime(table, commit_data, ts, txn_id);
                            return;
                        }
                        catch (Exception & e)
                        {
                            /// if remote quest got exception and cannot fallback to commit to current node, throw exception directly
                            throw Exception(
                                "Fail to redirect setCommitTime request to remote host : " + host_port.toDebugString()
                                    + ". Error message : " + e.what(),
                                ErrorCodes::CATALOG_COMMIT_PART_ERROR);
                        }
                    }
                    else
                    {
                        throw Exception("Cannot set commit time to non-host server.", ErrorCodes::CATALOG_COMMIT_PART_ERROR);
                    }
                }

                // set part commit time
                for (const auto & part : commit_data.data_parts)
                    part->commit_time = ts;

                for (const auto & bitmap : commit_data.delete_bitmaps)
                    bitmap->updateCommitTime(ts);

                for (const auto & part : commit_data.staged_parts)
                    part->commit_time = ts;

                // the same logic as write parts, just re-write data parts and update part cache.
                Protos::DataModelPartVector part_models;
                Protos::DataModelPartVector staged_part_models;

                /// check parts status
                std::vector<size_t> parts_to_remove;
                std::vector<String> expected_parts;
                checkItemsStatus<DataPartPtr>(
                    commit_data.data_parts,
                    [&](const DataPartPtr part) -> String {
                        return MetastoreProxy::dataPartKey(
                            name_space, UUIDHelpers::UUIDToString(table->getStorageUUID()), part->info.getPartName());
                    },
                    parts_to_remove,
                    expected_parts);

                /// check delete bitmaps status
                std::vector<size_t> bitmaps_to_remove;
                std::vector<String> expected_bitmap;
                checkItemsStatus<DeleteBitmapMetaPtr>(
                    commit_data.delete_bitmaps,
                    [&](const DeleteBitmapMetaPtr delete_bitmap) -> String {
                        return MetastoreProxy::deleteBitmapKey(
                            name_space, UUIDHelpers::UUIDToString(table->getStorageUUID()), *(delete_bitmap->getModel()));
                    },
                    bitmaps_to_remove,
                    expected_bitmap);

                /// check staged parts status
                std::vector<size_t> staged_parts_to_remove;
                std::vector<String> expected_staged_parts;
                checkItemsStatus<DataPartPtr>(
                    commit_data.staged_parts,
                    [&](const DataPartPtr part) -> String {
                        return MetastoreProxy::stagedDataPartKey(
                            name_space, UUIDHelpers::UUIDToString(table->getStorageUUID()), part->info.getPartName());
                    },
                    staged_parts_to_remove,
                    expected_staged_parts);

                if (parts_to_remove.empty() && bitmaps_to_remove.empty() && staged_parts_to_remove.empty())
                {
                    fillPartsModel(*table, commit_data.data_parts, *part_models.mutable_parts(), txn_id);
                    fillPartsModel(*table, commit_data.staged_parts, *staged_part_models.mutable_parts());

                    finishCommitInBatch(
                        table,
                        txn_id,
                        part_models,
                        commit_data.delete_bitmaps,
                        staged_part_models,
                        false,
                        false,
                        expected_parts,
                        expected_bitmap,
                        expected_staged_parts);
                }
                else
                {
                    LOG_INFO(
                        log,
                        "Some parts, bitmaps, staged_parts not found when set commit time, # of parts to remove: {}, # of bitmaps to remove: {}"
                        ,parts_to_remove.size(), bitmaps_to_remove.size());
                    DataPartsVector parts_to_write = commit_data.data_parts;
                    DeleteBitmapMetaPtrVector bitmap_to_write = commit_data.delete_bitmaps;
                    DataPartsVector staged_parts_to_write = commit_data.staged_parts;

                    // remove non existed parts
                    remove_not_exist_items<DataPartPtr>(parts_to_write, parts_to_remove);

                    // remove non existed parts
                    remove_not_exist_items<DeleteBitmapMetaPtr>(bitmap_to_write, bitmaps_to_remove);

                    // remove non existed parts
                    remove_not_exist_items<DataPartPtr>(staged_parts_to_write, staged_parts_to_remove);

                    // Perform logic checking
                    if (parts_to_write.size() != expected_parts.size() || bitmap_to_write.size() != expected_bitmap.size()
                        || staged_parts_to_write.size() != expected_staged_parts.size())
                        throw Exception(
                            "The part size or bitmap size want to insert does not match with the actual existed size in catalog.",
                            ErrorCodes::LOGICAL_ERROR);

                    fillPartsModel(*table, parts_to_write, *part_models.mutable_parts(), txn_id);
                    fillPartsModel(*table, staged_parts_to_write, *staged_part_models.mutable_parts());

                    finishCommitInBatch(
                        table,
                        txn_id,
                        part_models,
                        bitmap_to_write,
                        staged_part_models,
                        false,
                        false,
                        expected_parts,
                        expected_bitmap,
                        expected_staged_parts);
                }
                LOG_DEBUG(
                    log,
                    "Finish set commit time in kvstore for txn {}, elapsed {} ms, start set commit time in part cache."
                    ,txn_id
                    ,watch.elapsedMilliseconds());

                if (context.getPartCacheManager())
                {
                    if (!part_models.parts().empty())
                        context.getPartCacheManager()->insertDataPartsIntoCache(
                            *table, part_models.parts(), false, true, host_port.topology_version);
                    if (!commit_data.delete_bitmaps.empty())
                    {
                        context.getPartCacheManager()->insertDeleteBitmapsIntoCache(
                            *table, commit_data.delete_bitmaps, host_port.topology_version, part_models, &staged_part_models);
                    }
                }
                LOG_DEBUG(log, "Finish set commit time for txn {}, elapsed {} ms.", txn_id, watch.elapsedMilliseconds());
            },
            ProfileEvents::SetCommitTimeSuccess,
            ProfileEvents::SetCommitTimeFailed);
    }

    /// clear garbage parts generated by aborted or failed transaction.
    void Catalog::clearParts(const StoragePtr & storage, const CommitItems & commit_data)
    {
        runWithMetricSupport(
            [&] {
                if (commit_data.empty())
                {
                    return;
                }
                LOG_INFO(
                    log,
                    "Start clear metadata of {} parts, {} delete bitmaps, {} staged parts of table {}."
                    ,commit_data.data_parts.size()
                    ,commit_data.delete_bitmaps.size()
                    ,commit_data.staged_parts.size()
                    ,storage->getStorageID().getNameForLogs());

                const auto host_ports = context.getCnchTopologyMaster()->getTargetServer(
                    UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), false);

                if (!isLocalServer(host_ports.getRPCAddress(), std::to_string(context.getRPCPort())))
                {
                    try
                    {
                        LOG_DEBUG(
                            log,
                            "Redirect clearParts request to remote host : {} for table {}",
                            host_ports.toDebugString(),
                            storage->getStorageID().getNameForLogs());
                        context.getCnchServerClientPool().get(host_ports)->redirectClearParts(storage, commit_data);
                        return;
                    }
                    catch (Exception & e)
                    {
                        /// if remote quest got exception and cannot fallback to commit to current node, throw exception directly
                        throw Exception(
                            "Fail to redirect clearParts request to remote host : " + host_ports.toDebugString()
                                + ". Error message : " + e.what(),
                            ErrorCodes::CATALOG_COMMIT_PART_ERROR);
                    }
                }

                Strings drop_keys;
                drop_keys.reserve(commit_data.data_parts.size() + commit_data.delete_bitmaps.size() + commit_data.staged_parts.size());
                String table_uuid = UUIDHelpers::UUIDToString(storage->getStorageID().uuid);
                String part_meta_prefix = MetastoreProxy::dataPartPrefix(name_space, table_uuid);

                /// Add parts, delete_bitmaps and staged_parts into drop list. The order of adding into drop list matters. Should always add parts firstly.
                // Clear part meta
                for (auto & part : commit_data.data_parts)
                    drop_keys.emplace_back(part_meta_prefix + part->info.getPartName());

                // Clear bitmap meta
                for (const auto & bitmap : commit_data.delete_bitmaps)
                {
                    const auto & model = *(bitmap->getModel());
                    drop_keys.emplace_back(MetastoreProxy::deleteBitmapKey(name_space, table_uuid, model));
                }

                // Clear staged part meta
                const auto staged_part_prefix = MetastoreProxy::stagedDataPartPrefix(name_space, table_uuid);
                for (const auto & part : commit_data.staged_parts)
                    drop_keys.emplace_back(staged_part_prefix + part->info.getPartName());

                bool need_invalid_part_cache = context.getPartCacheManager() && !commit_data.data_parts.empty();
                bool need_invalid_bitmap_cache = context.getPartCacheManager() && !commit_data.delete_bitmaps.empty();
                /// drop in batch if the number of drop keys greater than max_drop_size_one_batch
                if (drop_keys.size() > settings.max_drop_size_one_batch)
                {
                    size_t batch_count{0};
                    const size_t mark1 = commit_data.data_parts.size();
                    const size_t mark2 = mark1 + commit_data.delete_bitmaps.size();

                    while(batch_count < drop_keys.size())
                    {
                        meta_proxy->multiDrop(Strings{
                            drop_keys.begin() + batch_count,
                            drop_keys.begin() + std::min(batch_count + settings.max_drop_size_one_batch, drop_keys.size())});

                        const size_t batch_count_end = batch_count + settings.max_drop_size_one_batch;
                        /// clear part cache immediately after drop from metastore
                        if (need_invalid_part_cache)
                        {
                            // │  parts  │  delete bitmaps  │  staged parts  │
                            // ▼         ▼                  ▼                ▼
                            // └─────────────────────────────────────────────┘

                            /// For parts.
                            {
                                const size_t left_bound = batch_count;
                                const size_t right_bound = std::min(batch_count_end, mark1);

                                if (left_bound < right_bound)
                                {
                                    context.getPartCacheManager()->invalidPartCache(
                                        storage->getStorageID().uuid,
                                        DataPartsVector{
                                            commit_data.data_parts.begin() + left_bound, commit_data.data_parts.begin() + right_bound});
                                }
                            }
                        }
                        if (need_invalid_bitmap_cache)
                        {
                            /// For delete bitmaps.
                            const size_t left_bound = std::max(batch_count, mark1);
                            const size_t right_bound = std::min(batch_count_end, mark2);

                            if (left_bound < right_bound)
                            {
                                context.getPartCacheManager()->invalidDeleteBitmapCache(
                                    storage->getStorageID().uuid,
                                    DeleteBitmapMetaPtrVector{
                                        commit_data.delete_bitmaps.begin() + left_bound - mark1,
                                        commit_data.delete_bitmaps.begin() + right_bound - mark1});
                            }
                        }
                        batch_count += settings.max_drop_size_one_batch;
                    }
                }
                else
                {
                    meta_proxy->multiDrop(drop_keys);
                    if (need_invalid_part_cache)
                        context.getPartCacheManager()->invalidPartCache(storage->getStorageID().uuid, commit_data.data_parts);
                    if (need_invalid_bitmap_cache)
                        context.getPartCacheManager()->invalidDeleteBitmapCache(storage->getStorageID().uuid, commit_data.delete_bitmaps);
                }

                LOG_INFO(
                    log,
                    "Finish clear metadata of {} parts, {} delete bitmaps, {} staged parts of table {}."
                    ,commit_data.data_parts.size()
                    ,commit_data.delete_bitmaps.size()
                    ,commit_data.staged_parts.size()
                    ,storage->getStorageID().getNameForLogs());
            },
            ProfileEvents::ClearPartsSuccess,
            ProfileEvents::ClearPartsFailed);
    }

    // write undo buffer before write vfs
    void Catalog::writeUndoBuffer(
        const StorageID & storage_id, const TxnTimestamp & txnID, const UndoResources & resources, PlanSegmentInstanceId instance_id)
    {
        runWithMetricSupport(
            [&] {
                if (storage_id.uuid == UUIDHelpers::Nil)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage uuid of table {} can't be empty", storage_id.getNameForLogs());
                String uuid_str = UUIDHelpers::UUIDToString(storage_id.uuid);

                /// write resources in batch, max batch size is max_commit_size_one_batch
                auto begin = resources.begin(), end = std::min(resources.end(), begin + settings.max_commit_size_one_batch);
                while (begin < end)
                {
                    UndoResources tmp{begin, end};
                    meta_proxy->writeUndoBuffer(
                        name_space, txnID.toUInt64(), context.getHostWithPorts().getRPCAddress(), uuid_str, tmp, instance_id, settings.write_undo_buffer_new_key);
                    begin = end;
                    end = std::min(resources.end(), begin + settings.max_commit_size_one_batch);
                }
            },
            ProfileEvents::WriteUndoBufferConstResourceSuccess,
            ProfileEvents::WriteUndoBufferConstResourceFailed);
    }

    void Catalog::writeUndoBuffer(
        const StorageID & storage_id, const TxnTimestamp & txnID, UndoResources && resources, PlanSegmentInstanceId instance_id)
    {
        runWithMetricSupport(
            [&] {
                if (storage_id.uuid == UUIDHelpers::Nil)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage uuid of table {} can't be empty", storage_id.getNameForLogs());
                String uuid_str = UUIDHelpers::UUIDToString(storage_id.uuid);

                auto begin = resources.begin(), end = std::min(resources.end(), begin + settings.max_commit_size_one_batch);
                while (begin < end)
                {
                    UndoResources tmp{std::make_move_iterator(begin), std::make_move_iterator(end)};
                    meta_proxy->writeUndoBuffer(
                        name_space, txnID.toUInt64(), context.getHostWithPorts().getRPCAddress(), uuid_str, tmp, instance_id, settings.write_undo_buffer_new_key);
                    begin = end;
                    end = std::min(resources.end(), begin + settings.max_commit_size_one_batch);
                }
            },
            ProfileEvents::WriteUndoBufferNoConstResourceSuccess,
            ProfileEvents::WriteUndoBufferNoConstResourceFailed);
    }

    // clear undo buffer
    void Catalog::clearUndoBuffer(const TxnTimestamp & txnID)
    {
        runWithMetricSupport(
            [&] {
                /// clear by prefix (txnID) for undo buffer.
                meta_proxy->clearUndoBuffer(name_space, txnID.toUInt64());
            },
            ProfileEvents::ClearUndoBufferSuccess,
            ProfileEvents::ClearUndoBufferFailed);
    }

    // clear undo buffer by segment instance
    void Catalog::clearUndoBuffer(const TxnTimestamp & txnID, const String & rpc_address, PlanSegmentInstanceId instance_id)
    {
        runWithMetricSupport(
            [&] {
                /// clear by prefix (txnID) for undo buffer.
                meta_proxy->clearUndoBuffer(name_space, txnID.toUInt64(), rpc_address, instance_id);
            },
            ProfileEvents::ClearUndoBufferSuccess,
            ProfileEvents::ClearUndoBufferFailed);
    }

    std::unordered_map<String, UndoResources> Catalog::getUndoBuffer(const TxnTimestamp & txnID)
    {
        std::unordered_map<String, UndoResources> res;
        runWithMetricSupport(
            [&] {
                auto get_func = [&](bool write_undo_buffer_new_key) {
                    auto it = meta_proxy->getUndoBuffer(name_space, txnID.toUInt64(), write_undo_buffer_new_key);
                    while (it->next())
                    {
                        UndoResource resource = UndoResource::deserialize(it->value());
                        resource.txn_id = txnID;
                        res[resource.uuid()].emplace_back(std::move(resource));
                    }
                };
                /// Get both old and new undo buffer keys;
                get_func(true);
                get_func(false);
            },
            ProfileEvents::GetUndoBufferSuccess,
            ProfileEvents::GetUndoBufferFailed);
        return res;
    }

    std::unordered_map<String, UndoResources>
    Catalog::getUndoBuffer(const TxnTimestamp & txnID, const String & rpc_address, PlanSegmentInstanceId instance_id)
    {
        std::unordered_map<String, UndoResources> res;
        runWithMetricSupport(
            [&] {
                auto get_func = [&](bool write_undo_buffer_new_key) {
                    auto it = meta_proxy->getUndoBuffer(name_space, txnID.toUInt64(), rpc_address, instance_id, write_undo_buffer_new_key);
                    while (it->next())
                    {
                        UndoResource resource = UndoResource::deserialize(it->value());
                        resource.txn_id = txnID;
                        res[resource.uuid()].emplace_back(std::move(resource));
                    }
                };
                /// Get both old and new undo buffer keys;
                get_func(true);
                get_func(false);
            },
            ProfileEvents::GetUndoBufferSuccess,
            ProfileEvents::GetUndoBufferFailed);
        return res;
    }

    uint32_t Catalog::applyUndos(
        const TransactionRecord & txn_record, const StoragePtr & table, const UndoResources & resources, bool & clean_fs_lock_by_scan)
    {
        auto * storage = dynamic_cast<MergeTreeMetaBase *>(table.get());
        if (!storage)
            throw Exception("Table is not of MergeTree class", ErrorCodes::LOGICAL_ERROR);

        // clean vfs
        for (const auto & resource : resources)
        {
            resource.clean(*this, storage);
        }

        UndoResourceNames names = integrateResources(resources);
        /// Release directory lock if any
        if (!names.kvfs_lock_keys.empty())
        {
            clearFilesysLocks({names.kvfs_lock_keys.begin(), names.kvfs_lock_keys.end()}, txn_record.txnID());
            clean_fs_lock_by_scan = false;
        }

        // Clean attach parts for s3 aborted
        S3AttachMetaAction::abortByUndoBuffer(context, table, resources);
        // Clean detach parts for s3 aborted
        S3DetachMetaAction::abortByUndoBuffer(context, table, resources);

        auto intermediate_parts = getDataPartsByNames(names.parts, table, 0);
        auto undo_bitmaps = getDeleteBitmapByKeys(table, names.bitmaps);
        auto staged_parts = getStagedDataPartsByNames(names.staged_parts, table, 0);
        clearParts(table, CommitItems{intermediate_parts, undo_bitmaps, staged_parts});

        return intermediate_parts.size() + undo_bitmaps.size();
    }

    std::unordered_map<UInt64, UndoResources> Catalog::getAllUndoBuffer()
    {
        std::unordered_map<UInt64, UndoResources> txn_undobuffers;
        runWithMetricSupport(
            [&] {
                auto it = meta_proxy->getAllUndoBuffer(name_space);
                size_t size = 0;
                const String ub_prefix{UNDO_BUFFER_PREFIX};
                while (it->next())
                {
                    /// pb_model
                    UndoResource resource = UndoResource::deserialize(it->value());

                    // txn_id
                    auto pos = it->key().find(ub_prefix);
                    if (pos == std::string::npos || pos + ub_prefix.size() > it->key().size())
                    {
                        LOG_ERROR(log, "Invalid undobuffer key: ", it->key());
                        continue;
                    }
                    UInt64 txn_id = std::stoull(it->key().substr(pos + ub_prefix.size()));
                    resource.txn_id = txn_id;
                    txn_undobuffers[txn_id].emplace_back(std::move(resource));
                    size++;
                }

                LOG_DEBUG(log, "Fetched {} undo buffers", size);
            },
            ProfileEvents::GetAllUndoBufferSuccess,
            ProfileEvents::GetAllUndoBufferFailed);
        return txn_undobuffers;
    }

    Catalog::UndoBufferIterator::UndoBufferIterator(IMetaStore::IteratorPtr metastore_iter_, Poco::Logger * log_)
        : metastore_iter{std::move(metastore_iter_)}, log{log_}
    {}

    bool Catalog::UndoBufferIterator::next()
    {
        if (!metastore_iter)
            return false;

        bool ret = false;
        while (true)
        {
            ret = metastore_iter->next();
            if (!ret)
                break;
            cur_undo_resource = UndoResource::deserialize(metastore_iter->value());
            const String & key = metastore_iter->key();
            UInt64 txn_id;
            if (!parseTxnIdFromUndoBufferKey(key, txn_id))
            {
                LOG_ERROR(log, "Invalid undobuffer key: {}", metastore_iter->key());
                continue;
            }
            cur_undo_resource->txn_id = txn_id;
            valid = true;
            break;
        }
        return ret;
    }

    const UndoResource & Catalog::UndoBufferIterator::getUndoResource() const
    {
        if (!valid)
            throw Exception("iterator is not valid, call next() before using it", ErrorCodes::LOGICAL_ERROR);
        return cur_undo_resource.value();
    }

    Catalog::UndoBufferIterator Catalog::getUndoBufferIterator() const
    {
        UndoBufferIterator ret{nullptr, log};
        runWithMetricSupport(
            [&] {
                auto it = meta_proxy->getAllUndoBuffer(name_space);
                ret = UndoBufferIterator{std::move(it), log};
            },
            ProfileEvents::GetUndoBufferIteratorSuccess,
            ProfileEvents::GetUndoBufferIteratorFailed);
        return ret;
    }

    /// get transaction records, if the records exists, we can check with the transaction coordinator to detect zombie record.
    /// the transaction record will be cleared only after all intents have been cleared and set commit time for all parts.
    /// For zombie record, the intents to be clear can be scanned from intents space with txnid. The parts can be get from undo buffer.
    std::vector<TransactionRecord> Catalog::getTransactionRecords()
    {
        std::vector<TransactionRecord> res;
        runWithMetricSupport(
            [&] {
                /// if exception occurs during get txn record, just return the partial result;
                try
                {
                    auto it = meta_proxy->getAllTransactionRecord(name_space);

                    while (it->next())
                    {
                        res.push_back(TransactionRecord::deserialize(it->value()));
                    }
                }
                catch (...)
                {
                    tryLogDebugCurrentException(__PRETTY_FUNCTION__);
                }
            },
            ProfileEvents::GetTransactionRecordsSuccess,
            ProfileEvents::GetTransactionRecordsFailed);
        return res;
    }

    std::vector<TransactionRecord> Catalog::getTransactionRecords(const std::vector<TxnTimestamp> & txn_ids, size_t batch_size)
    {
        std::vector<TransactionRecord> records;
        runWithMetricSupport(
            [&] {
                size_t total_txn_size = txn_ids.size();

                records.reserve(total_txn_size);

                auto fetch_records_in_batch = [&](size_t begin, size_t end) {
                    auto txn_values = meta_proxy->getTransactionRecords(
                        name_space, std::vector<TxnTimestamp>(txn_ids.begin() + begin, txn_ids.begin() + end));
                    for (size_t i = 0; i < txn_values.size(); ++i)
                    {
                        const auto & data = txn_values[i].first;
                        if (data.empty())
                        {
                            TransactionRecord record;
                            record.setID(txn_ids[i]);
                            records.emplace_back(std::move(record)); // txn_id does not exist
                        }
                        else
                        {
                            records.emplace_back(TransactionRecord::deserialize(data));
                        }
                    }
                };

                if (batch_size > 0)
                {
                    size_t batch_count{0};
                    while (batch_count + batch_size < total_txn_size)
                    {
                        fetch_records_in_batch(batch_count, batch_count + batch_size);
                        batch_count += batch_size;
                    }
                    fetch_records_in_batch(batch_count, total_txn_size);
                }
                else
                    fetch_records_in_batch(0, total_txn_size);
            },
            ProfileEvents::GetTransactionRecordsTxnIdsSuccess,
            ProfileEvents::GetTransactionRecordsTxnIdsFailed);
        return records;
    }

    TransactionRecords Catalog::getTransactionRecords(const ServerDataPartsVector & parts, const DeleteBitmapMetaPtrVector & bitmaps)
    {
        std::set<TxnTimestamp> txn_ids;
        for (const auto & part : parts)
        {
            UInt64 commit_time = detail::ServerDataPartOperation::getCommitTime(part);
            UInt64 txn_id = detail::ServerDataPartOperation::getTxnID(part);

            if (commit_time == IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
                txn_ids.insert(txn_id);
        }
        for (const auto & bitmap: bitmaps)
        {
            UInt64 commit_time = detail::BitmapOperation::getCommitTime(bitmap);
            UInt64 txn_id = detail::BitmapOperation::getTxnID(bitmap);

            if (commit_time == IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME)
                txn_ids.insert(txn_id);
        }
        return getTransactionRecords(std::vector<TxnTimestamp>(txn_ids.begin(), txn_ids.end()), 100000);
    }

    std::vector<TransactionRecord> Catalog::getTransactionRecordsForGC(String & start_key, size_t max_result_number)
    {
        std::vector<TransactionRecord> res;
        /// if exception occurs during get txn record, just return the partial result;
        std::set<UInt64> primary_txn_ids;
        runWithMetricSupport(
            [&] {
                try
                {
                    auto it = meta_proxy->getAllTransactionRecord(name_space, start_key, max_result_number);

                    if (!it->next())
                    {
                        if (start_key.empty())
                            return;

                        start_key.clear();
                        auto it = meta_proxy->getAllTransactionRecord(name_space, start_key, max_result_number);
                        if (!it->next())
                            return;
                    }

                    do
                    {
                        auto record = TransactionRecord::deserialize(it->value());
                        if (record.isSecondary())
                        {
                            /// Only clear if the primary transaction is lost.
                            if (primary_txn_ids.find(record.primaryTxnID()) == primary_txn_ids.end())
                            {
                                /// Get the transaction record of the primary transaction -- this is very rare operator
                                if (tryGetTransactionRecord(record.primaryTxnID()))
                                {
                                    primary_txn_ids.insert(record.primaryTxnID());
                                }
                                else
                                {
                                    res.emplace_back(std::move(record));
                                }
                            }
                        }
                        else
                        {
                            if (record.type() == CnchTransactionType::Explicit)
                            {
                                primary_txn_ids.insert(record.primaryTxnID());
                            }
                            res.push_back(std::move(record));
                        }

                    } while (it->next());

                    // Save key so we can resume iteration in the next call.
                    if (!res.empty())
                        start_key = meta_proxy->transactionRecordKey(name_space, res.back().txnID());

                    if (res.size() < max_result_number || max_result_number == 0)
                        start_key.clear();
                }
                catch (...)
                {
                    tryLogDebugCurrentException(__PRETTY_FUNCTION__);
                }
                LOG_DEBUG(log, "Get {} transaction records for GC.", res.size());
            },
            ProfileEvents::GetTransactionRecordsForGCSuccess,
            ProfileEvents::GetTransactionRecordsForGCFailed);
        return res;
    }


    /// Clear intents written by zombie transaction.
    void Catalog::clearZombieIntent(const TxnTimestamp & txnID)
    {
        runWithMetricSupport(
            [&] {
                /// scan intents by txnID
                /// clear with clearIntents and clearIntent APIs
                meta_proxy->clearZombieIntent(name_space, txnID.toUInt64());
            },
            ProfileEvents::ClearZombieIntentSuccess,
            ProfileEvents::ClearZombieIntentFailed);
    }


    TxnTimestamp Catalog::writeFilesysLock(TxnTimestamp txn_id, const String & dir, const IStorage& storage)
    {
        TxnTimestamp res;
        runWithMetricSupport(
            [&] {
                auto db = storage.getDatabaseName();
                auto table = storage.getTableName();
                if (dir.empty() || db.empty() || table.empty())
                {
                    throw Exception(
                        "Invalid parameter for writeFilesysLock: dir = " + dir + ", db = " + db + ", tb = " + table,
                        ErrorCodes::BAD_ARGUMENTS);
                }
                String normalized_dir = normalizePath(dir);
                if (normalized_dir == "/")
                {
                    throw Exception("Trying to lock the root directory, it's probably a bug", ErrorCodes::LOGICAL_ERROR);
                }
                /// if outer dir is locked, then we cannot lock the inner dir

                auto get_parent_dir = [](String & dir) {
                    auto pos = dir.rfind('/');
                    if (pos == std::string::npos)
                        dir = "";
                    else
                        dir.erase(dir.begin() + pos, dir.end());
                };

                String cur_dir = normalized_dir;
                std::vector<String> dir_paths;
                while (!cur_dir.empty())
                {
                    dir_paths.push_back(normalizePath(cur_dir));
                    get_parent_dir(cur_dir);
                }

                /// NOTE: Check parent path's lock is not atomic with write fs lock.
                /// there maybe race condition where two query lock parent and child
                /// directory concurrently and both acquire the lock
                std::vector<String> lock_metas = meta_proxy->getFilesysLocks(name_space, dir_paths);
                for (const String& lock_meta : lock_metas)
                {
                    if (!lock_meta.empty())
                    {
                        Protos::DataModelFileSystemLock lock;
                        lock.ParseFromString(lock_meta);
                        LOG_DEBUG(log, "Cannot lock {} because it is locked by another transaction {}",
                            normalized_dir, lock.ShortDebugString());
                        res = lock.txn_id();
                        return;
                    }
                }

                /// Write the undo resouce for the lock
                UndoResource ub(txn_id, UndoResourceType::KVFSLockKey, normalized_dir);
                writeUndoBuffer(storage.getCnchStorageID(), txn_id, {ub});
                /// Write the lock
                meta_proxy->writeFilesysLock(name_space, txn_id, normalized_dir, db, table);
                res = txn_id;
            },
            ProfileEvents::WriteFilesysLockSuccess,
            ProfileEvents::WriteFilesysLockFailed);
        return res;
    }

    void Catalog::clearFilesysLocks(const std::vector<String>& dirs, std::optional<TxnTimestamp> txn_id)
    {
        runWithMetricSupport(
            [&] {
                std::vector<String> normalized_dirs;
                for (const String& dir : dirs)
                {
                    String normalized_dir = normalizePath(dir);
                    if (normalized_dir == "/")
                    {
                        throw Exception("Trying to unlock the root directory, it's probably a bug", ErrorCodes::LOGICAL_ERROR);
                    }
                    normalized_dirs.push_back(normalized_dir);
                }

                /// In scenario like undo buffer clean for abort transaction, we need to check if this lock is owned by us
                if (txn_id.has_value())
                {
                    std::vector<String> lock_metas = meta_proxy->getFilesysLocks(name_space, normalized_dirs);
                    normalized_dirs.clear();
                    for (const String& lock_meta : lock_metas)
                    {
                        if (!lock_meta.empty())
                        {
                            Protos::DataModelFileSystemLock lock;
                            lock.ParseFromString(lock_meta);

                            if (lock.txn_id() != txn_id.value().toUInt64())
                            {
                                LOG_INFO(log, "Skip release lock for {} since it not owned by us({})",
                                    lock.ShortDebugString(), txn_id.value().toUInt64());
                            }
                            else
                            {
                                normalized_dirs.push_back(lock.directory());
                            }
                        }
                    }
                }

                meta_proxy->clearFilesysLocks(name_space, normalized_dirs);
            },
            ProfileEvents::ClearFilesysLockDirSuccess,
            ProfileEvents::ClearFilesysLockDirFailed);
    }

    void Catalog::clearFilesysLock(TxnTimestamp txn_id)
    {
        runWithMetricSupport(
            [&] {
                /// get all lock and filter by id, this is inefficient, but it is ok for now.
                auto it = meta_proxy->getAllFilesysLock(name_space);
                std::vector<String> to_delete;
                FilesysLock record;
                while (it->next())
                {
                    record.pb_model.ParseFromString(it->value());
                    if (record.pb_model.txn_id() == txn_id.toUInt64())
                        to_delete.push_back(std::move(record.pb_model.directory()));
                }
                clearFilesysLocks(to_delete, std::nullopt);
            },
            ProfileEvents::ClearFilesysLockTxnIdSuccess,
            ProfileEvents::ClearFilesysLockTxnIdFailed);
    }

    std::vector<FilesysLock> Catalog::getAllFilesysLock()
    {
        std::vector<FilesysLock> res;
        runWithMetricSupport(
            [&] {
                auto it = meta_proxy->getAllFilesysLock(name_space);

                FilesysLock record;
                while (it->next())
                {
                    record.pb_model.ParseFromString(it->value());
                    res.push_back(std::move(record));
                }
            },
            ProfileEvents::GetAllFilesysLockSuccess,
            ProfileEvents::GetAllFilesysLockFailed);
        return res;
    }

    void Catalog::insertTransaction(TxnTimestamp & txnID)
    {
        runWithMetricSupport(
            [&] { meta_proxy->insertTransaction(txnID.toUInt64()); },
            ProfileEvents::InsertTransactionSuccess,
            ProfileEvents::InsertTransactionFailed);
    }

    void Catalog::removeTransaction(const TxnTimestamp & txnID)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeTransaction(txnID.toUInt64()); },
            ProfileEvents::RemoveTransactionSuccess,
            ProfileEvents::RemoveTransactionFailed);
    }

    std::vector<TxnTimestamp> Catalog::getActiveTransactions()
    {
        std::vector<TxnTimestamp> res;
        runWithMetricSupport(
            [&] {
                IMetaStore::IteratorPtr it = meta_proxy->getActiveTransactions();

                while (it->next())
                {
                    UInt64 txnID = std::stoull(it->key().substr(String(TRANSACTION_STORE_PREFIX).size(), std::string::npos), nullptr);
                    res.emplace_back(txnID);
                }
            },
            ProfileEvents::GetActiveTransactionsSuccess,
            ProfileEvents::GetActiveTransactionsFailed);
        return res;
    }

    void Catalog::updateServerWorkerGroup(const String & vw_name, const String & worker_group_name, const HostWithPortsVec & workers)
    {
        runWithMetricSupport(
            [&] {
                Protos::DataModelWorkerGroup worker_group_model;
                worker_group_model.set_vw_name(vw_name);
                worker_group_model.set_worker_group_name(worker_group_name);

                for (auto & worker : workers)
                    RPCHelpers::fillHostWithPorts(worker, *worker_group_model.add_host_ports_vec());

                String worker_group_meta;
                worker_group_model.SerializeToString(&worker_group_meta);

                meta_proxy->updateServerWorkerGroup(worker_group_name, worker_group_meta);
            },
            ProfileEvents::UpdateServerWorkerGroupSuccess,
            ProfileEvents::UpdateServerWorkerGroupFailed);
    }

    HostWithPortsVec Catalog::getWorkersInWorkerGroup(const String & worker_group_name)
    {
        HostWithPortsVec res;
        runWithMetricSupport(
            [&] {
                String worker_group_meta;
                meta_proxy->getServerWorkerGroup(worker_group_name, worker_group_meta);

                if (worker_group_meta.empty())
                    throw Exception("Cannot find worker_group: " + worker_group_name, ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);

                Protos::DataModelWorkerGroup worker_group_model;
                worker_group_model.ParseFromString(worker_group_meta);


                for (const auto & host_ports : worker_group_model.host_ports_vec())
                    res.push_back(RPCHelpers::createHostWithPorts(host_ports));
            },
            ProfileEvents::GetWorkersInWorkerGroupSuccess,
            ProfileEvents::GetWorkersInWorkerGroupFailed);
        return res;
    }

    Catalog::DataModelDBs Catalog::getAllDataBases()
    {
        DataModelDBs res;
        runWithMetricSupport(
            [&] {
                IMetaStore::IteratorPtr it = meta_proxy->getAllDatabaseMeta(name_space);

                std::unordered_map<String, DB::Protos::DataModelDB> db_map{};
                while (it->next())
                {
                    DB::Protos::DataModelDB db_data;
                    db_data.ParseFromString(it->value());
                    if (db_map.count(db_data.name()))
                    {
                        // if multiple versions exists, only return the version with larger commit_time
                        if (db_map[db_data.name()].commit_time() < db_data.commit_time())
                            db_map[db_data.name()] = db_data;
                    }
                    else
                        db_map.emplace(db_data.name(), db_data);
                }

                for (auto & entry : db_map)
                    res.push_back(entry.second);
            },
            ProfileEvents::GetAllDataBasesSuccess,
            ProfileEvents::GetAllDataBasesFailed);
        return res;
    }

    Catalog::DataModelTables Catalog::getAllTables(const String & database_name)
    {
        DataModelTables res;
        runWithMetricSupport(
            [&] {
                /// there may be diferrent version of the same table, we only need the latest version of it. And meta data of a table is sorted by commit time as ascending order in kvstore.
                Protos::DataModelTable latest_version, table_data;
                auto it = meta_proxy->getAllTablesMeta(name_space);
                while (it->next())
                {
                    table_data.ParseFromString(it->value());
                    if (!database_name.empty() && table_data.database() != database_name)
                        continue;
                    if (latest_version.name().empty())
                    {
                        // initialize latest_version;
                        latest_version = table_data;
                    }
                    else
                    {
                        // will return the last version of table meta
                        if (latest_version.uuid().low() != table_data.uuid().low()
                            || latest_version.uuid().high() != table_data.uuid().high())
                            res.push_back(latest_version);

                        latest_version = table_data;
                    }
                }

                if (!latest_version.name().empty())
                    res.push_back(latest_version);
            },
            ProfileEvents::GetAllTablesSuccess,
            ProfileEvents::GetAllTablesFailed);
        // the result will be ordered by table uuid
        return res;
    }

    IMetaStore::IteratorPtr Catalog::getTrashTableIDIterator(uint32_t iterator_internal_batch_size)
    {
        IMetaStore::IteratorPtr res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTrashTableIDIterator(name_space, iterator_internal_batch_size); },
            ProfileEvents::GetTrashTableIDIteratorSuccess,
            ProfileEvents::GetTrashTableIDIteratorFailed);
        return res;
    }

    Catalog::DataModelUDFs Catalog::getAllUDFs(const String & prefix_name, const String & function_name)
    {
        DataModelUDFs res;
        runWithMetricSupport(
            [&] {
                if (!prefix_name.empty() && !function_name.empty())
                {
                    res = getUDFByName({prefix_name + "." + function_name});
                    return;
                }
                const auto & it = meta_proxy->getAllUDFsMeta(name_space, prefix_name);
                while (it->next())
                {
                    DB::Protos::DataModelUDF model;

                    model.ParseFromString(it->value());
                    res.emplace_back(std::move(model));
                }
            },
            ProfileEvents::GetAllUDFsSuccess,
            ProfileEvents::GetAllUDFsFailed);
        return res;
    }

    Catalog::DataModelUDFs Catalog::getUDFByName(const std::unordered_set<String> & function_names)
    {
        DataModelUDFs res;
        runWithMetricSupport(
            [&] {
                auto udfs_info = meta_proxy->getUDFsMetaByName(name_space, function_names);


                for (const auto & it : udfs_info)
                {
                    res.emplace_back();
                    if (!it.empty())
                        res.back().ParseFromString(it);
                }
            },
            ProfileEvents::GetUDFByNameSuccess,
            ProfileEvents::GetUDFByNameFailed);
        return res;
    }

    std::vector<std::shared_ptr<Protos::TableIdentifier>> Catalog::getTrashTableID()
    {
        std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTrashTableID(name_space); },
            ProfileEvents::GetTrashTableIDSuccess,
            ProfileEvents::GetTrashTableIDFailed);
        return res;
    }

    Catalog::DataModelTables Catalog::getTablesInTrash()
    {
        DataModelTables res;
        runWithMetricSupport(
            [&] {
                auto identifiers = meta_proxy->getTrashTableID(name_space);
                res = getTablesByIDs(identifiers);
            },
            ProfileEvents::GetTablesInTrashSuccess,
            ProfileEvents::GetTablesInTrashFailed);
        return res;
    }

    Catalog::DataModelDBs Catalog::getDatabaseInTrash()
    {
        DataModelDBs res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTrashDBs(name_space); },
            ProfileEvents::GetDatabaseInTrashSuccess,
            ProfileEvents::GetDatabaseInTrashFailed);
        return res;
    }

    std::vector<std::shared_ptr<Protos::TableIdentifier>> Catalog::getAllTablesID(const String & db)
    {
        std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllTablesId(name_space, db); },
            ProfileEvents::GetAllTablesIDSuccess,
            ProfileEvents::GetAllTablesIDFailed);
        return res;
    }

    std::vector<std::shared_ptr<Protos::TableIdentifier>> Catalog::getTablesIDByTenant(const String & tenant_id)
    {
        std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTablesIdByPrefix(name_space, tenant_id + "."); },
            ProfileEvents::GetTablesIDByTenantSuccess,
            ProfileEvents::GetTablesIDByTenantFailed);
        return res;
    }

    std::shared_ptr<Protos::TableIdentifier> Catalog::getTableIDByName(const String & db, const String & table)
    {
        std::shared_ptr<Protos::TableIdentifier> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTableID(name_space, db, table); },
            ProfileEvents::GetTableIDByNameSuccess,
            ProfileEvents::GetTableIDByNameFailed);
        return res;
    }

    std::shared_ptr<std::vector<std::shared_ptr<Protos::TableIdentifier>>> Catalog::getTableIDsByNames(const std::vector<std::pair<String, String>> & db_table_pairs)
    {
        std::shared_ptr<std::vector<std::shared_ptr<Protos::TableIdentifier>>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTableIDs(name_space, db_table_pairs); },
            ProfileEvents::GetTableIDsByNamesSuccess,
            ProfileEvents::GetTableIDsByNamesFailed);
        return res;
    }

    Catalog::DataModelWorkerGroups Catalog::getAllWorkerGroups()
    {
        DataModelWorkerGroups res;
        runWithMetricSupport(
            [&] {
                IMetaStore::IteratorPtr it = meta_proxy->getAllWorkerGroupMeta();

                while (it->next())
                {
                    DB::Protos::DataModelWorkerGroup worker_group_data;
                    worker_group_data.ParseFromString(it->value());
                    res.push_back(worker_group_data);
                }
            },
            ProfileEvents::GetAllWorkerGroupsSuccess,
            ProfileEvents::GetAllWorkerGroupsFailed);
        return res;
    }

    Catalog::DataModelDictionaries Catalog::getAllDictionaries()
    {
        DataModelDictionaries res;
        runWithMetricSupport(
            [&] {
                IMetaStore::IteratorPtr it = meta_proxy->getAllDictionaryMeta(name_space);

                while (it->next())
                {
                    DB::Protos::DataModelDictionary dict_data;
                    dict_data.ParseFromString(it->value());
                    res.push_back(dict_data);
                }
            },
            ProfileEvents::GetAllDictionariesSuccess,
            ProfileEvents::GetAllDictionariesFailed);
        return res;
    }

    void Catalog::clearDatabaseMeta(const String & database, const UInt64 & ts)
    {
        runWithMetricSupport(
            [&] {
                LOG_INFO(log, "Start clear metadata for database : {}", database);

                Strings databases_meta;
                meta_proxy->getDatabase(name_space, database, databases_meta);

                Protos::DataModelDB db_model;
                for (auto & meta : databases_meta)
                {
                    db_model.ParseFromString(meta);
                    if (db_model.commit_time() <= ts)
                        meta_proxy->dropDatabase(name_space, db_model);
                }

                LOG_INFO(log, "Finish clear metadata for database : {}", database);
            },
            ProfileEvents::ClearDatabaseMetaSuccess,
            ProfileEvents::ClearDatabaseMetaFailed);
    }

    void Catalog::clearTableMetaForGC(const String & database, const String & name, const UInt64 & ts)
    {
        runWithMetricSupport(
            [&] {
                String target_database;
                String table_uuid = meta_proxy->getTrashTableUUID(name_space, database, name, ts);

                /// The trash table may created by drop database. The trash key has database name with commit ts.
                if (table_uuid.empty())
                {
                    target_database = database + '_' + toString(ts);
                    table_uuid = meta_proxy->getTrashTableUUID(name_space, database + '_' + toString(ts), name, ts);
                }
                else
                {
                    target_database = database;
                }

                if (table_uuid.empty())
                {
                    LOG_WARNING(log, "Cannot find trashed table ID by name {}.{}, ts: {}", database, name, ts);
                    return;
                }

                auto table = tryGetTableFromMetastore(table_uuid, ts, false, true);

                Strings dependencies;
                if (table)
                    dependencies = tryGetDependency(parseCreateQuery(table->definition()));

                // clean all manifest data before delete table metadata
                auto manifests = getAllTableVersions(UUIDHelpers::toUUID(table_uuid));
                if (!manifests.empty())
                    cleanTableVersions(UUIDHelpers::toUUID(table_uuid), manifests);

                meta_proxy->clearTableMeta(name_space, target_database, name, table_uuid, dependencies, ts);

                if (!table_uuid.empty() && context.getPartCacheManager())
                {
                    UUID uuid = UUID(stringToUUID(table_uuid));
                    context.getPartCacheManager()->invalidPartAndDeleteBitmapCache(uuid);
                }
            },
            ProfileEvents::ClearTableMetaForGCSuccess,
            ProfileEvents::ClearTableMetaForGCFailed);
    }

    void Catalog::clearDataPartsMeta(const StoragePtr & storage, const DataPartsVector & parts)
    {
        runWithMetricSupport(
            [&] {
                clearParts(storage, CommitItems{parts, {}, {}});
            },
            ProfileEvents::ClearDataPartsMetaSuccess,
            ProfileEvents::ClearDataPartsMetaFailed);
    }

    void Catalog::clearStagePartsMeta(const StoragePtr & storage, const ServerDataPartsVector & parts)
    {
        if (parts.empty())
            return;
        runWithMetricSupport(
            [&] {
                LOG_INFO(
                    log,
                    "Start clear metadata of {} staged parts of table {}",
                    parts.size(),
                    storage->getStorageID().getNameForLogs());

                String table_uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
                String key_prefix = MetastoreProxy::stagedDataPartPrefix(name_space, table_uuid);

                for (size_t beg = 0; beg < parts.size(); beg += settings.max_drop_size_one_batch)
                {
                    size_t end = std::min(beg + settings.max_drop_size_one_batch, parts.size());

                    Strings drop_keys;
                    for (auto it = parts.begin() + beg; it != parts.begin() + end; ++it)
                    {
                        const auto & part = *it;
                        drop_keys.emplace_back(key_prefix + part->name());
                        LOG_DEBUG(
                            log,
                            "Will clear staged part meta of table {} : {} [{} - {}) {}",
                            table_uuid,
                            part->name(),
                            part->getCommitTime(),
                            part->getEndTime(),
                            (part->deleted() ? "tombstone" : ""));
                    }
                    meta_proxy->multiDrop(drop_keys);
                }

                LOG_INFO(
                    log,
                    "Finish clear metadata of {} staged parts of table {}",
                    parts.size(),
                    storage->getStorageID().getNameForLogs());
            },
            ProfileEvents::ClearStagePartsMetaSuccess,
            ProfileEvents::ClearStagePartsMetaFailed);
    }

    void Catalog::clearDataPartsMetaForTable(const StoragePtr & storage)
    {
        runWithMetricSupport(
            [&] {
                LOG_INFO(log, "Start clear all data parts for table : {}.{}", storage->getDatabaseName(), storage->getTableName());

                meta_proxy->dropAllPartInTable(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid));

                if (context.getPartCacheManager())
                {
                    auto cache_manager = context.getPartCacheManager();
                    cache_manager->invalidPartAndDeleteBitmapCache(storage->getStorageID().uuid);
                }

                LOG_INFO(log, "Finish clear all data parts for table : {}.{}", storage->getDatabaseName(), storage->getTableName());
            },
            ProfileEvents::ClearDataPartsMetaForTableSuccess,
            ProfileEvents::ClearDataPartsMetaForTableFailed);
    }

    void Catalog::clearMutationEntriesForTable(const StoragePtr & storage)
    {
        runWithMetricSupport(
            [&] {
                LOG_INFO(log, "Start clear all mutation entries for table: {}", storage->getStorageID().getNameForLogs());
                meta_proxy->dropAllMutationsInTable(name_space, UUIDHelpers::UUIDToString(storage->getStorageUUID()));
                LOG_INFO(log, "Finish clear all mutation entries for table: {}", storage->getStorageID().getNameForLogs());
            },
            ProfileEvents::ClearDataPartsMetaForTableSuccess,
            ProfileEvents::ClearDataPartsMetaForTableFailed);

    }

    void Catalog::clearDeleteBitmapsMetaForTable(const StoragePtr & storage)
    {
        runWithMetricSupport(
            [&] {
                LOG_INFO(log, "Start clear all delete bitmaps for table : {}.{}", storage->getDatabaseName(), storage->getTableName());
                meta_proxy->dropAllDeleteBitmapInTable(name_space, UUIDHelpers::UUIDToString(storage->getStorageUUID()));
                LOG_INFO(log, "Finish clear all delete bitmaps for table : {}.{}", storage->getDatabaseName(), storage->getTableName());
            },
            ProfileEvents::ClearDeleteBitmapsMetaForTableSuccess,
            ProfileEvents::ClearDeleteBitmapsMetaForTableFailed);
    }

    TrashItems Catalog::getDataItemsInTrash(const StoragePtr & storage, const size_t & limit, String * start_key)
    {
        TrashItems res;
        auto & merge_tree_storage = dynamic_cast<MergeTreeMetaBase &>(*storage);
        String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
        size_t prefix_length = MetastoreProxy::trashItemsPrefix(name_space, uuid).length();

        /// Try iterate from the last checkpoint.
        auto it = meta_proxy->getItemsInTrash(name_space, uuid, limit, start_key != nullptr ? *start_key : "");

        if (!it->next())
        {
            if (start_key == nullptr || start_key->empty())
                return res;

            /// If we encounter the empty range, try to iterate from the start.
            if (start_key)
                *start_key = "";
            it = meta_proxy->getItemsInTrash(name_space, uuid, limit, "");

            if (!it->next())
                return res;
        }

        do
        {
            const auto & key = it->key();
            String meta_key = key.substr(prefix_length, String::npos);
            if (startsWith(meta_key, PART_STORE_PREFIX))
            {
                Protos::DataModelPart part_model;
                part_model.ParseFromString(it->value());
                auto part_model_wrapper = createPartWrapperFromModel(merge_tree_storage, std::move(part_model), meta_key.substr(String(PART_STORE_PREFIX).length(), String::npos));
                res.data_parts.push_back(std::make_shared<ServerDataPart>(std::move(part_model_wrapper)));
            }
            else if (startsWith(meta_key, DELETE_BITMAP_PREFIX))
            {
                DataModelDeleteBitmapPtr model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
                model_ptr->ParseFromString(it->value());
                res.delete_bitmaps.push_back(std::make_shared<DeleteBitmapMeta>(merge_tree_storage, model_ptr));
            }
            // not handling staged parts because we never move them to trash

            // Save key so we can resume iteration in the next call.
            if (start_key)
                *start_key = key;
        } while (it->next());

        if (start_key && (res.size() < limit || limit == 0))
            *start_key = "";

        return res;
    }

    void Catalog::markPartitionDeleted(const StoragePtr & table, const Strings & partitions)
    {
        if (partitions.empty() || !context.getPartCacheManager())
            return;

        StorageCnchMergeTree & merge_tree = dynamic_cast<StorageCnchMergeTree &>(*table);
        String table_uuid = UUIDHelpers::UUIDToString(table->getStorageUUID());
        auto current_ts = UInt32(time(nullptr));
        // firstly, change the in-memory partition GC status into deleting
        auto deleting_partitions = context.getPartCacheManager()->updatePartitionGCTime(table, partitions, UInt32(current_ts));
        if (deleting_partitions.empty())
            return;
        // then, update the partition metadata

        BatchCommitRequest batch_write;
        for (auto it = deleting_partitions.begin(); it != deleting_partitions.end(); it++)
        {
            Protos::PartitionMeta partition_model;
            partition_model.set_id(it->first);
            partition_model.set_gctime(current_ts);
            WriteBufferFromString buffer(*partition_model.mutable_partition_minmax());
            it->second->partition_ptr->store(merge_tree, buffer);
            batch_write.AddPut(SinglePutRequest(MetastoreProxy::tablePartitionInfoKey(name_space, table_uuid, it->first), partition_model.SerializeAsString()));
        }

        BatchCommitResponse resp;
        meta_proxy->batchWrite(batch_write, resp);
        LOG_DEBUG(log, "Deleting {} partition records in table {} with gc time {}", deleting_partitions.size(), table->getStorageID().getNameForLogs(), current_ts);
    }

    void Catalog::deletePartitionsMetadata(const StoragePtr & table, const PartitionWithGCStatus & partitions)
    {
        if (partitions.empty() || !context.getPartCacheManager())
            return;

        // remove the partition metadata with CAS
        Strings deleted_partitions = meta_proxy->removePartitions(name_space, UUIDHelpers::UUIDToString(table->getStorageUUID()), partitions);
        LOG_DEBUG(log, "Removed {} partition records in table {} from metastore. Now remove them from cache.", deleted_partitions.size(), table->getStorageID().getNameForLogs());
        context.getPartCacheManager()->removeDeletedPartitions(table, deleted_partitions);
    }

    void Catalog::moveDataItemsToTrash(const StoragePtr & table, const TrashItems & items, bool is_zombie_with_staging_txn_id)
    {
        if (items.empty())
            return;

        if (!isHostServer(table))
            throw Exception("Cannot move parts to trash because current server is non-host server for table : "
                + UUIDHelpers::UUIDToString(table->getStorageUUID()),
                ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);

        LOG_INFO(
            log,
            "Start clear metadata of {} parts, {} delete bitmaps, {} staged parts of table {}",
            items.data_parts.size(),
            items.delete_bitmaps.size(),
            items.staged_parts.size(),
            table->getStorageID().getNameForLogs());

        String table_uuid = UUIDHelpers::UUIDToString(table->getStorageUUID());
        String part_meta_prefix = MetastoreProxy::dataPartPrefix(name_space, table_uuid);
        String staged_part_meta_prefix = MetastoreProxy::stagedDataPartPrefix(name_space, table_uuid);

        bool need_invalid_part_cache = context.getPartCacheManager() != nullptr;

        BatchCommitRequest batch_writes(false);

        for (const auto & part : items.data_parts)
        {
            batch_writes.AddDelete(part_meta_prefix + part->info().getPartName());
            if (!is_zombie_with_staging_txn_id)
                batch_writes.AddPut(
                    {MetastoreProxy::dataPartKeyInTrash(name_space, table_uuid, part->name()),
                    part->part_model_wrapper->part_model->SerializeAsString()});
            LOG_DEBUG(
                log,
                "Will move part of table {} to trash: {} [{} - {}) {}",
                table_uuid,
                part->name(),
                part->getCommitTime(),
                part->getEndTime(),
                (part->deleted() ? "tombstone" : ""));
        }

        for (const auto & bitmap : items.delete_bitmaps)
        {
            const auto & model = *(bitmap->getModel());
            batch_writes.AddDelete(MetastoreProxy::deleteBitmapKey(name_space, table_uuid, model));
            batch_writes.AddPut({MetastoreProxy::deleteBitmapKeyInTrash(name_space, table_uuid, model), model.SerializeAsString()});
            LOG_DEBUG(
                log,
                "Will move delete bitmap of table {} to trash: {} [{} - {}) {}",
                table_uuid,
                bitmap->getNameForLogs(),
                bitmap->getCommitTime(),
                bitmap->getEndTime(),
                (bitmap->isTombstone() ? "tombstone" : ""));
        }

        for (const auto & part : items.staged_parts)
        {
            // Drop staged part metadata instead of moving to trash because:
            // after staged part is published, it no longer own the underlying data file,
            // therfore garbage collection of staged parts only needs metadata cleaning.
            batch_writes.AddDelete(staged_part_meta_prefix + part->info().getPartName());
            LOG_DEBUG(
                log,
                "Will remove staged part metadata of table {}: {} [{} - {}) {}",
                table_uuid,
                part->name(),
                part->getCommitTime(),
                part->getEndTime(),
                (part->deleted() ? "tombstone" : ""));
        }

        meta_proxy->getMetastore()->adaptiveBatchWrite(batch_writes);

        if (need_invalid_part_cache)
        {
            context.getPartCacheManager()->invalidPartCache(table->getStorageUUID(), items.data_parts);
        }
        context.getPartCacheManager()->invalidDeleteBitmapCache(table->getStorageUUID(), items.delete_bitmaps);

        LOG_INFO(
            log,
            "Finish clear metadata of {} parts, {} delete bitmaps, {} staged parts of table {}",
            items.data_parts.size(),
            items.delete_bitmaps.size(),
            items.staged_parts.size(),
            table->getStorageID().getNameForLogs());

        /// Update trash items metrics.
        if (auto mgr = context.getPartCacheManager())
        {
            if (!is_zombie_with_staging_txn_id)
                mgr->updateTrashItemsMetrics(table->getStorageUUID(), items);
        }
    }

    void Catalog::clearTrashItems(const StoragePtr & table, const TrashItems & items)
    {
        if (items.empty())
            return;

        Strings drop_keys;
        drop_keys.reserve(items.data_parts.size() + items.delete_bitmaps.size());
        String table_uuid = UUIDHelpers::UUIDToString(table->getStorageUUID());

        // Clear part meta record from trash
        for (const auto & part : items.data_parts)
            drop_keys.emplace_back(MetastoreProxy::dataPartKeyInTrash(name_space, table_uuid, part->name()));

        // Clear bitmap meta record from trash
        for (const auto & bitmap : items.delete_bitmaps)
        {
            const auto & model = *(bitmap->getModel());
            drop_keys.emplace_back(MetastoreProxy::deleteBitmapKeyInTrash(name_space, table_uuid, model));
        }

        meta_proxy->multiDrop(drop_keys);

        LOG_DEBUG(log, "Removed trash record of {} parts and {} delete bitmaps.", items.data_parts.size(), items.delete_bitmaps.size());

        /// Update trash items metrics.
        if (auto mgr = context.getPartCacheManager())
            mgr->updateTrashItemsMetrics(table->getStorageUUID(), items, false);
    }

    std::vector<TxnTimestamp> Catalog::getSyncList(const StoragePtr & storage)
    {
        std::vector<TxnTimestamp> res;
        runWithMetricSupport(
            [&] {
                IMetaStore::IteratorPtr it = meta_proxy->getSyncList(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid));

                while (it->next())
                {
                    UInt64 commit_time = std::stoull(it->value(), nullptr);
                    res.emplace_back(commit_time);
                }
            },
            ProfileEvents::GetSyncListSuccess,
            ProfileEvents::GetSyncListFailed);
        return res;
    }

    void Catalog::clearSyncList(const StoragePtr & storage, std::vector<TxnTimestamp> & sync_list)
    {
        runWithMetricSupport(
            [&] { meta_proxy->clearSyncList(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid), sync_list); },
            ProfileEvents::ClearSyncListSuccess,
            ProfileEvents::ClearSyncListFailed);
    }

    DB::ServerDataPartsVector Catalog::getServerPartsByCommitTime(const StoragePtr & table, std::vector<TxnTimestamp> & sync_list)
    {
        ServerDataPartsVector res;
        runWithMetricSupport(
            [&] {
                std::unordered_set<UInt64> sync_set;
                for (auto & ele : sync_list)
                    sync_set.emplace(ele.toUInt64());

                IMetaStore::IteratorPtr it
                    = meta_proxy->getPartsInRange(name_space, UUIDHelpers::UUIDToString(table->getStorageUUID()), "");


                auto & storage = dynamic_cast<MergeTreeMetaBase &>(*table);

                while (it->next())
                {
                    const auto & key = it->key();
                    auto pos = key.find_last_of('_');
                    if (pos != String::npos)
                    {
                        UInt64 commit_time = std::stoull(key.substr(pos + 1, String::npos), nullptr);
                        if (sync_set.count(commit_time))
                        {
                            Protos::DataModelPart part_model;
                            part_model.ParseFromString(it->value());
                            res.push_back(std::make_shared<ServerDataPart>(createPartWrapperFromModel(storage, std::move(part_model))));
                        }
                    }
                }
            },
            ProfileEvents::GetServerPartsByCommitTimeSuccess,
            ProfileEvents::GetServerPartsByCommitTimeFailed);
        return res;
    }

    void Catalog::createRootPath(const String & path)
    {
        runWithMetricSupport(
            [&] { meta_proxy->createRootPath(path); }, ProfileEvents::CreateRootPathSuccess, ProfileEvents::CreateRootPathFailed);
    }

    void Catalog::deleteRootPath(const String & path)
    {
        runWithMetricSupport(
            [&] { meta_proxy->deleteRootPath(path); }, ProfileEvents::DeleteRootPathSuccess, ProfileEvents::DeleteRootPathFailed);
    }

    std::vector<std::pair<String, UInt32>> Catalog::getAllRootPath()
    {
        std::vector<std::pair<String, UInt32>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllRootPath(); }, ProfileEvents::GetAllRootPathSuccess, ProfileEvents::GetAllRootPathFailed);

        return res;
    }

    void Catalog::createMutation(const StorageID & storage_id, const String & mutation_name, const String & mutate_text)
    {
        LOG_TRACE(log, "createMutation: {}, {}", storage_id.getNameForLogs(), mutation_name);
        runWithMetricSupport(
            [&] { meta_proxy->createMutation(name_space, UUIDHelpers::UUIDToString(storage_id.uuid), mutation_name, mutate_text); },
            ProfileEvents::CreateMutationSuccess,
            ProfileEvents::CreateMutationFailed);
    }

    void Catalog::removeMutation(const StorageID & storage_id, const String & mutation_name)
    {
        LOG_TRACE(log, "removeMutation: {}, {}", storage_id.getNameForLogs(), mutation_name);
        runWithMetricSupport(
            [&] { meta_proxy->removeMutation(name_space, UUIDHelpers::UUIDToString(storage_id.uuid), mutation_name); },
            ProfileEvents::RemoveMutationSuccess,
            ProfileEvents::RemoveMutationFailed);
    }

    Strings Catalog::getAllMutations(const StorageID & storage_id)
    {
        Strings res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllMutations(name_space, UUIDHelpers::UUIDToString(storage_id.uuid)); },
            ProfileEvents::GetAllMutationsStorageIdSuccess,
            ProfileEvents::GetAllMutationsStorageIdFailed);
        return res;
    }

    // uuid, mutation
    std::multimap<String, String> Catalog::getAllMutations()
    {
        std::multimap<String, String> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllMutations(name_space); },
            ProfileEvents::GetAllMutationsSuccess,
            ProfileEvents::GetAllMutationsFailed);
        return res;
    }

    void Catalog::fillMutationsByStorage(const StorageID & storage_id, std::map<TxnTimestamp, CnchMergeTreeMutationEntry> & out_mutations)
    {
        assert(out_mutations.empty());
        const auto & mutations_str = getAllMutations(storage_id);
        LOG_TRACE(log, "Get {} mutations from catalog", mutations_str.size());
        for (const auto & mutation_str : mutations_str)
        {
            try
            {
                auto entry = CnchMergeTreeMutationEntry::parse(mutation_str);
                out_mutations.try_emplace(entry.commit_time, entry);
            }
            catch(...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Error when parsing mutation: " + mutation_str);
            }
        }
    }

    void Catalog::setTableClusterStatus(const UUID & table_uuid, const bool clustered, const TableDefinitionHash & table_definition_hash)
    {
        LOG_TRACE(log, "setTableClusterStatus: {} to {}", UUIDHelpers::UUIDToString(table_uuid), clustered);
        runWithMetricSupport(
            [&] {
                meta_proxy->setTableClusterStatus(name_space, UUIDHelpers::UUIDToString(table_uuid), clustered, table_definition_hash.getDeterminHash());
                /// keep the cache status up to date.
                if (context.getPartCacheManager())
                    context.getPartCacheManager()->setTableClusterStatus(table_uuid, clustered, table_definition_hash);
            },
            ProfileEvents::SetTableClusterStatusSuccess,
            ProfileEvents::SetTableClusterStatusFailed);
    }

    void Catalog::getTableClusterStatus(const UUID & table_uuid, bool & clustered)
    {
        runWithMetricSupport(
            [&] { meta_proxy->getTableClusterStatus(name_space, UUIDHelpers::UUIDToString(table_uuid), clustered); },
            ProfileEvents::GetTableClusterStatusSuccess,
            ProfileEvents::GetTableClusterStatusFailed);
    }

    bool Catalog::isTableClustered(const UUID & table_uuid)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                if (context.getPartCacheManager())
                {
                    res = context.getPartCacheManager()->getTableClusterStatus(table_uuid);
                }
                else
                {
                    getTableClusterStatus(table_uuid, res);
                }
            },
            ProfileEvents::IsTableClusteredSuccess,
            ProfileEvents::IsTableClusteredFailed);
        return res;
    }

    void Catalog::setBGJobStatus(const UUID & table_uuid, CnchBGThreadType type, CnchBGThreadStatus status)
    {
        runWithMetricSupport(
            [&] {
                    meta_proxy->setBGJobStatus(
                        name_space, UUIDHelpers::UUIDToString(table_uuid), type, status);
                },
            ProfileEvents::SetBGJobStatusSuccess,
            ProfileEvents::SetBGJobStatusFailed);
    }

    std::optional<CnchBGThreadStatus> Catalog::getBGJobStatus(const UUID & table_uuid, CnchBGThreadType type)
    {
        std::optional<CnchBGThreadStatus> res;
        runWithMetricSupport(
            [&] {
                    res = meta_proxy->getBGJobStatus(
                        name_space, UUIDHelpers::UUIDToString(table_uuid), type);
                },
            ProfileEvents::GetBGJobStatusSuccess,
            ProfileEvents::GetBGJobStatusFailed);

        return res;
    }

    std::unordered_map<UUID, CnchBGThreadStatus> Catalog::getBGJobStatuses(CnchBGThreadType type)
    {
        std::unordered_map<UUID, CnchBGThreadStatus> res;
        runWithMetricSupport(
            [&] {
                    res = meta_proxy->getBGJobStatuses(
                        name_space, type);
                },
            ProfileEvents::GetBGJobStatusesSuccess,
            ProfileEvents::GetBGJobStatusesFailed);

        return res;
    }

    void Catalog::dropBGJobStatus(const UUID & table_uuid, CnchBGThreadType type)
    {
        runWithMetricSupport(
            [&] {
                    meta_proxy->dropBGJobStatus(
                        name_space, UUIDHelpers::UUIDToString(table_uuid), type);
                },
            ProfileEvents::DropBGJobStatusSuccess,
            ProfileEvents::DropBGJobStatusFailed);
    }

    void Catalog::setTablePreallocateVW(const UUID & table_uuid, const String vw)
    {
        runWithMetricSupport(
            [&] {
                meta_proxy->setTablePreallocateVW(name_space, UUIDHelpers::UUIDToString(table_uuid), vw);
                /// keep the cache status up to date.
                if (context.getPartCacheManager())
                    context.getPartCacheManager()->setTablePreallocateVW(table_uuid, vw);
            },
            ProfileEvents::SetTablePreallocateVWSuccess,
            ProfileEvents::SetTablePreallocateVWFailed);
    }

    void Catalog::getTablePreallocateVW(const UUID & table_uuid, String & vw)
    {
        runWithMetricSupport(
            [&] { meta_proxy->getTablePreallocateVW(name_space, UUIDHelpers::UUIDToString(table_uuid), vw); },
            ProfileEvents::GetTablePreallocateVWSuccess,
            ProfileEvents::GetTablePreallocateVWFailed);
    }

    /// APIs for MaterializedMySQL
    std::shared_ptr<Protos::MaterializedMySQLManagerMetadata> Catalog::getOrSetMaterializedMySQLManagerMetadata(const StorageID & storage_id)
    {
        auto res = meta_proxy->tryGetMaterializedMySQLManagerMetadata(name_space, storage_id.uuid);
        if (res)
            return res;

        res = std::make_shared<Protos::MaterializedMySQLManagerMetadata>();
        res->set_database_name(storage_id.database_name);
        RPCHelpers::fillUUID(storage_id.uuid, *res->mutable_database_uuid());
        res->set_dumped_first_time(false);

        meta_proxy->setMaterializedMySQLManagerMetadata(name_space, storage_id.uuid, *res);
        return res;
    }

    void Catalog::updateMaterializedMySQLManagerMetadata(const StorageID & storage_id, const Protos::MaterializedMySQLManagerMetadata & metadata)
    {
        meta_proxy->setMaterializedMySQLManagerMetadata(name_space, storage_id.uuid, metadata);
    }

    void Catalog::removeMaterializedMySQLManagerMetadata(const UUID & uuid)
    {
        meta_proxy->removeMaterializedMySQLManagerMetadata(name_space, uuid);
    }

    std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> Catalog::getMaterializedMySQLBinlogMetadata(const String & binlog_name)
    {
        return meta_proxy->getMaterializedMySQLBinlogMetadata(name_space, binlog_name);
    }

    void Catalog::setMaterializedMySQLBinlogMetadata(const String & binlog_name, const Protos::MaterializedMySQLBinlogMetadata & binlog_data)
    {
        meta_proxy->setMaterializedMySQLBinlogMetadata(name_space, binlog_name, binlog_data);
    }

    void Catalog::removeMaterializedMySQLBinlogMetadata(const String & binlog_name)
    {
        meta_proxy->removeMaterializedMySQLBinlogMetadata(name_space, binlog_name);
    }

    void Catalog::updateMaterializedMySQLMetadataInBatch(const Strings &keys, const Strings &values, const Strings &delete_keys)
    {
        meta_proxy->updateMaterializedMySQLMetadataInBatch(name_space, keys, values, delete_keys);
    }

    std::unordered_map<String, PartitionFullPtr>
    Catalog::getPartsInfoMetrics(const DB::Protos::DataModelTable & table, bool & is_ready)
    {
        std::unordered_map<String, PartitionFullPtr> res;
        runWithMetricSupport(
            [&] {
                auto storage = createTableFromDataModel(context, table);

                /// getTablePartitionMetrics can only be called on server, so the second condition should always be true
                if (storage && context.getPartCacheManager())
                {
                    is_ready = context.getPartCacheManager()->getPartsInfoMetrics(*storage, res);
                }
                else
                {
                    /// Table may already be marked as deleted, just return empty partition metrics and set is_ready `true`
                    is_ready = true;
                }
            },
            ProfileEvents::GetPartsInfoMetricsSuccess,
            ProfileEvents::GetPartsInfoMetricsFailed);
        return res;
    }

    PartitionMetrics::PartitionMetricsStore Catalog::getPartitionMetricsStoreFromMetastore(
        const String & table_uuid, const String & partition_id, size_t max_commit_time, std::function<bool()> need_abort)

    {
        auto calculate_metrics_by_partition = [&](ServerDataPartsVector & parts) {
            PartitionMetricsStorePtr res = std::make_shared<PartitionMetrics::PartitionMetricsStore>();

            for (auto & part : parts)
            {
                if (unlikely(need_abort()))
                {
                    LOG_WARNING(log, "getPartitionMetricsStoreFromMetastore is aborted by caller.");
                    break;
                }

                /// For those blocks only have deleted part, just ignore them because the covered part may be already removed by GC.
                /// But we should still calculate it's `last_modification_time`.
                res->updateLastModificationTime(part->part_model());
            }

            std::sort(parts.begin(), parts.end(), CnchPartsHelper::PartComparator<ServerDataPartPtr>{});
            auto visible_parts = CnchPartsHelper::calcVisibleParts(parts, false);

            for (auto & part : visible_parts)
            {
                if (unlikely(need_abort()))
                {
                    LOG_WARNING(log, "getPartitionMetricsStoreFromMetastore is aborted by caller.");
                    break;
                }

                res->update(part->part_model());
            }

            return res;
        };

        PartitionMetrics::PartitionMetricsStore ret;
        runWithMetricSupport(
            [&] {
                Stopwatch watch;
                SCOPE_EXIT({
                    LOG_DEBUG(
                        log, "getPartitionMetricsStoreFromMetastore for table {} elapsed: {} ms", table_uuid, watch.elapsedMilliseconds());
                });

                /// Get latest table version.
                StoragePtr storage = getTableByUUID(context, table_uuid, max_commit_time);
                const auto & merge_tree_storage = dynamic_cast<const MergeTreeMetaBase &>(*storage);

                IMetaStore::IteratorPtr it = meta_proxy->getPartsInRange(name_space, table_uuid, partition_id);

                ServerDataPartsVector parts;
                while (it->next())
                {
                    if (unlikely(need_abort()))
                    {
                        LOG_WARNING(log, "getPartitionMetricsStoreFromMetastore is aborted by caller.");
                        break;
                    }
                    Protos::DataModelPart part_model;
                    part_model.ParseFromString(it->value());

                    /// Skip the Uncommitted parts or the parts that
                    /// cannot be seen by the time `max_commit_time`.
                    if (part_model.commit_time() == 0 || part_model.commit_time() > max_commit_time)
                    {
                        LOG_TRACE(log, "Skip parts: {}, max_commit_time: {}", part_model.ShortDebugString(), max_commit_time);
                        continue;
                    }

                    parts.emplace_back(std::make_shared<ServerDataPart>(createPartWrapperFromModel(merge_tree_storage, std::move(part_model))));
                }

                if (!parts.empty())
                {
                    ret = *calculate_metrics_by_partition(parts);
                }
            },
            ProfileEvents::GetPartitionMetricsFromMetastoreSuccess,
            ProfileEvents::GetPartitionMetricsFromMetastoreFailed);
        return ret;
    }

    void Catalog::updateTopologies(const std::list<CnchServerTopology> & topologies)
    {
        runWithMetricSupport(
            [&] {
                LOG_TRACE(log, "Updating topologies : {}", DB::dumpTopologies(topologies));

                if (topologies.empty())
                {
                    return;
                }
                Protos::DataModelTopologyVersions topology_versions;
                fillTopologyVersions(topologies, *topology_versions.mutable_topologies());
                meta_proxy->updateTopologyMeta(topology_key, topology_versions.SerializeAsString());
            },
            ProfileEvents::UpdateTopologiesSuccess,
            ProfileEvents::UpdateTopologiesFailed);
    }

    std::list<CnchServerTopology> Catalog::getTopologies()
    {
        std::list<CnchServerTopology> res;
        runWithMetricSupport(
            [&] {
                String topology_meta = meta_proxy->getTopologyMeta(topology_key);
                if (topology_meta.empty())
                {
                    res = {};
                    return;
                }
                Protos::DataModelTopologyVersions topology_versions;
                topology_versions.ParseFromString(topology_meta);
                res = createTopologyVersionsFromModel(topology_versions.topologies());
            },
            ProfileEvents::GetTopologiesSuccess,
            ProfileEvents::GetTopologiesFailed);
        return res;
    }

    std::vector<std::shared_ptr<Protos::VersionedPartitions>> Catalog::getMvBaseTables(const String & uuid)
    {
        std::vector<std::shared_ptr<Protos::VersionedPartitions>> res;
        runWithMetricSupport(
            [&] {
                    meta_proxy->getMvMetaVersion(name_space, uuid);
                    res = meta_proxy->getMvBaseTables(name_space, uuid);
                },
            ProfileEvents::GetMvBaseTableIDSuccess,
            ProfileEvents::GetMvBaseTableIDFailed);
        return res;
    }

    String Catalog::getMvMetaVersion(const String & uuid)
    {
        String res;
        runWithMetricSupport(
            [&] {
                    res = meta_proxy->getMvMetaVersion(name_space, uuid);
                },
            ProfileEvents::GetMvBaseTableVersionSuccess,
            ProfileEvents::GetMvBaseTableVersionFailed);
        return res;
    }

    BatchCommitRequest Catalog::constructMvMetaRequests(const String & uuid,
        std::vector<std::shared_ptr<Protos::VersionedPartition>> add_partitions,
        std::vector<std::shared_ptr<Protos::VersionedPartition>> drop_partitions,
        String mv_version_ts)
    {
        return meta_proxy->constructMvMetaRequests(name_space, uuid, add_partitions, drop_partitions, mv_version_ts);
    }

    void Catalog::updateMvMeta(const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions)
    {
        runWithMetricSupport(
            [&] { meta_proxy->updateMvMeta(name_space, uuid, versioned_partitions); },
            ProfileEvents::UpdateMvMetaIDSuccess,
            ProfileEvents::UpdateMvMetaIDFailed);
    }
    void Catalog::dropMvMeta(const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions)
    {
        runWithMetricSupport(
            [&] { meta_proxy->dropMvMeta(name_space, uuid, versioned_partitions); },
            ProfileEvents::UpdateMvMetaIDSuccess,
            ProfileEvents::UpdateMvMetaIDFailed);
    }

    void Catalog::cleanMvMeta(const String & uuid)
    {
        runWithMetricSupport(
            [&] { meta_proxy->cleanMvMeta(name_space, uuid); },
            ProfileEvents::UpdateMvMetaIDSuccess,
            ProfileEvents::UpdateMvMetaIDFailed);
    }

    std::vector<UInt64> Catalog::getTrashDBVersions(const String & database)
    {
        std::vector<UInt64> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTrashDBVersions(name_space, database); },
            ProfileEvents::GetTrashDBVersionsSuccess,
            ProfileEvents::GetTrashDBVersionsFailed);
        return res;
    }

    void Catalog::undropDatabase(const String & database, const UInt64 & ts)
    {
        runWithMetricSupport(
            [&] {
                auto db_model_ptr = tryGetDatabaseFromMetastore(database, ts);
                if (!db_model_ptr)
                    throw Exception("Cannot find database in trash to restore.", ErrorCodes::UNKNOWN_DATABASE);

                // delete the record from db trash as well as the corresponding version of db meta.
                BatchCommitRequest batch_writes;
                batch_writes.AddDelete(MetastoreProxy::dbTrashKey(name_space, database, ts));
                batch_writes.AddDelete(MetastoreProxy::dbKey(name_space, database, ts));

                // restore table and dictionary
                String trashDBName = database + "_" + toString(ts);
                auto table_id_ptrs = meta_proxy->getTablesFromTrash(name_space, trashDBName);
                auto dic_ptrs = meta_proxy->getDictionariesFromTrash(name_space, trashDBName);

                for (auto & table_id_ptr : table_id_ptrs)
                {
                    table_id_ptr->set_database(database);
                    batch_writes.AddDelete(MetastoreProxy::tableTrashKey(name_space, trashDBName, table_id_ptr->name(), ts));
                    restoreTableFromTrash(table_id_ptr, ts, batch_writes);
                }

                for (auto & dic_ptr : dic_ptrs)
                {
                    batch_writes.AddDelete(MetastoreProxy::dictionaryTrashKey(name_space, trashDBName, dic_ptr->name()));
                    batch_writes.AddPut(SinglePutRequest(
                        MetastoreProxy::dictionaryStoreKey(name_space, database, dic_ptr->name()), dic_ptr->SerializeAsString()));
                }

                BatchCommitResponse resp;
                meta_proxy->batchWrite(batch_writes, resp);
            },
            ProfileEvents::UndropDatabaseSuccess,
            ProfileEvents::UndropDatabaseFailed);
    }

    std::map<UInt64, std::shared_ptr<Protos::TableIdentifier>> Catalog::getTrashTableVersions(const String & database, const String & table)
    {
        std::map<UInt64, std::shared_ptr<Protos::TableIdentifier>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTrashTableVersions(name_space, database, table); },
            ProfileEvents::GetTrashTableVersionsSuccess,
            ProfileEvents::GetTrashTableVersionsFailed);
        return res;
    }

    void Catalog::undropTable(const String & database, const String & table, const UInt64 & ts)
    {
        runWithMetricSupport(
            [&] {
                auto trash_id_ptr = meta_proxy->getTrashTableID(name_space, database, table, ts);
                if (!trash_id_ptr)
                    throw Exception("Cannot find table in trash to restore", ErrorCodes::UNKNOWN_TABLE);

                BatchCommitRequest batch_writes;
                restoreTableFromTrash(trash_id_ptr, ts, batch_writes);
                BatchCommitResponse resp;
                meta_proxy->batchWrite(batch_writes, resp);
            },
            ProfileEvents::UndropTableSuccess,
            ProfileEvents::UndropTableFailed);
    }

    std::shared_ptr<Protos::DataModelDB> Catalog::tryGetDatabaseFromMetastore(const String & database, const UInt64 & ts)
    {
        Strings databases_meta;
        meta_proxy->getDatabase(name_space, database, databases_meta);

        std::map<UInt64, std::shared_ptr<DB::Protos::DataModelDB>, std::greater<>> db_versions;

        for (auto & meta : databases_meta)
        {
            auto model = std::make_shared<DB::Protos::DataModelDB>();
            model->ParseFromString(meta);
            if (model->commit_time() < ts)
                db_versions.emplace(model->commit_time(), model);
        }

        if (!db_versions.empty() && !Status::isDeleted(db_versions.begin()->second->status()))
            return db_versions.begin()->second;

        return {};
    }

    std::shared_ptr<Protos::DataModelTable> Catalog::tryGetTableFromMetastore(const String & table_uuid, const UInt64 & ts, bool with_prev_versions, bool with_deleted)
    {
        Strings tables_meta;
        meta_proxy->getTableByUUID(name_space, table_uuid, tables_meta);
        if (tables_meta.empty())
            return {};

        /// get latest table name. we may replace previous version's table name with the latest ones.
        DB::Protos::DataModelTable latest_version;
        latest_version.ParseFromString(tables_meta.back());
        const String & latest_db_name = latest_version.database();
        const String & latest_table_name = latest_version.name();
        const UInt64 & latest_version_commit_ts =  latest_version.commit_time();

        std::vector<std::shared_ptr<DB::Protos::DataModelTable>> table_versions;
        for (auto & table_meta : tables_meta)
        {
            std::shared_ptr<DB::Protos::DataModelTable> model(new DB::Protos::DataModelTable);
            model->ParseFromString(table_meta);
            if (model->commit_time() <= ts)
            {
                if (model->database() != latest_db_name || model->name() != latest_table_name)
                    replace_definition(*model, latest_db_name, latest_table_name);
                table_versions.push_back(model);
            }
            else
                break;
        }

        if (table_versions.empty() || (Status::isDeleted(table_versions.back()->status()) && !with_deleted))
            return {};

        auto & table = table_versions.back();
        table->set_latest_version(latest_version_commit_ts);

        /// collect all previous version's definition
        if (with_prev_versions)
        {
            table->clear_definitions();
            for (size_t i = 0; i < table_versions.size() - 1; i++)
            {
                auto added_definition = table->add_definitions();
                added_definition->set_commit_time(table_versions[i]->commit_time());
                added_definition->set_definition(table_versions[i]->definition());
            }
        }

        return table;
    }

    // MaskingPolicyExists Catalog::maskingPolicyExists(const Strings & masking_policy_names)
    // {
    //     MaskingPolicyExists outRes;
    //     runWithMetricSupport(
    //         [&] {
    //             MaskingPolicyExists res(masking_policy_names.size());
    //             if (masking_policy_names.empty())
    //             {
    //                 outRes = res;
    //             }
    //             auto policies = meta_proxy->getMaskingPolicies(name_space, masking_policy_names);
    //             for (const auto i : ext::range(0, res.size()))
    //                 res.set(i, !policies[i].empty());
    //             outRes = res;
    //         },
    //         ProfileEvents::MaskingPolicyExistsSuccess,
    //         ProfileEvents::MaskingPolicyExistsFailed);
    //     return outRes;
    // }

    // std::vector<std::optional<MaskingPolicyModel>> Catalog::getMaskingPolicies(const Strings & masking_policy_names)
    // {
    //     std::vector<std::optional<MaskingPolicyModel>> outRes;
    //     runWithMetricSupport(
    //         [&] {
    //             if (masking_policy_names.empty())
    //             {
    //                 outRes = {};
    //                 return;
    //             }
    //             std::vector<std::optional<MaskingPolicyModel>> res(masking_policy_names.size());
    //             auto policies = meta_proxy->getMaskingPolicies(name_space, masking_policy_names);
    //             for (const auto i : ext::range(0, res.size()))
    //             {
    //                 if (!policies[i].empty())
    //                 {
    //                     MaskingPolicyModel model;
    //                     model.ParseFromString(policies[i]);
    //                     res[i] = std::move(model);
    //                 }
    //             }
    //             outRes = res;
    //         },
    //         ProfileEvents::GetMaskingPoliciesSuccess,
    //         ProfileEvents::GetMaskingPoliciesFailed);
    //     return outRes;
    // }

    // void Catalog::putMaskingPolicy(MaskingPolicyModel & masking_policy)
    // {
    //     runWithMetricSupport(
    //         [&] { meta_proxy->putMaskingPolicy(name_space, masking_policy); },
    //         ProfileEvents::PutMaskingPolicySuccess,
    //         ProfileEvents::PutMaskingPolicyFailed);
    // }

    // std::optional<MaskingPolicyModel> Catalog::tryGetMaskingPolicy(const String & masking_policy_name)
    // {
    //     std::optional<MaskingPolicyModel> outRes;
    //     runWithMetricSupport(
    //         [&] {
    //             String data = meta_proxy->getMaskingPolicy(name_space, masking_policy_name);
    //             if (data.empty())
    //             {
    //                 outRes = {};
    //                 return;
    //             }
    //             MaskingPolicyModel mask;
    //             mask.ParseFromString(std::move(data));
    //             outRes = mask;
    //         },
    //         ProfileEvents::TryGetMaskingPolicySuccess,
    //         ProfileEvents::TryGetMaskingPolicyFailed);
    //     return outRes;
    // }

    // MaskingPolicyModel Catalog::getMaskingPolicy(const String & masking_policy_name)
    // {
    //     MaskingPolicyModel res;
    //     runWithMetricSupport(
    //         [&] {
    //             auto mask = tryGetMaskingPolicy(masking_policy_name);
    //             if (mask)
    //             {
    //                 res = *mask;
    //                 return;
    //             }
    //             throw Exception("Masking policy not found: " + masking_policy_name, ErrorCodes::UNKNOWN_MASKING_POLICY_NAME);
    //         },
    //         ProfileEvents::GetMaskingPolicySuccess,
    //         ProfileEvents::GetMaskingPolicyFailed);
    //     return res;
    // }

    // std::vector<MaskingPolicyModel> Catalog::getAllMaskingPolicy()
    // {
    //     std::vector<MaskingPolicyModel> masks;
    //     runWithMetricSupport(
    //         [&] {
    //             Strings data = meta_proxy->getAllMaskingPolicy(name_space);

    //             masks.reserve(data.size());

    //             for (const auto & s : data)
    //             {
    //                 MaskingPolicyModel model;
    //                 model.ParseFromString(std::move(s));
    //                 masks.push_back(std::move(model));
    //             }
    //         },
    //         ProfileEvents::GetAllMaskingPolicySuccess,
    //         ProfileEvents::GetAllMaskingPolicyFailed);
    //     return masks;
    // }

    // Strings Catalog::getMaskingPolicyAppliedTables(const String & masking_policy_name)
    // {
    //     Strings res;
    //     runWithMetricSupport(
    //         [&] {
    //             auto it = meta_proxy->getMaskingPolicyAppliedTables(name_space, masking_policy_name);
    //             while (it->next())
    //                 res.push_back(std::move(it->value()));
    //         },
    //         ProfileEvents::GetMaskingPolicyAppliedTablesSuccess,
    //         ProfileEvents::GetMaskingPolicyAppliedTablesFailed);
    //     return res;
    // }

    // Strings Catalog::getAllMaskingPolicyAppliedTables()
    // {
    //     Strings res;
    //     runWithMetricSupport(
    //         [&] { res = getMaskingPolicyAppliedTables(""); },
    //         ProfileEvents::GetAllMaskingPolicyAppliedTablesSuccess,
    //         ProfileEvents::GetAllMaskingPolicyAppliedTablesFailed);
    //     return res;
    // }

    // void Catalog::dropMaskingPolicies(const Strings & masking_policy_names)
    // {
    //     runWithMetricSupport(
    //         [&] { meta_proxy->dropMaskingPolicies(name_space, masking_policy_names); },
    //         ProfileEvents::DropMaskingPoliciesSuccess,
    //         ProfileEvents::DropMaskingPoliciesFailed);
    // }

    Strings Catalog::tryGetDependency(const ASTPtr & ast)
    {
        ASTCreateQuery * create_ast = ast->as<ASTCreateQuery>();

        Strings res;
        /// get the uuid of the table on which this view depends.
        if (create_ast && create_ast->isView())
        {
            ASTs tables;
            bool has_table_func = false;
            ASTSelectQuery::collectAllTables(create_ast->select, tables, has_table_func);
            if (!tables.empty())
            {
                std::unordered_set<String> table_set{};
                for (size_t i = 0; i < tables.size(); i++)
                {
                    DatabaseAndTableWithAlias table(tables[i]);
                    String qualified_name = table.database + "." + table.table;
                    /// do not add a dependency twice
                    if (table_set.count(qualified_name))
                        continue;
                    table_set.insert(qualified_name);
                    String dependency_uuid = meta_proxy->getTableUUID(name_space, table.database, table.table);
                    if (!dependency_uuid.empty())
                        res.emplace_back(std::move(dependency_uuid));
                }
            }
        }
        return res;
    }

    void Catalog::replace_definition(Protos::DataModelTable & table, const String & db_name, const String & table_name)
    {
        const String & definition = table.definition();
        const char * begin = definition.data();
        const char * end = begin + definition.size();
        ParserQuery parser(end);
        ASTPtr ast = parseQuery(parser, begin, end, "", 0, 0);
        ASTCreateQuery * create_ast = ast->as<ASTCreateQuery>();
        create_ast->database = db_name;
        create_ast->table = table_name;

        table.set_definition(queryToString(ast));
        table.set_database(db_name);
        table.set_name(table_name);
    }

    StoragePtr Catalog::createTableFromDataModel(const Context & context, const Protos::DataModelTable & data_model)
    {
        StoragePtr res = CatalogFactory::getTableByDataModel(Context::createCopy(context.shared_from_this()), &data_model);
        return res;
    }

    void Catalog::detachOrAttachTable(const String & db, const String & name, const TxnTimestamp & ts, bool is_detach)
    {
        auto table_id = meta_proxy->getTableID(name_space, db, name);
        const String & table_uuid = table_id->uuid();
        if (table_uuid.empty())
            throw Exception("Table not found.", ErrorCodes::UNKNOWN_TABLE);

        /// get latest table version.
        auto table = tryGetTableFromMetastore(table_uuid, ts.toUInt64());

        if (table)
        {
            /// detach or attach table
            if (is_detach)
            {
                table_id->set_detached(true);
                table->set_status(Status::setDetached(table->status()));
            }
            else
            {
                table_id->set_detached(false);
                table->set_status(Status::setAttached(table->status()));
            }
            /// directly rewrite the old table metadata rather than adding a new version
            meta_proxy->updateTableWithID(name_space, *table_id, *table);
            if (auto cache_manager = context.getPartCacheManager(); cache_manager)
                cache_manager->removeStorageCache(db, name);
        }
        else
        {
            throw Exception("Cannot get table metadata for UUID : " + table_uuid, ErrorCodes::UNKNOWN_TABLE);
        }
    }

    void Catalog::detachOrAttachDictionary(const String & database, const String & name, bool is_detach)
    {
        String dic_meta;
        meta_proxy->getDictionary(name_space, database, name, dic_meta);

        if (dic_meta.empty())
            throw Exception("Dictionary " + database + "." + name + " doesn't  exists.", ErrorCodes::DICTIONARY_NOT_EXIST);

        Protos::DataModelDictionary dic_model;
        dic_model.ParseFromString(dic_meta);

        if (is_detach)
            dic_model.set_status(Status::setDetached(dic_model.status()));
        else
            dic_model.set_status(Status::setAttached(dic_model.status()));

        dic_model.set_last_modification_time(Poco::Timestamp().raw());

        meta_proxy->createDictionary(name_space, database, name, dic_model.SerializeAsString());
    }

    DataModelPartWithNameVector Catalog::getDataPartsMetaFromMetastore(
        const ConstStoragePtr & storage, const Strings & required_partitions, const Strings & full_partitions, const TxnTimestamp & ts, bool from_trash)
    {
        String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
        String part_meta_prefix = MetastoreProxy::dataPartPrefix(name_space, uuid);
        auto create_func = [&](const String & key, const String & value) {
            Protos::DataModelPart part_model;
            part_model.ParseFromString(value);
            DataModelPartWithNamePtr res = nullptr;
            if (ts.toUInt64() && part_model.has_commit_time() && TxnTimestamp{part_model.commit_time()} > ts)
                return res;
            // compatible with old parts from alpha, old part doesn't have commit time field, the mutation is its commit time
            else if (ts.toUInt64() && !part_model.has_commit_time() && static_cast<UInt64>(part_model.part_info().mutation()) > ts)
                return res;
            String part_name = key.substr(part_meta_prefix.size(), std::string::npos);
            DataModelPartPtr partmodel_ptr = std::make_shared<Protos::DataModelPart>(std::move(part_model));
            res = std::make_shared<DataModelPartWithName>(std::move(part_name), std::move(partmodel_ptr));
            return res;

        };

        UInt32 time_out_ms = 1000 * (context.getSettingsRef().cnch_fetch_parts_timeout.totalSeconds());

        String meta_prefix = from_trash
            ? MetastoreProxy::trashItemsPrefix(name_space, uuid) + PART_STORE_PREFIX
            : part_meta_prefix;

        return getDataModelsByPartitions<DataModelPartWithNamePtr>(
            storage,
            meta_prefix,
            required_partitions,
            full_partitions,
            create_func,
            ts,
            time_out_ms);
    }

    DeleteBitmapMetaPtrVector Catalog::getDeleteBitmapByKeys(const StoragePtr & storage, const NameSet & keys)
    {
        DeleteBitmapMetaPtrVector res;
        runWithMetricSupport(
            [&] {
                if (keys.empty())
                    return;
                Strings full_keys;
                for (const auto & key : keys)
                {
                    String full_key
                        = MetastoreProxy::deleteBitmapPrefix(name_space, UUIDHelpers::UUIDToString(storage->getStorageID().uuid)) + key;
                    full_keys.emplace_back(full_key);
                }

                auto metas = meta_proxy->getDeleteBitmapByKeys(full_keys);
                for (const auto & meta : metas)
                {
                    // NOTE: If there are no data for a key, we simply ignores it here.
                    if (!meta.empty())
                    {
                        DataModelDeleteBitmapPtr model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
                        model_ptr->ParseFromString(meta);
                        auto & merge_tree_storage = dynamic_cast<MergeTreeMetaBase &>(*storage);
                        res.push_back(std::make_shared<DeleteBitmapMeta>(merge_tree_storage, model_ptr));
                    }
                }
            },
            ProfileEvents::GetDeleteBitmapByKeysSuccess,
            ProfileEvents::GetDeleteBitmapByKeysFailed);
        return res;
    }

    DeleteBitmapMetaPtrVector Catalog::getDeleteBitmapsInPartitionsFromMetastore(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, VisibilityLevel visibility)
    {
        return getDeleteBitmapsInPartitionsImpl(storage, partitions, ts, /*from_trash*/ false, visibility);
    }

    DeleteBitmapMetaPtrVector Catalog::getTrashedDeleteBitmapsInPartitions(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, VisibilityLevel visibility)
    {
        return getDeleteBitmapsInPartitionsImpl(storage, partitions, ts, /*from_trash*/ true, visibility);
    }

    DeleteBitmapMetaPtrVector
    Catalog::getDeleteBitmapsInPartitionsImpl(const ConstStoragePtr & storage, const Strings & partitions, const TxnTimestamp & ts, bool from_trash, VisibilityLevel visibility)
    {
        DeleteBitmapMetaPtrVector res;
        runWithMetricSupport(
            [&] {
                const auto & merge_tree_storage = dynamic_cast<const MergeTreeMetaBase &>(*storage);

                auto create_func = [&](const String &, const String & meta) -> DeleteBitmapMetaPtr {
                    DataModelDeleteBitmapPtr model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
                    model_ptr->ParseFromString(meta);
                    if (ts.toUInt64() && model_ptr->has_commit_time() && TxnTimestamp{model_ptr->commit_time()} > ts)
                        return nullptr;
                    return std::make_shared<DeleteBitmapMeta>(merge_tree_storage, model_ptr);
                };

                Strings all_partitions = getPartitionIDsFromMetastore(storage);
                String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
                String meta_prefix = from_trash
                    ? MetastoreProxy::trashItemsPrefix(name_space, uuid) + DELETE_BITMAP_PREFIX
                    : MetastoreProxy::deleteBitmapPrefix(name_space, uuid);

                res = getDataModelsByPartitions<DeleteBitmapMetaPtr>(
                    storage,
                    meta_prefix,
                    partitions,
                    all_partitions,
                    create_func,
                    ts);

                if (visibility != VisibilityLevel::All)
                {
                    auto * txn_record_cache =
                        context.getServerType() == ServerType::cnch_server ? context.getCnchTransactionCoordinator().getFinishedOrFailedTxnRecordCache() : nullptr;
                    /// filter out invisible bitmaps (uncommitted or invisible to current txn)
                    getVisibleBitmaps(res, ts, this, nullptr, txn_record_cache);
                }
            },
            ProfileEvents::GetDeleteBitmapsInPartitionsSuccess,
            ProfileEvents::GetDeleteBitmapsInPartitionsFailed);
        return res;
    }

    void Catalog::getKafkaOffsets(const String & consumer_group, cppkafka::TopicPartitionList & tpl)
    {
        runWithMetricSupport(
            [&] { meta_proxy->getKafkaTpl(name_space, consumer_group, tpl); },
            ProfileEvents::GetKafkaOffsetsVoidSuccess,
            ProfileEvents::GetKafkaOffsetsVoidFailed);
    }

    cppkafka::TopicPartitionList Catalog::getKafkaOffsets(const String & consumer_group, const String & kafka_topic)
    {
        cppkafka::TopicPartitionList res;

        runWithMetricSupport(
            [&] { res = meta_proxy->getKafkaTpl(name_space, consumer_group, kafka_topic); },
            ProfileEvents::GetKafkaOffsetsTopicPartitionListSuccess,
            ProfileEvents::GetKafkaOffsetsTopicPartitionListFailed);
        return res;
    }

    void Catalog::clearOffsetsForWholeTopic(const String & topic, const String & consumer_group)
    {
        runWithMetricSupport(
            [&] { meta_proxy->clearOffsetsForWholeTopic(name_space, topic, consumer_group); },
            ProfileEvents::ClearOffsetsForWholeTopicSuccess,
            ProfileEvents::ClearOffsetsForWholeTopicFailed);
    }

    void Catalog::setTransactionForKafkaConsumer(const UUID & uuid, const TxnTimestamp & txn_id, const size_t consumer_index)
    {
        runWithMetricSupport(
            [&] { meta_proxy->setTransactionForKafkaConsumer(name_space, UUIDHelpers::UUIDToString(uuid), txn_id, consumer_index); },
            ProfileEvents::SetTransactionForKafkaConsumerSuccess,
            ProfileEvents::SetTransactionForKafkaConsumerFailed);
    }

    TxnTimestamp Catalog::getTransactionForKafkaConsumer(const UUID & uuid, const size_t consumer_index)
    {
        TxnTimestamp txn = TxnTimestamp::maxTS();
        runWithMetricSupport(
            [&] { txn = meta_proxy->getTransactionForKafkaConsumer(name_space, UUIDHelpers::UUIDToString(uuid), consumer_index); },
            ProfileEvents::GetTransactionForKafkaConsumerSuccess,
            ProfileEvents::GetTransactionForKafkaConsumerFailed);

        return txn;
    }

    void Catalog::clearKafkaTransactions(const UUID & uuid)
    {
        runWithMetricSupport(
            [&] { meta_proxy->clearKafkaTransactions(name_space, UUIDHelpers::UUIDToString(uuid)); },
            ProfileEvents::ClearKafkaTransactionsForTableSuccess,
            ProfileEvents::ClearKafkaTransactionsForTableFailed);
    }

    Catalog::DataModelTables Catalog::getTableHistories(const String & table_uuid)
    {
        DataModelTables res;
        runWithMetricSupport(
            [&] {
                Strings tables_meta;
                meta_proxy->getTableByUUID(name_space, table_uuid, tables_meta);
                for (const auto & meta : tables_meta)
                {
                    DB::Protos::DataModelTable model;
                    model.ParseFromString(meta);
                    res.push_back(model);
                }
            },
            ProfileEvents::GetTableHistoriesSuccess,
            ProfileEvents::GetTableHistoriesFailed);
        return res;
    }

    Catalog::DataModelTables Catalog::getTablesByIDs(const std::vector<std::shared_ptr<Protos::TableIdentifier>> & identifiers)
    {
        DataModelTables res;
        runWithMetricSupport(
            [&] {
                for (auto & identifier : identifiers)
                {
                    const String & database_name = identifier->database();
                    const String & table_name = identifier->name();
                    const String & uuid = identifier->uuid();

                    Strings tables_meta;
                    meta_proxy->getTableByUUID(name_space, uuid, tables_meta);

                    if (!tables_meta.empty())
                    {
                        DB::Protos::DataModelTable table_data;
                        table_data.ParseFromString(tables_meta.back());
                        replace_definition(table_data, database_name, table_name);
                        res.push_back(table_data);
                    }
                }
            },
            ProfileEvents::GetTablesByIDSuccess,
            ProfileEvents::GetTablesByIDFailed);
        return res;
    }

    void Catalog::moveTableIntoTrash(
        Protos::DataModelTable & table,
        Protos::TableIdentifier & table_id,
        const TxnTimestamp & txnID,
        const TxnTimestamp & ts,
        BatchCommitRequest & batch_write)
    {
        if (table.commit_time() >= ts.toUInt64())
            throw Exception("Cannot drop table with an old timestamp.", ErrorCodes::CATALOG_SERVICE_INTERNAL_ERROR);

        table.clear_definitions();
        table.set_txnid(txnID.toUInt64());
        table.set_commit_time(ts.toUInt64());
        /// mark table as deleted
        table.set_status(Status::setDelete(table.status()));

        Strings dependencies = tryGetDependency(parseCreateQuery(table.definition()));

        /// remove dependency
        for (const String & dependency : dependencies)
            batch_write.AddDelete(MetastoreProxy::viewDependencyKey(name_space, dependency, table_id.uuid()));

        addPotentialLargeKVToBatchwrite(
            meta_proxy->getMetastore(),
            batch_write,
            name_space,
            MetastoreProxy::tableStoreKey(name_space, table_id.uuid(), ts.toUInt64()),
            table.SerializeAsString());
        // use database name and table name in table_id is required because it may different with that in table data model.
        batch_write.AddPut(SinglePutRequest(
            MetastoreProxy::tableTrashKey(name_space, table_id.database(), table_id.name(), ts.toUInt64()), table_id.SerializeAsString()));
        batch_write.AddDelete(MetastoreProxy::tableUUIDMappingKey(name_space, table.database(), table.name()));
    }

    void Catalog::restoreTableFromTrash(
        std::shared_ptr<Protos::TableIdentifier> table_id, const UInt64 & ts, BatchCommitRequest & batch_write)
    {
        auto table_model = tryGetTableFromMetastore(table_id->uuid(), UINT64_MAX, false, true);

        if (!table_model)
            throw Exception("Cannot found table meta with uuid " + table_id->uuid() + ".", ErrorCodes::UNKNOWN_TABLE);

        /// 1. remove trash record;
        /// 2.add table->uuid mapping;
        /// 3. remove last version of table meta(which is marked as delete);
        /// 4. try rebuild dependencies if any
        batch_write.AddDelete(MetastoreProxy::tableTrashKey(name_space, table_id->database(), table_id->name(), ts));
        batch_write.AddDelete(MetastoreProxy::tableStoreKey(name_space, table_id->uuid(), table_model->commit_time()));
        batch_write.AddPut(SinglePutRequest(
            MetastoreProxy::tableUUIDMappingKey(name_space, table_id->database(), table_id->name()),
            table_id->SerializeAsString(), true));
        Strings dependencies = tryGetDependency(parseCreateQuery(table_model->definition()));
        if (!dependencies.empty())
        {
            for (const String & dependency : dependencies)
                batch_write.AddPut(SinglePutRequest(MetastoreProxy::viewDependencyKey(name_space, dependency, table_id->uuid()), table_id->uuid()));
        }
    }

    void Catalog::createVirtualWarehouse(const String & vw_name, const VirtualWarehouseData & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->createVirtualWarehouse(name_space, vw_name, data); },
            ProfileEvents::CreateVirtualWarehouseSuccess,
            ProfileEvents::CreateVirtualWarehouseFailed);
    }

    void Catalog::alterVirtualWarehouse(const String & vw_name, const VirtualWarehouseData & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->alterVirtualWarehouse(name_space, vw_name, data); },
            ProfileEvents::AlterVirtualWarehouseSuccess,
            ProfileEvents::AlterVirtualWarehouseFailed);
    }

    bool Catalog::tryGetVirtualWarehouse(const String & vw_name, VirtualWarehouseData & data)
    {
        bool res;
        runWithMetricSupport(
            [&] { res = meta_proxy->tryGetVirtualWarehouse(name_space, vw_name, data); },
            ProfileEvents::TryGetVirtualWarehouseSuccess,
            ProfileEvents::TryGetVirtualWarehouseFailed);
        return res;
    }

    std::vector<VirtualWarehouseData> Catalog::scanVirtualWarehouses()
    {
        std::vector<VirtualWarehouseData> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->scanVirtualWarehouses(name_space); },
            ProfileEvents::TryGetVirtualWarehouseSuccess,
            ProfileEvents::ScanVirtualWarehousesFailed);
        return res;
    }

    void Catalog::dropVirtualWarehouse(const String & vm_name)
    {
        runWithMetricSupport(
            [&] { meta_proxy->dropVirtualWarehouse(name_space, vm_name); },
            ProfileEvents::DropVirtualWarehouseSuccess,
            ProfileEvents::DropVirtualWarehouseFailed);
    }

    void Catalog::createWorkerGroup(const String & worker_group_id, const WorkerGroupData & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->createWorkerGroup(name_space, worker_group_id, data); },
            ProfileEvents::CreateWorkerGroupSuccess,
            ProfileEvents::CreateWorkerGroupFailed);
    }

    void Catalog::updateWorkerGroup(const String & worker_group_id, const WorkerGroupData & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->updateWorkerGroup(name_space, worker_group_id, data); },
            ProfileEvents::UpdateWorkerGroupSuccess,
            ProfileEvents::UpdateWorkerGroupFailed);
    }

    bool Catalog::tryGetWorkerGroup(const String & worker_group_id, WorkerGroupData & data)
    {
        bool res;
        runWithMetricSupport(
            [&] { res = meta_proxy->tryGetWorkerGroup(name_space, worker_group_id, data); },
            ProfileEvents::TryGetWorkerGroupSuccess,
            ProfileEvents::TryGetWorkerGroupFailed);
        return res;
    }

    std::vector<WorkerGroupData> Catalog::scanWorkerGroups()
    {
        std::vector<WorkerGroupData> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->scanWorkerGroups(name_space); },
            ProfileEvents::ScanWorkerGroupsSuccess,
            ProfileEvents::ScanWorkerGroupsFailed);
        return res;
    }

    void Catalog::dropWorkerGroup(const String & worker_group_id)
    {
        runWithMetricSupport(
            [&] { meta_proxy->dropWorkerGroup(name_space, worker_group_id); },
            ProfileEvents::DropWorkerGroupSuccess,
            ProfileEvents::DropWorkerGroupFailed);
    }

    String Catalog::getInsertionLabelKey(const InsertionLabelPtr & label)
    {
        String res;
        runWithMetricSupport(
            [&] { res = MetastoreProxy::insertionLabelKey(name_space, toString(label->table_uuid), label->name); },
            ProfileEvents::GetInsertionLabelKeySuccess,
            ProfileEvents::GetInsertionLabelKeyFailed);
        return res;
    }

    void Catalog::precommitInsertionLabel(const InsertionLabelPtr & label)
    {
        runWithMetricSupport(
            [&] { meta_proxy->precommitInsertionLabel(name_space, label); },
            ProfileEvents::PrecommitInsertionLabelSuccess,
            ProfileEvents::PrecommitInsertionLabelFailed);
    }

    void Catalog::commitInsertionLabel(InsertionLabelPtr & label)
    {
        runWithMetricSupport(
            [&] { meta_proxy->commitInsertionLabel(name_space, label); },
            ProfileEvents::CommitInsertionLabelSuccess,
            ProfileEvents::CommitInsertionLabelFailed);
    }

    void Catalog::tryCommitInsertionLabel(InsertionLabelPtr & label)
    {
        runWithMetricSupport(
            [&] {
                auto label_in_kv = getInsertionLabel(label->table_uuid, label->name);
                if (!label_in_kv)
                    throw Exception("Label " + label->name + " not found", ErrorCodes::LOGICAL_ERROR);

                if (label_in_kv->status == InsertionLabel::Committed)
                {
                    return;
                }
                if (label->serializeValue() != label->serializeValue())
                    throw Exception("Label " + label->name + " has been changed", ErrorCodes::LOGICAL_ERROR);

                commitInsertionLabel(label);
            },
            ProfileEvents::TryCommitInsertionLabelSuccess,
            ProfileEvents::TryCommitInsertionLabelFailed);
    }

    bool Catalog::abortInsertionLabel(const InsertionLabelPtr & label, String & message)
    {
        bool res;
        runWithMetricSupport(
            [&] {
                auto [version, label_in_kv] = meta_proxy->getInsertionLabel(name_space, toString(label->table_uuid), label->name);
                if (label_in_kv.empty())
                {
                    message = "Label " + label->name + " not found.";
                    res = false;
                    return;
                }

                if (label_in_kv != label->serializeValue())
                {
                    message = "Label " + label->name + " has been changed.";
                    res = false;
                    return;
                }

                meta_proxy->removeInsertionLabel(name_space, toString(label->table_uuid), label->name, version);
                res = false;
            },
            ProfileEvents::AbortInsertionLabelSuccess,
            ProfileEvents::AbortInsertionLabelFailed);
        return res;
    }

    InsertionLabelPtr Catalog::getInsertionLabel(UUID uuid, const String & name)
    {
        InsertionLabelPtr res;
        runWithMetricSupport(
            [&] {
                auto [_, value] = meta_proxy->getInsertionLabel(name_space, toString(uuid), name);
                if (value.empty())
                {
                    res = nullptr;
                    return;
                }


                auto label = std::make_shared<InsertionLabel>(uuid, name);
                label->parseValue(value);
                res = label;
            },
            ProfileEvents::GetInsertionLabelSuccess,
            ProfileEvents::GetInsertionLabelFailed);
        return res;
    }

    void Catalog::removeInsertionLabel(UUID uuid, const String & name)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeInsertionLabel(name_space, toString(uuid), name); },
            ProfileEvents::RemoveInsertionLabelSuccess,
            ProfileEvents::RemoveInsertionLabelFailed);
    }

    void Catalog::removeInsertionLabels(const std::vector<InsertionLabel> & labels)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeInsertionLabels(name_space, labels); },
            ProfileEvents::RemoveInsertionLabelsSuccess,
            ProfileEvents::RemoveInsertionLabelsFailed);
    }

    std::vector<InsertionLabel> Catalog::scanInsertionLabels(UUID uuid)
    {
        std::vector<InsertionLabel> res;
        runWithMetricSupport(
            [&] {
                auto it = meta_proxy->scanInsertionLabels(name_space, toString(uuid));
                while (it->next())
                {
                    auto && key = it->key();
                    auto pos = key.rfind('_');
                    if (pos == std::string::npos)
                        continue;

                    res.emplace_back();
                    res.back().name = key.substr(pos + 1);
                    res.back().table_uuid = uuid;
                    res.back().parseValue(it->value());
                }
            },
            ProfileEvents::ScanInsertionLabelsFailed,
            ProfileEvents::ScanInsertionLabelsSuccess);
        return res;
    }

    void Catalog::clearInsertionLabels(UUID uuid)
    {
        runWithMetricSupport(
            [&] { meta_proxy->clearInsertionLabels(name_space, toString(uuid)); },
            ProfileEvents::ClearInsertionLabelsSuccess,
            ProfileEvents::ClearInsertionLabelsFailed);
    }

    void Catalog::updateTableStatistics(const String & uuid, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->updateTableStatistics(name_space, uuid, data); },
            ProfileEvents::UpdateTableStatisticsSuccess,
            ProfileEvents::UpdateTableStatisticsFailed);
    }

    std::unordered_map<StatisticsTag, StatisticsBasePtr> Catalog::getTableStatistics(const String & uuid)
    {
        std::unordered_map<StatisticsTag, StatisticsBasePtr> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getTableStatistics(name_space, uuid); },
            ProfileEvents::GetTableStatisticsSuccess,
            ProfileEvents::GetTableStatisticsFailed);
        return res;
    }

    void Catalog::removeTableStatistics(const String & uuid)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeTableStatistics(name_space, uuid); },
            ProfileEvents::RemoveTableStatisticsSuccess,
            ProfileEvents::RemoveTableStatisticsFailed);
    }

    void Catalog::updateColumnStatistics(
        const String & uuid, const String & column, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->updateColumnStatistics(name_space, uuid, column, data); },
            ProfileEvents::UpdateColumnStatisticsSuccess,
            ProfileEvents::UpdateColumnStatisticsFailed);
    }

    std::unordered_map<StatisticsTag, StatisticsBasePtr> Catalog::getColumnStatistics(const String & uuid, const String & column)
    {
        std::unordered_map<StatisticsTag, StatisticsBasePtr> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getColumnStatistics(name_space, uuid, column); },
            ProfileEvents::GetColumnStatisticsSuccess,
            ProfileEvents::GetColumnStatisticsFailed);
        return res;
    }

    std::unordered_map<String, std::unordered_map<StatisticsTag, StatisticsBasePtr>> Catalog::getAllColumnStatistics(const String & uuid)
    {
        std::unordered_map<String, std::unordered_map<StatisticsTag, StatisticsBasePtr>> res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllColumnStatistics(name_space, uuid); },
            ProfileEvents::GetAllColumnStatisticsSuccess,
            ProfileEvents::GetAllColumnStatisticsFailed);
        return res;
    }

    std::vector<String> Catalog::getAllColumnStatisticsKey(const String & uuid)
    {
        std::vector<String> res;
        // use same profile events with getAllColumnStatistics
        runWithMetricSupport(
            [&] { res = meta_proxy->getAllColumnStatisticsKey(name_space, uuid); },
            ProfileEvents::GetAllColumnStatisticsSuccess,
            ProfileEvents::GetAllColumnStatisticsFailed);
        return res;
    }

    void Catalog::removeColumnStatistics(const String & uuid, const String & column)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeColumnStatistics(name_space, uuid, column); },
            ProfileEvents::RemoveColumnStatisticsSuccess,
            ProfileEvents::RemoveColumnStatisticsFailed);
    }

    void Catalog::removeAllColumnStatistics(const String & uuid)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeAllColumnStatistics(name_space, uuid); },
            ProfileEvents::RemoveAllColumnStatisticsSuccess,
            ProfileEvents::RemoveAllColumnStatisticsFailed);
    }

    void Catalog::updateSQLBinding(const SQLBindingItemPtr data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->updateSQLBinding(name_space, data); },
            ProfileEvents::UpdateSQLBindingSuccess,
            ProfileEvents::UpdateSQLBindingFailed);
    }

    SQLBindings Catalog::getSQLBindings()
    {
        SQLBindings res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getSQLBindings(name_space); },
            ProfileEvents::GetSQLBindingsSuccess,
            ProfileEvents::GetSQLBindingsFailed);
        return res;
    }

    SQLBindings Catalog::getReSQLBindings(const bool & is_re_expression)
    {
        SQLBindings res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getReSQLBindings(name_space, is_re_expression); },
            ProfileEvents::GetSQLBindingsSuccess,
            ProfileEvents::GetSQLBindingsFailed);
        return res;
    }

    SQLBindingItemPtr Catalog::getSQLBinding(const String & uuid, const String & tenant_id, const bool & is_re_expression)
    {
        SQLBindingItemPtr res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getSQLBinding(name_space, uuid, tenant_id, is_re_expression); },
            ProfileEvents::GetSQLBindingSuccess,
            ProfileEvents::GetSQLBindingFailed);
        return res;
    }

    void Catalog::removeSQLBinding(const String & uuid, const String & tenant_id, const bool & is_re_expression)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removeSQLBinding(name_space, uuid, tenant_id, is_re_expression); },
            ProfileEvents::RemoveSQLBindingSuccess,
            ProfileEvents::RemoveSQLBindingFailed);
    }

    void Catalog::updatePreparedStatement(const PreparedStatementItemPtr & data)
    {
        runWithMetricSupport(
            [&] { meta_proxy->updatePreparedStatement(name_space, data); },
            ProfileEvents::UpdatePreparedStatementSuccess,
            ProfileEvents::UpdatePreparedStatementFailed);
    }

    PreparedStatements Catalog::getPreparedStatements()
    {
        PreparedStatements res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getPreparedStatements(name_space); },
            ProfileEvents::GetPreparedStatementSuccess,
            ProfileEvents::GetPreparedStatementFailed);
        return res;
    }

    PreparedStatementItemPtr Catalog::getPreparedStatement(const String & name)
    {
        PreparedStatementItemPtr res;
        runWithMetricSupport(
            [&] { res = meta_proxy->getPreparedStatement(name_space, name); },
            ProfileEvents::GetPreparedStatementSuccess,
            ProfileEvents::GetPreparedStatementFailed);
        return res;
    }

    void Catalog::removePreparedStatement(const String & name)
    {
        runWithMetricSupport(
            [&] { meta_proxy->removePreparedStatement(name_space, name); },
            ProfileEvents::RemovePreparedStatementSuccess,
            ProfileEvents::RemovePreparedStatementFailed);
    }

    void Catalog::setMergeMutateThreadStartTime(const StorageID & storage_id, const UInt64 & startup_time) const
    {
        meta_proxy->setMergeMutateThreadStartTime(name_space, UUIDHelpers::UUIDToString(storage_id.uuid), startup_time);
    }

    UInt64 Catalog::getMergeMutateThreadStartTime(const StorageID & storage_id) const
    {
        return meta_proxy->getMergeMutateThreadStartTime(name_space, UUIDHelpers::UUIDToString(storage_id.uuid));
    }

    void Catalog::setAsyncQueryStatus(const String & id, const Protos::AsyncQueryStatus & status) const
    {
        meta_proxy->setAsyncQueryStatus(name_space, id, status, context.getRootConfig().async_query_status_ttl);
    }

    void Catalog::markBatchAsyncQueryStatusFailed(std::vector<Protos::AsyncQueryStatus> & statuses, const String & reason) const
    {
        meta_proxy->markBatchAsyncQueryStatusFailed(name_space, statuses, reason);
    }

    bool Catalog::tryGetAsyncQueryStatus(const String & id, Protos::AsyncQueryStatus & status) const
    {
        return meta_proxy->tryGetAsyncQueryStatus(name_space, id, status);
    }

    std::vector<Protos::AsyncQueryStatus> Catalog::getIntermidiateAsyncQueryStatuses() const
    {
        return meta_proxy->getIntermidiateAsyncQueryStatuses(name_space);
    }

    /// APIs for attach parts from s3
    void Catalog::attachDetachedParts(
        const StoragePtr & from_tbl,
        const StoragePtr & to_tbl,
        const Strings & detached_part_names,
        const IMergeTreeDataPartsVector & parts,
        const IMergeTreeDataPartsVector & staged_parts,
        const DeleteBitmapMetaPtrVector & detached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps)
    {
        if (detached_part_names.size() != parts.size() + staged_parts.size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Detached former parts count {} and (parts count {} plus staged_parts count {}) mismatch",
                detached_part_names.size(),
                parts.size(),
                staged_parts.size());
        }
        if (detached_part_names.empty())
        {
            return;
        }

        auto [is_host_server, target_host] = checkIfHostServer(to_tbl);

        if (!is_host_server)
        {
            if (target_host.empty())
                throw Exception("Cannot attachDetachedParts because there is no available target server.", ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
            if (!context.getSettingsRef().enable_write_non_host_server)
                throw Exception("AttachDetachedParts failed because commit parts to non host server is not allowed.", ErrorCodes::CATALOG_COMMIT_PART_ERROR);

            try
            {
                LOG_DEBUG(
                    log,
                    "Redirect attachDetachedParts request to remote host : {} for table {}"
                        , target_host.toDebugString(), to_tbl->getStorageID().getNameForLogs());
                context.getCnchServerClientPool()
                    .get(target_host)
                    ->redirectAttachDetachedS3Parts(
                        to_tbl,
                        from_tbl->getStorageUUID(),
                        to_tbl->getStorageUUID(),
                        parts,
                        staged_parts,
                        detached_part_names,
                        parts.size(),
                        detached_part_names.size(),
                        {},
                        detached_bitmaps,
                        bitmaps,
                        DB::Protos::DetachAttachType::ATTACH_DETACHED_PARTS);
                return;
            }
            catch (Exception & e)
            {
                throw Exception(
                    "Fail to redirect attachDetachedParts request to remote host : " + target_host.toDebugString()
                        + ". Error message : " + e.what(),
                    ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            }
        }

        Protos::DataModelPartVector commit_parts;
        fillPartsModel(*to_tbl, parts, *commit_parts.mutable_parts());

        Protos::DataModelPartVector commit_staged_parts;
        fillPartsModel(*to_tbl, staged_parts, *commit_staged_parts.mutable_parts());

        meta_proxy->attachDetachedParts(
            name_space,
            UUIDHelpers::UUIDToString(from_tbl->getStorageUUID()),
            UUIDHelpers::UUIDToString(to_tbl->getStorageUUID()),
            detached_part_names,
            commit_parts,
            commit_staged_parts,
            getPartitionIDs(to_tbl, nullptr),
            detached_bitmaps,
            bitmaps,
            settings.max_commit_size_one_batch,
            settings.max_drop_size_one_batch);

        if (context.getPartCacheManager())
        {
            context.getPartCacheManager()->insertDataPartsIntoCache(
                *to_tbl, commit_parts.parts(), false, false, target_host.topology_version);
            if (!bitmaps.empty())
            {
                context.getPartCacheManager()->insertDeleteBitmapsIntoCache(
                    *to_tbl, bitmaps, target_host.topology_version, commit_parts, &commit_staged_parts);
            }
        }
    }

    void Catalog::detachAttachedParts(
        const StoragePtr & from_tbl,
        const StoragePtr & to_tbl,
        const IMergeTreeDataPartsVector & attached_parts,
        const IMergeTreeDataPartsVector & attached_staged_parts,
        const IMergeTreeDataPartsVector & parts,
        const DeleteBitmapMetaPtrVector & attached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps)
    {
        if (attached_parts.size() + attached_staged_parts.size() != parts.size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "(Attached part's size {} plus attached staged part's size {})and part size {} mismatch",
                attached_parts.size(),
                attached_staged_parts.size(),
                parts.size());
        }
        if (parts.empty())
        {
            return;
        }

        auto [is_host_server, target_host] = checkIfHostServer(from_tbl);

        if (!is_host_server)
        {
            if (target_host.empty())
                throw Exception("Cannot detachAttachedParts because there is no available target server.", ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
            if (!context.getSettingsRef().enable_write_non_host_server)
                throw Exception("DetachAttachedParts failed because commit parts to non host server is not allowed.", ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            try
            {
                LOG_DEBUG(
                    log,
                    "Redirect detachAttachedPartsrequest to remote host : {} for table {}"
                        , target_host.toDebugString(), from_tbl->getStorageID().getNameForLogs());
                context.getCnchServerClientPool().get(target_host)->redirectDetachAttachedS3Parts(
                    to_tbl, from_tbl->getStorageUUID(), to_tbl->getStorageUUID(), attached_parts, attached_staged_parts, parts, {}, {}, attached_bitmaps, bitmaps, {}, {}
                    , DB::Protos::DetachAttachType::DETACH_ATTACHED_PARTS);
                return;
            }
            catch (Exception & e)
            {
                throw Exception(
                    "Fail to redirect detachAttachedParts request to remote host : " + target_host.toDebugString()
                        + ". Error message : " + e.what(),
                    ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            }
        }

        std::vector<std::optional<Protos::DataModelPart>> commit_parts;
        commit_parts.reserve(parts.size());
        for (const auto & part : parts)
        {
            if (part != nullptr)
            {
                commit_parts.emplace_back(Protos::DataModelPart());
                fillPartModel(*to_tbl, *part, commit_parts.back().value());
            }
            else
            {
                commit_parts.emplace_back(std::nullopt);
            }
        }

        std::vector<String> attached_part_names, attached_bitmap_names;
        attached_part_names.reserve(attached_parts.size());
        for (const auto & part : attached_parts)
        {
            attached_part_names.push_back(part->info.getPartName());
        }

        attached_bitmap_names.reserve(attached_bitmaps.size());
        for (const auto & bitmap : attached_bitmaps)
        {
            attached_bitmap_names.push_back(dataModelName(*bitmap->getModel()));
        }

        std::vector<String> attached_staged_part_names;
        attached_part_names.reserve(attached_staged_parts.size());
        for (const auto & part : attached_staged_parts)
        {
            attached_staged_part_names.push_back(part->info.getPartName());
        }

        meta_proxy->detachAttachedParts(
            name_space,
            UUIDHelpers::UUIDToString(from_tbl->getStorageUUID()),
            UUIDHelpers::UUIDToString(to_tbl->getStorageUUID()),
            attached_part_names,
            attached_staged_part_names,
            commit_parts,
            attached_bitmaps,
            bitmaps,
            settings.max_commit_size_one_batch,
            settings.max_drop_size_one_batch);

        if (context.getPartCacheManager())
        {
            if (std::shared_ptr<MergeTreeMetaBase> storage = std::dynamic_pointer_cast<MergeTreeMetaBase>(from_tbl);
                storage != nullptr)
            {
                context.getPartCacheManager()->invalidPartCache(from_tbl->getStorageUUID(), attached_part_names, storage->format_version);
                context.getPartCacheManager()->invalidDeleteBitmapCache(from_tbl->getStorageUUID(), attached_bitmap_names);
            }
            else
            {
                throw Exception("Expected MergeTreeMetaBase when detachAttachedParts",
                    ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    void Catalog::attachDetachedPartsRaw(
        const StoragePtr & tbl,
        const std::vector<String> & part_names,
        size_t detached_visible_part_size,
        size_t detached_staged_part_size,
        const std::vector<String> & bitmap_names)
    {
        if (part_names.empty())
        {
            return;
        }

        auto [is_host_server, target_host] = checkIfHostServer(tbl);

        if (!is_host_server)
        {
            if (target_host.empty())
                throw Exception("Cannot attachDetachedPartsRaw because there is no available target server.", ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
            if (!context.getSettingsRef().enable_write_non_host_server)
                throw Exception("AttachDetachedPartsRaw failed because commit parts to non host server is not allowed.", ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            try
            {
                LOG_DEBUG(
                    log,
                    "Redirect attachDetachedPartsRaw request to remote host : {} for table {}"
                        , target_host.toDebugString(), tbl->getStorageID().getNameForLogs());
                context.getCnchServerClientPool()
                    .get(target_host)
                    ->redirectAttachDetachedS3Parts(
                        tbl,
                        UUIDHelpers::Nil,
                        tbl->getStorageUUID(),
                        {},
                        {},
                        part_names,
                        detached_visible_part_size,
                        detached_staged_part_size,
                        bitmap_names,
                        {},
                        {},
                        DB::Protos::DetachAttachType::ATTACH_DETACHED_RAW);
                return;
            }
            catch (Exception & e)
            {
                throw Exception(
                    "Fail to redirect attachDetachedPartsRaw part request to remote host : " + target_host.toDebugString()
                        + ". Error message : " + e.what(),
                    ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            }
        }

        std::vector<std::pair<String, UInt64>> attached_metas = meta_proxy->attachDetachedPartsRaw(
            name_space,
            UUIDHelpers::UUIDToString(tbl->getStorageUUID()),
            part_names,
            detached_visible_part_size,
            detached_staged_part_size,
            bitmap_names,
            settings.max_commit_size_one_batch,
            settings.max_drop_size_one_batch);

        Protos::DataModelPartVector attached_parts;
        for (const auto& [meta, version] : attached_metas)
        {
            if (!meta.empty())
            {
                attached_parts.add_parts()->ParseFromString(meta);
            }
        }

        if (context.getPartCacheManager())
        {
            context.getPartCacheManager()->insertDataPartsIntoCache(
                *tbl, attached_parts.parts(), false, false, target_host.topology_version);
            /// No need change delete bitmap cache since we have only removed bitmaps in detach folder.
        }
    }

    void Catalog::detachAttachedPartsRaw(
        const StoragePtr & from_tbl,
        const String & to_uuid,
        const std::vector<String> & attached_part_names,
        const std::vector<std::pair<String, String>> & detached_part_metas,
        const std::vector<String> & attached_bitmap_names,
        const std::vector<std::pair<String, String>> & detached_bitmap_metas)
    {
        if (attached_part_names.empty() && detached_part_metas.empty())
        {
            return;
        }

        auto [is_host_server, target_host] = checkIfHostServer(from_tbl);

        if (!is_host_server)
        {
            if (target_host.empty())
                throw Exception("Cannot detachAttachedPartsRaw because there is no available target server.", ErrorCodes::CNCH_TOPOLOGY_NOT_MATCH_ERROR);
            if (!context.getSettingsRef().enable_write_non_host_server)
                throw Exception("DetachAttachedPartsRaw failed because commit parts to non host server is not allowed.", ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            try
            {
                LOG_DEBUG(
                    log,
                    "Redirect detachAttachedPartsRaw request to remote host : {} for table  {}"
                        ,target_host.toDebugString(), from_tbl->getStorageID().getNameForLogs());
                context.getCnchServerClientPool().get(target_host)->redirectDetachAttachedS3Parts(
                    nullptr, from_tbl->getStorageUUID() , UUID(stringToUUID(to_uuid)), {}, {}, {}, attached_part_names, attached_bitmap_names, {}, {}, detached_part_metas, detached_bitmap_metas
                    , DB::Protos::DetachAttachType::DETACH_ATTACHED_RAW);
                return;
            }
            catch (Exception & e)
            {
                throw Exception(
                    "Fail to redirect detachAttachedPartsRaw part request to remote host : " + target_host.toDebugString()
                        + ". Error message : " + e.what(),
                    ErrorCodes::CATALOG_COMMIT_PART_ERROR);
            }
        }

        MergeTreeDataFormatVersion format_version;
        if (std::shared_ptr<MergeTreeMetaBase> storage = std::dynamic_pointer_cast<MergeTreeMetaBase>(from_tbl);
            storage != nullptr)
        {
            format_version = storage->format_version;
        }
        else
        {
            throw Exception("Expect MergeTreeMetaBase when detachAttachedPartsRaw",
                ErrorCodes::LOGICAL_ERROR);
        }

        meta_proxy->detachAttachedPartsRaw(
            name_space,
            UUIDHelpers::UUIDToString(from_tbl->getStorageUUID()),
            to_uuid,
            attached_part_names,
            detached_part_metas,
            attached_bitmap_names,
            detached_bitmap_metas,
            settings.max_commit_size_one_batch,
            settings.max_drop_size_one_batch);

        if (context.getPartCacheManager())
        {
            context.getPartCacheManager()->invalidPartCache(from_tbl->getStorageUUID(), attached_part_names, format_version);
            context.getPartCacheManager()->invalidDeleteBitmapCache(from_tbl->getStorageUUID(), attached_bitmap_names);
        }
    }

    ServerDataPartsVector Catalog::listDetachedParts(const MergeTreeMetaBase& storage,
        const AttachFilter& filter)
    {
        IMetaStore::IteratorPtr iter = nullptr;
        switch (filter.mode)
        {
            case AttachFilter::Mode::PARTS:
            {
                iter = meta_proxy->getDetachedPartsInRange(name_space,
                    UUIDHelpers::UUIDToString(storage.getStorageID().uuid),
                    "", "", true, true);
                break;
            }
            case AttachFilter::Mode::PART:
            {
                iter = meta_proxy->getDetachedPartsInRange(name_space,
                    UUIDHelpers::UUIDToString(storage.getStorageID().uuid),
                    filter.object_id, filter.object_id, true, true);
                break;
            }
            case AttachFilter::Mode::PARTITION:
            {
                iter = meta_proxy->getDetachedPartsInRange(name_space,
                    UUIDHelpers::UUIDToString(storage.getStorageID().uuid),
                    filter.object_id + "_", filter.object_id + "_", true, true);
                break;
            }
        }

        ServerDataPartsVector parts_vec;
        while (iter->next())
        {
            Protos::DataModelPart part_model;
            part_model.ParseFromString(iter->value());
            parts_vec.push_back(std::make_shared<ServerDataPart>(
                createPartWrapperFromModel(storage, std::move(part_model))));
        }
        return parts_vec;
    }

    // Sensitive Resources
    void Catalog::putSensitiveResource(const String & database, const String & table, const String & column, const String & target, bool value)
    {
        runWithMetricSupport(
            [&] {
                meta_proxy->putSensitiveResource(name_space, database, table, column, target, value);
            },
            ProfileEvents::PutSensitiveResourceSuccess,
            ProfileEvents::PutSensitiveResourceFailed);
    }

    std::shared_ptr<Protos::DataModelSensitiveDatabase> Catalog::getSensitiveResource(const String & database)
    {
        std::shared_ptr<Protos::DataModelSensitiveDatabase> res;
        runWithMetricSupport(
            [&] {
                res = meta_proxy->getSensitiveResource(name_space, database);
            },
            ProfileEvents::GetSensitiveResourceSuccess,
            ProfileEvents::GetSensitiveResourceFailed);
        return res;

    }

    // Access Entities
    std::optional<AccessEntityModel> Catalog::tryGetAccessEntity(EntityType type, const String & name)
    {
        String data;
        runWithMetricSupport(
            [&] {
                data = meta_proxy->getAccessEntity(type, name_space, name);
            },
            ProfileEvents::TryGetAccessEntitySuccess,
            ProfileEvents::TryGetAccessEntityFailed);

        if (data.empty())
            return {};

        AccessEntityModel entity;
        entity.ParseFromString(std::move(data));
        return entity;
    }

    std::vector<AccessEntityModel> Catalog::getEntities(EntityType type, const std::unordered_set<UUID> & ids)
    {
        std::vector<AccessEntityModel> entities;
        runWithMetricSupport(
            [&] {
                auto responses = meta_proxy->getEntities(type, name_space, ids);
                entities.reserve(responses.size());

                for (const auto & [s, version] : responses)
                {
                    if (s.empty())
                        continue;

                    AccessEntityModel model;
                    model.ParseFromString(std::move(s));
                    entities.push_back(std::move(model));
                }
            },
            ProfileEvents::GetAllAccessEntitySuccess,
            ProfileEvents::GetAllAccessEntityFailed);
        return entities;
    }

    std::vector<AccessEntityModel> Catalog::getAllAccessEntities(EntityType type)
    {
        std::vector<AccessEntityModel> entities;
        runWithMetricSupport(
            [&] {
                Strings data = meta_proxy->getAllAccessEntities(type, name_space);
                entities.reserve(data.size());
                for (const auto & s : data)
                {
                    AccessEntityModel model;
                    model.ParseFromString(std::move(s));
                    entities.push_back(std::move(model));
                }
            },
            ProfileEvents::GetAllAccessEntitySuccess,
            ProfileEvents::GetAllAccessEntityFailed);
        return entities;
    }

    std::optional<String> Catalog::tryGetAccessEntityName(const UUID & uuid)
    {
        String data;
        runWithMetricSupport(
            [&] {
                data = meta_proxy->getAccessEntityNameByUUID(name_space, uuid);
            },
            ProfileEvents::TryGetAccessEntityNameSuccess,
            ProfileEvents::TryGetAccessEntityNameFailed);

        return data;
    }

    static void notifyOtherServersOnAccessEntityChange(const Context & context, EntityType type, const String & name, const UUID & uuid);

    void Catalog::dropAccessEntity(EntityType type, const UUID & uuid, const String & name)
    {
        bool isSuccessful = false;
        runWithMetricSupport(
            [&] {
                isSuccessful = meta_proxy->dropAccessEntity(type, name_space, uuid, name);
            },
            ProfileEvents::DropAccessEntitySuccess,
            ProfileEvents::DropAccessEntityFailed);
        if (isSuccessful)
        {
            auto ctx = context.getGlobalContext();
            ThreadFromGlobalPool([=] {
                notifyOtherServersOnAccessEntityChange(*ctx, type, name, uuid);
            }).detach();
        }
    }

    void Catalog::putAccessEntity(EntityType type, AccessEntityModel & new_access_entity, const AccessEntityModel & old_access_entity, bool replace_if_exists)
    {
        new_access_entity.set_commit_time(context.getTimestamp());
        bool isSuccessful = false;
        runWithMetricSupport(
            [&] {
                isSuccessful = meta_proxy->putAccessEntity(type, name_space, new_access_entity, old_access_entity, replace_if_exists);
                if (!isSuccessful) // RBAC TODO: remove this check once FDB batchWrite throws exception on CAS fail like ByteKV batchWrite
                {
                    String error_msg = replace_if_exists ? "Failed to perform operation on KV Storage" : fmt::format("Access entity with name {} already exists", new_access_entity.name());
                    throw Exception(error_msg, ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS);
                }
            },
            ProfileEvents::PutAccessEntitySuccess,
            ProfileEvents::PutAccessEntityFailed);
        if (isSuccessful)
        {
            auto ctx = context.getGlobalContext();
            ThreadFromGlobalPool([=, old_name = old_access_entity.name(), new_name = new_access_entity.name(),
             old_uuid = old_access_entity.uuid(), new_uuid = new_access_entity.uuid()] {
                if (old_name != new_name)
                    notifyOtherServersOnAccessEntityChange(*ctx, type, old_name, RPCHelpers::createUUID(old_uuid));
                notifyOtherServersOnAccessEntityChange(*ctx, type, new_name, RPCHelpers::createUUID(new_uuid));
            }).detach();
        }
    }

    void notifyOtherServersOnAccessEntityChange(const Context & context, EntityType type, const String & name, const UUID & uuid)
    {
        static Poco::Logger * log = &Poco::Logger::get("Catalog::notifyOtherServersOnAccessEntityChange");
        std::shared_ptr<CnchTopologyMaster> topology_master = context.getCnchTopologyMaster();
        if (!topology_master)
        {
            LOG_ERROR(log, "Failed to get topology master, skip notifying other servers of access entity change");
            return;
        }

        std::vector<CnchServerClientPtr> clients;
        std::list<CnchServerTopology> server_topologies = topology_master->getCurrentTopology();
        if (server_topologies.empty())
        {
            LOG_ERROR(log, "Server topology is empty, something wrong with topology, return empty result");
            return;
        }

        HostWithPortsVec host_ports = server_topologies.back().getServerList();

        for (const auto & host_port : host_ports)
        {
            String rpc_address = host_port.getRPCAddress();
            if (isLocalServer(rpc_address, std::to_string(context.getRPCPort())))
                continue;
            CnchServerClientPtr client_ptr = context.getCnchServerClientPool().get(host_port);
            if (!client_ptr)
                continue;
            else
                clients.push_back(client_ptr);
        }

        for (const auto & client : clients)
        {
            if (!client)
                continue;
            String rpc_address = client->getRPCAddress();
            try
            {
                client->notifyAccessEntityChange(type, name, uuid);
            }
            catch (...)
            {
                LOG_INFO(log, "Failed to reach server: {}, detail: ", rpc_address);
                tryLogCurrentException(log, "Failed to reach server: " + rpc_address);
            }
        }
    }

    DeleteBitmapMetaPtrVector Catalog::listDetachedDeleteBitmaps(const MergeTreeMetaBase & storage, const AttachFilter & filter)
    {
        IMetaStore::IteratorPtr iter = nullptr;
        switch (filter.mode)
        {
            case AttachFilter::Mode::PARTS: {
                iter = meta_proxy->getDetachedDeleteBitmapsInRange(
                    name_space, UUIDHelpers::UUIDToString(storage.getStorageID().uuid), "", "", true, true);
                break;
            }
            case AttachFilter::Mode::PART: {
                iter = meta_proxy->getDetachedDeleteBitmapsInRange(
                    name_space, UUIDHelpers::UUIDToString(storage.getStorageID().uuid), filter.object_id, filter.object_id, true, true);
                break;
            }
            case AttachFilter::Mode::PARTITION: {
                iter = meta_proxy->getDetachedDeleteBitmapsInRange(
                    name_space,
                    UUIDHelpers::UUIDToString(storage.getStorageID().uuid),
                    filter.object_id + "_",
                    filter.object_id + "_",
                    true,
                    true);
                break;
            }
        }

        DeleteBitmapMetaPtrVector res;
        while (iter->next())
        {
            DataModelDeleteBitmapPtr delete_bitmap_model = std::make_shared<Protos::DataModelDeleteBitmap>();
            delete_bitmap_model->ParseFromString(iter->value());
            res.push_back(std::make_shared<DeleteBitmapMeta>(storage, delete_bitmap_model));
        }
        return res;
    }

    void fillUUIDForDictionary(DB::Protos::DataModelDictionary & d)
    {
        UUID final_uuid = UUIDHelpers::Nil;
        UUID uuid = RPCHelpers::createUUID(d.uuid());
        ASTPtr ast = CatalogFactory::getCreateDictionaryByDataModel(d);
        ASTCreateQuery * create_ast = ast->as<ASTCreateQuery>();
        UUID uuid_in_create_query = create_ast->uuid;
        if (uuid != UUIDHelpers::Nil)
            final_uuid = uuid;

        if (final_uuid == UUIDHelpers::Nil)
            final_uuid = uuid_in_create_query;

        if (final_uuid == UUIDHelpers::Nil)
            final_uuid = UUIDHelpers::generateV4();

        RPCHelpers::fillUUID(final_uuid, *(d.mutable_uuid()));
        create_ast->uuid = final_uuid;
        String create_query = serializeAST(*ast);
        d.set_definition(create_query);
    }

    void Catalog::appendObjectPartialSchema(
        const StoragePtr & table, const TxnTimestamp & txn_id, const MutableMergeTreeDataPartsCNCHVector & parts)
    {
        //txn partial schema
        //multi column
        auto cnch_table = std::dynamic_pointer_cast<StorageCnchMergeTree>(table);
        if (!cnch_table)
            return;

        auto subcolumns_limit = cnch_table->getSettings()->json_subcolumns_threshold;

        //check schema compatibility and merge part schema
        auto partial_schema = DB::getConcreteObjectColumns(
            parts.begin(), parts.end(), table->getInMemoryMetadata().columns, [](const auto & part) { return part->getColumns(); });

        // compare with existed schema , check if it need to insert
        // Attention: this comparison will scan existed partial schema from meta store, it may cost too many meta store resource.
        // if it cause meta store performance fallback, just remove this comparison
        auto assembled_schema = tryGetTableObjectAssembledSchema(table->getStorageUUID());
        auto existed_partial_schemas = tryGetTableObjectPartialSchemas(table->getStorageUUID());
        std::vector<TxnTimestamp> existed_partial_schema_txnids(existed_partial_schemas.size());
        std::for_each(
            existed_partial_schemas.begin(),
            existed_partial_schemas.end(),
            [&existed_partial_schema_txnids](const auto & existed_partial_schema) {
                existed_partial_schema_txnids.emplace_back(existed_partial_schema.first);
            });
        auto committed_partial_schema_txnids = filterUncommittedObjectPartialSchemas(existed_partial_schema_txnids);
        std::vector<ObjectPartialSchema> committed_partial_schema_list(committed_partial_schema_txnids.size() + 2);
        std::for_each(
            committed_partial_schema_txnids.begin(),
            committed_partial_schema_txnids.end(),
            [&committed_partial_schema_list, &existed_partial_schemas](const auto & txn_id) {
                committed_partial_schema_list.emplace_back(existed_partial_schemas[txn_id]);
            });

        committed_partial_schema_list.emplace_back(assembled_schema);

        auto existed_assembled_schema = DB::getConcreteObjectColumns(
            committed_partial_schema_list.begin(),
            committed_partial_schema_list.end(),
            cnch_table->getInMemoryMetadata().getColumns(),
            [](const auto & partial_schema_) { return partial_schema_; });

        committed_partial_schema_list.emplace_back(partial_schema);
        auto new_assembled_schema = DB::getConcreteObjectColumns(
            committed_partial_schema_list.begin(),
            committed_partial_schema_list.end(),
            cnch_table->getInMemoryMetadata().getColumns(),
            [](const auto & partial_schema_) { return partial_schema_; });

        if (new_assembled_schema != existed_assembled_schema)
        {
            DB::limitObjectSubcolumns(new_assembled_schema, subcolumns_limit);

            meta_proxy->appendObjectPartialSchema(
                name_space, UUIDHelpers::UUIDToString(table->getStorageUUID()), txn_id.toUInt64(), partial_schema.toString());
            cnch_table->appendObjectPartialSchema(txn_id, partial_schema);

            LOG_DEBUG(
                log,
                "Append dynamic object partial schema [TxnTimestamp:{}, Partial Schema:{}]",
                txn_id.toString(),
                partial_schema.toString());
        }
    }

    ObjectAssembledSchema Catalog::tryGetTableObjectAssembledSchema(const UUID & table_uuid) const
    {
        auto serialized_assembled_schema = meta_proxy->getObjectAssembledSchema(name_space, UUIDHelpers::UUIDToString(table_uuid));

        if (serialized_assembled_schema.empty())
            return ColumnsDescription();
        return ColumnsDescription::parse(serialized_assembled_schema);
    }

    ObjectPartialSchemas Catalog::tryGetTableObjectPartialSchemas(const UUID & table_uuid, const int & limit_size) const
    {
        auto serialized_partial_schemas
            = meta_proxy->scanObjectPartialSchemas(name_space, UUIDHelpers::UUIDToString(table_uuid), limit_size);
        ObjectPartialSchemas partial_schemas;
        partial_schemas.reserve(serialized_partial_schemas.size());
        std::for_each(
            serialized_partial_schemas.begin(),
            serialized_partial_schemas.end(),
            [&partial_schemas](std::pair<String, String> serialized_partial_schema) {
                partial_schemas.emplace(
                    std::stoll(serialized_partial_schema.first),
                    ColumnsDescription::parse(serialized_partial_schema.second));
            });

        return partial_schemas;
    }

    bool Catalog::resetObjectAssembledSchemaAndPurgePartialSchemas(
        const UUID & table_uuid,
        const ObjectAssembledSchema & old_assembled_schema,
        const ObjectAssembledSchema & new_assembled_schema,
        const std::vector<TxnTimestamp> & partial_schema_txnids)
    {
        return meta_proxy->resetObjectAssembledSchemaAndPurgePartialSchemas(
            name_space,
            UUIDHelpers::UUIDToString(table_uuid),
            old_assembled_schema.empty() ? "" : old_assembled_schema.toString(),
            new_assembled_schema.toString(),
            partial_schema_txnids);
    }

    std::vector<TxnTimestamp> Catalog::filterUncommittedObjectPartialSchemas(std::vector<TxnTimestamp> & unfiltered_partial_schema_txnids)
    {
        std::vector<TxnTimestamp> committed_partial_schema_txnids;
        std::unordered_map<TxnTimestamp, TxnTimestamp, TxnTimestampHasher> unfiltered_partial_schema_txnid_map;
        unfiltered_partial_schema_txnid_map.reserve(unfiltered_partial_schema_txnids.size());
        std::for_each(
            unfiltered_partial_schema_txnids.begin(),
            unfiltered_partial_schema_txnids.end(),
            [&unfiltered_partial_schema_txnid_map](const auto & txn_id) { unfiltered_partial_schema_txnid_map[txn_id] = txn_id; });

        // query partial schema status in meta store
        auto partial_schema_statuses = batchGetObjectPartialSchemaStatuses(unfiltered_partial_schema_txnids);
        std::for_each(
            partial_schema_statuses.begin(),
            partial_schema_statuses.end(),
            [&committed_partial_schema_txnids, &unfiltered_partial_schema_txnid_map](const auto & partial_schema_status_pair) {
                if (partial_schema_status_pair.second == ObjectPartialSchemaStatus::Finished)
                {
                    committed_partial_schema_txnids.emplace_back(partial_schema_status_pair.first);
                    unfiltered_partial_schema_txnid_map.erase(partial_schema_status_pair.first);
                }
            });

        // query remaining partial schemas by its co-responding txn record status
        unfiltered_partial_schema_txnids.clear();
        std::transform(
            unfiltered_partial_schema_txnid_map.begin(),
            unfiltered_partial_schema_txnid_map.end(),
            std::back_inserter(unfiltered_partial_schema_txnids),
            [](const auto & txn_id_pair) { return txn_id_pair.first; });
        auto txn_record_statuses = getTransactionRecords(unfiltered_partial_schema_txnids, 10000);

        std::for_each(
            txn_record_statuses.begin(), txn_record_statuses.end(), [&committed_partial_schema_txnids](TransactionRecord txn_record) {
                auto txn_id = txn_record.txnID();
                auto status = txn_record.status();
                if (status == CnchTransactionStatus::Finished)
                    committed_partial_schema_txnids.emplace_back(txn_id);
            });

        return committed_partial_schema_txnids;
    }

    ObjectPartialSchemaStatuses
    Catalog::batchGetObjectPartialSchemaStatuses(const std::vector<TxnTimestamp> & txn_ids, const int & batch_size)
    {
        ObjectPartialSchemaStatuses partial_schema_statuses;
        size_t total_txn_size = txn_ids.size();

        partial_schema_statuses.reserve(total_txn_size);

        auto fetch_records_in_batch = [&](size_t begin, size_t end) {
            auto statuses_in_metastore = meta_proxy->batchGetObjectPartialSchemaStatuses(
                name_space, std::vector<TxnTimestamp>(txn_ids.begin() + begin, txn_ids.begin() + end));

            for (const auto & serialized_partial_schema_status : statuses_in_metastore)
            {
                auto txn_id = serialized_partial_schema_status.first;
                auto status = ObjectSchemas::deserializeObjectPartialSchemaStatus(serialized_partial_schema_status.second);
                partial_schema_statuses.emplace(txn_id, status);
            }
        };

        if (batch_size > 0)
        {
            size_t batch_count{0};
            while (batch_count + batch_size < total_txn_size)
            {
                fetch_records_in_batch(batch_count, batch_count + batch_size);
                batch_count += batch_size;
            }
            fetch_records_in_batch(batch_count, total_txn_size);
        }
        else
            fetch_records_in_batch(0, total_txn_size);

        return partial_schema_statuses;
    }

    void Catalog::batchDeleteObjectPartialSchemaStatus(const std::vector<TxnTimestamp> &txn_ids)
    {
        meta_proxy->batchDeletePartialSchemaStatus(name_space, txn_ids);
    }

    void Catalog::commitObjectPartialSchema(const TxnTimestamp &txn_id)
    {
        meta_proxy->updateObjectPartialSchemaStatus(name_space, txn_id, ObjectPartialSchemaStatus::Finished);
    }

    void Catalog::abortObjectPartialSchema(const TxnTimestamp & txn_id)
    {
        meta_proxy->updateObjectPartialSchemaStatus(name_space, txn_id, ObjectPartialSchemaStatus::Aborted);
    }

    std::unordered_map<String, std::shared_ptr<PartitionMetrics>>
    Catalog::loadPartitionMetricsSnapshotFromMetastore(const String & table_uuid)
    {
        std::unordered_map<String, std::shared_ptr<PartitionMetrics>> res;
        LOG_DEBUG(log, "Load parts partition level metrics from metastore, table: {}", table_uuid);
        auto it = meta_proxy->getTablePartitionMetricsSnapshots(name_space, table_uuid);
        size_t prefix_length = MetastoreProxy::partitionPartsMetricsSnapshotPrefix(name_space, table_uuid, "").length();

        while (it->next())
        {
            const auto & key = it->key();
            String partition_id = key.substr(prefix_length, String::npos);

            Protos::PartitionPartsMetricsSnapshot snapshot;
            snapshot.ParseFromString(it->value());
            res.emplace(partition_id, std::make_shared<PartitionMetrics>(snapshot, table_uuid, partition_id));
        }

        return res;
    }
    void Catalog::savePartitionMetricsSnapshotToMetastore(
        const String & table_uuid, const String & partition_id, const Protos::PartitionPartsMetricsSnapshot & snapshot)
    {
        meta_proxy->updatePartitionMetricsSnapshot(name_space, table_uuid, partition_id, snapshot.SerializeAsString());
    }
    TableMetrics::TableMetricsData
    Catalog::getTableTrashItemsMetricsDataFromMetastore(const String & table_uuid, const TxnTimestamp ts, std::function<bool()> need_abort)
    {
        TableMetrics::TableMetricsData res;
        runWithMetricSupport(
            [&] {
                auto storage = getTableByUUID(context, table_uuid, TxnTimestamp::maxTS());
                String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
                size_t prefix_length = MetastoreProxy::trashItemsPrefix(name_space, uuid).length();

                auto it = meta_proxy->getItemsInTrash(name_space, uuid, 0);
                while (it->next() && !need_abort())
                {
                    const auto & key = it->key();
                    String meta_key = key.substr(prefix_length, String::npos);
                    if (startsWith(meta_key, PART_STORE_PREFIX))
                    {
                        Protos::DataModelPart part_model;
                        part_model.ParseFromString(it->value());
                        res.update(part_model, ts);
                    }
                    else if (startsWith(meta_key, DELETE_BITMAP_PREFIX))
                    {
                        Protos::DataModelDeleteBitmap delete_bitmap_model;
                        delete_bitmap_model.ParseFromString(it->value());
                        res.update(delete_bitmap_model, ts);
                    }
                    // not handling staged parts because we never move them to trash
                }
            },
            ProfileEvents::GetTableTrashItemsMetricsDataFromMetastoreSuccess,
            ProfileEvents::GetTableTrashItemsMetricsDataFromMetastoreFailed);
        return res;
    }
    Protos::TableTrashItemsMetricsSnapshot Catalog::loadTableTrashItemsMetricsSnapshotFromMetastore(const String & table_uuid)
    {
        String snapshot_str = meta_proxy->getTableTrashItemsSnapshot(name_space, table_uuid);
        Protos::TableTrashItemsMetricsSnapshot snapshot;
        if (!snapshot_str.empty())
        {
            snapshot.ParseFromString(snapshot_str);
        }
        LOG_DEBUG(
            log, "Load trash items table level metrics from metastore, table: {}, snapshot: {}", table_uuid, snapshot.ShortDebugString());
        return snapshot;
    }
    void Catalog::saveTableTrashItemsMetricsToMetastore(const String & table_uuid, const Protos::TableTrashItemsMetricsSnapshot & snapshot)
    {
        meta_proxy->updateTableTrashItemsSnapshot(name_space, table_uuid, snapshot.SerializeAsString());
    }

    String Catalog::getDictionaryBucketUpdateTimeKey(const StorageID & storage_id, Int64 bucket_number) const
    {
        return MetastoreProxy::dictionaryBucketUpdateTimeKey(name_space, toString(storage_id.uuid), bucket_number);
    }

    String Catalog::getByKey(const String & key)
    {
        String res;
        runWithMetricSupport([&] { res = meta_proxy->getByKey(key); }, ProfileEvents::GetByKeySuccess, ProfileEvents::GetByKeyFailed);
        return res;
    }
    std::vector<Protos::LastModificationTimeHint> Catalog::getLastModificationTimeHints(const ConstStoragePtr & table)
    {
        if (!table)
        {
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Receive an empty table.");
        }

        /// if storage is hive, las, hudi, use hive meta store client to get partition last updated time (no cache)
        if (auto * cnch_hive = const_cast<StorageCnchHive *>(dynamic_cast<const StorageCnchHive *>(table.get())))
        {
            StorageMetadataPtr metadata_snapshot = cnch_hive->getInMemoryMetadataPtr();
            std::vector<std::pair<String, UInt64>> partition_modified_times
                = cnch_hive->getPartitionLastModificationTime(metadata_snapshot);
            std::vector<Protos::LastModificationTimeHint> result;
            for ( const auto & part_modify_time : partition_modified_times)
            {
                Protos::LastModificationTimeHint hint = Protos::LastModificationTimeHint{};
                hint.set_partition_id(part_modify_time.first);
                hint.set_last_modification_time(part_modify_time.second);
                result.push_back(std::move(hint));
            }
            return result;
        }
        const auto host_port = context.getCnchTopologyMaster()->getTargetServer(
            UUIDHelpers::UUIDToString(table->getStorageUUID()), table->getServerVwName(), true);

        if (!host_port.empty() && !isLocalServer(host_port.getRPCAddress(), std::to_string(context.getRPCPort())))
        {
            try
            {
                auto ret = context.getCnchServerClientPool().get(host_port)->getLastModificationTimeHints(table->getStorageID());
                return ret;
            }
            catch (...)
            {
                tryLogCurrentException(&Poco::Logger::get("Catalog::getLastModificationTimeHints"));
            }
        }

        std::vector<Protos::LastModificationTimeHint> ret;

        if (auto mgr = context.getPartCacheManager())
            return mgr->getLastModificationTimeHints(table);

        return ret;
    }

    DataModelDeleteBitmapPtrVector Catalog::getDeleteBitmapsInPartitionsImpl(
        const ConstStoragePtr & storage, const Strings & required_partitions, const Strings & full_partitions, const TxnTimestamp & ts)
    {
        auto create_func = [&](const String &, const String & meta) -> DataModelDeleteBitmapPtr {
            DataModelDeleteBitmapPtr model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
            model_ptr->ParseFromString(meta);
            if (ts.toUInt64() && model_ptr->has_commit_time() && TxnTimestamp{model_ptr->commit_time()} > ts)
                return nullptr;
            return model_ptr;
        };

        String uuid = UUIDHelpers::UUIDToString(storage->getStorageUUID());
        String meta_prefix = MetastoreProxy::deleteBitmapPrefix(name_space, uuid);

        auto res = getDataModelsByPartitions<DataModelDeleteBitmapPtr>(
            storage, meta_prefix, required_partitions, full_partitions, create_func, ts);
        return res;
    }

    std::vector<std::shared_ptr<Protos::ManifestListModel>> Catalog::getAllTableVersions(const UUID & uuid)
    {
        std::vector<std::shared_ptr<Protos::ManifestListModel>> res;

        auto it = meta_proxy->getByPrefix(MetastoreProxy::manifestListPrefix(name_space, UUIDHelpers::UUIDToString(uuid)));
        while (it->next())
        {
            std::shared_ptr<Protos::ManifestListModel> manifest_model_ptr = std::make_shared<Protos::ManifestListModel>();
            manifest_model_ptr->ParseFromString(it->value());
            res.push_back(manifest_model_ptr);
        }
        return res;
    }

    UInt64 Catalog::getCurrentTableVersion(const UUID & uuid, const TxnTimestamp & ts)
    {
        String target_version_str;
        auto it = meta_proxy->getByPrefix(MetastoreProxy::manifestListPrefix(name_space, UUIDHelpers::UUIDToString(uuid)));
        while (it->next())
        {
            const String & key = it->key();
            // manifest list key has the format `{prefix}_{tableversion}`
            UInt64 table_version = std::stoul(key.substr(key.find_last_of('_') + 1));
            if (table_version <= ts)
                target_version_str = it->value();
        }

        if (!target_version_str.empty())
        {
            Protos::ManifestListModel data_model;
            data_model.ParseFromString(target_version_str);
            return data_model.version();
        }
        return 0;
    }

    DataModelPartWrapperVector Catalog::getCommittedPartsFromManifest(const MergeTreeMetaBase & storage, const std::vector<UInt64> & txn_list)
    {
        DataModelPartWrapperVector res;
        String uuid_str = UUIDHelpers::UUIDToString(storage.getStorageUUID());

        for (const auto & txn : txn_list)
        {
            String manifest_part_prefix = MetastoreProxy::manifestDataPrefix(name_space, uuid_str, txn) + PART_STORE_PREFIX;
            auto it = meta_proxy->getByPrefix(manifest_part_prefix);
            while (it->next())
            {
                Protos::DataModelPart part_model;
                part_model.ParseFromString(it->value());
                String part_name = it->key().substr(manifest_part_prefix.size(), std::string::npos);
                auto part_model_wrapper = createPartWrapperFromModel(storage, std::move(part_model), std::move(part_name));
                res.push_back(part_model_wrapper);
            }
        }
        return res;
    }

    DeleteBitmapMetaPtrVector Catalog::getDeleteBitmapsFromManifest(const MergeTreeMetaBase & storage, const std::vector<UInt64> & txn_list)
    {
        DeleteBitmapMetaPtrVector res;
        String uuid_str = UUIDHelpers::UUIDToString(storage.getStorageUUID());

        for (const auto & txn : txn_list)
        {
            String manifest_dbm_prefix = MetastoreProxy::manifestDataPrefix(name_space, uuid_str, txn) + DELETE_BITMAP_PREFIX;
            auto it = meta_proxy->getByPrefix(manifest_dbm_prefix);
            while (it->next())
            {
                auto dbm_model_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
                dbm_model_ptr->ParseFromString(it->value());
                ///NOTE: work around for no commit time when table version committed. Do we need commit time if txns committed linearly?
                dbm_model_ptr->set_commit_time(txn);
                res.push_back(std::make_shared<DeleteBitmapMeta>(storage, dbm_model_ptr));
            }
        }
        return res;
    }

    void Catalog::commitCheckpointVersion(const UUID & uuid, std::shared_ptr<DB::Protos::ManifestListModel> checkpoint_version)
    {
        meta_proxy->getMetastore()->put(MetastoreProxy::manifestListKey(name_space, UUIDHelpers::UUIDToString(uuid), checkpoint_version->version()), checkpoint_version->SerializeAsString());
    }

    void Catalog::cleanTableVersions(const UUID & uuid, std::vector<std::shared_ptr<DB::Protos::ManifestListModel>> versions_to_clean)
    {
        BatchCommitRequest clear_manifest_data_request, clear_manifest_list_request;
        for (const auto & version : versions_to_clean)
        {
            // remove committed data in this version
            auto txn_ids = version->txn_ids();
            for (auto & txn_id : txn_ids)
            {
                auto it = meta_proxy->getByPrefix(MetastoreProxy::manifestDataPrefix(name_space, UUIDHelpers::UUIDToString(uuid), txn_id));
                while (it->next())
                    clear_manifest_data_request.AddDelete(it->key());
            }

            // remove manifest list
            clear_manifest_list_request.AddDelete(MetastoreProxy::manifestListKey(name_space, UUIDHelpers::UUIDToString(uuid), version->version()));
        }

        // firstly, clear manifest data info(including parts and delete bitmap) from kv.
        meta_proxy->getMetastore()->adaptiveBatchWrite(clear_manifest_data_request);

        // then, clear manifest list metadata from kv.
        meta_proxy->getMetastore()->adaptiveBatchWrite(clear_manifest_list_request);
    }
}
}

#pragma once

#include <string>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Server/IPrometheusMetricsWriter.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ClickHouseRevision.h>
#include <Common/HistogramMetrics.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Context_fwd.h>

#define DB_NAME_DELIMITER '.'  /// database name may contain the account id, i.e. account_id.database_name

namespace LabelledMetrics
{
    const extern Metric VwQuery;
    const extern Metric UnlimitedQuery;
    const extern Metric QueriesFailed;
}

namespace ProfileEvents
{
    const extern Event Query;
    const extern Event TimedOutQuery;
    const extern Event InsufficientConcurrencyQuery;
    const extern Event Manipulation;
    const extern Event ManipulationSuccess;
    const extern Event InsertedRows;
    const extern Event InsertedBytes;
    const extern Event MergedRows;
    const extern Event MergedUncompressedBytes;
    const extern Event FailedQuery;
    // const extern Event CloudMergeStarted;
    // const extern Event CloudMergeEnded;

    /// Storage
    // const extern Event CnchReadRowsFromDiskCache;
    // const extern Event CnchReadRowsFromRemote;
    const extern Event HDFSReadElapsedMicroseconds;
    // const extern Event HDFSReadElapsedCpuMilliseconds;
    const extern Event HDFSWriteElapsedMicroseconds;
    const extern Event ReadBufferFromHdfsRead;
    const extern Event ReadBufferFromHdfsReadBytes;
    const extern Event ReadBufferFromHdfsReadFailed;
    const extern Event ReadBufferFromFileDescriptorRead;
    const extern Event ReadBufferFromFileDescriptorReadFailed;
    const extern Event ReadBufferFromFileDescriptorReadBytes;

    const extern Event WriteBufferFromHdfsWrite;
    const extern Event WriteBufferFromHdfsWriteBytes;
    const extern Event WriteBufferFromHdfsWriteFailed;
    const extern Event WriteBufferFromFileDescriptorWrite;
    const extern Event WriteBufferFromFileDescriptorWriteFailed;
    const extern Event WriteBufferFromFileDescriptorWriteBytes;

    const extern Event DiskReadElapsedMicroseconds;
    const extern Event DiskWriteElapsedMicroseconds;

    /// SD
    const extern Event SDRequest;
    const extern Event SDRequestFailed;
    const extern Event SDRequestUpstream;
    const extern Event SDRequestUpstreamFailed;

    /// Cnch transaction
    const extern Event CnchTxnAborted;
    const extern Event CnchTxnCommitted;
    const extern Event CnchTxnExpired;
    const extern Event CnchTxnReadTxnCreated;
    const extern Event CnchTxnWriteTxnCreated;
    const extern Event CnchTxnCommitV1Failed;
    const extern Event CnchTxnCommitV2Failed;
    const extern Event CnchTxnCommitV1ElapsedMilliseconds;
    const extern Event CnchTxnCommitV2ElapsedMilliseconds;
    const extern Event CnchTxnPrecommitElapsedMilliseconds;
    const extern Event CnchTxnCommitKVElapsedMilliseconds;
    const extern Event CnchTxnCleanFailed;
    const extern Event CnchTxnCleanElapsedMilliseconds;
    const extern Event CnchTxnAllTransactionRecord;
    const extern Event CnchTxnFinishedTransactionRecord;
    // const extern Event IntentLockAcquiredAttempt;
    const extern Event IntentLockAcquiredSuccess;
    // const extern Event IntentLockAcquiredElapsedMilliseconds;
    // const extern Event IntentLockWriteIntentAttempt;
    // const extern Event IntentLockWriteIntentSuccess;
    // const extern Event IntentLockWriteIntentConflict;
    // const extern Event IntentLockWriteIntentPreemption;
    const extern Event IntentLockWriteIntentElapsedMilliseconds;
    // const extern Event TsCacheCheckSuccess;
    // const extern Event TsCacheCheckFailed;
    const extern Event TsCacheCheckElapsedMilliseconds;
    const extern Event TsCacheUpdateElapsedMilliseconds;
    extern const Event TSORequest;
    extern const Event TSOError;

    /// Disk cache
    extern const Event DiskCacheGetMetaMicroSeconds;
    extern const Event DiskCacheGetTotalOps;
    extern const Event DiskCacheSetTotalOps;
    extern const Event CnchReadDataMicroSeconds;
    // extern const Event CnchReadDataCpuMicroSeconds;
    extern const Event CnchReadDataDecompressMicroSeconds;
    // extern const Event CnchReadDataDecompressCpuMicroSeconds;
    // extern const Event CnchReadDataDeserialMicroSeconds;
    // extern const Event CnchReadDataDeserialCpuMicroSeconds;
    extern const Event DiskCacheAcquireStatsLock;
    extern const Event DiskCacheScheduleCacheTaskMicroseconds;
    extern const Event DiskCacheUpdateStatsMicroSeconds;
    extern const Event DiskCacheTaskDropCount;
    // extern const Event PreloadSubmitTotalOps;
    // extern const Event PreloadSendTotalOps;
    // extern const Event PreloadExecTotalOps;

    /// Unique key
    extern const Event DeleteBitmapCacheHit;
    extern const Event DeleteBitmapCacheMiss;
    // extern const Event BackgroundDedupSchedulePoolTask;
    extern const Event UniqueKeyIndexMetaCacheHit;
    extern const Event UniqueKeyIndexMetaCacheMiss;
    extern const Event UniqueKeyIndexBlockCacheHit;
    extern const Event UniqueKeyIndexBlockCacheMiss;

    /// Cnch catalog
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
    extern const Event UpdateStatisticsSettingsSuccess;
    extern const Event UpdateStatisticsSettingsFailed;
    extern const Event GetStatisticsSettingsSuccess;
    extern const Event GetStatisticsSettingsFailed;
    // extern const Event UpdateSQLBindingSuccess;
    // extern const Event UpdateSQLBindingFailed;
    // extern const Event GetSQLBindingSuccess;
    // extern const Event GetSQLBindingFailed;
    // extern const Event GetSQLBindingsSuccess;
    // extern const Event GetSQLBindingsFailed;
    // extern const Event RemoveSQLBindingSuccess;
    // extern const Event RemoveSQLBindingFailed;
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
    extern const Event TryLockPartInKVSuccess;
    extern const Event TryLockPartInKVFailed;
    extern const Event UnLockPartInKVSuccess;
    extern const Event UnLockPartInKVFailed;
    extern const Event TryResetAndLockConflictPartsInKVSuccess;
    extern const Event TryResetAndLockConflictPartsInKVFailed;
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
    // extern const Event SetTransactionRecordStatusWithMemoryBufferSuccess;
    // extern const Event SetTransactionRecordStatusWithMemoryBufferFailed;
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
    extern const Event GetTableIDByNameSuccess;
    extern const Event GetTableIDByNameFailed;
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
    extern const Event GetTableTrashItemsMetricsDataFromMetastoreSuccess;
    extern const Event GetTableTrashItemsMetricsDataFromMetastoreFailed;
    extern const Event GetPartsInfoMetricsSuccess;
    extern const Event GetPartsInfoMetricsFailed;
    // extern const Event GetOrSetBufferManagerMetadataSuccess;
    // extern const Event GetOrSetBufferManagerMetadataFailed;
    // extern const Event RemoveBufferManagerMetadataSuccess;
    // extern const Event RemoveBufferManagerMetadataFailed;
    // extern const Event GetBufferLogMetadataVecSuccess;
    // extern const Event GetBufferLogMetadataVecFailed;
    // extern const Event SetCnchLogMetadataSuccess;
    // extern const Event SetCnchLogMetadataFailed;
    // extern const Event SetCnchLogMetadataInBatchSuccess;
    // extern const Event SetCnchLogMetadataInBatchFailed;
    // extern const Event GetCnchLogMetadataSuccess;
    // extern const Event GetCnchLogMetadataFailed;
    // extern const Event RemoveCnchLogMetadataSuccess;
    // extern const Event RemoveCnchLogMetadataFailed;
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
    extern const Event UpdateResourceGroupSuccess;
    extern const Event UpdateResourceGroupFailed;
    extern const Event GetResourceGroupSuccess;
    extern const Event GetResourceGroupFailed;
    extern const Event RemoveResourceGroupSuccess;
    extern const Event RemoveResourceGroupFailed;
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
    // extern const Event PutAccessPolicySuccess;
    // extern const Event PutAccessPolicyFailed;
    // extern const Event TryGetAccessPolicySuccess;
    // extern const Event TryGetAccessPolicyFailed;
    extern const Event GetMaskingPolicySuccess;
    extern const Event GetMaskingPolicyFailed;
    // extern const Event GetAllAccessPolicySuccess;
    // extern const Event GetAllAccessPolicyFailed;
    // extern const Event GetPolicyAppliedTablesSuccess;
    // extern const Event GetPolicyAppliedTablesFailed;
    extern const Event GetAllMaskingPolicyAppliedTablesSuccess;
    extern const Event GetAllMaskingPolicyAppliedTablesFailed;
    // extern const Event DropPolicyMappingsSuccess;
    // extern const Event DropPolicyMappingsFailed;
    // extern const Event DropAccessPolicySuccess;
    // extern const Event DropAccessPolicyFailed;
    extern const Event IsHostServerSuccess;
    extern const Event IsHostServerFailed;
    // extern const Event S3GETMicroseconds;
    // extern const Event S3GETBytes;
    // extern const Event S3GETRequestsCount;
    // extern const Event S3GETRequestsErrors;
    // extern const Event S3GETRequestsThrottling;
    // extern const Event S3GETRequestsRedirects;
    // extern const Event S3HEADMicroseconds;
    // extern const Event S3HEADBytes;
    // extern const Event S3HEADRequestsCount;
    // extern const Event S3HEADRequestsErrors;
    // extern const Event S3HEADRequestsThrottling;
    // extern const Event S3HEADRequestsRedirects;
    // extern const Event S3POSTMicroseconds;
    // extern const Event S3POSTBytes;
    // extern const Event S3POSTRequestsCount;
    // extern const Event S3POSTRequestsErrors;
    // extern const Event S3POSTRequestsThrottling;
    // extern const Event S3POSTRequestsRedirects;
    // extern const Event S3DELETEMicroseconds;
    // extern const Event S3DELETEBytes;
    // extern const Event S3DELETERequestsCount;
    // extern const Event S3DELETERequestsErrors;
    // extern const Event S3DELETERequestsThrottling;
    // extern const Event S3DELETERequestsRedirects;
    // extern const Event S3PATCHMicroseconds;
    // extern const Event S3PATCHBytes;
    // extern const Event S3PATCHRequestsCount;
    // extern const Event S3PATCHRequestsErrors;
    // extern const Event S3PATCHRequestsThrottling;
    // extern const Event S3PATCHRequestsRedirects;
    // extern const Event S3PUTMicroseconds;
    // extern const Event S3PUTBytes;
    // extern const Event S3PUTRequestsCount;
    // extern const Event S3PUTRequestsErrors;
    // extern const Event S3PUTRequestsThrottling;
    // extern const Event S3PUTRequestsRedirects;
    // extern const Event WriteBufferFromS3WriteMicroseconds;
    // extern const Event WriteBufferFromS3WriteBytes;
    // extern const Event WriteBufferFromS3WriteErrors;
    // extern const Event ReadFromS3BufferCount;
    // extern const Event ReadBufferFromS3ReadFailed;
    // extern const Event ReadBufferFromS3ReadBytes;
    // extern const Event ReadBufferFromS3ReadMicroseconds;
    // extern const Event S3ReadAheadReaderRead;
    extern const Event QueryMemoryLimitExceeded;
    extern const Event InsertQuery;
    extern const Event Merge;
    extern const Event SelectedRows;
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
    extern const Event SelectedBytes;
    extern const Event SelectQuery;
    extern const Event FailedSelectQuery;
    extern const Event FailedInsertQuery;
    extern const Event QueryTimeMicroseconds;
    extern const Event SelectQueryTimeMicroseconds;
    extern const Event InsertQueryTimeMicroseconds;
    extern const Event RejectedInserts;
    extern const Event DelayedInserts;
    extern const Event DelayedInsertsMilliseconds;
    extern const Event SlowRead;
    extern const Event ReadBackoff;
    extern const Event MergeTreeDataWriterRows;
    extern const Event MergeTreeDataWriterUncompressedBytes;
    extern const Event MergeTreeDataWriterCompressedBytes;
    extern const Event MergeTreeDataWriterBlocks;
    extern const Event MergeTreeDataWriterBlocksAlreadySorted;
    extern const Event DNSError;
    extern const Event CatalogTime;
    extern const Event PrunePartsTime;
    extern const Event TotalPartitions;
    extern const Event PrunedPartitions;
    extern const Event MergesTimeMilliseconds;
    extern const Event DiskCacheDeviceBytesRead;
    extern const Event DiskCacheDeviceBytesWritten;
    extern const Event DiskCacheDeviceWriteIOLatency;
    extern const Event DiskCacheDeviceReadIOLatency;
    extern const Event DiskCacheDeviceWriteIOErrors;
    extern const Event DiskCacheDeviceReadIOErrors;
    extern const Event CnchSendResourceRpcCallElapsedMilliseconds;
    extern const Event CnchSendResourceElapsedMilliseconds;
    extern const Event TsCacheCheckElapsedMilliseconds;
    extern const Event TsCacheUpdateElapsedMilliseconds;
    extern const Event CnchReadDataMicroSeconds;
    extern const Event PartsToAttach;
    extern const Event ConnectionPoolIsFullMicroseconds;
    extern const Event HeavyLoadWorkerSize;
    extern const Event AllWorkerSize;
}

namespace CurrentMetrics
{
    extern const Metric Query;
    extern const Metric DefaultQuery;
    extern const Metric InsertQuery;
    extern const Metric SystemQuery;
    extern const Metric Merge;
    extern const Metric Manipulation;
    extern const Metric QueryPreempted;
    extern const Metric QueryThread;
    extern const Metric TCPConnection;
    extern const Metric HTTPConnection;
    extern const Metric InterserverConnection;
    extern const Metric ZooKeeperWatch;
    extern const Metric ZooKeeperRequest;
    extern const Metric CnchTxnActiveTransactions;
    extern const Metric CnchTxnTransactionRecords;
    extern const Metric CnchSDRequestsUpstream;
    extern const Metric DiskCacheEvictQueueLength;
    extern const Metric DiskCacheRoughSingleStatsBucketSize;
    // extern const Metric DiskCacheTasks;
    // extern const Metric DiskCacheTaskQueue;
    extern const Metric PartsOutdated;
    extern const Metric GlobalThread;
    extern const Metric GlobalThreadActive;
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric Revision;
    extern const Metric VersionInteger;
    extern const Metric Consumer;
    extern const Metric Deduper;
    extern const Metric UniqueTableBackgroundPoolTask;
    extern const Metric PartsTemporary;
    extern const Metric PartsDeleting;
    extern const Metric PartsDeleteOnDestroy;
    extern const Metric PartsCommitted;
    extern const Metric PartsWide;
    extern const Metric PartsCompact;
    extern const Metric PartsInMemory;
    extern const Metric PartsCNCH;
    extern const Metric PartsPreCommitted;
    extern const Metric MemoryTracking;
    extern const Metric MemoryTrackingForMerges;
    extern const Metric MemoryTrackingForConsuming;
    extern const Metric BackgroundPoolTask;
    extern const Metric BackgroundCNCHTopologySchedulePoolTask;
    extern const Metric BackgroundFetchesPoolTask;
    extern const Metric BackgroundMovePoolTask;
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric PartMutation;
    extern const Metric BackgroundMessageBrokerSchedulePoolTask;
    extern const Metric UniqueTableBackgroundPoolTask;
    extern const Metric BackgroundBufferFlushSchedulePoolTask;
    extern const Metric DiskSpaceReservedForMerge;
}

namespace HistogramMetrics
{
    extern const Metric QueryLatency;
    extern const Metric UnlimitedQueryLatency;
}

namespace DB
{

/** Metric exporter to export metrics to Prometheus.
 * Prometheus HTTP address and export interval are set in server config
 * Currently, metrics are exported only for server and worker
 */
class ServerPrometheusMetricsWriter : public IPrometheusMetricsWriter
{
public:
    ServerPrometheusMetricsWriter(
        const Poco::Util::AbstractConfiguration & config, ContextMutablePtr context,
        const std::string & config_name, const AsynchronousMetrics & async_metrics_);

    /// Main function to export metrics
    virtual void write(WriteBuffer & wb) override;
    virtual ~ServerPrometheusMetricsWriter() override = default;

protected:
    virtual void writeConfigMetrics(WriteBuffer & wb) override;

private:
    struct MetricEntry
    {
        MetricLabels labels;
        String type;
        UInt64 value;
    };
    using MetricEntries = std::vector<MetricEntry>;
    using MetricMap = std::unordered_map<String, MetricEntries>;

    ContextMutablePtr context;
    const AsynchronousMetrics & async_metrics;

    // Config-related metrics
    const int max_concurrent_default_queries;
    const int max_concurrent_insert_queries;
    const int max_concurrent_system_queries;

    const bool send_events;
    const bool send_metrics;
    const bool send_asynchronous_metrics;
    const bool send_part_metrics;  /// Only for verification test

    const FormatSettings format_settings;

    /// Maps/vectors to retrieve and export metrics
    MetricMap getInternalMetrics();

    void writeProfileEvents(WriteBuffer & wb);
    void writeLabelledMetrics(WriteBuffer & wb);
    void writeCurrentMetrics(WriteBuffer & wb);
    void writeAsyncMetrics(WriteBuffer & wb);
    void writeHistogramMetrics(WriteBuffer & wb);
    void writeInternalMetrics(WriteBuffer & wb);
    void writePartMetrics(WriteBuffer & wb);

    static constexpr auto MAX_CONCURRENT_DEFAULT_QUERIES_KEY = "max_concurrent_default_queries";
    static constexpr auto MAX_CONCURRENT_INSERT_QUERIES_KEY = "max_concurrent_insert_queries";
    static constexpr auto MAX_CONCURRENT_SYSTEM_QUERIES_KEY = "max_concurrent_system_queries";

    const std::unordered_map<String, String> config_namedoc_map =
    {
        {MAX_CONCURRENT_DEFAULT_QUERIES_KEY, "Total number of maximum concurrent queries of Default type allowed"},
        {MAX_CONCURRENT_INSERT_QUERIES_KEY, "Total number of maximum concurrent queries of Insert type allowed"},
        {MAX_CONCURRENT_SYSTEM_QUERIES_KEY, "Total number of maximum concurrent queries of System type allowed"},
        {BUILD_INFO_KEY, "Build info"},
    };

    const std::vector<ProfileEvents::Event> catalog_profile_events_list = {
        ProfileEvents::CatalogConstructorSuccess,
        ProfileEvents::CatalogConstructorFailed,
        ProfileEvents::UpdateTableStatisticsSuccess,
        ProfileEvents::UpdateTableStatisticsFailed,
        ProfileEvents::GetTableStatisticsSuccess,
        ProfileEvents::GetTableStatisticsFailed,
        ProfileEvents::RemoveTableStatisticsSuccess,
        ProfileEvents::RemoveTableStatisticsFailed,
        ProfileEvents::UpdateColumnStatisticsSuccess,
        ProfileEvents::UpdateColumnStatisticsFailed,
        ProfileEvents::GetColumnStatisticsSuccess,
        ProfileEvents::GetColumnStatisticsFailed,
        ProfileEvents::GetAllColumnStatisticsSuccess,
        ProfileEvents::GetAllColumnStatisticsFailed,
        ProfileEvents::RemoveColumnStatisticsSuccess,
        ProfileEvents::RemoveColumnStatisticsFailed,
        ProfileEvents::RemoveAllColumnStatisticsSuccess,
        ProfileEvents::RemoveAllColumnStatisticsFailed,
        ProfileEvents::UpdateStatisticsSettingsSuccess,
        ProfileEvents::UpdateStatisticsSettingsFailed,
        ProfileEvents::GetStatisticsSettingsSuccess,
        ProfileEvents::GetStatisticsSettingsFailed,
        // ProfileEvents::UpdateSQLBindingSuccess,
        // ProfileEvents::UpdateSQLBindingFailed,
        // ProfileEvents::GetSQLBindingSuccess,
        // ProfileEvents::GetSQLBindingFailed,
        // ProfileEvents::GetSQLBindingsSuccess,
        // ProfileEvents::GetSQLBindingsFailed,
        // ProfileEvents::RemoveSQLBindingSuccess,
        // ProfileEvents::RemoveSQLBindingFailed,
        ProfileEvents::CreateDatabaseSuccess,
        ProfileEvents::CreateDatabaseFailed,
        ProfileEvents::GetDatabaseSuccess,
        ProfileEvents::GetDatabaseFailed,
        ProfileEvents::IsDatabaseExistsSuccess,
        ProfileEvents::IsDatabaseExistsFailed,
        ProfileEvents::DropDatabaseSuccess,
        ProfileEvents::DropDatabaseFailed,
        ProfileEvents::RenameDatabaseSuccess,
        ProfileEvents::RenameDatabaseFailed,
        ProfileEvents::AlterDatabaseSuccess,
        ProfileEvents::AlterDatabaseFailed,
        ProfileEvents::CreateTableSuccess,
        ProfileEvents::CreateTableFailed,
        ProfileEvents::DropTableSuccess,
        ProfileEvents::DropTableFailed,
        ProfileEvents::CreateUDFSuccess,
        ProfileEvents::CreateUDFFailed,
        ProfileEvents::DropUDFSuccess,
        ProfileEvents::DropUDFFailed,
        ProfileEvents::DetachTableSuccess,
        ProfileEvents::DetachTableFailed,
        ProfileEvents::AttachTableSuccess,
        ProfileEvents::AttachTableFailed,
        ProfileEvents::IsTableExistsSuccess,
        ProfileEvents::IsTableExistsFailed,
        ProfileEvents::AlterTableSuccess,
        ProfileEvents::AlterTableFailed,
        ProfileEvents::RenameTableSuccess,
        ProfileEvents::RenameTableFailed,
        ProfileEvents::SetWorkerGroupForTableSuccess,
        ProfileEvents::SetWorkerGroupForTableFailed,
        ProfileEvents::GetTableSuccess,
        ProfileEvents::GetTableFailed,
        ProfileEvents::TryGetTableSuccess,
        ProfileEvents::TryGetTableFailed,
        ProfileEvents::TryGetTableByUUIDSuccess,
        ProfileEvents::TryGetTableByUUIDFailed,
        ProfileEvents::GetTableByUUIDSuccess,
        ProfileEvents::GetTableByUUIDFailed,
        ProfileEvents::GetTablesInDBSuccess,
        ProfileEvents::GetTablesInDBFailed,
        ProfileEvents::GetAllViewsOnSuccess,
        ProfileEvents::GetAllViewsOnFailed,
        ProfileEvents::SetTableActivenessSuccess,
        ProfileEvents::SetTableActivenessFailed,
        ProfileEvents::GetTableActivenessSuccess,
        ProfileEvents::GetTableActivenessFailed,
        ProfileEvents::GetServerDataPartsInPartitionsSuccess,
        ProfileEvents::GetServerDataPartsInPartitionsFailed,
        ProfileEvents::GetAllServerDataPartsSuccess,
        ProfileEvents::GetAllServerDataPartsFailed,
        ProfileEvents::GetAllServerDataPartsWithDBMSuccess,
        ProfileEvents::GetAllServerDataPartsWithDBMFailed,
        ProfileEvents::GetDataPartsByNamesSuccess,
        ProfileEvents::GetDataPartsByNamesFailed,
        ProfileEvents::GetStagedDataPartsByNamesSuccess,
        ProfileEvents::GetStagedDataPartsByNamesFailed,
        ProfileEvents::GetAllDeleteBitmapsSuccess,
        ProfileEvents::GetAllDeleteBitmapsFailed,
        ProfileEvents::GetStagedPartsSuccess,
        ProfileEvents::GetStagedPartsFailed,
        ProfileEvents::GetDeleteBitmapsInPartitionsSuccess,
        ProfileEvents::GetDeleteBitmapsInPartitionsFailed,
        ProfileEvents::GetDeleteBitmapByKeysSuccess,
        ProfileEvents::GetDeleteBitmapByKeysFailed,
        ProfileEvents::AddDeleteBitmapsSuccess,
        ProfileEvents::AddDeleteBitmapsFailed,
        ProfileEvents::RemoveDeleteBitmapsSuccess,
        ProfileEvents::RemoveDeleteBitmapsFailed,
        ProfileEvents::FinishCommitSuccess,
        ProfileEvents::FinishCommitFailed,
        ProfileEvents::GetKafkaOffsetsVoidSuccess,
        ProfileEvents::GetKafkaOffsetsVoidFailed,
        ProfileEvents::GetKafkaOffsetsTopicPartitionListSuccess,
        ProfileEvents::GetKafkaOffsetsTopicPartitionListFailed,
        ProfileEvents::SetTransactionForKafkaConsumerSuccess,
        ProfileEvents::SetTransactionForKafkaConsumerFailed,
        ProfileEvents::GetTransactionForKafkaConsumerSuccess,
        ProfileEvents::GetTransactionForKafkaConsumerFailed,
        ProfileEvents::ClearKafkaTransactionsForTableSuccess,
        ProfileEvents::ClearKafkaTransactionsForTableFailed,
        ProfileEvents::ClearOffsetsForWholeTopicSuccess,
        ProfileEvents::ClearOffsetsForWholeTopicFailed,
        ProfileEvents::DropAllPartSuccess,
        ProfileEvents::DropAllPartFailed,
        ProfileEvents::GetPartitionListSuccess,
        ProfileEvents::GetPartitionListFailed,
        ProfileEvents::GetPartitionsFromMetastoreSuccess,
        ProfileEvents::GetPartitionsFromMetastoreFailed,
        ProfileEvents::GetPartitionIDsSuccess,
        ProfileEvents::GetPartitionIDsFailed,
        ProfileEvents::CreateDictionarySuccess,
        ProfileEvents::CreateDictionaryFailed,
        ProfileEvents::GetCreateDictionarySuccess,
        ProfileEvents::GetCreateDictionaryFailed,
        ProfileEvents::DropDictionarySuccess,
        ProfileEvents::DropDictionaryFailed,
        ProfileEvents::DetachDictionarySuccess,
        ProfileEvents::DetachDictionaryFailed,
        ProfileEvents::AttachDictionarySuccess,
        ProfileEvents::AttachDictionaryFailed,
        ProfileEvents::GetDictionariesInDBSuccess,
        ProfileEvents::GetDictionariesInDBFailed,
        ProfileEvents::GetDictionarySuccess,
        ProfileEvents::GetDictionaryFailed,
        ProfileEvents::IsDictionaryExistsSuccess,
        ProfileEvents::IsDictionaryExistsFailed,
        ProfileEvents::TryLockPartInKVSuccess,
        ProfileEvents::TryLockPartInKVFailed,
        ProfileEvents::UnLockPartInKVSuccess,
        ProfileEvents::UnLockPartInKVFailed,
        ProfileEvents::TryResetAndLockConflictPartsInKVSuccess,
        ProfileEvents::TryResetAndLockConflictPartsInKVFailed,
        ProfileEvents::CreateTransactionRecordSuccess,
        ProfileEvents::CreateTransactionRecordFailed,
        ProfileEvents::RemoveTransactionRecordSuccess,
        ProfileEvents::RemoveTransactionRecordFailed,
        ProfileEvents::RemoveTransactionRecordsSuccess,
        ProfileEvents::RemoveTransactionRecordsFailed,
        ProfileEvents::GetTransactionRecordSuccess,
        ProfileEvents::GetTransactionRecordFailed,
        ProfileEvents::TryGetTransactionRecordSuccess,
        ProfileEvents::TryGetTransactionRecordFailed,
        ProfileEvents::SetTransactionRecordSuccess,
        ProfileEvents::SetTransactionRecordFailed,
        ProfileEvents::SetTransactionRecordWithRequestsSuccess,
        ProfileEvents::SetTransactionRecordWithRequestsFailed,
        ProfileEvents::SetTransactionRecordCleanTimeSuccess,
        ProfileEvents::SetTransactionRecordCleanTimeFailed,
        // ProfileEvents::SetTransactionRecordStatusWithMemoryBufferSuccess,
        // ProfileEvents::SetTransactionRecordStatusWithMemoryBufferFailed,
        ProfileEvents::SetTransactionRecordStatusWithOffsetsSuccess,
        ProfileEvents::SetTransactionRecordStatusWithOffsetsFailed,
        ProfileEvents::RollbackTransactionSuccess,
        ProfileEvents::RollbackTransactionFailed,
        ProfileEvents::WriteIntentsSuccess,
        ProfileEvents::WriteIntentsFailed,
        ProfileEvents::TryResetIntentsIntentsToResetSuccess,
        ProfileEvents::TryResetIntentsIntentsToResetFailed,
        ProfileEvents::TryResetIntentsOldIntentsSuccess,
        ProfileEvents::TryResetIntentsOldIntentsFailed,
        ProfileEvents::ClearIntentsSuccess,
        ProfileEvents::ClearIntentsFailed,
        ProfileEvents::WritePartsSuccess,
        ProfileEvents::WritePartsFailed,
        ProfileEvents::SetCommitTimeSuccess,
        ProfileEvents::SetCommitTimeFailed,
        ProfileEvents::ClearPartsSuccess,
        ProfileEvents::ClearPartsFailed,
        ProfileEvents::WriteUndoBufferConstResourceSuccess,
        ProfileEvents::WriteUndoBufferConstResourceFailed,
        ProfileEvents::WriteUndoBufferNoConstResourceSuccess,
        ProfileEvents::WriteUndoBufferNoConstResourceFailed,
        ProfileEvents::ClearUndoBufferSuccess,
        ProfileEvents::ClearUndoBufferFailed,
        ProfileEvents::GetUndoBufferSuccess,
        ProfileEvents::GetUndoBufferFailed,
        ProfileEvents::GetAllUndoBufferSuccess,
        ProfileEvents::GetAllUndoBufferFailed,
        ProfileEvents::GetTransactionRecordsSuccess,
        ProfileEvents::GetTransactionRecordsFailed,
        ProfileEvents::GetTransactionRecordsTxnIdsSuccess,
        ProfileEvents::GetTransactionRecordsTxnIdsFailed,
        ProfileEvents::GetTransactionRecordsForGCSuccess,
        ProfileEvents::GetTransactionRecordsForGCFailed,
        ProfileEvents::ClearZombieIntentSuccess,
        ProfileEvents::ClearZombieIntentFailed,
        ProfileEvents::WriteFilesysLockSuccess,
        ProfileEvents::WriteFilesysLockFailed,
        ProfileEvents::GetFilesysLockSuccess,
        ProfileEvents::GetFilesysLockFailed,
        ProfileEvents::ClearFilesysLockDirSuccess,
        ProfileEvents::ClearFilesysLockDirFailed,
        ProfileEvents::ClearFilesysLockTxnIdSuccess,
        ProfileEvents::ClearFilesysLockTxnIdFailed,
        ProfileEvents::GetAllFilesysLockSuccess,
        ProfileEvents::GetAllFilesysLockFailed,
        ProfileEvents::InsertTransactionSuccess,
        ProfileEvents::InsertTransactionFailed,
        ProfileEvents::RemoveTransactionSuccess,
        ProfileEvents::RemoveTransactionFailed,
        ProfileEvents::GetActiveTransactionsSuccess,
        ProfileEvents::GetActiveTransactionsFailed,
        ProfileEvents::UpdateServerWorkerGroupSuccess,
        ProfileEvents::UpdateServerWorkerGroupFailed,
        ProfileEvents::GetWorkersInWorkerGroupSuccess,
        ProfileEvents::GetWorkersInWorkerGroupFailed,
        ProfileEvents::GetTableHistoriesSuccess,
        ProfileEvents::GetTableHistoriesFailed,
        ProfileEvents::GetTablesByIDSuccess,
        ProfileEvents::GetTablesByIDFailed,
        ProfileEvents::GetAllDataBasesSuccess,
        ProfileEvents::GetAllDataBasesFailed,
        ProfileEvents::GetAllTablesSuccess,
        ProfileEvents::GetAllTablesFailed,
        ProfileEvents::GetTrashTableIDIteratorSuccess,
        ProfileEvents::GetTrashTableIDIteratorFailed,
        ProfileEvents::GetAllUDFsSuccess,
        ProfileEvents::GetAllUDFsFailed,
        ProfileEvents::GetUDFByNameSuccess,
        ProfileEvents::GetUDFByNameFailed,
        ProfileEvents::GetTrashTableIDSuccess,
        ProfileEvents::GetTrashTableIDFailed,
        ProfileEvents::GetTablesInTrashSuccess,
        ProfileEvents::GetTablesInTrashFailed,
        ProfileEvents::GetDatabaseInTrashSuccess,
        ProfileEvents::GetDatabaseInTrashFailed,
        ProfileEvents::GetAllTablesIDSuccess,
        ProfileEvents::GetAllTablesIDFailed,
        ProfileEvents::GetTableIDByNameSuccess,
        ProfileEvents::GetTableIDByNameFailed,
        ProfileEvents::GetAllWorkerGroupsSuccess,
        ProfileEvents::GetAllWorkerGroupsFailed,
        ProfileEvents::GetAllDictionariesSuccess,
        ProfileEvents::GetAllDictionariesFailed,
        ProfileEvents::ClearDatabaseMetaSuccess,
        ProfileEvents::ClearDatabaseMetaFailed,
        ProfileEvents::ClearTableMetaForGCSuccess,
        ProfileEvents::ClearTableMetaForGCFailed,
        ProfileEvents::ClearDataPartsMetaSuccess,
        ProfileEvents::ClearDataPartsMetaFailed,
        ProfileEvents::ClearStagePartsMetaSuccess,
        ProfileEvents::ClearStagePartsMetaFailed,
        ProfileEvents::ClearDataPartsMetaForTableSuccess,
        ProfileEvents::ClearDataPartsMetaForTableFailed,
        ProfileEvents::ClearDeleteBitmapsMetaForTableSuccess,
        ProfileEvents::ClearDeleteBitmapsMetaForTableFailed,
        ProfileEvents::GetSyncListSuccess,
        ProfileEvents::GetSyncListFailed,
        ProfileEvents::ClearSyncListSuccess,
        ProfileEvents::ClearSyncListFailed,
        ProfileEvents::GetServerPartsByCommitTimeSuccess,
        ProfileEvents::GetServerPartsByCommitTimeFailed,
        ProfileEvents::CreateRootPathSuccess,
        ProfileEvents::CreateRootPathFailed,
        ProfileEvents::DeleteRootPathSuccess,
        ProfileEvents::DeleteRootPathFailed,
        ProfileEvents::GetAllRootPathSuccess,
        ProfileEvents::GetAllRootPathFailed,
        ProfileEvents::CreateMutationSuccess,
        ProfileEvents::CreateMutationFailed,
        ProfileEvents::RemoveMutationSuccess,
        ProfileEvents::RemoveMutationFailed,
        ProfileEvents::GetAllMutationsStorageIdSuccess,
        ProfileEvents::GetAllMutationsStorageIdFailed,
        ProfileEvents::GetAllMutationsSuccess,
        ProfileEvents::GetAllMutationsFailed,
        ProfileEvents::SetTableClusterStatusSuccess,
        ProfileEvents::SetTableClusterStatusFailed,
        ProfileEvents::GetTableClusterStatusSuccess,
        ProfileEvents::GetTableClusterStatusFailed,
        ProfileEvents::IsTableClusteredSuccess,
        ProfileEvents::IsTableClusteredFailed,
        ProfileEvents::SetTablePreallocateVWSuccess,
        ProfileEvents::SetTablePreallocateVWFailed,
        ProfileEvents::GetTablePreallocateVWSuccess,
        ProfileEvents::GetTablePreallocateVWFailed,
        ProfileEvents::GetTableTrashItemsMetricsDataFromMetastoreSuccess,
        ProfileEvents::GetTableTrashItemsMetricsDataFromMetastoreFailed,
        ProfileEvents::GetPartsInfoMetricsSuccess,
        ProfileEvents::GetPartsInfoMetricsFailed,
        // ProfileEvents::GetOrSetBufferManagerMetadataSuccess,
        // ProfileEvents::GetOrSetBufferManagerMetadataFailed,
        // ProfileEvents::RemoveBufferManagerMetadataSuccess,
        // ProfileEvents::RemoveBufferManagerMetadataFailed,
        // ProfileEvents::GetBufferLogMetadataVecSuccess,
        // ProfileEvents::GetBufferLogMetadataVecFailed,
        // ProfileEvents::SetCnchLogMetadataSuccess,
        // ProfileEvents::SetCnchLogMetadataFailed,
        // ProfileEvents::SetCnchLogMetadataInBatchSuccess,
        // ProfileEvents::SetCnchLogMetadataInBatchFailed,
        // ProfileEvents::GetCnchLogMetadataSuccess,
        // ProfileEvents::GetCnchLogMetadataFailed,
        // ProfileEvents::RemoveCnchLogMetadataSuccess,
        // ProfileEvents::RemoveCnchLogMetadataFailed,
        ProfileEvents::UpdateTopologiesSuccess,
        ProfileEvents::UpdateTopologiesFailed,
        ProfileEvents::GetTopologiesSuccess,
        ProfileEvents::GetTopologiesFailed,
        ProfileEvents::GetTrashDBVersionsSuccess,
        ProfileEvents::GetTrashDBVersionsFailed,
        ProfileEvents::UndropDatabaseSuccess,
        ProfileEvents::UndropDatabaseFailed,
        ProfileEvents::GetTrashTableVersionsSuccess,
        ProfileEvents::GetTrashTableVersionsFailed,
        ProfileEvents::UndropTableSuccess,
        ProfileEvents::UndropTableFailed,
        ProfileEvents::UpdateResourceGroupSuccess,
        ProfileEvents::UpdateResourceGroupFailed,
        ProfileEvents::GetResourceGroupSuccess,
        ProfileEvents::GetResourceGroupFailed,
        ProfileEvents::RemoveResourceGroupSuccess,
        ProfileEvents::RemoveResourceGroupFailed,
        ProfileEvents::GetInsertionLabelKeySuccess,
        ProfileEvents::GetInsertionLabelKeyFailed,
        ProfileEvents::PrecommitInsertionLabelSuccess,
        ProfileEvents::PrecommitInsertionLabelFailed,
        ProfileEvents::CommitInsertionLabelSuccess,
        ProfileEvents::CommitInsertionLabelFailed,
        ProfileEvents::TryCommitInsertionLabelSuccess,
        ProfileEvents::TryCommitInsertionLabelFailed,
        ProfileEvents::AbortInsertionLabelSuccess,
        ProfileEvents::AbortInsertionLabelFailed,
        ProfileEvents::GetInsertionLabelSuccess,
        ProfileEvents::GetInsertionLabelFailed,
        ProfileEvents::RemoveInsertionLabelSuccess,
        ProfileEvents::RemoveInsertionLabelFailed,
        ProfileEvents::RemoveInsertionLabelsSuccess,
        ProfileEvents::RemoveInsertionLabelsFailed,
        ProfileEvents::ScanInsertionLabelsSuccess,
        ProfileEvents::ScanInsertionLabelsFailed,
        ProfileEvents::ClearInsertionLabelsSuccess,
        ProfileEvents::ClearInsertionLabelsFailed,
        ProfileEvents::CreateVirtualWarehouseSuccess,
        ProfileEvents::CreateVirtualWarehouseFailed,
        ProfileEvents::AlterVirtualWarehouseSuccess,
        ProfileEvents::AlterVirtualWarehouseFailed,
        ProfileEvents::TryGetVirtualWarehouseSuccess,
        ProfileEvents::TryGetVirtualWarehouseFailed,
        ProfileEvents::ScanVirtualWarehousesSuccess,
        ProfileEvents::ScanVirtualWarehousesFailed,
        ProfileEvents::DropVirtualWarehouseSuccess,
        ProfileEvents::DropVirtualWarehouseFailed,
        ProfileEvents::CreateWorkerGroupSuccess,
        ProfileEvents::CreateWorkerGroupFailed,
        ProfileEvents::UpdateWorkerGroupSuccess,
        ProfileEvents::UpdateWorkerGroupFailed,
        ProfileEvents::TryGetWorkerGroupSuccess,
        ProfileEvents::TryGetWorkerGroupFailed,
        ProfileEvents::ScanWorkerGroupsSuccess,
        ProfileEvents::ScanWorkerGroupsFailed,
        ProfileEvents::DropWorkerGroupSuccess,
        ProfileEvents::DropWorkerGroupFailed,
        ProfileEvents::GetNonHostUpdateTimestampFromByteKVSuccess,
        ProfileEvents::GetNonHostUpdateTimestampFromByteKVFailed,
        ProfileEvents::MaskingPolicyExistsSuccess,
        ProfileEvents::MaskingPolicyExistsFailed,
        ProfileEvents::GetMaskingPoliciesSuccess,
        ProfileEvents::GetMaskingPoliciesFailed,
        // ProfileEvents::PutAccessPolicySuccess,
        // ProfileEvents::PutAccessPolicyFailed,
        // ProfileEvents::TryGetAccessPolicySuccess,
        // ProfileEvents::TryGetAccessPolicyFailed,
        ProfileEvents::GetMaskingPolicySuccess,
        ProfileEvents::GetMaskingPolicyFailed,
        // ProfileEvents::GetAllAccessPolicySuccess,
        // ProfileEvents::GetAllAccessPolicyFailed,
        // ProfileEvents::GetPolicyAppliedTablesSuccess,
        // ProfileEvents::GetPolicyAppliedTablesFailed,
        ProfileEvents::GetAllMaskingPolicyAppliedTablesSuccess,
        ProfileEvents::GetAllMaskingPolicyAppliedTablesFailed,
        // ProfileEvents::DropPolicyMappingsSuccess,
        // ProfileEvents::DropPolicyMappingsFailed,
        // ProfileEvents::DropAccessPolicySuccess,
        // ProfileEvents::DropAccessPolicyFailed,
        ProfileEvents::IsHostServerSuccess,
        ProfileEvents::IsHostServerFailed,
    };

    const std::vector<ProfileEvents::Event> profile_events_list =
    {
        ProfileEvents::Query,
        ProfileEvents::TimedOutQuery,
        ProfileEvents::InsufficientConcurrencyQuery,
        ProfileEvents::Manipulation,
        ProfileEvents::ManipulationSuccess,
        ProfileEvents::InsertedRows,
        ProfileEvents::InsertedBytes,
        ProfileEvents::MergedRows,
        ProfileEvents::MergedUncompressedBytes,
        // ProfileEvents::CloudMergeStarted,
        // ProfileEvents::CloudMergeEnded,
        ///About HDFS
        ProfileEvents::ReadBufferFromHdfsRead,
        ProfileEvents::ReadBufferFromHdfsReadFailed,
        ProfileEvents::ReadBufferFromHdfsReadBytes,
        ProfileEvents::ReadBufferFromFileDescriptorRead,
        ProfileEvents::ReadBufferFromFileDescriptorReadFailed,
        ProfileEvents::ReadBufferFromFileDescriptorReadBytes,
        ProfileEvents::WriteBufferFromHdfsWrite,
        ProfileEvents::WriteBufferFromHdfsWriteFailed,
        ProfileEvents::WriteBufferFromHdfsWriteBytes,
        ProfileEvents::WriteBufferFromFileDescriptorWrite,
        ProfileEvents::WriteBufferFromFileDescriptorWriteFailed,
        ProfileEvents::WriteBufferFromFileDescriptorWriteBytes,
        // ProfileEvents::CnchReadRowsFromDiskCache,
        // ProfileEvents::CnchReadRowsFromRemote,
        ProfileEvents::HDFSReadElapsedMicroseconds,
        // ProfileEvents::HDFSReadElapsedCpuMilliseconds,
        ProfileEvents::HDFSWriteElapsedMicroseconds,
        ProfileEvents::DiskReadElapsedMicroseconds,
        ProfileEvents::DiskWriteElapsedMicroseconds,
        ///About SD
        ProfileEvents::SDRequest,
        ProfileEvents::SDRequestFailed,
        ProfileEvents::SDRequestUpstream,
        ProfileEvents::SDRequestUpstreamFailed,
        ///About Cnch transaction
        ProfileEvents::CnchTxnAborted,
        ProfileEvents::CnchTxnCommitted,
        ProfileEvents::CnchTxnExpired,
        ProfileEvents::CnchTxnReadTxnCreated,
        ProfileEvents::CnchTxnWriteTxnCreated,
        ProfileEvents::CnchTxnCommitV1Failed,
        ProfileEvents::CnchTxnCommitV2Failed,
        ProfileEvents::CnchTxnCommitV1ElapsedMilliseconds,
        ProfileEvents::CnchTxnCommitV2ElapsedMilliseconds,
        ProfileEvents::CnchTxnPrecommitElapsedMilliseconds,
        ProfileEvents::CnchTxnCommitKVElapsedMilliseconds,
        ProfileEvents::CnchTxnCleanFailed,
        ProfileEvents::CnchTxnCleanElapsedMilliseconds,
        ProfileEvents::CnchTxnAllTransactionRecord,
        ProfileEvents::CnchTxnFinishedTransactionRecord,
        // ProfileEvents::IntentLockAcquiredAttempt,
        // ProfileEvents::IntentLockAcquiredSuccess,
        // ProfileEvents::IntentLockAcquiredElapsedMilliseconds,
        // ProfileEvents::IntentLockWriteIntentAttempt,
        // ProfileEvents::IntentLockWriteIntentSuccess,
        // ProfileEvents::IntentLockWriteIntentConflict,
        // ProfileEvents::IntentLockWriteIntentPreemption,
        ProfileEvents::IntentLockWriteIntentElapsedMilliseconds,
        // ProfileEvents::TsCacheCheckSuccess,
        // ProfileEvents::TsCacheCheckFailed,
        ProfileEvents::TsCacheCheckElapsedMilliseconds,
        ProfileEvents::TsCacheUpdateElapsedMilliseconds,
        ProfileEvents::TSORequest,
        ProfileEvents::TSOError,
        /// About disk cache
        ProfileEvents::DiskCacheGetMetaMicroSeconds,
        ProfileEvents::DiskCacheGetTotalOps,
        ProfileEvents::DiskCacheSetTotalOps,
        ProfileEvents::CnchReadDataMicroSeconds,
        // ProfileEvents::CnchReadDataCpuMicroSeconds,
        // ProfileEvents::CnchReadDataDecompressMicroSeconds,
        // ProfileEvents::CnchReadDataDecompressCpuMicroSeconds,
        // ProfileEvents::CnchReadDataDeserialMicroSeconds,
        // ProfileEvents::CnchReadDataDeserialCpuMicroSeconds,
        ProfileEvents::DiskCacheAcquireStatsLock,
        ProfileEvents::DiskCacheScheduleCacheTaskMicroseconds,
        ProfileEvents::DiskCacheUpdateStatsMicroSeconds,
        ProfileEvents::DiskCacheTaskDropCount,
        // ProfileEvents::PreloadSubmitTotalOps,
        // ProfileEvents::PreloadSendTotalOps,
        // ProfileEvents::PreloadExecTotalOps,
        /// About Unique key
        ProfileEvents::DeleteBitmapCacheHit,
        ProfileEvents::DeleteBitmapCacheMiss,
        // ProfileEvents::BackgroundDedupSchedulePoolTask,
        ProfileEvents::UniqueKeyIndexMetaCacheHit,
        ProfileEvents::UniqueKeyIndexMetaCacheMiss,
        ProfileEvents::UniqueKeyIndexBlockCacheHit,
        ProfileEvents::UniqueKeyIndexBlockCacheMiss,
        /// About s3
        // ProfileEvents::S3GETMicroseconds,
        // ProfileEvents::S3GETBytes,
        // ProfileEvents::S3GETRequestsCount,
        // ProfileEvents::S3GETRequestsErrors,
        // ProfileEvents::S3GETRequestsThrottling,
        // ProfileEvents::S3GETRequestsRedirects,
        // ProfileEvents::S3HEADMicroseconds,
        // ProfileEvents::S3HEADBytes,
        // ProfileEvents::S3HEADRequestsCount,
        // ProfileEvents::S3HEADRequestsErrors,
        // ProfileEvents::S3HEADRequestsThrottling,
        // ProfileEvents::S3HEADRequestsRedirects,
        // ProfileEvents::S3POSTMicroseconds,
        // ProfileEvents::S3POSTBytes,
        // ProfileEvents::S3POSTRequestsCount,
        // ProfileEvents::S3POSTRequestsErrors,
        // ProfileEvents::S3POSTRequestsThrottling,
        // ProfileEvents::S3POSTRequestsRedirects,
        // ProfileEvents::S3DELETEMicroseconds,
        // ProfileEvents::S3DELETEBytes,
        // ProfileEvents::S3DELETERequestsCount,
        // ProfileEvents::S3DELETERequestsErrors,
        // ProfileEvents::S3DELETERequestsThrottling,
        // ProfileEvents::S3DELETERequestsRedirects,
        // ProfileEvents::S3PATCHMicroseconds,
        // ProfileEvents::S3PATCHBytes,
        // ProfileEvents::S3PATCHRequestsCount,
        // ProfileEvents::S3PATCHRequestsErrors,
        // ProfileEvents::S3PATCHRequestsThrottling,
        // ProfileEvents::S3PATCHRequestsRedirects,
        // ProfileEvents::S3PUTMicroseconds,
        // ProfileEvents::S3PUTBytes,
        // ProfileEvents::S3PUTRequestsCount,
        // ProfileEvents::S3PUTRequestsErrors,
        // ProfileEvents::S3PUTRequestsThrottling,
        // ProfileEvents::S3PUTRequestsRedirects,
        // ProfileEvents::WriteBufferFromS3WriteMicroseconds,
        // ProfileEvents::WriteBufferFromS3WriteBytes,
        // ProfileEvents::WriteBufferFromS3WriteErrors,
        // ProfileEvents::ReadBufferFromS3Read,
        // ProfileEvents::ReadBufferFromS3ReadFailed,
        // ProfileEvents::ReadBufferFromS3ReadBytes,
        // ProfileEvents::ReadBufferFromS3ReadMicroseconds,
        // ProfileEvents::S3ReadAheadReaderRead,
        ProfileEvents::QueryMemoryLimitExceeded,
        ProfileEvents::InsertQuery,
        ProfileEvents::Merge,
        ProfileEvents::SelectedRows,
        ProfileEvents::SelectedParts,
        ProfileEvents::SelectedRanges,
        ProfileEvents::SelectedMarks,
        ProfileEvents::SelectedBytes,
        ProfileEvents::SelectQuery,
        ProfileEvents::FailedSelectQuery,
        ProfileEvents::FailedInsertQuery,
        ProfileEvents::QueryTimeMicroseconds,
        ProfileEvents::SelectQueryTimeMicroseconds,
        ProfileEvents::InsertQueryTimeMicroseconds,
        ProfileEvents::RejectedInserts,
        ProfileEvents::DelayedInserts,
        ProfileEvents::DelayedInsertsMilliseconds,
        ProfileEvents::SlowRead,
        ProfileEvents::ReadBackoff,
        ProfileEvents::MergeTreeDataWriterRows,
        ProfileEvents::MergeTreeDataWriterUncompressedBytes,
        ProfileEvents::MergeTreeDataWriterCompressedBytes,
        ProfileEvents::MergeTreeDataWriterBlocks,
        ProfileEvents::MergeTreeDataWriterBlocksAlreadySorted,
        ProfileEvents::DNSError,
        ProfileEvents::CatalogTime,
        ProfileEvents::PrunePartsTime,
        ProfileEvents::TotalPartitions,
        ProfileEvents::PrunedPartitions,
        ProfileEvents::MergesTimeMilliseconds,
        ProfileEvents::DiskCacheDeviceBytesRead,
        ProfileEvents::DiskCacheDeviceBytesWritten,
        ProfileEvents::DiskCacheDeviceWriteIOLatency,
        ProfileEvents::DiskCacheDeviceReadIOLatency,
        ProfileEvents::DiskCacheDeviceWriteIOErrors,
        ProfileEvents::DiskCacheDeviceReadIOErrors,
        ProfileEvents::CnchSendResourceRpcCallElapsedMilliseconds,
        ProfileEvents::CnchSendResourceElapsedMilliseconds,
        ProfileEvents::TsCacheCheckElapsedMilliseconds,
        ProfileEvents::TsCacheUpdateElapsedMilliseconds,
        ProfileEvents::CnchReadDataMicroSeconds,
        ProfileEvents::PartsToAttach,
        ProfileEvents::ConnectionPoolIsFullMicroseconds,
        ProfileEvents::HeavyLoadWorkerSize,
        ProfileEvents::AllWorkerSize,
    };
    const std::vector<CurrentMetrics::Metric> current_metrics_list =
    {
        CurrentMetrics::DefaultQuery,
        CurrentMetrics::InsertQuery,
        CurrentMetrics::SystemQuery,
        CurrentMetrics::Merge,
        CurrentMetrics::Manipulation,
        CurrentMetrics::QueryPreempted,
        CurrentMetrics::QueryThread,
        CurrentMetrics::TCPConnection,
        CurrentMetrics::HTTPConnection,
        CurrentMetrics::InterserverConnection,
        CurrentMetrics::ZooKeeperWatch,
        CurrentMetrics::ZooKeeperRequest,
        CurrentMetrics::CnchTxnActiveTransactions,
        CurrentMetrics::CnchTxnTransactionRecords,
        CurrentMetrics::CnchSDRequestsUpstream,
        CurrentMetrics::DiskCacheEvictQueueLength,
        CurrentMetrics::DiskCacheRoughSingleStatsBucketSize,
        // CurrentMetrics::DiskCacheTasks,
        // CurrentMetrics::DiskCacheTaskQueue,
        CurrentMetrics::PartsOutdated,
        CurrentMetrics::GlobalThread,
        CurrentMetrics::GlobalThreadActive,
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::Revision,
        CurrentMetrics::VersionInteger,
        CurrentMetrics::Consumer,
        CurrentMetrics::Deduper,
        CurrentMetrics::UniqueTableBackgroundPoolTask,
        CurrentMetrics::PartsTemporary,
        CurrentMetrics::PartsDeleting,
        CurrentMetrics::PartsDeleteOnDestroy,
        CurrentMetrics::PartsCommitted,
        CurrentMetrics::PartsWide,
        CurrentMetrics::PartsCompact,
        CurrentMetrics::PartsInMemory,
        CurrentMetrics::PartsCNCH,
        CurrentMetrics::PartsPreCommitted,
        CurrentMetrics::MemoryTracking,
        CurrentMetrics::MemoryTrackingForMerges,
        CurrentMetrics::MemoryTrackingForConsuming,
        CurrentMetrics::BackgroundPoolTask,
        CurrentMetrics::BackgroundCNCHTopologySchedulePoolTask,
        CurrentMetrics::BackgroundFetchesPoolTask,
        CurrentMetrics::BackgroundMovePoolTask,
        CurrentMetrics::BackgroundSchedulePoolTask,
        CurrentMetrics::PartMutation,
        CurrentMetrics::BackgroundMessageBrokerSchedulePoolTask,
        CurrentMetrics::UniqueTableBackgroundPoolTask,
        CurrentMetrics::BackgroundBufferFlushSchedulePoolTask,
        CurrentMetrics::DiskSpaceReservedForMerge,
    };
    const std::vector<String> async_metrics_list =
    {
        "MarkCacheBytes",
        "MarkCacheFiles",
        "UncompressedCacheBytes",
        "UncompressedCacheCells",
        "CompiledExpressionCacheCount",
        "MaxPartCountForPartition",
        "NumberOfDatabases",
        "NumberOfTables",
        "TotalBytesOfMergeTreeTables",
        "TotalRowsOfMergeTreeTables",
        "TotalPartsOfMergeTreeTables",
        "Uptime"
    };
    const std::vector<HistogramMetrics::Metric> histogram_metrics_list =
    {
        HistogramMetrics::QueryLatency,
        HistogramMetrics::UnlimitedQueryLatency,
    };

    static constexpr auto PROFILE_EVENTS_PREFIX = "cnch_profile_events_";
    static constexpr auto CURRENT_METRICS_PREFIX = "cnch_current_metrics_";
    static constexpr auto ASYNCHRONOUS_METRICS_PREFIX = "cnch_async_metrics_";
    static constexpr auto INTERNAL_METRICS_PREFIX = "cnch_internal_metrics_";
    static constexpr auto PART_METRICS_PREFIX = "cnch_part_metrics_";  /// Only for verification purpose (high overhead)
    static constexpr auto HISTOGRAM_METRICS_PREFIX = "cnch_histogram_metrics_";
    static constexpr auto CATALOG_METRICS_PREFIX = "cnch_catalog_metrics_";

    /// The below labels are used for "PART_METRICS_PREFIX"
    static constexpr auto PARTS_NUMBER_LABEL = "parts_number";
    static constexpr auto PARTS_SIZE_LABEL = "parts_size";
    static constexpr auto ROWS_COUNT_LABEL = "rows_count";

    /// CurrentMetrics custom variable names
    static constexpr auto CURRENT_METRICS_QUERY_KEY = "query";

    /// ProfileEvents custom variable names
    static constexpr auto PROFILE_EVENTS_LABELLED_QUERY_KEY = "labelled_query";

    /// Internal metrics variables
    static constexpr auto QUEUED_QUERIES_TIME_TOTAL_KEY = "queued_queries_time_milliseconds_total";
    static constexpr auto RUNNING_QUERIES_TIME_TOTAL_KEY = "running_queries_time_milliseconds_total";
    static constexpr auto RUNNING_QUERIES_KEY = "running_queries";
    static constexpr auto QUEUED_QUERIES_KEY= "queued_queries";
    static constexpr auto INTENT_QUERIES_ONDEMAND_KEY = "intent_queries_ondemand";
    static constexpr auto INTENT_QUERIES_PREALLOCATE_KEY = "intent_queries_preallocate";
    static constexpr auto READY_TO_SHUT_DOWN_KEY = "ready_to_shut_down";
    static constexpr auto UPTIME_SECONDS_KEY = "uptime_seconds";
    static constexpr auto POD_UUID_ENVVAR = "POD_UUID";
    static constexpr auto VW_ID_ENVVAR = "VW_ID";


    static constexpr auto TOTAL_SUFFIX = "_total";
    static constexpr auto BUCKET_SUFFIX = "_bucket";
    static constexpr auto SUM_SUFFIX = "_sum";
    static constexpr auto COUNT_SUFFIX = "_count";


    const std::unordered_map<String, String> part_metrics_namedoc_map =
    {
        {PARTS_NUMBER_LABEL, "Number of parts (total / visible / invisible) in a table"},
        {PARTS_SIZE_LABEL, "Size of parts (total / visible / invisible) in a table"},
        {ROWS_COUNT_LABEL, "Number of rows (total / visible / invisible) in a table"},
    };

    const std::unordered_map<String, String> internal_metrics_namedoc_map =
    {
        {QUEUED_QUERIES_TIME_TOTAL_KEY, "Total time spent queuing queries "},
        {RUNNING_QUERIES_TIME_TOTAL_KEY, "Total time spent running queries"},
        {RUNNING_QUERIES_KEY, "Number of currently running queries"},
        {QUEUED_QUERIES_KEY, "Number of currently pending queries"},
        {INTENT_QUERIES_ONDEMAND_KEY, "Number of intent queries in ondemand mode"},
        {INTENT_QUERIES_PREALLOCATE_KEY, "Number of intent queries in preallocate mode"},
        {READY_TO_SHUT_DOWN_KEY, "Indicate whether the node is ready to shut down"},
        {UPTIME_SECONDS_KEY, "Uptime of server"}
    };

    // Metric names used frequently in write() initialized here for performance
    const String sd_request_metric{ProfileEvents::getSnakeName(ProfileEvents::SDRequest)};
    const String profile_event_query_metric{ProfileEvents::getSnakeName(ProfileEvents::Query)};
    const String failed_queries_metrics{LabelledMetrics::getSnakeName(LabelledMetrics::QueriesFailed)};
};

}

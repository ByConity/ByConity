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

//#include <Catalog/MetastoreByteKVImpl.h>
#include <Catalog/MetastoreFDBImpl.h>
#include <Catalog/StringHelper.h>
#include <Catalog/CatalogUtils.h>
#include <Protos/data_models.pb.h>
#include <Protos/data_models_mv.pb.h>

#include <Storages/MergeTree/DeleteBitmapMeta.h>
// #include <Transaction/ICnchTransaction.h>
#include <algorithm>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <Access/IAccessEntity.h>
#include <Backups/BackupStatus.h>
#include <Catalog/IMetastore.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <Core/UUID.h>
#include <Interpreters/DistributedStages/PlanSegmentInstance.h>
#include <Interpreters/SQLBinding/SQLBinding.h>
#include <Interpreters/PreparedStatement/PreparedStatementCatalog.h>
#include <MergeTreeCommon/InsertionLabel.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Protos/RPCHelpers.h>
#include <ResourceManagement/CommonData.h>
#include <Statistics/ExportSymbols.h>
#include <Statistics/StatisticsBase.h>
#include <Storages/StorageSnapshot.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>
#include <cppkafka/cppkafka.h>
#include <google/protobuf/repeated_field.h>
#include <Common/Config/MetastoreConfig.h>
#include <common/types.h>

namespace DB::ErrorCodes
{
    extern const int METASTORE_ACCESS_ENTITY_NOT_IMPLEMENTED;
    extern const int READONLY;
}

namespace DB::Catalog
{

#define EX_CATALOG_STORE_PREFIX "EXCATA_"
#define DB_STORE_PREFIX "DB_"
#define DB_UUID_UNIQUE_PREFIX "DU_"
#define DELETE_BITMAP_PREFIX "DLB_"
#define TABLE_STORE_PREFIX "TB_"
#define SNAPSHOT_STORE_PREFIX "SNST_"
#define MASKING_POLICY_PREFIX "MP_"
#define MASKING_POLICY_TABLE_MAPPING "MPT_"
#define TABLE_UUID_MAPPING "TM_"
#define TABLE_UUID_UNIQUE_PREFIX "TU_"
#define PART_STORE_PREFIX "PT_"
#define STAGED_PART_STORE_PREFIX "STG_PT_"
#define TABLE_PARTITION_INFO_PREFIX "TP_"
#define WORKER_GROUP_STORE_PREFIX "WC_"
#define RM_VW_PREFIX "RMVW_" /// Resource Management Virtual Warehouse
#define RM_WG_PREFIX "RMWG_" /// Resource Management Worker Group
#define TRANSACTION_STORE_PREFIX "TXN_"
#define TRANSACTION_RECORD_PREFIX "TR_"
#define UNDO_BUFFER_PREFIX "UB_"
#define KV_LOCK_PREFIX "LK_"
#define VIEW_DEPENDENCY_PREFIX "VD_"
#define MAT_VIEW_BASETABLES_PREFIX "MVB_"
#define MAT_VIEW_VERSION_PREFIX "MVV_"
#define SYNC_LIST_PREFIX "SL_"
#define KAFKA_OFFSETS_PREFIX "KO_"
#define KAFKA_TRANSACTION_PREFIX "KT_"
#define ROOT_PATH_PREFIX "RP_"
#define ROOT_PATH_ID_UNIQUE_PREFIX "RU_"
#define TABLE_MUTATION_PREFIX "MT_"
#define WRITE_INTENT_PREFIX "WI_"
#define TABLE_TRASH_PREFIX "TRASH_"
#define DICTIONARY_TRASH_PREFIX "DICTRASH_"
#define CNCH_LOG_PREFIX "LG_"
#define DATABASE_TRASH_PREFIX "DTRASH_"
#define DATA_ITEM_TRASH_PREFIX "GCTRASH_"
#define SERVERS_TOPOLOGY_KEY "SERVERS_TOPOLOGY"
#define TABLE_CLUSTER_STATUS "TCS_"
#define TABLE_DEFINITION_HASH "TDH_"
#define CLUSTER_BG_JOB_STATUS "CLUSTER_BGJS_"
#define MERGE_BG_JOB_STATUS "MERGE_BGJS_"
#define PARTGC_BG_JOB_STATUS "PARTGC_BGJS_"
#define PARTMOVER_BG_JOB_STATUS "PARTMOVER_BGJS_"
#define CONSUMER_BG_JOB_STATUS "CONSUMER_BGJS_"
#define DEDUPWORKER_BG_JOB_STATUS "DEDUPWORKER_BGJS_"
#define CHECKPOINT_BG_JOB_STATUS "CHECKPOINT_BGJS_"
#define OBJECT_SCHEMA_ASSEMBLE_BG_JOB_STATUS "OBJECT_SCHEMA_ASSEMBLE_BGJS_"
#define REFRESH_VIEW_JOB_STATUS "REFRESH_VIEW_BGJS_"
#define PREALLOCATE_VW "PVW_"
#define DICTIONARY_STORE_PREFIX "DIC_"
#define RESOURCE_GROUP_PREFIX "RG_"
#define NONHOST_UPDATE_TIMESTAMP_PREFIX "NHUT_"
#define INSERTION_LABEL_PREFIX "ILB_"
#define TABLE_STATISTICS_PREFIX "TS_"
#define TABLE_STATISTICS_TAG_PREFIX "TST_" // deprecated, just remove it
#define COLUMN_STATISTICS_PREFIX "CS_"
#define COLUMN_STATISTICS_TAG_PREFIX "CST_" // deprecated, just remove it
#define SQL_BINDING_PREFIX "SBI_"
#define PREPARED_STATEMENT_PREFIX "PSTAT_"
#define FILESYS_LOCK_PREFIX "FSLK_"
#define UDF_STORE_PREFIX "UDF_"
#define MERGEMUTATE_THREAD_START_TIME "MTST_"
#define DETACHED_PART_PREFIX "DP_"
#define ENTITY_UUID_MAPPING "EUM_"
#define MATERIALIZEDMYSQL_PREFIX "MMYSQL_"
#define MATERIALIZEDMYSQL_BG_JOB_STATUS "MATERIALIZEDMYSQL_BGJS_"
#define DETACHED_DELETE_BITMAP_PREFIX "DDLB_"
#define OBJECT_PARTIAL_SCHEMA_PREFIX "PS_"
#define OBJECT_ASSEMBLED_SCHEMA_PREFIX "AS_"
#define OBJECT_PARTIAL_SCHEMA_STATUS_PREFIX "PSS_"
#define PARTITION_PARTS_METRICS_SNAPSHOT_PREFIX "PPS_"
#define TABLE_TRASHITEMS_METRICS_SNAPSHOT_PREFIX "TTS_"
#define DICTIONARY_BUCKET_UPDATE_TIME_PREFIX "DBUT_"
#define ENTITY_UUID_MAPPING "EUM_"
#define BACKUP_TASK_PREFIX "BT_"
#define SENSITIVE_RESOURCE_PREFIX "SR_"
#define MANIFEST_DATA_PREFIX "MFST_"
#define MANIFEST_LIST_PREFIX "MFSTS_"

#define LARGE_KV_DATA_PREFIX "LGKV_"
#define LARGE_KV_REFERENCE "LGKVRF_"

using EntityType = IAccessEntity::Type;
struct EntityMetastorePrefix
{
    const char* prefix;
};

static EntityMetastorePrefix getEntityMetastorePrefix(EntityType type)
{
    switch(type)
    {
        case EntityType::USER:
            return EntityMetastorePrefix{"UE_"};
        case EntityType::ROLE:
            return EntityMetastorePrefix{"RE_"};
        case EntityType::SETTINGS_PROFILE:
            return EntityMetastorePrefix{"SPE_"};
        case EntityType::ROW_POLICY:
            return EntityMetastorePrefix{"RPE_"};
        case EntityType::QUOTA:
            return EntityMetastorePrefix{"QE_"};
        default:
            throw Exception("Access Entity not implemented", ErrorCodes::METASTORE_ACCESS_ENTITY_NOT_IMPLEMENTED);
    }
}

using SerializedObjectSchemas = std::unordered_map<String, String>;
using SerializedObjectSchemaStatuses = std::unordered_map<TxnTimestamp, String, TxnTimestampHasher>;
using SerializedObjectSchema = String;

static std::shared_ptr<MetastoreFDBImpl> getFDBInstance(const String & cluster_config_path)
{
    /// Notice: A single process can only have fdb instance
    static std::shared_ptr<MetastoreFDBImpl> metastore_fdb(new MetastoreFDBImpl(cluster_config_path));
    return metastore_fdb;
}

inline std::shared_ptr<IMetaStore> getMetastorePtr(const MetastoreConfig & config)
{
    if (config.type == MetaStoreType::FDB)
        return getFDBInstance(config.fdb_conf.cluster_conf_path);

    throw Exception("Catalog must be correctly configured. Only support foundationdb and bytekv now.", ErrorCodes::METASTORE_EXCEPTION);
}

class MetastoreProxy
{
public:
    using MetastorePtr = std::shared_ptr<IMetaStore>;
    using RepeatedFields = google::protobuf::RepeatedPtrField<std::string>;

    explicit MetastoreProxy(const MetastoreConfig & config, bool writable)
        : metastore_ptr(getMetastorePtr(config))
    {
        if(!writable)
            metastore_ptr->setReadOnlyChecker(assertNotReadonly);
    }

    explicit MetastoreProxy(MetastorePtr metastore_ptr_)
        : metastore_ptr(std::move(metastore_ptr_))
    {
    }

    ~MetastoreProxy() = default;

    MetastorePtr getMetastore()
    {
        return metastore_ptr;
    }

    /**
     * Metastore Proxy keying schema
     *
     */
    static std::string tableUUIDMappingPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + TABLE_UUID_MAPPING;
    }

    static std::string tableUUIDMappingPrefix(const std::string & name_space, const std::string & db)
    {
        return tableUUIDMappingPrefix(name_space) + escapeString(db) + (db.empty() ? "" : "_");
    }

    static std::string tableUUIDMappingKey(const std::string & name_space, const std::string & db, const std::string & table_name)
    {
        return tableUUIDMappingPrefix(name_space, db) + escapeString(table_name);
    }

    static std::string allDbPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + DB_STORE_PREFIX;
    }

    static std::string allExternalCatalogPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + EX_CATALOG_STORE_PREFIX;
    }

    static std::string dbKeyPrefix(const std::string & name_space, const std::string & db)
    {
        return allDbPrefix(name_space) + escapeString(db) + '_';
    }

    // static std::string externalCatalogPrefix(const std::string & name_space, const std::string & catalog)
    // {
    //     return allDbPrefix(name_space) + escapeString(catalog) + '_';
    // }

    static std::string externalCatalogKey(const std::string & name_space, const std::string & catalog)
    {
        return allExternalCatalogPrefix(name_space) + escapeString(catalog);
    }

    static std::string dbUUIDUniqueKey(const std::string & name_space, const std::string & uuid)
    {
        return escapeString(name_space) + '_' + DB_UUID_UNIQUE_PREFIX + uuid;
    }

    static std::string dbKey(const std::string & name_space, const std::string & db, UInt64 ts)
    {
        return allDbPrefix(name_space) + escapeString(db) + '_' + toString(ts);
    }

    static std::string snapshotPrefix(const std::string & name_space, const std::string & db_uuid)
    {
        return escapeString(name_space) + "_" + SNAPSHOT_STORE_PREFIX + db_uuid + "_";
    }

    static std::string snapshotKey(const std::string & name_space, const std::string & db_uuid, const std::string & name)
    {
        return snapshotPrefix(name_space, db_uuid) + escapeString(name);
    }

    static std::string backupPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + "_" + BACKUP_TASK_PREFIX;
    }

    static std::string backupKey(const std::string & name_space, const std::string & backup_uuid)
    {
        return escapeString(name_space) + "_" + BACKUP_TASK_PREFIX + backup_uuid;
    }

    static std::string deleteBitmapPrefix(const std::string & name_space, const std::string & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << "_" << DELETE_BITMAP_PREFIX << uuid << "_";
        return ss.str();
    }

    static std::string deleteBitmapKey(const std::string & name_space, const std::string & uuid, const String & bitmap_key)
    {
        std::stringstream ss;
        ss << deleteBitmapPrefix(name_space, uuid) << bitmap_key;
        return ss.str();
    }

    /// Prefix_PartitionID_PartMinBlock_PartMaxBlock_Reserved_Type_TxnID
    static std::string deleteBitmapKey(const std::string & name_space, const std::string & uuid, const Protos::DataModelDeleteBitmap & bitmap)
    {
        return deleteBitmapKey(name_space, uuid, dataModelName(bitmap));
    }

    static std::string deleteBitmapKeyInTrash(const std::string & name_space, const std::string & uuid, const Protos::DataModelDeleteBitmap & bitmap)
    {
        std::stringstream ss;
        ss << trashItemsPrefix(name_space, uuid) << DELETE_BITMAP_PREFIX << dataModelName(bitmap);
        return ss.str();
    }

    static std::string detachedDeleteBitmapKeyPrefix(const std::string & name_space, const std::string & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << "_" << DETACHED_DELETE_BITMAP_PREFIX << uuid << "_";
        return ss.str();
    }

    static std::string detachedDeleteBitmapKey(const std::string & name_space, const std::string & uuid, const std::string & bitmap_key)
    {
        std::stringstream ss;
        ss << detachedDeleteBitmapKeyPrefix(name_space, uuid) << bitmap_key;
        return ss.str();
    }

    static std::string detachedDeleteBitmapKey(const std::string & name_space, const std::string & uuid, const Protos::DataModelDeleteBitmap & bitmap)
    {
        return detachedDeleteBitmapKey(name_space, uuid, dataModelName(bitmap));
    }

    static std::string tableMetaPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + TABLE_STORE_PREFIX;
    }

    static std::string udfMetaPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + UDF_STORE_PREFIX;
    }

    static std::string dbTrashPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + DATABASE_TRASH_PREFIX;
    }

    static std::string dbTrashKey(const std::string & name_space, const std::string & db, const UInt64 & ts)
    {
        return dbTrashPrefix(name_space) + escapeString(db) + "_" + toString(ts);
    }

    static std::string tableTrashPrefix(const std::string & name_space, const std::string & db = "")
    {
        return escapeString(name_space) + '_' + TABLE_TRASH_PREFIX + escapeString(db) + (db.empty() ? "" : "_");
    }

    static std::string tableTrashKey(const std::string & name_space, const std::string & db, const std::string & table, const UInt64 & ts)
    {
        return tableTrashPrefix(name_space, db) + escapeString(table) + "_" + toString(ts);
    }

    static std::string dictionaryTrashPrefix(const std::string & name_space, const std::string & db)
    {
        return escapeString(name_space) + '_' + DICTIONARY_TRASH_PREFIX + escapeString(db);
    }

    static std::string dictionaryTrashKey(const std::string & name_space, const std::string & db, const std::string & name)
    {
        return dictionaryTrashPrefix(name_space, db) + "_" + escapeString(name);
    }

    static std::string tableStorePrefix(const std::string & name_space, const std::string & uuid)
    {
        return tableMetaPrefix(name_space) + uuid + '_';
    }

    static std::string udfStoreKey(const std::string & name_space, const std::string & prefix_name, const std::string & function_name)
    {
        return udfMetaPrefix(name_space) + prefix_name + '.' + function_name;
    }

    static std::string udfStoreKey(const std::string & name_space, const std::string & name)
    {
        return udfMetaPrefix(name_space) + name;
    }

    static std::string tableStoreKey(const std::string & name_space, const std::string & uuid, UInt64 commit_time)
    {
        return tableStorePrefix(name_space, uuid) + toString(commit_time);
    }

    static std::string maskingPolicyMetaPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + MASKING_POLICY_PREFIX;
    }

    static std::string maskingPolicyKey(const std::string & name_space, const std::string & masking_policy_name)
    {
        return fmt::format("{}_{}{}", escapeString(name_space), MASKING_POLICY_PREFIX, masking_policy_name);
    }

    static std::string maskingPolicyTableMappingPrefix(const std::string & name_space, const std::string & masking_policy_name)
    {
        return fmt::format("{}_{}{}", escapeString(name_space), MASKING_POLICY_TABLE_MAPPING, masking_policy_name);
    }

    static std::string maskingPolicyTableMappingKey(const std::string & name_space, const std::string & masking_policy_name, const std::string & uuid)
    {
        return fmt::format("{}_{}", maskingPolicyTableMappingPrefix(name_space, masking_policy_name), uuid);
    }

    static std::string nonHostUpdateKey(const std::string & name_space, const String & table_uuid)
    {
        return escapeString(name_space) + "_" + NONHOST_UPDATE_TIMESTAMP_PREFIX + table_uuid;
    }

    static std::string dictionaryPrefix(const std::string & name_space, const std::string & db)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << DICTIONARY_STORE_PREFIX << escapeString(db) << "_";
        return ss.str();
    }

    static std::string dictionaryStoreKey(const std::string & name_space, const std::string & db, const std::string & name)
    {
        std::stringstream ss;
        ss << dictionaryPrefix(name_space, db) << escapeString(name);
        return ss.str();
    }

    static std::string allDictionaryPrefix(const std::string & name_space)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << DICTIONARY_STORE_PREFIX;
        return ss.str();
    }

    static std::string viewDependencyPrefix(const std::string & name_space, const std::string & dependency)
    {
        return escapeString(name_space) + '_' + VIEW_DEPENDENCY_PREFIX + dependency + '_';
    }

    static std::string viewDependencyKey(const std::string & name_space, const std::string & dependency, const std::string & uuid)
    {
        return viewDependencyPrefix(name_space, dependency) + uuid;
    }

    static std::string matViewVersionKey(const std::string & name_space, const std::string & mv_uuid)
    {
        return escapeString(name_space) + '_' + MAT_VIEW_VERSION_PREFIX + mv_uuid;
    }

    static std::string matViewBaseTablesPrefix(const std::string & name_space, const std::string & mv_uuid)
    {
        return escapeString(name_space) + '_' + MAT_VIEW_BASETABLES_PREFIX + mv_uuid + '_';
    }

    static std::string matViewBaseTablesKey(const std::string & name_space, const std::string & mv_uuid, const std::string & base_uuid, const std::string & partition_id)
    {
        return matViewBaseTablesPrefix(name_space, mv_uuid) + base_uuid + '_' + partition_id;
    }

    static std::string tableUUIDUniqueKey(const std::string & name_space, const std::string & uuid)
    {
        return escapeString(name_space) + '_' + TABLE_UUID_UNIQUE_PREFIX + uuid;
    }

    static std::string tablePartitionInfoPrefix(const std::string & name_space, const std::string & uuid)
    {
        return escapeString(name_space) + '_' + TABLE_PARTITION_INFO_PREFIX + uuid + '_';
    }

    static std::string tablePartitionInfoKey(const std::string & name_space, const std::string & uuid, const std::string & partition_id)
    {
        /// To keep the partitions have the same order as data parts in bytekv, we add an extra "_" in the key of partition meta
        return tablePartitionInfoPrefix(name_space, uuid) + partition_id + "_";
    }

    static std::string tableMutationPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + "_" + TABLE_MUTATION_PREFIX;
    }

    static std::string tableMutationPrefix(const std::string & name_space, const std::string & uuid)
    {
        return escapeString(name_space) + "_" + TABLE_MUTATION_PREFIX + uuid + "_";
    }

    static std::string tableMutationKey(const std::string & name_space, const std::string & uuid, const std::string & mutation)
    {
        return escapeString(name_space) + "_" + TABLE_MUTATION_PREFIX + uuid + "_" + mutation;
    }

    static std::string dataPartPrefix(const std::string & name_space, const std::string & uuid)
    {
        return  escapeString(name_space) + '_' + PART_STORE_PREFIX + uuid + '_';
    }

    static std::string stagedDataPartPrefix(const std::string & name_space, const std::string & uuid)
    {
        return  escapeString(name_space) + '_' + STAGED_PART_STORE_PREFIX + uuid + '_';
    }

    static std::string dataPartKey(const std::string & name_space, const std::string & uuid, const String & part_name)
    {
        return dataPartPrefix(name_space, uuid) + part_name;
    }

    static std::string dataPartKeyInTrash(const std::string & name_space, const std::string & uuid, const String & part_name)
    {
        return trashItemsPrefix(name_space, uuid) + PART_STORE_PREFIX + part_name;
    }

    static std::string stagedDataPartKey(const std::string & name_space, const std::string & uuid, const String & part_name)
    {
        return stagedDataPartPrefix(name_space, uuid) + part_name;
    }

    static std::string syncListPrefix(const std::string & name_space, const std::string & uuid)
    {
        return escapeString(name_space) + '_' + SYNC_LIST_PREFIX + uuid + '_';
    }

    static std::string syncListKey(const std::string & name_space, const std::string & uuid, UInt64 ts)
    {
        return syncListPrefix(name_space, uuid) + toString(ts);
    }

    static std::string syncListKey(const std::string & name_space, const std::string & uuid, TxnTimestamp & ts)
    {
        return syncListPrefix(name_space, uuid) + ts.toString();
    }

    static std::string kafkaOffsetsKey(const std::string & name_space, const std::string & group_name,
                                              const std::string & topic, const int partition_num)
    {
        return escapeString(name_space) + "_" + KAFKA_OFFSETS_PREFIX + escapeString(group_name) + "_"
               + escapeString(topic) + "_" + std::to_string(partition_num);
    }

    static std::string kafkaTransactionKey(const std::string & name_space, const std::string & uuid, const size_t consumer_index)
    {
        return escapeString(name_space) + "_" + KAFKA_TRANSACTION_PREFIX + escapeString(uuid) + "_" + std::to_string(consumer_index);
    }

    static std::string transactionRecordKey(const std::string & name_space, const UInt64 & txn_id)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << "_" << TRANSACTION_RECORD_PREFIX << txn_id;
        return ss.str();
    }

    static std::string transactionKey(UInt64 txn)
    {
        return TRANSACTION_STORE_PREFIX + toString(txn);
    }

    static std::string writeIntentKey(const std::string & name_space, const String & intent_prefix, const std::string & intent_name)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << "_" << WRITE_INTENT_PREFIX << intent_prefix << "_" << intent_name;
        return ss.str();
    }

    static std::string undoBufferKey(const std::string & name_space, const UInt64 & txn, bool write_undo_buffer_new_key)
    {
        if (write_undo_buffer_new_key)
        {
            /* Not make increment key to avoid ByteKV partition server performance issue
            * Reverse transaction_id with a `R` suffix to make sure it will not conflict with old way
            */
            String txn_str = toString(txn);
            std::reverse(txn_str.begin(), txn_str.end());
            return escapeString(name_space) + '_' + UNDO_BUFFER_PREFIX + txn_str + "R";
        }
        return escapeString(name_space) + '_' + UNDO_BUFFER_PREFIX + toString(txn);
    }

    /// Make sure only touch wanted transaction id's undo buffer keys
    static std::string undoBufferKeyPrefix(const std::string & name_space, const UInt64 & txn, bool write_undo_buffer_new_key)
    {
        return undoBufferKey(name_space, txn, write_undo_buffer_new_key) + "_";
    }

    static std::string undoBufferStoreKey(
        const std::string & name_space,
        const UInt64 & txn,
        const String & rpc_address,
        const UndoResource & resource,
        PlanSegmentInstanceId instance_id,
        bool write_undo_buffer_new_key)
    {
        return fmt::format(
            "{}_{}_{}_{}_{}",
            undoBufferKey(name_space, txn, write_undo_buffer_new_key),
            escapeString(rpc_address),
            instance_id.segment_id,
            instance_id.parallel_index,
            escapeString(toString(resource.id)));
    }

    static std::string undoBufferSegmentInstanceKey(
        const std::string & name_space, const UInt64 & txn, const String & rpc_address, PlanSegmentInstanceId instance_id, bool write_undo_buffer_new_key)
    {
        return fmt::format(
            "{}_{}_{}_{}",
            undoBufferKey(name_space, txn, write_undo_buffer_new_key),
            escapeString(rpc_address),
            instance_id.segment_id,
            instance_id.parallel_index);
    }

    static std::string kvLockKey(const std::string & name_space, const std::string & uuid, const std::string & part_name)
    {
        return escapeString(name_space) + '_' + KV_LOCK_PREFIX + uuid + '_' + part_name;
    }

    static std::string clusterStatusKey(const std::string & name_space, const std::string & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << TABLE_CLUSTER_STATUS << uuid;
        return ss.str();
    }

    static std::string tableDefinitionHashKey(const std::string & name_space, const std::string & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << TABLE_DEFINITION_HASH << uuid;
        return ss.str();
    }

    static std::string allClusterBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + CLUSTER_BG_JOB_STATUS;
    }

    static std::string clusterBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allClusterBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allMergeBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + MERGE_BG_JOB_STATUS;
    }

    static std::string mergeBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allMergeBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allPartGCBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + PARTGC_BG_JOB_STATUS;
    }

    static std::string partGCBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allPartGCBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allPartMoverBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + PARTMOVER_BG_JOB_STATUS;
    }

    static std::string partMoverBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allPartMoverBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allConsumerBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + CONSUMER_BG_JOB_STATUS;
    }

    static std::string consumerBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allConsumerBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allMmysqlBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + MATERIALIZEDMYSQL_BG_JOB_STATUS;
    }

    static std::string refreshViewBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allRefreshViewJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allRefreshViewJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + REFRESH_VIEW_JOB_STATUS;
    }

    static std::string mmysqlBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allMmysqlBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allDedupWorkerBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + DEDUPWORKER_BG_JOB_STATUS;
    }

    static std::string allCheckpointBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + CHECKPOINT_BG_JOB_STATUS;
    }

    static std::string checkpointBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allCheckpointBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string dedupWorkerBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allDedupWorkerBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static std::string allObjectSchemaAssembleBGJobStatusKeyPrefix(const std::string & name_space)
    {
        return escapeString(name_space) + '_' + OBJECT_SCHEMA_ASSEMBLE_BG_JOB_STATUS;
    }

    static std::string objectSchemaAssembleBGJobStatusKey(const std::string & name_space, const std::string & uuid)
    {
        return allObjectSchemaAssembleBGJobStatusKeyPrefix(name_space) + uuid;
    }

    static UUID parseUUIDFromBGJobStatusKey(const std::string & key);

    static std::string preallocateVW(const std::string & name_space, const std::string & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << PREALLOCATE_VW << uuid;
        return ss.str();
    }

    static String insertionLabelKey(const String & name_space, const std::string & uuid, const std::string & label)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << INSERTION_LABEL_PREFIX << uuid << '_' << label;
        return ss.str();
    }

    static String SQLBindingKey(const String name_space, const String & uuid, const String & tenant_id, const bool & is_re)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << SQL_BINDING_PREFIX << is_re << '_' << uuid << '_' << tenant_id;
        return ss.str();
    }

    static String SQLBindingRePrefix(const String name_space, const bool & is_re)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << SQL_BINDING_PREFIX << is_re;
        return ss.str();
    }

    static String SQLBindingPrefix(const String name_space)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << SQL_BINDING_PREFIX;
        return ss.str();
    }

    static String preparedStatementKey(const String name_space, const String & key)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << PREPARED_STATEMENT_PREFIX << '_' << key;
        return ss.str();
    }

    static String preparedStatementPrefix(const String name_space)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << PREPARED_STATEMENT_PREFIX;
        return ss.str();
    }

    static String tableStatisticKey(const String name_space, const String & uuid, const StatisticsTag & tag)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << TABLE_STATISTICS_PREFIX << uuid << '_' << static_cast<UInt64>(tag);
        return ss.str();
    }

    static String tableStatisticPrefix(const String name_space, const String & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << TABLE_STATISTICS_PREFIX << uuid << '_';
        return ss.str();
    }

    static String tableStatisticTagKey(const String name_space, const String & uuid, const StatisticsTag & tag)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << TABLE_STATISTICS_TAG_PREFIX << uuid << '_' << static_cast<UInt64>(tag);
        return ss.str();
    }

    static String tableStatisticTagPrefix(const String name_space, const String & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << TABLE_STATISTICS_TAG_PREFIX << uuid << '_';
        return ss.str();
    }

     static String columnStatisticKey(const String name_space, const String & uuid, const String & column, const StatisticsTag & tag)
     {
         std::stringstream ss;
         ss << escapeString(name_space) << '_' << COLUMN_STATISTICS_PREFIX << uuid << '_' << escapeString(column) << '_' << static_cast<UInt64>(tag);
         return ss.str();
     }

    static String columnStatisticPrefixWithoutColumn(const String name_space, const String & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << COLUMN_STATISTICS_PREFIX << uuid << '_';
        return ss.str();
    }

    static String columnStatisticPrefix(const String name_space, const String & uuid, const String & column)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << COLUMN_STATISTICS_PREFIX << uuid << '_' << escapeString(column) << '_';
        return ss.str();
    }

    static String columnStatisticTagKey(const String name_space, const String & uuid, const String & column, const StatisticsTag & tag)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << COLUMN_STATISTICS_TAG_PREFIX << uuid << '_' << escapeString(column) << '_' << static_cast<UInt64>(tag);
        return ss.str();
    }

    static String columnStatisticTagPrefix(const String name_space, const String & uuid, const String & column)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << COLUMN_STATISTICS_TAG_PREFIX << uuid << '_' << escapeString(column) << '_';
        return ss.str();
    }

    static String columnStatisticTagPrefixWithoutColumn(const String name_space, const String & uuid)
    {
        std::stringstream ss;
        ss << escapeString(name_space) << '_' << COLUMN_STATISTICS_TAG_PREFIX << uuid << '_';
        return ss.str();
    }

    static String VWKey(const String & name_space, const String & vw_name)
    {
        return escapeString(name_space) + "_" + RM_VW_PREFIX + vw_name;
    }

    static String WorkerGroupKey(const String & name_space, const String & worker_group_id)
    {
        return escapeString(name_space) + "_" + RM_WG_PREFIX + worker_group_id;
    }

    static String filesysLockKey(const String & name_space, const String hdfs_path)
    {
        return escapeString(name_space) + '_' + FILESYS_LOCK_PREFIX + escapeString(hdfs_path);
    }

    static String mergeMutateThreadStartTimeKey(const String & name_space, const String & uuid)
    {
        return escapeString(name_space) + '_' + MERGEMUTATE_THREAD_START_TIME + uuid;
    }

    static String materializedMySQLMetadataKey(const String & name_space, const String & name)
    {
        return escapeString(name_space) + "_" + MATERIALIZEDMYSQL_PREFIX + escapeString(name);
    }

    inline static String AYSNC_QUERY_STATUS_PREFIX = "ASYNC_QUERY_STATUS_";
    inline static String FINAL_AYSNC_QUERY_STATUS_PREFIX = "F_ASYNC_QUERY_STATUS_";

    static String asyncQueryStatusKey(const String & name_space, const String & id)
    {
        return escapeString(name_space) + '_' + AYSNC_QUERY_STATUS_PREFIX + id;
    }

    static String finalAsyncQueryStatusKey(const String & name_space, const String & id)
    {
        return escapeString(name_space) + '_' + FINAL_AYSNC_QUERY_STATUS_PREFIX + id;
    }

    static String detachedPartPrefix(const String& name_space, const String& uuid)
    {
        return escapeString(name_space) + '_' + DETACHED_PART_PREFIX + uuid + '_';
    }

    static String detachedPartKey(const String& name_space, const String& uuid,
        const String& part_name)
    {
        return escapeString(name_space) + '_' + DETACHED_PART_PREFIX + uuid + '_' + part_name;
    }

    static String sensitiveResourcePrefix(const String & name_space)
    {
        return fmt::format("{}_{}", escapeString(name_space), SENSITIVE_RESOURCE_PREFIX);
    }
    static String sensitiveResourceKey(const String & name_space, const String & db_name)
    {
        return fmt::format("{}{}", sensitiveResourcePrefix(name_space), db_name);
    }

    static String accessEntityPrefix(EntityType type, const String & name_space)
    {
        return fmt::format("{}_{}", escapeString(name_space), getEntityMetastorePrefix(type).prefix);
    }
    static String accessEntityKey(EntityType type, const String & name_space, const String & name)
    {
        return fmt::format("{}{}", accessEntityPrefix(type, name_space), name);
    }

    static String accessEntityUUIDNameMappingPrefix(const String & name_space)
    {
        return fmt::format("{}_{}", escapeString(name_space), ENTITY_UUID_MAPPING);
    }

    static String accessEntityUUIDNameMappingKey(const String & name_space, const String & uuid)
    {
        return fmt::format("{}{}", accessEntityUUIDNameMappingPrefix(name_space), uuid);
    }

    static String trashItemsPrefix(const String & name_space, const String & uuid)
    {
        return escapeString(name_space) + "_" + DATA_ITEM_TRASH_PREFIX + uuid + "_";
    }

    static String partialSchemaPrefix(const String & name_space, const String & table_uuid)
    {
         return escapeString(name_space) + "_" + OBJECT_PARTIAL_SCHEMA_PREFIX + table_uuid + "_";
    }

    static String partialSchemaKey(const String & name_space, const String & table_uuid, const UInt64 & txn_id)
    {
        return escapeString(name_space) + "_" + OBJECT_PARTIAL_SCHEMA_PREFIX + table_uuid + "_" + toString(txn_id);
    }

    static String assembledSchemaKey(const String & name_space, const String & table_uuid)
    {
        return escapeString(name_space) + "_" + OBJECT_ASSEMBLED_SCHEMA_PREFIX + table_uuid;
    }

    static String partialSchemaStatusKey(const String & name_space, const UInt64 & txn_id)
    {
        return escapeString(name_space) + "-" + OBJECT_PARTIAL_SCHEMA_STATUS_PREFIX + "_" + toString(txn_id);
    }

    static String partitionPartsMetricsSnapshotPrefix(const String & name_space, const String & table_uuid, const String & partition_id)
    {
        return escapeString(name_space) + "_" + PARTITION_PARTS_METRICS_SNAPSHOT_PREFIX + table_uuid + "_" + partition_id;
    }

    static String tableTrashItemsMetricsSnapshotPrefix(const String & name_space, const String & table_uuid)
    {
        return escapeString(name_space) + "_" + TABLE_TRASHITEMS_METRICS_SNAPSHOT_PREFIX + table_uuid;
    }

    static String dictionaryBucketUpdateTimeKey(const String & name_space, const std::string & uuid, const Int64 bucket_number)
    {
        return escapeString(name_space) + '_' + DICTIONARY_BUCKET_UPDATE_TIME_PREFIX + uuid + '_' + std::to_string(bucket_number);
    }

    static String manifestDataPrefix(const String & name_space, const String & uuid, const UInt64 txn_id)
    {
        return escapeString(name_space) + '_' + MANIFEST_DATA_PREFIX + uuid + "_" + toString(txn_id) + "_";
    }

    static String manifestKeyForPart(const String & name_space, const String & uuid, const UInt64 txn_id, const String & part_name)
    {
        return manifestDataPrefix(name_space, uuid, txn_id) + PART_STORE_PREFIX + part_name;
    }

    static String manifestKeyForDeleteBitmap(const String & name_space, const String & uuid, const UInt64 txn_id, const String & bitmap_key)
    {
        return manifestDataPrefix(name_space, uuid, txn_id) + DELETE_BITMAP_PREFIX + bitmap_key;
    }

    static String manifestListPrefix(const String & name_space, const String & uuid)
    {
        return escapeString(name_space) + '_' + MANIFEST_LIST_PREFIX + uuid + '_';
    }

    static String manifestListKey(const String & name_space, const String & uuid, const UInt64 table_version)
    {
        return manifestListPrefix(name_space, uuid) + toString(table_version);
    }

    static String largeKVDataPrefix(const String & name_space, const String & uuid)
    {
        return escapeString(name_space) + '_' +  LARGE_KV_DATA_PREFIX + uuid + '_';
    }

    static String largeKVDataKey(const String & name_space, const String & uuid, UInt64 index)
    {
        // keep records in the kv storage with the same order as index. Support at most 10k sub-kv
        std::ostringstream oss;
        oss << std::setw(5) << std::setfill('0') << index;
        return largeKVDataPrefix(name_space, uuid) + oss.str();
    }

    static String largeKVReferencePrefix(const String & name_space)
    {
        return escapeString(name_space) + '_' + LARGE_KV_REFERENCE;
    }

    static String largeKVReferenceKey(const String & name_space, const String & uuid)
    {
        return largeKVReferencePrefix(name_space) + uuid;
    }

    // parse the first key in format of '{prefix}{escapedString(first_key)}_postfix'
    // note that prefix should contains _, like TCS_
    // return [first_key, postfix]
    static std::pair<String, String> splitFirstKey(const String & key, const String & prefix);

    /// end of Metastore Proxy keying schema

    /**
     * @brief Remove keys by prefix. This is a very general function, which can
     * be used to multiple type of data. By providing a function, users can
     * `hook into` pre-commit process to delete the data.
     *          This method is designed to be:
     * 1. Memory-efficient, no more than batch_size keys will be loaded into memory.
     * 2. Fast, because it will scan keys in batch.
     * 3. Multi-thread friendly, because it returns KVs in batch, which can
     *          be executed in parallel by caller.
     *
     * @param prefix A prefix of the keys to be scanned.
     * @param kvs Metastore (Key, Value), caller need to parse it manually.
     * @param func Will be called before removing the keys. Skip current batch when
     *              func return `false`. And the procedure will continue until
     *              all keys are processed.
     * @param batch_size Data will be feed into func and committed in batch.
     * @return Total number of keys scanned. (Useful for checking if the prefix is cleaned)
     */
    size_t removeByPrefix(
        const String & prefix,
        std::function<bool(const std::vector<std::pair<const String, const String>> & kvs)> func,
        const size_t batch_size = 10000);
    void createTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data);
    void removeTransactionRecord(const String & name_space, const UInt64 & txn_id);
    void removeTransactionRecords(const String & name_space, const std::vector<TxnTimestamp> & txn_ids);
    String getTransactionRecord(const String & name_space, const UInt64 & txn_id);
    IMetaStore::IteratorPtr
    getAllTransactionRecord(const String & name_space, const String & start_key = "", const size_t & max_result_number = 0);
    std::pair<bool, String> updateTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data_old, const String & txn_data_new);
    std::vector<std::pair<String, UInt64>> getTransactionRecords(const String & name_space, const std::vector<TxnTimestamp> & txn_ids);

    bool updateTransactionRecordWithOffsets(const String & name_space, const UInt64 & txn_id, const String & txn_data_old, const String & txn_data_new, const String & consumer_group, const cppkafka::TopicPartitionList &);
    bool updateTransactionRecordWithBinlog(const String & name_space, const UInt64 & txn_id, const String & txn_data_old, const String & txn_data_new, const String & binlog_name, const std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> & binlog);
    void setTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data, UInt64 ttl = 0);

    std::pair<bool, String> updateTransactionRecordWithRequests(
        SinglePutRequest & txn_request, BatchCommitRequest & requests, BatchCommitResponse & response);

    bool writeIntent(const String & name_space, const String & uuid, const std::vector<WriteIntent> & intents, std::vector<String> & cas_failed_list);
    bool resetIntent(const String & name_space, const String & uuid, const std::vector<WriteIntent> & intents, const UInt64 & new_txn_id, const String & new_location);
    void clearIntents(const String & name_space, const String & uuid, const std::vector<WriteIntent> & intents);
    void clearZombieIntent(const String & name_space, const UInt64 & txn_id);

    void writeFilesysLock(const String & name_space, UInt64 txn_id, const String & dir, const String & db, const String & table);
    std::vector<String> getFilesysLocks(const String& name_space, const std::vector<String>& dirs);
    void clearFilesysLocks(const String & name_space, const std::vector<String>& dirs);

    IMetaStore::IteratorPtr getAllFilesysLock(const String & name_space);

    /// TODO: remove old transaction api
    void insertTransaction(UInt64 txn);
    void removeTransaction(UInt64 txn);
    IMetaStore::IteratorPtr getActiveTransactions();
    std::unordered_set<UInt64> getActiveTransactionsSet();

    void updateServerWorkerGroup(const String & worker_group_name, const String & worker_group_info);
    void getServerWorkerGroup(const String & worker_group_name, String & worker_group_info);
    void dropServerWorkerGroup(const String & worker_group_name);
    IMetaStore::IteratorPtr getAllWorkerGroupMeta();

    // for external catalog
    void addExternalCatalog(const String & name_space, const Protos::DataModelCatalog & catalog_model);
    // Note(renming):: we follow the getDatabase here, in case in the future we will use mvcc.
    void getExternalCatalog(const String & name_space, const String & name, Strings & catalog_info);
    void dropExternalCatalog(const String & name_space, const Protos::DataModelCatalog & catalog_model);
    IMetaStore::IteratorPtr  getAllExternalCatalogMeta(const String& name_space);

    void addDatabase(const String & name_space, const Protos::DataModelDB & db_model);
    void getDatabase(const String & name_space, const String & name, Strings & db_info);
    void dropDatabase(const String & name_space, const Protos::DataModelDB & db_model);
    IMetaStore::IteratorPtr getAllDatabaseMeta(const String & name_space);
    std::vector<Protos::DataModelDB> getTrashDBs(const String & name_space);
    std::vector<UInt64> getTrashDBVersions(const String & name_space, const String & database);

    void createSnapshot(const String & name_space, const String & db_uuid, const Protos::DataModelSnapshot & snapshot);
    void removeSnapshot(const String & name_space, const String & db_uuid, const String & name);
    /// return snapshot from KV, nullptr if not found.
    std::shared_ptr<Protos::DataModelSnapshot> tryGetSnapshot(const String & name_space, const String & db_uuid, const String & name);
    std::vector<std::shared_ptr<Protos::DataModelSnapshot>> getAllSnapshots(const String & name_space, const String & db_uuid);

    void createBackupJob(const String & name_space, const String & backup_uuid, const Protos::DataModelBackupTask & backup_task);
    void removeBackupJob(const String & name_space, const String & backup_uuid);
    std::shared_ptr<Protos::DataModelBackupTask> tryGetBackupJob(const String & name_space, const String & backup_uuid);
    std::vector<std::shared_ptr<Protos::DataModelBackupTask>> getAllBackupJobs(const String & name_space);
    void updateBackupJobCAS(const String & name_space, std::shared_ptr<Protos::DataModelBackupTask> & backup_task, const String & expected_value);

    String getMaskingPolicy(const String & name_space, const String & masking_policy_name) const;
    Strings getMaskingPolicies(const String & name_space, const Strings & masking_policy_names) const;
    Strings getAllMaskingPolicy(const String & name_space) const;
    void putMaskingPolicy(const String & name_space, const Protos::DataModelMaskingPolicy & new_masking_policy) const;
    IMetaStore::IteratorPtr getMaskingPolicyAppliedTables(const String & name_space, const String & masking_policy_name) const;
    void dropMaskingPolicies(const String & name_space, const Strings & masking_policy_name);

    String getTableUUID(const String & name_space, const String & database, const String & name);
    std::shared_ptr<Protos::TableIdentifier> getTableID(const String & name_space, const String & database, const String & name);
    std::shared_ptr<std::vector<std::shared_ptr<Protos::TableIdentifier>>> getTableIDs(const String & name_space,
                    const std::vector<std::pair<String, String>> & db_name_pairs);
    String getTrashTableUUID(const String & name_space, const String & database, const String & name, const UInt64 & ts);
    void createTable(const String & name_space, const UUID & db_uuid, const DB::Protos::DataModelTable & table_data, const Strings & dependencies, const Strings & masking_policy_mapping);
    void createUDF(const String & name_space, const DB::Protos::DataModelUDF & udf_data);
    void dropUDF(const String & name_space, const String &resolved_function_name);
    void updateTable(const String & name_space, const String & table_uuid, const String & table_info_new, const UInt64 & ts);
    void updateTableWithID(const String & name_space, const Protos::TableIdentifier & table_id, const DB::Protos::DataModelTable & table_data);
    void getTableByUUID(const String & name_space, const String & table_uuid, Strings & tables_info);
    void clearTableMeta(const String & name_space, const String & database, const String & table, const String & uuid, const Strings & dependencies, const UInt64 & ts = 0);
    void prepareRenameTable(const String & name_space, const String & table_uuid, const String & from_db, const String & from_table, const UUID & to_db_uuid, Protos::DataModelTable & to_table, BatchCommitRequest & batch_write);
    bool alterTable(const String & name_space, const Protos::DataModelTable & table, const Strings & masks_to_remove, const Strings & masks_to_add);
    Strings getAllTablesInDB(const String & name_space, const String & database);
    IMetaStore::IteratorPtr getAllTablesMeta(const String & name_space);
    IMetaStore::IteratorPtr getAllUDFsMeta(const String & name_space, const String & database_name = "");
    Strings getUDFsMetaByName(const String & name_space, const std::unordered_set<String> &function_names);
    std::vector<std::shared_ptr<Protos::TableIdentifier>> getAllTablesId(const String & name_space, const String & db = "");
    std::vector<std::shared_ptr<Protos::TableIdentifier>> getTablesIdByPrefix(const String & name_space, const String & prefix = "");
    Strings getAllDependence(const String & name_space, const String & uuid);
    std::vector<std::shared_ptr<Protos::VersionedPartitions>> getMvBaseTables(const String & name_space, const String & uuid);
    String getMvMetaVersion(const String & name_space, const String & uuid);
    BatchCommitRequest constructMvMetaRequests(const String & name_space, const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartition>> add_partitions,std::vector<std::shared_ptr<Protos::VersionedPartition>> drop_partitions, String mv_version_ts);
    void updateMvMeta(const String & name_space, const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions);
    void dropMvMeta(const String & name_space, const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions);
    void cleanMvMeta(const String & name_space, const String & uuid);

    IMetaStore::IteratorPtr getTrashTableIDIterator(const String & name_space, uint32_t iterator_internal_batch_size);
    std::vector<std::shared_ptr<Protos::TableIdentifier>> getTrashTableID(const String & name_space);
    std::shared_ptr<Protos::TableIdentifier> getTrashTableID(const String & name_space, const String & database, const String & table, const UInt64 & ts);
    std::vector<std::shared_ptr<Protos::TableIdentifier>> getTablesFromTrash(const String & name_space, const String & database);
    std::map<UInt64, std::shared_ptr<Protos::TableIdentifier>> getTrashTableVersions(const String & name_space, const String & database, const String & table);

    void createDictionary(const String & name_space, const String & db, const String & name, const String & dic_meta);
    void getDictionary(const String & name_space, const String & db, const String & name, String & dic_meta);
    void dropDictionary(const String & name_space, const String & db, const String & name);
    std::vector<std::shared_ptr<DB::Protos::DataModelDictionary>> getDictionariesInDB(const String & name_space, const String & database);
    IMetaStore::IteratorPtr getAllDictionaryMeta(const String & name_space);
    std::vector<std::shared_ptr<DB::Protos::DataModelDictionary>> getDictionariesFromTrash(const String & name_space, const String & database);

    void prepareAddDataParts(
        const String & name_space,
        const String & table_uuid,
        const Strings & current_partitions,
        std::unordered_set<String> & deleting_partitions,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts,
        BatchCommitRequest & batch_write,
        const std::vector<String> & expected_parts,
        const UInt64 txn_id,
        bool update_sync_list = false,
        bool write_manifest = false);
    void prepareAddStagedParts(
        const String & name_space,
        const String & table_uuid,
        const Strings & current_partitions,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts,
        BatchCommitRequest & batch_write,
        const std::vector<String> & expected_staged_parts);

    /// mvcc version drop part
    void dropDataPart(const String & name_space, const String & table_uuid, const String & part_name, const String & part_info);
    Strings getPartsByName(const String & name_space, const String & uuid, const Strings & parts_name);
    IMetaStore::IteratorPtr getPartsInRange(const String & name_space, const String & uuid, const String & partition_id);
    IMetaStore::IteratorPtr getPartsInRange(const String & name_space, const String & table_uuid, const String & range_start, const String & range_end, bool include_start, bool include_end);
    void dropDataPart(const String & name_space, const String & uuid, const String & part_name);
    void dropAllPartInTable(const String & name_space, const String & uuid);
    void dropAllMutationsInTable(const String & name_space, const String & uuid);

    /// scan staged parts
    IMetaStore::IteratorPtr getStagedParts(const String & name_space, const String & uuid);
    IMetaStore::IteratorPtr getStagedPartsInPartition(const String & name_space, const String & uuid, const String & partition);
    void dropAllDeleteBitmapInTable(const String & name_space, const String & uuid);

    /// check part/delete-bitmap exist
    template <typename T, typename GenerateKeyFunc>
    std::vector<std::pair<String, UInt64>> getItemStatus(const std::vector<T> & items, GenerateKeyFunc&& generateKey)
    {
        if (items.size() == 0)
            return {};

        Strings keys;
        for (const T item: items)
            keys.emplace_back(generateKey(item));

        return metastore_ptr->multiGet(keys);
    }

    void createRootPath(const String & root_path);
    void deleteRootPath(const String & root_path);
    std::vector<std::pair<String, UInt32>> getAllRootPath();

    void createMutation(const String & name_space, const String & uuid, const String & mutation_name, const String & mutation_text);
    void removeMutation(const String & name_space, const String & uuid, const String & mutation_name);
    Strings getAllMutations(const String & name_space, const String & uuid);
    std::multimap<String, String> getAllMutations(const String & name_space);

    void writeUndoBuffer(
        const String & name_space,
        const UInt64 & txnID,
        const String & rpc_address,
        const String & uuid,
        UndoResources & resources,
        PlanSegmentInstanceId instance_id,
        bool write_undo_buffer_new_key);

    void clearUndoBuffer(const String & name_space, const UInt64 & txnID);
    void clearUndoBuffer(const String & name_space, const UInt64 & txnID, const String & rpc_address, PlanSegmentInstanceId instance_id);
    IMetaStore::IteratorPtr getUndoBuffer(const String & name_space, UInt64 txnID, bool write_undo_buffer_new_key);
    IMetaStore::IteratorPtr getUndoBuffer(
        const String & name_space,
        UInt64 txnID,
        const String & rpc_address,
        PlanSegmentInstanceId instance_id,
        bool write_undo_buffer_new_key);
    IMetaStore::IteratorPtr getAllUndoBuffer(const String & name_space);

    void multiDrop(const Strings & keys);

    bool batchWrite(const BatchCommitRequest & request, BatchCommitResponse & response);
    /// tmp api to help debug drop keys failed issue. remove this later.
    std::vector<String> multiDropAndCheck(const Strings & keys);

    IMetaStore::IteratorPtr getPartitionList(const String & name_space, const String & uuid);

    void updateTopologyMeta(const String & name_space, const String & topology);
    String getTopologyMeta(const String & name_space);

    IMetaStore::IteratorPtr getSyncList(const String & name_space, const String & uuid);

    void clearSyncList(const String & name_space, const String & uuid, const std::vector<TxnTimestamp> & sync_list);

    /// operations on kafka offsets
    void getKafkaTpl(const String & name_space, const String & consumer_group, cppkafka::TopicPartitionList & tpl);
    cppkafka::TopicPartitionList getKafkaTpl(const String & name_space, const String & consumer_group, const String & topic_name);
    void clearOffsetsForWholeTopic(const String & name_space, const String & topic, const String & consumer_group);
    /// operations on kafka consumer transactions
    void setTransactionForKafkaConsumer(const String & name_space, const String & uuid, const TxnTimestamp & txn_id, size_t consumer_index);
    TxnTimestamp getTransactionForKafkaConsumer(const String & name_space, const String & uuid, size_t consumer_index);
    void clearKafkaTransactions(const String & name_space, const String & uuid);

    void setTableClusterStatus(const String & name_space, const String & uuid, const bool & already_clustered, const UInt64 & table_definition_hash);
    void getTableClusterStatus(const String & name_space, const String & uuid, bool & is_clustered);

    /// BackgroundJob related API
    void setBGJobStatus(const String & name_space, const String & uuid, CnchBGThreadType type, CnchBGThreadStatus status);
    std::optional<CnchBGThreadStatus> getBGJobStatus(const String & name_space, const String & uuid, CnchBGThreadType type);

    std::unordered_map<UUID, CnchBGThreadStatus> getBGJobStatuses(const String & name_space, CnchBGThreadType type);

    void dropBGJobStatus(const String & name_space, const String & uuid, CnchBGThreadType type);

    void setTablePreallocateVW(const String & name_space, const String & uuid, const String & vw);
    void getTablePreallocateVW(const String & name_space, const String & uuid, String & vw);

    /// delete bitmap/keys related api
    void prepareAddDeleteBitmaps(
        const String & name_space,
        const String & table_uuid,
        const DeleteBitmapMetaPtrVector & bitmaps,
        BatchCommitRequest & batch_write,
        const UInt64 txn_id,
        const std::vector<String> & expected_bitmaps = {},
        bool write_manifest = false);

    Strings getDeleteBitmapByKeys(const Strings & key);

    IMetaStore::IteratorPtr getMetaInRange(const String & prefix, const String & range_start, const String & range_end, bool include_start, bool include_end);

    void precommitInsertionLabel(const String & name_space, const InsertionLabelPtr & label);
    void commitInsertionLabel(const String & name_space, InsertionLabelPtr & label);

    std::pair<uint64_t, String> getInsertionLabel(const String & name_space, const String & uuid, const String & name);
    void removeInsertionLabel(const String & name_space, const String & uuid, const String & name, uint64_t expected_version = 0);
    void removeInsertionLabels(const String & name_space, const std::vector<InsertionLabel> & labels);
    IMetaStore::IteratorPtr scanInsertionLabels(const String & name_space, const String & uuid);
    void clearInsertionLabels(const String & name_space, const String & uuid);

    void createVirtualWarehouse(const String & name_space, const String & vw_name, const VirtualWarehouseData & data);
    void alterVirtualWarehouse(const String & name_space, const String & vw_name, const VirtualWarehouseData & data);
    bool tryGetVirtualWarehouse(const String & name_space, const String & vw_name, VirtualWarehouseData & data);
    std::vector<VirtualWarehouseData> scanVirtualWarehouses(const String & name_space);
    void dropVirtualWarehouse(const String & name_space, const String & vw_name);

    void createWorkerGroup(const String & name_space, const String & worker_group_id, const WorkerGroupData & data);
    void updateWorkerGroup(const String & name_space, const String & worker_group_id, const WorkerGroupData & data);
    bool tryGetWorkerGroup(const String & name_space, const String & worker_group_id, WorkerGroupData & data);
    std::vector<WorkerGroupData> scanWorkerGroups(const String & name_space);
    void dropWorkerGroup(const String & name_space, const String & worker_group_id);

    UInt64 getNonHostUpdateTimeStamp(const String & name_space, const String & table_uuid);
    void setNonHostUpdateTimeStamp(const String & name_space, const String & table_uuid, const UInt64 pts);

    void updateSQLBinding(const String & name_space, const SQLBindingItemPtr& data);
    SQLBindings getSQLBindings(const String & name_space);
    SQLBindings getReSQLBindings(const String & name_space, const bool & is_re_expression);
    SQLBindingItemPtr getSQLBinding(const String & name_space, const String & uuid, const String & tenant_id, const bool & is_re_expression);
    void removeSQLBinding(const String & name_space, const String & uuid, const String & tenant_id, const bool & is_re_expression);

    void updatePreparedStatement(const String & name_space, const PreparedStatementItemPtr & data);
    PreparedStatements getPreparedStatements(const String & name_space);
    PreparedStatementItemPtr getPreparedStatement(const String & name_space, const String & name);
    void removePreparedStatement(const String & name_space, const String & name);

    void updateTableStatistics(const String & name_space, const String & uuid, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data);
    // new api
    std::unordered_map<StatisticsTag, StatisticsBasePtr> getTableStatistics(const String & name_space, const String & uuid);

    void removeTableStatistics(const String & name_space, const String & uuid);

    void updateColumnStatistics(const String & name_space, const String & uuid, const String & column, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data);

    std::unordered_map<StatisticsTag, StatisticsBasePtr>
    getColumnStatistics(const String & name_space, const String & uuid, const String & column);
    std::unordered_map<String, std::unordered_map<StatisticsTag, StatisticsBasePtr>>
    getAllColumnStatistics(const String & name_space, const String & uuid);
    std::vector<String> getAllColumnStatisticsKey(const String & name_space, const String & uuid);
    void removeColumnStatistics(const String & name_space, const String & uuid, const String & column);
    void removeAllColumnStatistics(const String & name_space, const String & uuid);

    void setMergeMutateThreadStartTime(const String & name_space, const String & uuid, const UInt64 & start_time);
    UInt64 getMergeMutateThreadStartTime(const String & name_space, const String & uuid);

    std::shared_ptr<Protos::MaterializedMySQLManagerMetadata> tryGetMaterializedMySQLManagerMetadata(const String & name_space, const UUID & uuid);
    void setMaterializedMySQLManagerMetadata(const String & name_space, const UUID & uuid, const Protos::MaterializedMySQLManagerMetadata & metadata);
    void removeMaterializedMySQLManagerMetadata(const String & name_space, const UUID & uuid);
    std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> getMaterializedMySQLBinlogMetadata(const String & name_space, const String & binlog_name);
    void setMaterializedMySQLBinlogMetadata(const String & name_space, const String & binlog_name, const Protos::MaterializedMySQLBinlogMetadata & metadata);
    void removeMaterializedMySQLBinlogMetadata(const String & name_space, const String & binlog_name);
    void updateMaterializedMySQLMetadataInBatch(const String & name_space, const Strings & names, const Strings & values, const Strings & delete_names);

    void setAsyncQueryStatus(
        const String & name_space, const String & id, const Protos::AsyncQueryStatus & status, UInt64 ttl /* 1 day */ = 86400) const;
    void markBatchAsyncQueryStatusFailed(
        const String & name_space,
        std::vector<Protos::AsyncQueryStatus> & statuses,
        const String & reason,
        UInt64 ttl /* 1 day */ = 86400) const;
    bool tryGetAsyncQueryStatus(const String & name_space, const String & id, Protos::AsyncQueryStatus & status) const;
    std::vector<Protos::AsyncQueryStatus> getIntermidiateAsyncQueryStatuses(const String & name_space) const;

    void attachDetachedParts(
        const String & name_space,
        const String & from_uuid,
        const String & to_uuid,
        const std::vector<String> & detached_part_names,
        const Protos::DataModelPartVector & parts,
        const Protos::DataModelPartVector & staged_parts,
        const Strings & current_partitions,
        const DeleteBitmapMetaPtrVector & detached_bitmaps,
        const DeleteBitmapMetaPtrVector & bitmaps,
        size_t batch_write_size,
        size_t batch_delete_size);
    void detachAttachedParts(
        const String & name_space,
        const String & from_uuid,
        const String & to_uuid,
        const std::vector<String> & attached_part_names,
        const std::vector<String> & attached_staged_part_names,
        const std::vector<std::optional<Protos::DataModelPart>> & parts,
        const DeleteBitmapMetaPtrVector & attached_bitmaps,
        const DeleteBitmapMetaPtrVector & committed_bitmaps,
        size_t batch_write_size,
        size_t batch_delete_size);
    std::vector<std::pair<String, UInt64>> attachDetachedPartsRaw(
        const String & name_space,
        const String & tbl_uuid,
        const std::vector<String> & part_names,
        size_t detached_visible_part_size,
        size_t detached_staged_part_size,
        const std::vector<String> & bitmap_names,
        size_t batch_write_size,
        size_t batch_delete_size);
    void detachAttachedPartsRaw(
        const String & name_space,
        const String & from_uuid,
        const String & to_uuid,
        const std::vector<String> & attached_part_names,
        const std::vector<std::pair<String, String>> & detached_part_metas,
        const std::vector<String> & attached_bitmap_names,
        const std::vector<std::pair<String, String>> & detached_bitmap_metas,
        size_t batch_write_size,
        size_t batch_delete_size);
    IMetaStore::IteratorPtr getDetachedPartsInRange(const String& name_space,
        const String& tbl_uuid, const String& range_start, const String& range_end,
        bool include_start = true, bool include_end = false);

    // Sensitive Resources
    void putSensitiveResource(const String & name_space, const String & database, const String & table, const String & column, const String & target, bool value) const;
    std::shared_ptr<Protos::DataModelSensitiveDatabase> getSensitiveResource(const String & name_space, const String & database) const;

    // Access Entities
    std::vector<std::pair<String, UInt64>> getEntities(EntityType type, const String & name_space, const std::unordered_set<UUID> & ids) const;
    String getAccessEntity(EntityType type, const String & name_space, const String & name) const;
    Strings getAllAccessEntities(EntityType type, const String & name_space) const;
    String getAccessEntityNameByUUID(const String & name_space, const UUID & id) const;
    bool dropAccessEntity(EntityType type, const String & name_space, const UUID & id, const String & name) const;
    bool putAccessEntity(EntityType type, const String & name_space, const AccessEntityModel & new_access_entity, const AccessEntityModel & old_access_entity, bool replace_if_exists) const;

    IMetaStore::IteratorPtr getDetachedDeleteBitmapsInRange(
        const String & name_space,
        const String & tbl_uuid,
        const String & range_start,
        const String & range_end,
        bool include_start = true,
        bool include_end = false);

    String getByKey(const String & key);

    IMetaStore::IteratorPtr getByPrefix(const String & prefix, size_t limit = 0);

    /**
     * @brief Get all items in the trash state. This is a GC related function.
     *
     * @param limit Limit the results, disabled by passing 0.
     * @param start_key KV Scan will begin from `start_key` if it's not empty.
     */
    IMetaStore::IteratorPtr
    getItemsInTrash(const String & name_space, const String & table_uuid, const size_t & limit, const String & start_key = "");

    //Object column schema related API
    static String extractTxnIDFromPartialSchemaKey(const String & partial_schema_key);
    void appendObjectPartialSchema(
        const String & name_space, const String & table_uuid, const UInt64 & txn_id, const SerializedObjectSchema & partial_schema);
    SerializedObjectSchema getObjectPartialSchema(const String & name_space, const String & table_uuid, const UInt64 & txn_id);
    SerializedObjectSchemas scanObjectPartialSchemas(const String & name_space, const String & table_uuid, const UInt64 & limit_size);
    SerializedObjectSchema getObjectAssembledSchema(const String & name_space, const String & table_uuid);
    bool resetObjectAssembledSchemaAndPurgePartialSchemas(
        const String & name_space,
        const String & table_uuid,
        const SerializedObjectSchema & old_assembled_schema,
        const SerializedObjectSchema & new_assembled_schema,
        const std::vector<TxnTimestamp> & partial_schema_txnids);

    SerializedObjectSchemaStatuses batchGetObjectPartialSchemaStatuses(const String & name_space, const std::vector<TxnTimestamp> & txn_ids);
    void batchDeletePartialSchemaStatus(const String & name_space, const std::vector<TxnTimestamp> & txn_ids);
    void updateObjectPartialSchemaStatus(const String &name_space, const TxnTimestamp & txn_id, const ObjectPartialSchemaStatus & status);

    IMetaStore::IteratorPtr getAllDeleteBitmaps(const String & name_space, const String & table_uuid);

    // return successfully removed partitions
    Strings removePartitions(const String & name_space, const String & table_uuid, const PartitionWithGCStatus & partitions);

    /**
     * @brief Get parts partition metrics snapshots in table level.
     */
    IMetaStore::IteratorPtr getTablePartitionMetricsSnapshots(const String & name_space, const String & table_uuid);
    /**
     * @brief Update a partition level parts metrics snapshot.
     */
    void updatePartitionMetricsSnapshot(
        const String & name_space, const String & table_uuid, const String & partition_id, const String & snapshot);
    /**
     * @brief Update a table level trash items metrics snapshot.
     */
    void updateTableTrashItemsSnapshot(const String & name_space, const String & table_uuid, const String & snapshot);
    /**
     * @brief Get a table level trash items metrics snapshot.
     */
    String getTableTrashItemsSnapshot(const String & name_space, const String & table_uuid);

    static void assertNotReadonly(const String & key)
    {
        // only server topology and leader election are allowed to update
        if(!key.ends_with(SERVERS_TOPOLOGY_KEY) && !key.ends_with("-election"))
            throw Exception("metadata is not writable according to config 'enable_cnch_write_remote_catalog' ", ErrorCodes::READONLY);
    }

private:

    MetastorePtr metastore_ptr;
};

} // namespace DB::Catalog

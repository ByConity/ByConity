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

#include <cstddef>
#include <random>
#include <sstream>
#include <vector>
#include <string.h>
#include <Catalog/MetastoreCommon.h>
#include <Catalog/MetastoreProxy.h>
#include <Catalog/LargeKVHandler.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <DaemonManager/BGJobStatusInCatalog.h>
#include <Databases/MySQL/MaterializedMySQLCommon.h>
#include <IO/ReadHelpers.h>
#include <Protos/DataModelHelpers.h>
#include <Protos/plan_node.pb.h>
#include <Statistics/StatisticsBase.h>
#include <Storages/MergeTree/IMetastore.h>
#include <Storages/StorageSnapshot.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include "Interpreters/DistributedStages/PlanSegmentInstance.h"
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptors.hpp>

namespace DB::ErrorCodes
{
extern const int METASTORE_DB_UUID_CAS_ERROR;
extern const int METASTORE_TABLE_UUID_CAS_ERROR;
extern const int METASTORE_TABLE_NAME_CAS_ERROR;
extern const int METASTORE_ROOT_PATH_ALREADY_EXISTS;
extern const int METASTORE_ROOT_PATH_ID_NOT_UNIQUE;
extern const int METASTORE_CLEAR_INTENT_CAS_FAILURE;
extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int METASTORE_COMMIT_CAS_FAILURE;
extern const int METASTORE_TABLE_TDH_CAS_ERROR;
extern const int METASTORE_ACCESS_ENTITY_CAS_ERROR;
extern const int METASTORE_ACCESS_ENTITY_EXISTS_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace DB::Catalog
{

void MetastoreProxy::updateServerWorkerGroup(const DB::String & worker_group_name, const DB::String & worker_group_info)
{
    metastore_ptr->put(WORKER_GROUP_STORE_PREFIX + worker_group_name, worker_group_info);
}

void MetastoreProxy::getServerWorkerGroup(const String & worker_group_name, DB::String & worker_group_info)
{
    metastore_ptr->get(WORKER_GROUP_STORE_PREFIX + worker_group_name, worker_group_info);
}

void MetastoreProxy::dropServerWorkerGroup(const DB::String & worker_group_name)
{
    metastore_ptr->drop(WORKER_GROUP_STORE_PREFIX + worker_group_name);
}

IMetaStore::IteratorPtr MetastoreProxy::getAllWorkerGroupMeta()
{
    return metastore_ptr->getByPrefix(WORKER_GROUP_STORE_PREFIX);
}

void MetastoreProxy::addExternalCatalog(const String & name_space, const Protos::DataModelCatalog & catalog_model)
{
    String catalog_meta;
    catalog_model.SerializeToString(&catalog_meta);
    BatchCommitRequest batch_write;
    batch_write.AddPut(SinglePutRequest(externalCatalogKey(name_space, catalog_model.name()), catalog_meta));
    BatchCommitResponse resp;
    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            /// check if db uuid has conflict with current metainfo
            if (resp.puts.count(0))
                throw Exception(
                    "Catalog with the same uuid(" + catalog_model.name()
                        + ") already exists. Please use another name or "
                          "drop old version of this external catalog and try again.",
                    ErrorCodes::METASTORE_DB_UUID_CAS_ERROR);
        }
        else
            throw;
    }
}

void MetastoreProxy::getExternalCatalog(const String & name_space, const String & name, Strings & catalog_info)
{
    auto it = metastore_ptr->getByPrefix(externalCatalogKey(name_space, name));
    if (it->next())
    {
        catalog_info.emplace_back(it->value());
    }
}


void MetastoreProxy::dropExternalCatalog(const String & name_space, const Protos::DataModelCatalog & catalog_model)
{
    metastore_ptr->drop(externalCatalogKey(name_space, catalog_model.name()));
}

IMetaStore::IteratorPtr MetastoreProxy::getAllExternalCatalogMeta(const String & name_space)
{
    return metastore_ptr->getByPrefix(allExternalCatalogPrefix(name_space));
}


void MetastoreProxy::addDatabase(const String & name_space, const Protos::DataModelDB & db_model)
{
    UUID uuid = RPCHelpers::createUUID(db_model.uuid());
    if (uuid == UUIDHelpers::Nil)
        throw Exception("missing UUID for addDatabase", ErrorCodes::LOGICAL_ERROR);

    String db_meta;
    db_model.SerializeToString(&db_meta);

    BatchCommitRequest batch_write;
    batch_write.AddPut(SinglePutRequest(dbUUIDUniqueKey(name_space, UUIDHelpers::UUIDToString(uuid)), "", true));
    batch_write.AddPut(SinglePutRequest(dbKey(name_space, db_model.name(), db_model.commit_time()), db_meta));

    BatchCommitResponse resp;
    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
             /// check if db uuid has conflict with current metainfo
            if (resp.puts.count(0))
                throw Exception(
                        "Database with the same uuid(" + UUIDHelpers::UUIDToString(RPCHelpers::createUUID(db_model.uuid())) + ") already exists in catalog. Please use another uuid or "
                        "clear old version of this database and try again.",
                        ErrorCodes::METASTORE_DB_UUID_CAS_ERROR);
        }
        else
            throw;
    }
}

void MetastoreProxy::getDatabase(const DB::String & name_space, const DB::String & name, DB::Strings & db_info)
{
    auto it = metastore_ptr->getByPrefix(dbKeyPrefix(name_space, name));
    while(it->next())
    {
        db_info.emplace_back(it->value());
    }
}

IMetaStore::IteratorPtr MetastoreProxy::getAllDatabaseMeta(const DB::String & name_space)
{
    return metastore_ptr->getByPrefix(allDbPrefix(name_space));
}

std::vector<Protos::DataModelDB> MetastoreProxy::getTrashDBs(const String & name_space)
{
    std::vector<Protos::DataModelDB> res;
    auto it = metastore_ptr->getByPrefix(dbTrashPrefix(name_space));
    while(it->next())
    {
        Protos::DataModelDB db_model;
        db_model.ParseFromString(it->value());
        res.emplace_back(db_model);
    }
    return res;
}

std::vector<UInt64> MetastoreProxy::getTrashDBVersions(const String & name_space, const String & database)
{
    std::vector<UInt64> res;
    auto it = metastore_ptr->getByPrefix(dbTrashPrefix(name_space) + escapeString(database));
    while(it->next())
    {
        const auto & key = it->key();
        auto pos = key.find_last_of('_');
        UInt64 drop_ts = std::stoull(key.substr(pos + 1, String::npos), nullptr);
        res.emplace_back(drop_ts);
    }
    return res;
}

void MetastoreProxy::dropDatabase(const String & name_space, const Protos::DataModelDB & db_model)
{
    String name = db_model.name();
    UInt64 ts = db_model.commit_time();

    BatchCommitRequest batch_write;

    /// get all trashed dictionaries of current db and remove them with db metadata
    auto dic_ptrs = getDictionariesFromTrash(name_space, name + "_" + toString(ts));
    for (auto & dic_ptr : dic_ptrs)
        batch_write.AddDelete(dictionaryTrashKey(name_space, dic_ptr->database(), dic_ptr->name()));

    batch_write.AddDelete(dbKey(name_space, name, ts));
    batch_write.AddDelete(dbTrashKey(name_space, name, ts));
    if (db_model.has_uuid())
        batch_write.AddDelete(dbUUIDUniqueKey(name_space, UUIDHelpers::UUIDToString(RPCHelpers::createUUID(db_model.uuid()))));

    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

void MetastoreProxy::createSnapshot(const String & name_space, const String & db_uuid, const Protos::DataModelSnapshot & snapshot)
{
    auto key = snapshotKey(name_space, db_uuid, snapshot.name());
    metastore_ptr->put(key, snapshot.SerializeAsString(), /*if_not_exists*/true);
}

void MetastoreProxy::removeSnapshot(const String & name_space, const String & db_uuid, const String & name)
{
    metastore_ptr->drop(snapshotKey(name_space, db_uuid, name));
}

std::shared_ptr<Protos::DataModelSnapshot>
MetastoreProxy::tryGetSnapshot(const String & name_space, const String & db_uuid, const String & name)
{
    auto key = snapshotKey(name_space, db_uuid, name);
    String value;
    metastore_ptr->get(key, value);
    if (value.empty())
        return {};

    auto res = std::make_shared<Protos::DataModelSnapshot>();
    res->ParseFromString(value);
    return res;
}

std::vector<std::shared_ptr<Protos::DataModelSnapshot>>
MetastoreProxy::getAllSnapshots(const String & name_space, const String & db_uuid)
{
    std::vector<std::shared_ptr<Protos::DataModelSnapshot>> res;
    auto it = metastore_ptr->getByPrefix(snapshotPrefix(name_space, db_uuid));
    while (it->next())
    {
        auto item = std::make_shared<Protos::DataModelSnapshot>();
        item->ParseFromString(it->value());
        res.push_back(std::move(item));
    }
    return res;
}

String MetastoreProxy::getTableUUID(const String & name_space, const String & database, const String & name)
{
    String identifier_meta;
    metastore_ptr->get(tableUUIDMappingKey(name_space, database, name), identifier_meta);
    if (identifier_meta.empty())
        return "";
    Protos::TableIdentifier identifier;
    identifier.ParseFromString(identifier_meta);
    return identifier.uuid();
}

void MetastoreProxy::dropMaskingPolicies(const String & name_space, const Strings & names)
{
    if (names.empty())
        return;

    Strings keys;
    keys.reserve(names.size());
    std::transform(names.begin(), names.end(), std::back_inserter(keys),
        [&name_space](const auto & name) {
            return maskingPolicyKey(name_space, name);
        }
    );

    multiDrop(keys);
}

std::shared_ptr<Protos::TableIdentifier> MetastoreProxy::getTableID(const String & name_space, const String & database, const String & name)
{
    String identifier_meta;
    metastore_ptr->get(tableUUIDMappingKey(name_space, database, name), identifier_meta);
    std::shared_ptr<Protos::TableIdentifier> res = nullptr;
    if (!identifier_meta.empty())
    {
        res = std::make_shared<Protos::TableIdentifier>();
        res->ParseFromString(identifier_meta);
    }
    return res;
}

std::shared_ptr<std::vector<std::shared_ptr<Protos::TableIdentifier>>> MetastoreProxy::getTableIDs(const String & name_space,
        const std::vector<std::pair<String, String>> & db_table_pairs)
{
    Strings keys;
    for (const auto & pair : db_table_pairs)
    {
        keys.push_back(tableUUIDMappingKey(name_space, pair.first, pair.second));
    }

    auto res = std::make_shared<std::vector<std::shared_ptr<Protos::TableIdentifier>>>(std::vector<std::shared_ptr<Protos::TableIdentifier>>{});

    auto values = metastore_ptr->multiGet(keys);
    for (const auto & value : values)
    {
        if (value.first.empty())
            continue;
        res->emplace_back(std::make_shared<Protos::TableIdentifier>());
        res->back()->ParseFromString(std::move(value.first));
    }

    return res;
}

String MetastoreProxy::getTrashTableUUID(const String & name_space, const String & database, const String & name, const UInt64 & ts)
{
    String identifier_meta;
    metastore_ptr->get(tableTrashKey(name_space, database, name, ts), identifier_meta);
    if (identifier_meta.empty())
        return "";
    Protos::TableIdentifier identifier;
    identifier.ParseFromString(identifier_meta);
    return identifier.uuid();
}

void MetastoreProxy::createTable(const String & name_space, const UUID & db_uuid, const DB::Protos::DataModelTable & table_data, const Strings & dependencies, const Strings & masking_policy_mapping)
{
    const String & database = table_data.database();
    const String & name = table_data.name();
    const String & uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table_data.uuid()));
    String serialized_meta;
    table_data.SerializeToString(&serialized_meta);

    BatchCommitRequest batch_write;
    batch_write.AddPut(SinglePutRequest(nonHostUpdateKey(name_space, uuid), "0", true));

    // insert table meta. Handle by largeKVHandler in case the table meta exceeds KV size limitation
    addPotentialLargeKVToBatchwrite(
        metastore_ptr,
        batch_write,
        name_space,
        tableStoreKey(name_space, uuid, table_data.commit_time()),
        serialized_meta,
        true/*if_not_exists*/);

    /// add dependency mapping if need
    for (const String & dependency : dependencies)
        batch_write.AddPut(SinglePutRequest(viewDependencyKey(name_space, dependency, uuid), uuid));

    for (const String & mask : masking_policy_mapping)
        batch_write.AddPut(SinglePutRequest(maskingPolicyTableMappingKey(name_space, mask, uuid), uuid));

    /// add name to identifier mapping
    Protos::TableIdentifier identifier;
    identifier.set_database(database);
    identifier.set_name(name);
    identifier.set_uuid(uuid);
    if (table_data.has_server_vw_name())
        identifier.set_server_vw_name(table_data.server_vw_name());
    if (db_uuid != UUIDHelpers::Nil)
        RPCHelpers::fillUUID(db_uuid, *identifier.mutable_db_uuid());

    batch_write.AddPut(SinglePutRequest(tableUUIDMappingKey(name_space, database, name), identifier.SerializeAsString(), true));
    batch_write.AddPut(SinglePutRequest(tableUUIDUniqueKey(name_space, uuid), "", true));

    BatchCommitResponse resp;
    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            auto putssize = batch_write.puts.size();
            if (resp.puts.count(putssize-1))
            {
                throw Exception(
                    "Table with the same uuid already exists in catalog. Please use another uuid or "
                    "clear old version of this table and try again.",
                    ErrorCodes::METASTORE_TABLE_UUID_CAS_ERROR);
            }
            else if (resp.puts.count(putssize-2))
            {
                throw Exception(
                    "Table with the same name already exists in catalog. Please use another name and try again.",
                    ErrorCodes::METASTORE_TABLE_NAME_CAS_ERROR);
            }
        }
        throw;
    }
}

void MetastoreProxy::createUDF(const String & name_space, const DB::Protos::DataModelUDF & udf_data)
{
    const String & prefix = udf_data.prefix_name();
    const String & name = udf_data.function_name();
    String serialized_meta;
    udf_data.SerializeToString(&serialized_meta);

    try
    {
        metastore_ptr->put(udfStoreKey(name_space, prefix, name), serialized_meta, true);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE) {
            throw Exception("UDF with function name - " + name + " with prefix - " +  prefix + " already exists.", ErrorCodes::FUNCTION_ALREADY_EXISTS);
        }
        throw;
    }
}

void MetastoreProxy::dropUDF(const String & name_space, const String &resolved_name)
{
    metastore_ptr->drop(udfStoreKey(name_space, resolved_name));
}

void MetastoreProxy::updateTable(const String & name_space, const String & table_uuid, const String & table_info_new, const UInt64 & ts)
{
    if (table_info_new.size() > metastore_ptr->getMaxKVSize())
    {
        BatchCommitRequest batch_write;
        addPotentialLargeKVToBatchwrite(
            metastore_ptr,
            batch_write,
            name_space,
            tableStoreKey(name_space, table_uuid, ts),
            table_info_new);
        BatchCommitResponse resp;
        metastore_ptr->batchWrite(batch_write, resp);
    }
    else
        metastore_ptr->put(tableStoreKey(name_space, table_uuid, ts), table_info_new);
}

void MetastoreProxy::updateTableWithID(const String & name_space, const Protos::TableIdentifier & table_id, const DB::Protos::DataModelTable & table_data)
{
    BatchCommitRequest batch_write;
    batch_write.AddPut(SinglePutRequest(tableUUIDMappingKey(name_space, table_id.database(), table_id.name()), table_id.SerializeAsString()));

    addPotentialLargeKVToBatchwrite(
        metastore_ptr,
        batch_write,
        name_space,
        tableStoreKey(name_space, table_id.uuid(), table_data.commit_time()),
        table_data.SerializeAsString());

    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

void MetastoreProxy::getTableByUUID(const String & name_space, const String & table_uuid, Strings & tables_info)
{
    auto it = metastore_ptr->getByPrefix(tableStorePrefix(name_space, table_uuid));
    while(it->next())
    {
        String table_meta = it->value();
        /// NOTE: Too many large KVs will cause severe performance regression. It rarely happens
        tryGetLargeValue(metastore_ptr, name_space, it->key(), table_meta);
        tables_info.emplace_back(std::move(table_meta));
    }
}

IMetaStore::IteratorPtr MetastoreProxy::getAllTablesMeta(const DB::String &name_space)
{
    return metastore_ptr->getByPrefix(tableMetaPrefix(name_space));
}

IMetaStore::IteratorPtr MetastoreProxy::getAllUDFsMeta(const DB::String &name_space, const DB::String & database_name)
{
    return metastore_ptr->getByPrefix(udfStoreKey(name_space, database_name));
}

Strings MetastoreProxy::getUDFsMetaByName(const String & name_space, const std::unordered_set<String> &function_names)
{
    Strings keys;
    for (const auto & function_name: function_names)
        keys.push_back(udfStoreKey(name_space, function_name));

    Strings udf_info;
    auto values = metastore_ptr->multiGet(keys);
    for (const auto & ele : values)
        udf_info.emplace_back(std::move(ele.first));
    return udf_info;
}

std::vector<std::shared_ptr<Protos::TableIdentifier>> MetastoreProxy::getAllTablesId(const String & name_space, const String & db)
{
    return getTablesIdByPrefix(name_space, db.empty() ? escapeString(db) : escapeString(db) + "_");
}

std::vector<std::shared_ptr<Protos::TableIdentifier>> MetastoreProxy::getTablesIdByPrefix(const String & name_space, const String & prefix)
{
    std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
    String meta_prefix = tableUUIDMappingPrefix(name_space) + prefix;
    auto it = metastore_ptr->getByPrefix(meta_prefix);
    while(it->next())
    {
        std::shared_ptr<Protos::TableIdentifier> identifier_ptr(new Protos::TableIdentifier());
        identifier_ptr->ParseFromString(it->value());
        res.push_back(identifier_ptr);
    }
    return res;
}

Strings MetastoreProxy::getAllDependence(const String & name_space, const String & uuid)
{
    Strings res;
    auto it = metastore_ptr->getByPrefix(viewDependencyPrefix(name_space, uuid));

    while(it->next())
        res.push_back(it->value());

    return res;
}

std::vector<std::shared_ptr<Protos::VersionedPartitions>> MetastoreProxy::getMvBaseTables(const String & name_space, const String & uuid)
{
    std::vector<std::shared_ptr<Protos::VersionedPartitions>> res;
    auto it = metastore_ptr->getByPrefix(matViewBaseTablesPrefix(name_space, uuid));
    while(it->next())
    {
        std::shared_ptr<Protos::VersionedPartitions> identifier_ptr(new Protos::VersionedPartitions());
        identifier_ptr->add_versioned_partition()->ParseFromString(it->value());
        StorageID storage_id = RPCHelpers::createStorageID(identifier_ptr->versioned_partition(0).storage_id());
        RPCHelpers::fillStorageID(storage_id, *identifier_ptr->mutable_storage_id());
        if (!res.empty()
            && identifier_ptr->storage_id().uuid().low() == res.back()->storage_id().uuid().low()
            && identifier_ptr->storage_id().uuid().high() == res.back()->storage_id().uuid().high())
        {
            for (const auto & p : identifier_ptr->versioned_partition())
                res.back()->add_versioned_partition()->CopyFrom(p);
        }
        else
            res.push_back(identifier_ptr);
    }
    return res;
}

String MetastoreProxy::getMvMetaVersion(const String & name_space, const String & uuid)
{
    String mv_meta_version_ts;
    metastore_ptr->get(matViewVersionKey(name_space, uuid), mv_meta_version_ts);
    LOG_TRACE(&Poco::Logger::get("MetaStore"), "get mv meta, version {}.", mv_meta_version_ts);
    if (mv_meta_version_ts.empty())
        return "";

    return mv_meta_version_ts;
}

BatchCommitRequest MetastoreProxy::constructMvMetaRequests(const String & name_space, const String & uuid,
    std::vector<std::shared_ptr<Protos::VersionedPartition>> add_partitions,
    std::vector<std::shared_ptr<Protos::VersionedPartition>> drop_partitions,
    String mv_version_ts)
{
    LOG_TRACE(&Poco::Logger::get("MetaStore"), "construct mv meta, version {}.", mv_version_ts);

    BatchCommitRequest multi_write;
    for (const auto & add : add_partitions)
    {
        String base_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(add->storage_id().uuid()));
        String value;
        add->SerializeToString(&value);
        String key = matViewBaseTablesKey(name_space, uuid, base_uuid, add->partition());
        multi_write.AddPut(SinglePutRequest(key, value));
        LOG_TRACE(&Poco::Logger::get("MetaStore"), "add key {} value size {}.", key, value.size());
    }

    multi_write.AddPut(SinglePutRequest(matViewVersionKey(name_space, uuid), mv_version_ts));

    for (const auto & drop : drop_partitions)
    {
        String base_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(drop->storage_id().uuid()));
        String value;
        drop->SerializeToString(&value);
        String key = matViewBaseTablesKey(name_space, uuid, base_uuid, drop->partition());
        multi_write.AddDelete(SinglePutRequest(key, value));
        LOG_TRACE(&Poco::Logger::get("MetaStore"), "drop key {}.", key);
    }

    return multi_write;
}

void MetastoreProxy::updateMvMeta(const String & name_space, const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions)
{
    for (const auto & ele : versioned_partitions)
    {
        if (!ele->versioned_partition().empty())
        {
            String base_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(ele->storage_id().uuid()));
            for (const auto & p : ele->versioned_partition())
            {
                String serialized_meta;
                p.SerializeToString(&serialized_meta);
                metastore_ptr->put(matViewBaseTablesKey(name_space, uuid, base_uuid, p.partition()), serialized_meta);
                LOG_TRACE(&Poco::Logger::get("MetaStore"), "value size {}.", serialized_meta.size());
            }
        }
    }
}

void MetastoreProxy::cleanMvMeta(const String & name_space, const String & uuid)
{
    metastore_ptr->clean(matViewVersionKey(name_space, uuid));
    metastore_ptr->clean(matViewBaseTablesPrefix(name_space, uuid));
}

void MetastoreProxy::dropMvMeta(const String & name_space, const String & uuid, std::vector<std::shared_ptr<Protos::VersionedPartitions>> versioned_partitions)
{
    for (const auto & ele : versioned_partitions)
    {
        if (!ele->versioned_partition().empty())
        {
            String base_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(ele->storage_id().uuid()));
            for (const auto & p : ele->versioned_partition())
                metastore_ptr->drop(matViewBaseTablesKey(name_space, uuid, base_uuid, p.partition()));
        }
    }
}

IMetaStore::IteratorPtr MetastoreProxy::getTrashTableIDIterator(const String & name_space, uint32_t iterator_internal_batch_size)
{
    return metastore_ptr->getByPrefix(tableTrashPrefix(name_space), 0, iterator_internal_batch_size);
}

std::vector<std::shared_ptr<Protos::TableIdentifier>> MetastoreProxy::getTrashTableID(const String & name_space)
{
    std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
    auto it = metastore_ptr->getByPrefix(tableTrashPrefix(name_space));
    while(it->next())
    {
        std::shared_ptr<Protos::TableIdentifier> identifier_ptr(new Protos::TableIdentifier());
        identifier_ptr->ParseFromString(it->value());
        res.push_back(identifier_ptr);
    }
    return res;
}

void MetastoreProxy::createDictionary(const String & name_space, const String & db, const String & name, const String & dic_meta)
{
    metastore_ptr->put(dictionaryStoreKey(name_space, db, name), dic_meta);
}

void MetastoreProxy::getDictionary(const String & name_space, const String & db, const String & name, String & dic_meta)
{
    metastore_ptr->get(dictionaryStoreKey(name_space, db, name), dic_meta);
}

void MetastoreProxy::dropDictionary(const String & name_space, const String & db, const String & name)
{
    metastore_ptr->drop(dictionaryStoreKey(name_space, db, name));
}

std::vector<std::shared_ptr<Protos::DataModelDictionary>> MetastoreProxy::getDictionariesInDB(const String & name_space, const String & database)
{
    std::vector<std::shared_ptr<Protos::DataModelDictionary>> res;
    auto it = metastore_ptr->getByPrefix(dictionaryPrefix(name_space, database));
    while(it->next())
    {
        std::shared_ptr<Protos::DataModelDictionary> dic_ptr(new Protos::DataModelDictionary());
        dic_ptr->ParseFromString(it->value());
        res.push_back(dic_ptr);
    }
    return res;
}

std::shared_ptr<Protos::TableIdentifier> MetastoreProxy::getTrashTableID(const String & name_space, const String & database, const String & table, const UInt64 & ts)
{
    std::shared_ptr<Protos::TableIdentifier> res = nullptr;
    String meta;
    metastore_ptr->get(tableTrashKey(name_space, database, table, ts), meta);
    if (!meta.empty())
    {
        res = std::make_shared<Protos::TableIdentifier>();
        res->ParseFromString(meta);
    }
    return res;
}

std::vector<std::shared_ptr<Protos::TableIdentifier>> MetastoreProxy::getTablesFromTrash(const String & name_space, const String & database)
{
    std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
    auto it = metastore_ptr->getByPrefix(tableTrashPrefix(name_space, database));
    while(it->next())
    {
        std::shared_ptr<Protos::TableIdentifier> identifier_ptr(new Protos::TableIdentifier());
        identifier_ptr->ParseFromString(it->value());
        res.push_back(identifier_ptr);
    }
    return res;
}

std::map<UInt64, std::shared_ptr<Protos::TableIdentifier>> MetastoreProxy::getTrashTableVersions(const String & name_space, const String & database, const String & table)
{
    std::map<UInt64, std::shared_ptr<Protos::TableIdentifier>> res;
    auto it = metastore_ptr->getByPrefix(tableTrashPrefix(name_space) + escapeString(database) + "_" + escapeString(table));
    while(it->next())
    {
        const auto & key = it->key();
        auto pos = key.find_last_of('_');
        UInt64 drop_ts = std::stoull(key.substr(pos + 1, String::npos), nullptr);
        auto table_id = std::make_shared<Protos::TableIdentifier>();
        table_id->ParseFromString(it->value());
        res.emplace(drop_ts, table_id);
    }
    return res;
}

IMetaStore::IteratorPtr MetastoreProxy::getAllDictionaryMeta(const DB::String & name_space)
{
    return metastore_ptr->getByPrefix(allDictionaryPrefix(name_space));
}

std::vector<std::shared_ptr<DB::Protos::DataModelDictionary>> MetastoreProxy::getDictionariesFromTrash(const String & name_space, const String & database)
{
    std::vector<std::shared_ptr<Protos::DataModelDictionary>> res;
    auto it = metastore_ptr->getByPrefix(dictionaryTrashPrefix(name_space, database));
    while(it->next())
    {
        std::shared_ptr<Protos::DataModelDictionary> dic_ptr(new Protos::DataModelDictionary());
        dic_ptr->ParseFromString(it->value());
        res.push_back(dic_ptr);
    }
    return res;
}

void MetastoreProxy::clearTableMeta(const String & name_space, const String & database, const String & table, const String & uuid, const Strings & dependencies, const UInt64 & ts)
{
    /// uuid should not be empty
    if (uuid.empty())
        return;

    BatchCommitRequest batch_write;

    /// remove all versions of table meta info.
    auto it_t = metastore_ptr->getByPrefix(tableStorePrefix(name_space, uuid));
    while(it_t->next())
    {
        batch_write.AddDelete(it_t->key());
    }

    /// remove table partition list;
    String partition_list_prefix = tablePartitionInfoPrefix(name_space, uuid);
    auto it_p = metastore_ptr->getByPrefix(partition_list_prefix);
    while(it_p->next())
    {
        batch_write.AddDelete(it_p->key());
    }
    /// remove dependency
    for (const String & dependency : dependencies)
        batch_write.AddDelete(viewDependencyKey(name_space, dependency, uuid));

    /// remove trash record if the table marked as deleted before be cleared
    batch_write.AddDelete(tableTrashKey(name_space, database, table, ts));

    /// remove MergeMutateThread meta
    batch_write.AddDelete(mergeMutateThreadStartTimeKey(name_space, uuid));
    /// remove table uuid unique key
    batch_write.AddDelete(tableUUIDUniqueKey(name_space, uuid));
    batch_write.AddDelete(nonHostUpdateKey(name_space, uuid));

    /// remove all statistics
    auto table_statistics_prefix = tableStatisticPrefix(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(table_statistics_prefix); it->next(); )
    {
        batch_write.AddDelete(it->key());
    }
    auto table_statistics_tag_prefix = tableStatisticTagPrefix(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(table_statistics_tag_prefix); it->next(); )
    {
        batch_write.AddDelete(it->key());
    }
    auto column_statistics_prefix = columnStatisticPrefixWithoutColumn(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(column_statistics_prefix); it->next(); )
    {
        batch_write.AddDelete(it->key());
    }
    auto column_statistics_tag_prefix = columnStatisticTagPrefixWithoutColumn(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(column_statistics_tag_prefix); it->next(); )
    {
        batch_write.AddDelete(it->key());
    }

    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

String MetastoreProxy::getMaskingPolicy(const String & name_space, const String & masking_policy_name) const
{
    String data;
    metastore_ptr->get(maskingPolicyKey(name_space, masking_policy_name), data);
    return data;
}

Strings MetastoreProxy::getMaskingPolicies(const String & name_space, const Strings & masking_policy_names) const
{
    if (masking_policy_names.empty())
        return {};

    Strings keys;
    keys.reserve(masking_policy_names.size());
    for (const auto & name : masking_policy_names)
        keys.push_back(maskingPolicyKey(name_space, name));

    Strings res;
    auto values = metastore_ptr->multiGet(keys);
    for (auto & ele : values)
        res.push_back(std::move(ele.first));

    return res;
}

Strings MetastoreProxy::getAllMaskingPolicy(const String & name_space) const
{
    Strings models;
    auto it = metastore_ptr->getByPrefix(maskingPolicyMetaPrefix(name_space));
    models.reserve(models.size());
    while (it->next())
    {
        models.push_back(it->value());
    }

    return models;
}

void MetastoreProxy::putMaskingPolicy(const String & name_space, const Protos::DataModelMaskingPolicy & new_masking_policy) const
{
    metastore_ptr->put(maskingPolicyKey(name_space, new_masking_policy.name()), new_masking_policy.SerializeAsString());
}

IMetaStore::IteratorPtr MetastoreProxy::getMaskingPolicyAppliedTables(const String & name_space, const String & masking_policy_name) const
{
    return metastore_ptr->getByPrefix(maskingPolicyTableMappingPrefix(name_space, masking_policy_name));
}

void MetastoreProxy::prepareRenameTable(const String & name_space,
                                 const String & table_uuid,
                                 const String & from_db,
                                 const String & from_table,
                                 const UUID & to_db_uuid,
                                 Protos::DataModelTable & to_table,
                                 BatchCommitRequest & batch_write)
{
    /// update `table`->`uuid` mapping.
    batch_write.AddDelete(tableUUIDMappingKey(name_space, from_db, from_table));
    Protos::TableIdentifier identifier;
    identifier.set_database(to_table.database());
    identifier.set_name(to_table.name());
    identifier.set_uuid(table_uuid);
    if (to_db_uuid != UUIDHelpers::Nil)
        RPCHelpers::fillUUID(to_db_uuid, *identifier.mutable_db_uuid());
    batch_write.AddPut(SinglePutRequest(tableUUIDMappingKey(name_space, to_table.database(), to_table.name()), identifier.SerializeAsString(), true));

    /// add new table meta data with new name
    addPotentialLargeKVToBatchwrite(
        metastore_ptr,
        batch_write,
        name_space,
        tableStoreKey(name_space, table_uuid, to_table.commit_time()),
        to_table.SerializeAsString(),
        true/*if_not_exists*/);
}

bool MetastoreProxy::alterTable(const String & name_space, const Protos::DataModelTable & table, const Strings & masks_to_remove, const Strings & masks_to_add)
{
    BatchCommitRequest batch_write;

    String table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table.uuid()));

    addPotentialLargeKVToBatchwrite(
        metastore_ptr,
        batch_write,
        name_space,
        tableStoreKey(name_space, table_uuid, table.commit_time()),
        table.SerializeAsString(),
        true/*if_not_exists*/);

    Protos::TableIdentifier identifier;
    identifier.set_database(table.database());
    identifier.set_name(table.name());
    identifier.set_uuid(table_uuid);
    if (table.has_server_vw_name())
        identifier.set_server_vw_name(table.server_vw_name());
    batch_write.AddPut(SinglePutRequest(tableUUIDMappingKey(name_space, table.database(), table.name()), identifier.SerializeAsString()));

    for (const auto & name : masks_to_remove)
        batch_write.AddDelete(maskingPolicyTableMappingKey(name_space, name, table_uuid));

    for (const auto & name : masks_to_add)
        batch_write.AddPut(SinglePutRequest(maskingPolicyTableMappingKey(name_space, name, table_uuid), table_uuid));

    BatchCommitResponse resp;
    return metastore_ptr->batchWrite(batch_write, resp);
}

Strings MetastoreProxy::getAllTablesInDB(const String & name_space, const String & database)
{
    Strings res;
    auto it = metastore_ptr->getByPrefix(tableUUIDMappingPrefix(name_space, database));
    Protos::TableIdentifier identifier;
    while(it->next())
    {
        identifier.ParseFromString(it->value());
        res.push_back(identifier.name());
    }
    return res;
}

void MetastoreProxy::dropDataPart(const String & name_space, const String & uuid, const String & part_name, const String & part_info)
{
    metastore_ptr->put(dataPartKey(name_space, uuid, part_name), part_info);
}

Strings MetastoreProxy::getPartsByName(const String & name_space, const String & uuid, const Strings & parts_name)
{
    Strings keys;
    for (const auto & part_name : parts_name)
        keys.push_back(dataPartKey(name_space, uuid, part_name));

    Strings parts_meta;
    auto values = metastore_ptr->multiGet(keys);
    for (auto & ele : values)
        parts_meta.emplace_back(std::move(ele.first));
    return parts_meta;
}

IMetaStore::IteratorPtr MetastoreProxy::getPartsInRange(const String & name_space, const String & uuid, const String & partition_id)
{
    if (partition_id.empty())
        /// when partition_id is empty, it should return all data parts in current table.
        return metastore_ptr->getByPrefix(dataPartPrefix(name_space, uuid));

    std::stringstream ss;
    ss << dataPartPrefix(name_space, uuid) << partition_id << '_';
    return metastore_ptr->getByPrefix(ss.str());
}

IMetaStore::IteratorPtr MetastoreProxy::getPartsInRange(const String & name_space, const String & table_uuid, const String & range_start, const String & range_end, bool include_start, bool include_end)
{
    auto prefix = dataPartPrefix(name_space, table_uuid);
    return metastore_ptr->getByRange(prefix + range_start, prefix + range_end, include_start, include_end);
}

UInt64 MetastoreProxy::getNonHostUpdateTimeStamp(const String & name_space, const String & table_uuid)
{
    String nhut = "";
    metastore_ptr->get(nonHostUpdateKey(name_space, table_uuid), nhut);
    return nhut.empty() ? 0 : std::stoul(nhut);
}

void MetastoreProxy::setNonHostUpdateTimeStamp(const String & name_space, const String & table_uuid, const UInt64 pts)
{
    metastore_ptr->put(nonHostUpdateKey(name_space, table_uuid), toString(pts));
}

void MetastoreProxy::prepareAddDataParts(
    const String & name_space,
    const String & table_uuid,
    const Strings & current_partitions,
    std::unordered_set<String> & deleting_partitions,
    const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts,
    BatchCommitRequest & batch_write,
    const std::vector<String> & expected_parts,
    const UInt64 txn_id,
    bool update_sync_list,
    bool write_manifest)
{
    if (parts.empty())
        return;

    std::unordered_set<String> partitions_found_in_deleting_set;
    std::unordered_map<String, String > partition_map;

    UInt64 commit_time = 0;
    size_t expected_parts_size = expected_parts.size();

    if (expected_parts_size != static_cast<size_t>(parts.size()))
        throw Exception("Part size wants to write does not match the expected part size.", ErrorCodes::LOGICAL_ERROR);

    for (auto it = parts.begin(); it != parts.end(); it++)
    {
        auto info_ptr = createPartInfoFromModel(it->part_info());
        /// TODO: replace to part commit time?
        if (!commit_time)
            commit_time = info_ptr->mutation;
        String part_meta = it->SerializeAsString();

        batch_write.AddPut(SinglePutRequest(dataPartKey(name_space, table_uuid, info_ptr->getPartName()), part_meta, expected_parts[it - parts.begin()]));
        if (write_manifest)
            batch_write.AddPut(SinglePutRequest(manifestKeyForPart(name_space, table_uuid, txn_id, info_ptr->getPartName()), part_meta));

        if (deleting_partitions.count(info_ptr->partition_id) && !partitions_found_in_deleting_set.count(info_ptr->partition_id))
            partitions_found_in_deleting_set.emplace(info_ptr->partition_id);

        if (!partition_map.count(info_ptr->partition_id))
            partition_map.emplace(info_ptr->partition_id, it->partition_minmax());
    }

    if (update_sync_list)
        batch_write.AddPut(SinglePutRequest(syncListKey(name_space, table_uuid, commit_time), std::to_string(commit_time)));

    // Prepare partition metadata. Skip those already exists non-deleting partitions
    for (const auto & exist_partition : current_partitions)
    {
        auto it = partition_map.find(exist_partition);
        if (it != partition_map.end() && !partitions_found_in_deleting_set.count(exist_partition))
            partition_map.erase(it);
    }

    Protos::PartitionMeta partition_model;
    for (auto it = partition_map.begin(); it != partition_map.end(); it++)
    {
        partition_model.set_id(it->first);
        partition_model.set_partition_minmax(it->second);

        batch_write.AddPut(SinglePutRequest(tablePartitionInfoKey(name_space, table_uuid, it->first), partition_model.SerializeAsString()));
    }

    partitions_found_in_deleting_set.swap(deleting_partitions);
}

void MetastoreProxy::prepareAddStagedParts(
    const String & name_space,
    const String & table_uuid,
    const Strings & current_partitions,
    const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts,
    BatchCommitRequest & batch_write,
    const std::vector<String> & expected_staged_parts)
{
    if (parts.empty())
        return;

    std::unordered_map<String, String> partition_map;
    size_t expected_staged_part_size = expected_staged_parts.size();
    if (expected_staged_part_size != static_cast<size_t>(parts.size()))
        throw Exception("Staged part size wants to write does not match the expected staged part size.", ErrorCodes::LOGICAL_ERROR);

    for (auto it = parts.begin(); it != parts.end(); it++)
    {
        auto info_ptr = createPartInfoFromModel(it->part_info());
        String part_meta = it->SerializeAsString();
        batch_write.AddPut(SinglePutRequest(stagedDataPartKey(name_space, table_uuid, info_ptr->getPartName()), part_meta, expected_staged_parts[it - parts.begin()]));

        if (!partition_map.count(info_ptr->partition_id))
            partition_map.emplace(info_ptr->partition_id, it->partition_minmax());
    }

    // Prepare partition metadata. Skip those already exists partitions
    for (const auto & exist_partition : current_partitions)
    {
        auto it = partition_map.find(exist_partition);
        if (it != partition_map.end())
            partition_map.erase(it);
    }

    Protos::PartitionMeta partition_model;
    for (auto & it : partition_map)
    {
        std::stringstream ss;
        /// To keep the partitions have the same order as data parts in bytekv, we add an extra "_" in the key of partition meta
        ss << tablePartitionInfoPrefix(name_space, table_uuid) << it.first << '_';

        partition_model.set_id(it.first);
        partition_model.set_partition_minmax(it.second);

        batch_write.AddPut(SinglePutRequest(ss.str(), partition_model.SerializeAsString()));
    }
}

void MetastoreProxy::dropDataPart(const String & name_space, const String & uuid, const String & part_name)
{
    metastore_ptr->drop(dataPartKey(name_space, uuid, part_name));
}

void MetastoreProxy::dropAllPartInTable(const String & name_space, const String & uuid)
{
    /// clear data parts metadata as well as partition metadata
    metastore_ptr->clean(dataPartPrefix(name_space, uuid));
    metastore_ptr->clean(trashItemsPrefix(name_space, uuid));
    metastore_ptr->clean(stagedDataPartPrefix(name_space, uuid));
    metastore_ptr->clean(tablePartitionInfoPrefix(name_space, uuid));
    metastore_ptr->clean(detachedPartPrefix(name_space, uuid));
    metastore_ptr->clean(partitionPartsMetricsSnapshotPrefix(name_space,uuid, ""));
    metastore_ptr->clean(tableTrashItemsMetricsSnapshotPrefix(name_space, uuid));
}

void MetastoreProxy::dropAllMutationsInTable(const String & name_space, const String & uuid)
{
    /// clear mutations metadata
    metastore_ptr->clean(tableMutationPrefix(name_space, uuid));
}

void MetastoreProxy::dropAllDeleteBitmapInTable(const String & name_space, const String & uuid)
{
    /// clear delete bitmaps metadata
    metastore_ptr->clean(deleteBitmapPrefix(name_space, uuid));
    metastore_ptr->clean(detachedDeleteBitmapKeyPrefix(name_space, uuid));
}

IMetaStore::IteratorPtr MetastoreProxy::getStagedParts(const String & name_space, const String & uuid)
{
    return metastore_ptr->getByPrefix(stagedDataPartPrefix(name_space, uuid));
}

IMetaStore::IteratorPtr MetastoreProxy::getStagedPartsInPartition(const String & name_space, const String & uuid, const String & partition)
{
    return metastore_ptr->getByPrefix(stagedDataPartPrefix(name_space, uuid) + partition + "_");
}

void MetastoreProxy::createRootPath(const String & root_path)
{
    std::mt19937 gen(std::random_device{}());
    BatchCommitRequest batch_write;

    String path_id = std::to_string(gen());
    /// avoid generate reserved path id
    while (path_id == "0")
        path_id = std::to_string(gen());

    batch_write.AddPut(SinglePutRequest(ROOT_PATH_PREFIX + root_path, path_id, true));
    batch_write.AddPut(SinglePutRequest(ROOT_PATH_ID_UNIQUE_PREFIX + path_id, "", true));

    BatchCommitResponse resp;
    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            if (resp.puts.count(0))
            {
                throw Exception("Root path " + root_path + " already exists.", ErrorCodes::METASTORE_ROOT_PATH_ALREADY_EXISTS);
            }
            else if (resp.puts.count(1))
            {
                throw Exception("Failed to create new root path because of path id collision, please try again.", ErrorCodes::METASTORE_ROOT_PATH_ID_NOT_UNIQUE);
            }
        }
        throw;
    }
}

void MetastoreProxy::deleteRootPath(const String & root_path)
{
    String path_id;
    metastore_ptr->get(ROOT_PATH_PREFIX + root_path, path_id);
    if (!path_id.empty())
    {
        BatchCommitRequest batch_write;
        BatchCommitResponse resp;
        batch_write.AddDelete(ROOT_PATH_PREFIX + root_path);
        batch_write.AddDelete(ROOT_PATH_ID_UNIQUE_PREFIX + path_id);
        metastore_ptr->batchWrite(batch_write, resp);
    }
}

std::vector<std::pair<String, UInt32>> MetastoreProxy::getAllRootPath()
{
    std::vector<std::pair<String, UInt32>> res;
    auto it = metastore_ptr->getByPrefix(ROOT_PATH_PREFIX);
    while(it->next())
    {
        String path = it->key().substr(String(ROOT_PATH_PREFIX).size(), std::string::npos);
        UInt32 path_id = std::stoul(it->value(), nullptr);
        res.emplace_back(path, path_id);
    }
    return res;
}

void MetastoreProxy::createMutation(const String & name_space, const String & uuid, const String & mutation_name, const String & mutation_text)
{
    metastore_ptr->put(tableMutationKey(name_space, uuid, mutation_name), mutation_text);
}

void MetastoreProxy::removeMutation(const String & name_space, const String & uuid, const String & mutation_name)
{
    LOG_TRACE(&Poco::Logger::get(__func__), "Removing mutation {}", mutation_name);
    metastore_ptr->drop(tableMutationKey(name_space, uuid, mutation_name));
}

Strings MetastoreProxy::getAllMutations(const String & name_space, const String & uuid)
{
    Strings res;
    auto it = metastore_ptr->getByPrefix(tableMutationPrefix(name_space, uuid));
    while(it->next())
        res.push_back(it->value());
    return res;
}

std::pair<String, String> MetastoreProxy::splitFirstKey(const String & key, const String & prefix)
{
    if (!key.starts_with(prefix))
    {
        auto err_msg = fmt::format("unexpected prefix '{}' for key '{}'", prefix, key);
        throw Exception(err_msg, ErrorCodes::METASTORE_EXCEPTION);
    }
    String first_key;
    auto iter = key.begin() + prefix.size();
    for (; iter != key.end(); ++iter)
    {
        if (*iter == '\\')
            ++iter;
        else if (*iter == '_')
        {
            ++iter;
            break;
        }

        if (iter == key.end())
        {
            auto err_msg = fmt::format("unexpected key '{}' with wrong escape", key);
            throw Exception(err_msg, ErrorCodes::METASTORE_EXCEPTION);
        }
        auto ch = *iter;
        first_key.push_back(ch);
    }
    auto postfix = key.substr(iter - key.begin());
    return std::make_pair(first_key, postfix);
}

void MetastoreProxy::createTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data)
{
    metastore_ptr->put(transactionRecordKey(name_space, txn_id), txn_data);
}

void MetastoreProxy::removeTransactionRecord(const String & name_space, const UInt64 & txn_id)
{
    metastore_ptr->drop(transactionRecordKey(name_space, txn_id));
}

void MetastoreProxy::removeTransactionRecords(const String & name_space, const std::vector<TxnTimestamp> & txn_ids) {
    if (txn_ids.empty())
        return;

    BatchCommitRequest batch_write(false);

    for (const auto & txn_id : txn_ids)
        batch_write.AddDelete(transactionRecordKey(name_space, txn_id.toUInt64()));

    metastore_ptr->adaptiveBatchWrite(batch_write);
}

String MetastoreProxy::getTransactionRecord(const String & name_space, const UInt64 & txn_id)
{
    String txn_data;
    metastore_ptr->get(transactionRecordKey(name_space, txn_id), txn_data);
    return txn_data;
}

std::vector<std::pair<String, UInt64>> MetastoreProxy::getTransactionRecords(const String & name_space, const std::vector<TxnTimestamp> & txn_ids)
{
    if (txn_ids.empty())
        return {};

    Strings txn_keys;
    txn_keys.reserve(txn_ids.size());
    for (const auto & txn_id : txn_ids)
        txn_keys.push_back(transactionRecordKey(name_space, txn_id.toUInt64()));

    return metastore_ptr->multiGet(txn_keys);
}

IMetaStore::IteratorPtr
MetastoreProxy::getAllTransactionRecord(const String & name_space, const String & start_key, const size_t & max_result_number)
{
    return metastore_ptr->getByPrefix(
        escapeString(name_space) + "_" + TRANSACTION_RECORD_PREFIX, max_result_number, DEFAULT_SCAN_BATCH_COUNT, start_key);
}

std::pair<bool, String> MetastoreProxy::updateTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data_old, const String & txn_data_new)
{
    return metastore_ptr->putCAS(transactionRecordKey(name_space, txn_id), txn_data_new, txn_data_old, true);
}

bool MetastoreProxy::updateTransactionRecordWithOffsets(const String &name_space, const UInt64 &txn_id,
                                                        const String &txn_data_old, const String &txn_data_new,
                                                        const String & consumer_group,
                                                        const cppkafka::TopicPartitionList & tpl)
{
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;

    batch_write.AddPut(SinglePutRequest(transactionRecordKey(name_space, txn_id), txn_data_new, txn_data_old));
    for (auto & tp : tpl)
        batch_write.AddPut(SinglePutRequest(kafkaOffsetsKey(name_space, consumer_group, tp.get_topic(), tp.get_partition()), std::to_string(tp.get_offset())));

    return metastore_ptr->batchWrite(batch_write, resp);

    /*** offsets committing may don't need CAS now
    Strings keys, old_values, new_values;

    /// Pack txn data
    keys.emplace_back(transactionRecordKey(name_space,txn_id));
    old_values.emplace_back(txn_data_old);
    new_values.emplace_back(txn_data_new);

    /// Pack offsets
    for (size_t i = 0; i < tpl.size(); ++i)
    {
        keys.emplace_back(kafkaOffsetsKey(name_space, consumer_group, tpl[i].get_topic(), tpl[i].get_partition()));
        old_values.emplace_back(std::to_string(undo_tpl[i].get_offset()));
        new_values.emplace_back(std::to_string(tpl[i].get_offset()));
    }

    return metastore_ptr->multiWriteCAS(keys, new_values, old_values);
     **/
}

bool MetastoreProxy::updateTransactionRecordWithBinlog(const String & name_space, const UInt64 & txn_id,
                                                       const String & txn_data_old, const String & txn_data_new,
                                                       const String & binlog_name, const std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> & binlog)
{
    BatchCommitRequest multi_write;
    BatchCommitResponse resp;

    multi_write.AddPut(SinglePutRequest(transactionRecordKey(name_space, txn_id), txn_data_new, txn_data_old));

    String value;
    if (!binlog->SerializeToString(&value))
        throw Exception("Failed to serialize metadata of Binlog to string", ErrorCodes::LOGICAL_ERROR);

    auto binlog_key = materializedMySQLMetadataKey(name_space, binlog_name);
    multi_write.AddPut(SinglePutRequest(binlog_key, value));

    return metastore_ptr->batchWrite(multi_write, resp);
}

std::pair<bool, String> MetastoreProxy::MetastoreProxy::updateTransactionRecordWithRequests(
    SinglePutRequest & txn_request, BatchCommitRequest & requests, BatchCommitResponse & response)
{
    if (requests.puts.empty() && requests.deletes.empty())
    {
        return metastore_ptr->putCAS(
            String(txn_request.key), String(txn_request.value), String(*txn_request.expected_value), true);
    }
    else
    {
        requests.AddPut(txn_request);
        metastore_ptr->batchWrite(requests, response);
        return {response.puts.empty(), String{}};
        /// TODO: set txn_result
    }
}

void MetastoreProxy::setTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data, UInt64 /*ttl*/)
{
    return metastore_ptr->put(transactionRecordKey(name_space, txn_id), txn_data);
}

bool MetastoreProxy::writeIntent(const String & name_space, const String & intent_prefix, const std::vector<WriteIntent> & intents, std::vector<String> & cas_failed_list)
{
    BatchCommitRequest batch_write;
    for (auto  & intent : intents)
    {
        batch_write.AddPut(SinglePutRequest(writeIntentKey(name_space, intent_prefix, intent.intent()), intent.serialize(), true));
    }

    BatchCommitResponse resp;
    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
        return true;
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            for (auto & [index, new_value]: resp.puts)
            {
                cas_failed_list.push_back(new_value);
            }
        }
        else
            throw;
    }
    return false;
}

bool MetastoreProxy::resetIntent(const String & name_space, const String & intent_prefix, const std::vector<WriteIntent> & intents, const UInt64 & new_txn_id, const String & new_location)
{
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;
    for (const auto & intent : intents)
    {
        WriteIntent new_intent(new_txn_id, new_location, intent.intent());
        batch_write.AddPut(SinglePutRequest(writeIntentKey(name_space, intent_prefix, intent.intent()), new_intent.serialize(), intent.serialize()));
    }

    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
        return true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
            return false;
        else
            throw;
    }
}

void MetastoreProxy::clearIntents(const String & name_space, const String & intent_prefix, const std::vector<WriteIntent> & intents)
{
    /// TODO: Because bytekv does not support `CAS delete`, now we firstly get all intent from bytekv and compare with expected value, then
    /// we delete them with version info in one transaction. we may optimize this if bytekv could support `CAS delete` later.

    Strings intent_names;
    for (const auto & intent : intents)
        intent_names.emplace_back(writeIntentKey(name_space, intent_prefix, intent.intent()));

    auto snapshot = metastore_ptr->multiGet(intent_names);

    Poco::Logger * log = &Poco::Logger::get(__func__);
    std::vector<size_t> matched_intent_index;

    for (size_t i = 0; i < intents.size(); i++)
    {
        WriteIntent actual_intent = WriteIntent::deserialize(snapshot[i].first);
        if (intents[i] == actual_intent)
            matched_intent_index.push_back(i);
        else
            LOG_WARNING(
                log, "Clear WriteIntent got CAS_FAILED. expected: " + intents[i].toString() + ", actual: " + actual_intent.toString());
    }

    if (matched_intent_index.empty())
        return;

    /// then remove intents from metastore.
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;

     /// CAS delete is needed becuase the intent could be overwrite by other transactions
    for (auto idx : matched_intent_index)
    {
        batch_write.AddDelete(intent_names[idx], intents[idx].serialize());
    }

    bool cas_success = metastore_ptr->batchWrite(batch_write, resp);

    if (!cas_success && intent_names.size() > 1)
    {
        LOG_WARNING(log, "Clear WriteIntent got CAS_FAILED. This happens because other tasks concurrently reset the WriteIntent.");

        // try clean for each intent
        for (auto idx : matched_intent_index)
        {
            try
            {
                /// ByConity don't have MetastoreByteKVImpl
                //if (dynamic_cast<MetastoreByteKVImpl *>(metastore_ptr.get()))
                //    metastore_ptr->drop(intent_names[idx], snapshot[idx].second);
                //else
                metastore_ptr->drop(intent_names[idx], snapshot[idx].first);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
                    LOG_WARNING(log, "CAS fail when cleaning the intent: " + intent_names[idx]);
                else
                    throw;
            }
        }
    }
}

void MetastoreProxy::clearZombieIntent(const String & name_space, const UInt64 & txn_id)
{
    auto it = metastore_ptr->getByPrefix(escapeString(name_space) + "_" + WRITE_INTENT_PREFIX);
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;
    while(it->next())
    {
        Protos::DataModelWriteIntent intent_model;
        intent_model.ParseFromString(it->value());
        if (intent_model.txn_id() == txn_id)
        {
            batch_write.AddDelete(it->value());
        }
    }

    if (!batch_write.isEmpty())
    {
        metastore_ptr->batchWrite(batch_write, resp);
    }
}

std::multimap<String, String> MetastoreProxy::getAllMutations(const String & name_space)
{
    std::multimap<String, String> res;
    auto it = metastore_ptr->getByPrefix(tableMutationPrefix(name_space));
    while (it->next())
    {
        auto prefix = escapeString(name_space) + "_" + TABLE_MUTATION_PREFIX;
        auto uuid_len = it->key().find('_', prefix.size()) - prefix.size();
        auto uuid = it->key().substr(prefix.size(), uuid_len);
        res.emplace(uuid, it->value());
    }
    return res;
}

void MetastoreProxy::insertTransaction(UInt64 txn)
{
    metastore_ptr->put(transactionKey(txn), "");
}

void MetastoreProxy::removeTransaction(UInt64 txn)
{
    metastore_ptr->drop(transactionKey(txn));
}

IMetaStore::IteratorPtr MetastoreProxy::getActiveTransactions()
{
    return metastore_ptr->getByPrefix(TRANSACTION_STORE_PREFIX);
}

std::unordered_set<UInt64> MetastoreProxy::getActiveTransactionsSet()
{
    IMetaStore::IteratorPtr it = getActiveTransactions();

    std::unordered_set<UInt64> res;

    while(it->next())
    {
        UInt64 txnID = std::stoull(it->key().substr(String(TRANSACTION_STORE_PREFIX).size(), std::string::npos), nullptr);
        res.insert(txnID);
    }

    return res;
}

void MetastoreProxy::writeUndoBuffer(
    const String & name_space,
    const UInt64 & txnID,
    const String & rpc_address,
    const String & uuid,
    UndoResources & resources,
    PlanSegmentInstanceId instance_id,
    bool write_undo_buffer_new_key)
{
    if (resources.empty())
        return;

    BatchCommitRequest batch_write(false);
    BatchCommitResponse resp;
    for (auto & resource : resources)
    {
        resource.setUUID(uuid);
        batch_write.AddPut(
            SinglePutRequest(undoBufferStoreKey(name_space, txnID, rpc_address, resource, instance_id, write_undo_buffer_new_key), resource.serialize()));
    }
    metastore_ptr->batchWrite(batch_write, resp);
}

void MetastoreProxy::clearUndoBuffer(const String & name_space, const UInt64 & txnID)
{
    /// Clean both new and old keys
    metastore_ptr->clean(undoBufferKeyPrefix(name_space, txnID, true));
    metastore_ptr->clean(undoBufferKeyPrefix(name_space, txnID, false));
}

void MetastoreProxy::clearUndoBuffer(
    const String & name_space, const UInt64 & txnID, const String & rpc_address, PlanSegmentInstanceId instance_id)
{
    /// Clean both new and old keys
    metastore_ptr->clean(undoBufferSegmentInstanceKey(name_space, txnID, rpc_address, instance_id, true));
    metastore_ptr->clean(undoBufferSegmentInstanceKey(name_space, txnID, rpc_address, instance_id, false));
}

IMetaStore::IteratorPtr MetastoreProxy::getUndoBuffer(const String & name_space, UInt64 txnID, bool write_undo_buffer_new_key)
{
    return metastore_ptr->getByPrefix(undoBufferKeyPrefix(name_space, txnID, write_undo_buffer_new_key));
}

IMetaStore::IteratorPtr MetastoreProxy::getUndoBuffer(
    const String & name_space, UInt64 txnID, const String & rpc_address, PlanSegmentInstanceId instance_id, bool write_undo_buffer_new_key)
{
    return metastore_ptr->getByPrefix(undoBufferSegmentInstanceKey(name_space, txnID, rpc_address, instance_id, write_undo_buffer_new_key));
}

IMetaStore::IteratorPtr MetastoreProxy::getAllUndoBuffer(const String & name_space)
{
    return metastore_ptr->getByPrefix(escapeString(name_space) + '_' + UNDO_BUFFER_PREFIX);
}

void MetastoreProxy::multiDrop(const Strings & keys)
{
    BatchCommitRequest batch_write(false);
    for (const auto & key : keys)
    {
        batch_write.AddDelete(key);
    }
    metastore_ptr->adaptiveBatchWrite(batch_write);
}

bool MetastoreProxy::batchWrite(const BatchCommitRequest & request, BatchCommitResponse response)
{
    return metastore_ptr->batchWrite(request, response);
}

void MetastoreProxy::writeFilesysLock(const String & name_space, UInt64 txn_id, const String & dir, const String & db, const String & table)
{
    Protos::DataModelFileSystemLock data;
    data.set_txn_id(txn_id);
    data.set_directory(dir);
    data.set_database(db);
    data.set_table(table);

    metastore_ptr->put(filesysLockKey(name_space, dir), data.SerializeAsString(), true);
}

std::vector<String> MetastoreProxy::getFilesysLocks(const String& name_space,
    const std::vector<String>& dirs)
{
    if (dirs.empty())
    {
        return {};
    }

    Strings keys;
    for (const String& dir : dirs)
    {
        keys.push_back(filesysLockKey(name_space, dir));
    }
    auto res = metastore_ptr->multiGet(keys);
    std::vector<String> lock_metas;
    for (const auto& entry : res)
    {
        lock_metas.push_back(entry.first);
    }
    return lock_metas;
}

void MetastoreProxy::clearFilesysLocks(const String& name_space,
    const std::vector<String>& dirs)
{
    if (!dirs.empty())
    {
        Strings keys;
        for (const String& dir : dirs)
        {
            keys.push_back(filesysLockKey(name_space, dir));
        }
        multiDrop(keys);
    }
}

IMetaStore::IteratorPtr MetastoreProxy::getAllFilesysLock(const String & name_space)
{
    return metastore_ptr->getByPrefix(escapeString(name_space) + '_' + FILESYS_LOCK_PREFIX);
}

std::vector<String> MetastoreProxy::multiDropAndCheck(const Strings & keys)
{
    multiDrop(keys);
    /// check if all keys have been deleted, return drop failed keys if any;
    std::vector<String> keys_drop_failed{};
    auto values = metastore_ptr->multiGet(keys);
    for (auto & pair : values)
    {
        if (!pair.first.empty())
        {
            keys_drop_failed.emplace_back(pair.first);
        }
    }
    return keys_drop_failed;
}

IMetaStore::IteratorPtr MetastoreProxy::getPartitionList(const String & name_space, const String & uuid)
{
    return metastore_ptr->getByPrefix(tablePartitionInfoPrefix(name_space, uuid));
}

void MetastoreProxy::updateTopologyMeta(const String & name_space, const String & topology)
{
    BatchCommitRequest batch_write(false);
    batch_write.SetTimeout(3000);
    batch_write.AddPut(SinglePutRequest(escapeString(name_space) + "_" + SERVERS_TOPOLOGY_KEY, topology));
    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

String MetastoreProxy::getTopologyMeta(const String & name_space)
{
    String topology_meta;
    metastore_ptr->get(escapeString(name_space) + "_" + SERVERS_TOPOLOGY_KEY, topology_meta);
    return topology_meta;
}

IMetaStore::IteratorPtr MetastoreProxy::getSyncList(const String & name_space, const String & uuid)
{
    return metastore_ptr->getByPrefix(syncListPrefix(name_space, uuid));
}

void MetastoreProxy::clearSyncList(const String & name_space, const String & uuid, const std::vector<TxnTimestamp> & sync_list)
{
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;
    for (auto & ts : sync_list)
        batch_write.AddDelete(syncListKey(name_space, uuid, ts));

    metastore_ptr->batchWrite(batch_write, resp);
}

void MetastoreProxy::clearOffsetsForWholeTopic(const String &name_space, const String &topic, const String &consumer_group)
{
    auto prefix = escapeString(name_space) + "_" + KAFKA_OFFSETS_PREFIX + escapeString(consumer_group) + "_" + escapeString(topic) + "_";
    metastore_ptr->clean(prefix);
}

cppkafka::TopicPartitionList MetastoreProxy::getKafkaTpl(const String & name_space, const String & consumer_group,
                                                         const String & topic_name)
{
    cppkafka::TopicPartitionList res;

    for (int partition = 0; ; ++partition)
    {
        String key = kafkaOffsetsKey(name_space, consumer_group, topic_name, partition);
        String value;
        metastore_ptr->get(key, value);
        /// If cannot get value, we think it get max-partition which maybe optimized later
        if (value.empty())
            break;
        auto tp = cppkafka::TopicPartition(topic_name, partition, std::stoll(value));
        res.emplace_back(std::move(tp));
    }

    std::sort(res.begin(), res.end());
    return res;
}

/// TODO: performance?
void MetastoreProxy::getKafkaTpl(const String & name_space, const String & consumer_group, cppkafka::TopicPartitionList & tpl)
{
    if (tpl.empty())
        return;

    Strings keys;
    for (auto & tp : tpl)
    {
        auto key = kafkaOffsetsKey(name_space, consumer_group, tp.get_topic(), tp.get_partition());
        keys.emplace_back(std::move(key));
    }

    auto values = metastore_ptr->multiGet(keys);
    if (tpl.size() != values.size())
        throw Exception("Got wrong size of offsets while getting tpl", ErrorCodes::LOGICAL_ERROR);

    size_t idx = 0;
    for (auto & ele : values)
    {
        if (unlikely(ele.first.empty()))
            tpl[idx].set_offset(RD_KAFKA_OFFSET_INVALID);
        else
            tpl[idx].set_offset(std::stoll(ele.first));
        ++idx;
    }
}

void MetastoreProxy::setTransactionForKafkaConsumer(const String & name_space, const String & uuid, const TxnTimestamp & txn_id, const size_t consumer_index)
{
    auto key = kafkaTransactionKey(name_space, uuid, consumer_index);
    metastore_ptr->put(key, std::to_string(txn_id.toUInt64()));
}

TxnTimestamp MetastoreProxy::getTransactionForKafkaConsumer(const String & name_space, const String & uuid, const size_t consumer_index)
{
    auto key = kafkaTransactionKey(name_space, uuid, consumer_index);

    String value;
    metastore_ptr->get(key, value);

    if (value.empty())
        return TxnTimestamp::maxTS();
    else
        return TxnTimestamp(std::stoull(value));
}

void MetastoreProxy::clearKafkaTransactions(const String & name_space, const String & uuid)
{
    auto prefix = escapeString(name_space) + "_" + KAFKA_TRANSACTION_PREFIX + escapeString(uuid);
    metastore_ptr->clean(prefix);
}

void MetastoreProxy::setTableClusterStatus(const String & name_space, const String & uuid, const bool & already_clustered, const UInt64 & table_definition_hash)
{
    // TDH key may not exist in KV either because the table does not exist or there is an upgrade of CNCH version
    String table_definition_hash_meta;
    metastore_ptr->get(tableDefinitionHashKey(name_space, uuid), table_definition_hash_meta);
    String expected_table_definition_hash = toString(table_definition_hash_meta);
    bool if_not_exists = false;
    if (table_definition_hash_meta.empty())
        if_not_exists = true;

    auto table_definition_hash_put_request = SinglePutRequest(tableDefinitionHashKey(name_space, uuid), toString(table_definition_hash), expected_table_definition_hash);
    table_definition_hash_put_request.if_not_exists = if_not_exists;

    BatchCommitRequest batch_write;
    batch_write.AddPut(SinglePutRequest(clusterStatusKey(name_space, uuid), already_clustered ? "true" : "false"));
    batch_write.AddPut(table_definition_hash_put_request);

    BatchCommitResponse resp;
    try
    {
        metastore_ptr->batchWrite(batch_write, resp);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            String error_message;
            if (resp.puts.count(1) && if_not_exists)
                error_message = "table_definition_hash of Table with uuid(" + uuid + ") already exists.";
            else if (resp.puts.count(1))
                error_message = "table_definition_hash of Table with uuid(" + uuid + ") has recently been changed in catalog. Please try the request again.";
            throw Exception(error_message, ErrorCodes::METASTORE_TABLE_TDH_CAS_ERROR);
        }
        else
            throw;
    }
}

void MetastoreProxy::getTableClusterStatus(const String & name_space, const String & uuid, bool & is_clustered)
{
    String meta;
    metastore_ptr->get(clusterStatusKey(name_space, uuid), meta);
    if (meta == "true")
        is_clustered = true;
    else
        is_clustered = false;
}

/// BackgroundJob related API
void MetastoreProxy::setBGJobStatus(const String & name_space, const String & uuid, CnchBGThreadType type, CnchBGThreadStatus status)
{
    if (type == CnchBGThreadType::Clustering)
        metastore_ptr->put(
            clusterBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::MergeMutate)
        metastore_ptr->put(
            mergeBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::PartGC)
        metastore_ptr->put(
            partGCBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::Consumer)
        metastore_ptr->put(
            consumerBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::MaterializedMySQL)
        metastore_ptr->put(
            mmysqlBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::DedupWorker)
        metastore_ptr->put(
            dedupWorkerBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::ObjectSchemaAssemble)
        metastore_ptr->put(
            objectSchemaAssembleBGJobStatusKey(name_space, uuid),
            String {BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::CnchRefreshMaterializedView)
        metastore_ptr->put(
            refreshViewBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)});
    else if (type == CnchBGThreadType::PartMover)
        metastore_ptr->put(
            partMoverBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else if (type == CnchBGThreadType::ManifestCheckpoint)
        metastore_ptr->put(
            checkpointBGJobStatusKey(name_space, uuid),
            String{BGJobStatusInCatalog::serializeToChar(status)}
        );
    else
        throw Exception(String{"persistent status is not support for "} + toString(type), ErrorCodes::LOGICAL_ERROR);
}

std::optional<CnchBGThreadStatus> MetastoreProxy::getBGJobStatus(const String & name_space, const String & uuid, CnchBGThreadType type)
{
    String status_store_data;
    if (type == CnchBGThreadType::Clustering)
        metastore_ptr->get(clusterBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::MergeMutate)
        metastore_ptr->get(mergeBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::PartGC)
        metastore_ptr->get(partGCBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::Consumer)
        metastore_ptr->get(consumerBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::MaterializedMySQL)
        metastore_ptr->get(mmysqlBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::DedupWorker)
        metastore_ptr->get(dedupWorkerBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::ObjectSchemaAssemble)
        metastore_ptr->get(objectSchemaAssembleBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::CnchRefreshMaterializedView)
        metastore_ptr->get(refreshViewBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::PartMover)
        metastore_ptr->get(partMoverBGJobStatusKey(name_space, uuid), status_store_data);
    else if (type == CnchBGThreadType::ManifestCheckpoint)
        metastore_ptr->get(checkpointBGJobStatusKey(name_space, uuid), status_store_data);
    else
        throw Exception(String{"persistent status is not support for "} + toString(type), ErrorCodes::LOGICAL_ERROR);

    if (status_store_data.empty())
        return {};

    return BGJobStatusInCatalog::deserializeFromString(status_store_data);
}

UUID MetastoreProxy::parseUUIDFromBGJobStatusKey(const std::string & key)
{
    auto pos = key.rfind("_");
    if (pos == std::string::npos || pos == (key.size() -1))
        throw Exception("invalid BGJobStatusKey", ErrorCodes::LOGICAL_ERROR);
    std::string uuid = key.substr(pos + 1);
    return UUIDHelpers::toUUID(uuid);
}

std::unordered_map<UUID, CnchBGThreadStatus> MetastoreProxy::getBGJobStatuses(const String & name_space, CnchBGThreadType type)
{
    auto get_iter_lambda = [&] ()
        {
            if (type == CnchBGThreadType::Clustering)
                return metastore_ptr->getByPrefix(allClusterBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::MergeMutate)
                return metastore_ptr->getByPrefix(allMergeBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::PartGC)
                return metastore_ptr->getByPrefix(allPartGCBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::Consumer)
                return metastore_ptr->getByPrefix(allConsumerBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::MaterializedMySQL)
                return metastore_ptr->getByPrefix(allMmysqlBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::DedupWorker)
                return metastore_ptr->getByPrefix(allDedupWorkerBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::ObjectSchemaAssemble)
                return metastore_ptr->getByPrefix(allObjectSchemaAssembleBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::CnchRefreshMaterializedView)
                return metastore_ptr->getByPrefix(allRefreshViewJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::PartMover)
                return metastore_ptr->getByPrefix(allPartMoverBGJobStatusKeyPrefix(name_space));
            else if (type == CnchBGThreadType::ManifestCheckpoint)
                return metastore_ptr->getByPrefix(allCheckpointBGJobStatusKeyPrefix(name_space));
            else
                throw Exception(String{"persistent status is not support for "} + toString(type), ErrorCodes::LOGICAL_ERROR);
        };

    std::unordered_map<UUID, CnchBGThreadStatus> res;
    IMetaStore::IteratorPtr it = get_iter_lambda();
    while (it->next())
    {
        res.insert(
            std::make_pair(
                parseUUIDFromBGJobStatusKey(it->key()),
                BGJobStatusInCatalog::deserializeFromString(it->value())
                )
        );
    }

    return res;
}

void MetastoreProxy::dropBGJobStatus(const String & name_space, const String & uuid, CnchBGThreadType type)
{
    switch (type)
    {
        case CnchBGThreadType::Clustering:
            metastore_ptr->drop(clusterBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::MergeMutate:
            metastore_ptr->drop(mergeBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::PartGC:
            metastore_ptr->drop(partGCBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::Consumer:
            metastore_ptr->drop(consumerBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::MaterializedMySQL:
            metastore_ptr->drop(mmysqlBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::DedupWorker:
            metastore_ptr->drop(dedupWorkerBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::ObjectSchemaAssemble:
            metastore_ptr->drop(objectSchemaAssembleBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::CnchRefreshMaterializedView:
            metastore_ptr->drop(dedupWorkerBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::PartMover:
            metastore_ptr->drop(partMoverBGJobStatusKey(name_space, uuid));
            break;
        case CnchBGThreadType::ManifestCheckpoint:
            metastore_ptr->drop(checkpointBGJobStatusKey(name_space, uuid));
            break;
        default:
            throw Exception(String{"persistent status is not support for "} + toString(type), ErrorCodes::LOGICAL_ERROR);
    }
}

void MetastoreProxy::setTablePreallocateVW(const String & name_space, const String & uuid, const String & vw)
{
    metastore_ptr->put(preallocateVW(name_space, uuid), vw);
}

void MetastoreProxy::getTablePreallocateVW(const String & name_space, const String & uuid, String & vw)
{
    metastore_ptr->get(preallocateVW(name_space, uuid), vw);
}

IMetaStore::IteratorPtr MetastoreProxy::getMetaInRange(const String & prefix, const String & range_start, const String & range_end, bool include_start, bool include_end)
{
    return metastore_ptr->getByRange(prefix + range_start, prefix + range_end, include_start, include_end);
}

void MetastoreProxy::prepareAddDeleteBitmaps(
    const String & name_space,
    const String & table_uuid,
    const DeleteBitmapMetaPtrVector & bitmaps,
    BatchCommitRequest & batch_write,
    const UInt64 txn_id, 
    const std::vector<String> & expected_bitmaps,
    bool write_manifest)
{
    size_t expected_bitmaps_size = expected_bitmaps.size();
    if (expected_bitmaps_size > 0 && expected_bitmaps_size != static_cast<size_t>(bitmaps.size()))
        throw Exception("The size of deleted bitmaps wants to write does not match the actual size in catalog", ErrorCodes::LOGICAL_ERROR);

    size_t idx {0};
    for (const auto & dlb_ptr : bitmaps)
    {
        const Protos::DataModelDeleteBitmap & model = *(dlb_ptr->getModel());
        String serialized_data = model.SerializeAsString();
        if (expected_bitmaps_size == 0)
            batch_write.AddPut(SinglePutRequest(deleteBitmapKey(name_space, table_uuid, model), serialized_data));
        else
            batch_write.AddPut(SinglePutRequest(deleteBitmapKey(name_space, table_uuid, model), serialized_data, expected_bitmaps[idx]));
        
        if (write_manifest)
            batch_write.AddPut(SinglePutRequest(manifestKeyForDeleteBitmap(name_space, table_uuid, txn_id, dataModelName(model)), serialized_data));
        ++idx;
    }
}

Strings MetastoreProxy::getDeleteBitmapByKeys(const Strings & keys)
{
    Strings parts_meta;
    auto values = metastore_ptr->multiGet(keys);
    for (auto & ele : values)
        parts_meta.emplace_back(std::move(ele.first));
    return parts_meta;
}

void MetastoreProxy::precommitInsertionLabel(const String & name_space, const InsertionLabelPtr & label)
{
    auto label_key = insertionLabelKey(name_space, toString(label->table_uuid), label->name);
    /// TODO: catch exception here
    metastore_ptr->put(label_key, label->serializeValue(), true /* if_not_exists */);
}

void MetastoreProxy::commitInsertionLabel(const String & name_space, InsertionLabelPtr & label)
{
    auto label_key = insertionLabelKey(name_space, toString(label->table_uuid), label->name);

    String old_value = label->serializeValue();
    label->status = InsertionLabel::Committed;
    String new_value = label->serializeValue();

    try
    {
        metastore_ptr->putCAS(label_key, new_value, old_value);
    }
    catch (...)
    {
        /// TODO: handle exception here
        label->status = InsertionLabel::Precommitted;
        throw;
    }
}

std::pair<uint64_t, String> MetastoreProxy::getInsertionLabel(const String & name_space, const String & uuid, const String & name)
{
    String value;
    auto label_key = insertionLabelKey(name_space, uuid, name);
    auto version = metastore_ptr->get(label_key, value);
    // the version return here is not correct, not suppported
    return {version, value};
}

void MetastoreProxy::removeInsertionLabel(const String & name_space, const String & uuid, const String & name, [[maybe_unused]] uint64_t expected_version)
{
    auto label_key = insertionLabelKey(name_space, uuid, name);
    // the version is not supported
    metastore_ptr->drop(label_key);
}

void MetastoreProxy::removeInsertionLabels(const String & name_space, const std::vector<InsertionLabel> & labels)
{
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;
    for (auto & label : labels)
        batch_write.AddDelete(insertionLabelKey(name_space, toString(label.table_uuid), label.name));
    metastore_ptr->batchWrite(batch_write, resp);
}

IMetaStore::IteratorPtr MetastoreProxy::scanInsertionLabels(const String & name_space, const String & uuid)
{
    auto label_key_prefix = insertionLabelKey(name_space, uuid, {});
    return metastore_ptr->getByPrefix(label_key_prefix);
}

void MetastoreProxy::clearInsertionLabels(const String & name_space, const String & uuid)
{
    auto label_key = insertionLabelKey(name_space, uuid, {});
    metastore_ptr->clean(label_key);
}

void MetastoreProxy::updateTableStatistics(
    const String & name_space, const String & uuid, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data)
{

    BatchCommitRequest batch_write;
    for (const auto & [tag, statisticPtr] : data)
    {
        auto key = tableStatisticKey(name_space, uuid, tag);
        auto tag_key = tableStatisticTagKey(name_space, uuid, tag);
        if (!statisticPtr)
        {
            batch_write.AddDelete(key);
            batch_write.AddDelete(tag_key);
            continue;
        }
        Protos::TableStatistic table_statistic;
        table_statistic.set_tag(static_cast<UInt64>(tag));
        table_statistic.set_timestamp(0); // currently this is deprecated
        table_statistic.set_blob(statisticPtr->serialize());
        batch_write.AddPut(SinglePutRequest(key, table_statistic.SerializeAsString()));
        // TODO: deprecate tag
        batch_write.AddPut(SinglePutRequest(tag_key, std::to_string(static_cast<UInt64>(tag))));
    }

    if (!batch_write.isEmpty())
    {
        BatchCommitResponse resp;
        metastore_ptr->batchWrite(batch_write, resp);
    }
}

void MetastoreProxy::removeTableStatistics(const String & name_space, const String & uuid)
{
    BatchCommitRequest batch_write;
    {
        auto prefix = tableStatisticPrefix(name_space, uuid);
        auto iter = metastore_ptr->getByPrefix(prefix);
        while (iter->next())
        {
            batch_write.AddDelete(iter->key());
        }
    }
    {
        // explicit tag store will be deprecated in the future.
        // only remove logic will be maintained
        auto prefix = tableStatisticTagPrefix(name_space, uuid);
        auto iter = metastore_ptr->getByPrefix(prefix);
        while (iter->next())
        {
            batch_write.AddDelete(iter->key());
        }
    }

    if (!batch_write.isEmpty())
    {
        BatchCommitResponse resp;
        metastore_ptr->batchWrite(batch_write, resp);
    }
}


std::unordered_map<StatisticsTag, StatisticsBasePtr> MetastoreProxy::getTableStatistics(const String & name_space, const String & uuid)
{
    // new version won't use tag related function
    auto prefix = tableStatisticPrefix(name_space, uuid);
    auto iter = metastore_ptr->getByPrefix(prefix);
    std::unordered_map<StatisticsTag, StatisticsBasePtr> result;
    while (iter->next())
    {
        Protos::TableStatistic proto;
        proto.ParseFromString(iter->value());

        auto tag = static_cast<Statistics::StatisticsTag>(proto.tag());
        auto obj_ptr = createStatisticsBase(tag, proto.blob());
        result.emplace(tag, std::move(obj_ptr));
    }
    return result;
}

void MetastoreProxy::updateColumnStatistics(const String & name_space, const String & uuid, const String & column, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data)
{
    BatchCommitRequest batch_write;
    for (const auto & [tag, statisticPtr] : data)
    {
        Protos::ColumnStatistic column_statistic;
        column_statistic.set_tag(static_cast<UInt64>(tag));
        column_statistic.set_timestamp(0);
        column_statistic.set_column(column);
        column_statistic.set_blob(statisticPtr->serialize());
        batch_write.AddPut(SinglePutRequest(columnStatisticKey(name_space, uuid, column, tag), column_statistic.SerializeAsString()));
        // TODO: in future should deprecate it.
        // TODO: currently keep it just for front&back-compatibility
        batch_write.AddPut(SinglePutRequest(columnStatisticTagKey(name_space, uuid, column, tag), std::to_string(static_cast<UInt64>(tag))));
    }
    if (!batch_write.isEmpty())
    {
        BatchCommitResponse resp;
        metastore_ptr->batchWrite(batch_write, resp);
    }
}


std::unordered_map<StatisticsTag, StatisticsBasePtr>
MetastoreProxy::getColumnStatistics(const String & name_space, const String & uuid, const String & column)
{
    auto prefix = columnStatisticPrefix(name_space, uuid, column);
    auto iter = metastore_ptr->getByPrefix(prefix);
    std::unordered_map<StatisticsTag, StatisticsBasePtr> res;
    while (iter->next())
    {
        if (iter->value().empty())
            continue;
        Protos::ColumnStatistic column_statistic;
        column_statistic.ParseFromString(iter->value());
        StatisticsTag tag = static_cast<StatisticsTag>(column_statistic.tag());
        TxnTimestamp ts(column_statistic.timestamp());
        auto statisticPtr = createStatisticsBase(tag, column_statistic.blob());
        if (statisticPtr)
            res.emplace(tag, statisticPtr);
    }
    return res;
}


std::unordered_map<String, std::unordered_map<StatisticsTag, StatisticsBasePtr>>
MetastoreProxy::getAllColumnStatistics(const String & name_space, const String & uuid)
{
    auto prefix = columnStatisticPrefixWithoutColumn(name_space, uuid);
    auto iter = metastore_ptr->getByPrefix(prefix);
    std::unordered_map<String, std::unordered_map<StatisticsTag, StatisticsBasePtr>> res;
    while (iter->next())
    {
        if (iter->value().empty())
            continue;
        auto [column, postfix] = splitFirstKey(iter->key(), prefix);
        Protos::ColumnStatistic column_statistic;
        column_statistic.ParseFromString(iter->value());
        StatisticsTag tag = static_cast<StatisticsTag>(column_statistic.tag());
        auto statisticPtr = createStatisticsBase(tag, column_statistic.blob());
        res[column].emplace(tag, statisticPtr);
    }
    return res;
}


std::vector<String> MetastoreProxy::getAllColumnStatisticsKey(const String & name_space, const String & uuid)
{
    auto prefix = columnStatisticPrefixWithoutColumn(name_space, uuid);
    auto iter = metastore_ptr->getByPrefix(prefix);
    std::unordered_set<String> res;
    while (iter->next())
    {
        if (iter->value().empty())
            continue;
        auto [column, postfix] = splitFirstKey(iter->key(), prefix);
        if (column.empty())
            continue;
        res.emplace(column);
    }
    return std::vector(res.begin(), res.end());
}

void MetastoreProxy::removeColumnStatistics(const String & name_space, const String & uuid, const String & column)
{
    BatchCommitRequest batch_write;
    {
        auto prefix = columnStatisticPrefix(name_space, uuid, column);
        auto iter = metastore_ptr->getByPrefix(prefix);
        while (iter->next())
        {
            batch_write.AddDelete(iter->key());
        }
    }
    {
        // TODO: explicit tag store will be deprecated in the future.
        auto prefix = columnStatisticTagPrefix(name_space, uuid, column);
        auto iter = metastore_ptr->getByPrefix(prefix);
        while (iter->next())
        {
            batch_write.AddDelete(iter->key());
        }
    }

    if (!batch_write.isEmpty())
    {
        BatchCommitResponse resp;
        metastore_ptr->batchWrite(batch_write, resp);
    }
}

void MetastoreProxy::removeAllColumnStatistics(const String & name_space, const String & uuid)
{
    BatchCommitRequest batch_write;
    {
        auto prefix = columnStatisticPrefixWithoutColumn(name_space, uuid);
        auto iter = metastore_ptr->getByPrefix(prefix);
        while (iter->next())
        {
            batch_write.AddDelete(iter->key());
        }
    }
    {
        // TODO: explicit tag store will be deprecated in the future.
        auto prefix = columnStatisticTagPrefixWithoutColumn(name_space, uuid);
        auto iter = metastore_ptr->getByPrefix(prefix);
        while (iter->next())
        {
            batch_write.AddDelete(iter->key());
        }
    }
    if (!batch_write.isEmpty())
    {
        BatchCommitResponse resp;
        metastore_ptr->batchWrite(batch_write, resp);
    }
}

void MetastoreProxy::updateSQLBinding(const String & name_space, const SQLBindingItemPtr & data)
{
    BatchCommitRequest batch_write;

    Protos::SQLBinding sql_binding;
    sql_binding.set_is_regular_expression(data->is_regular_expression);
    RPCHelpers::fillUUID(data->uuid, *sql_binding.mutable_uuid());
    sql_binding.set_pattern(data->pattern);
    sql_binding.set_serialized_ast(data->serialized_ast);
    sql_binding.set_timestamp(data->timestamp);
    sql_binding.set_tenant_id(data->tenant_id);
    batch_write.AddPut(SinglePutRequest(SQLBindingKey(name_space, UUIDHelpers::UUIDToString(data->uuid), data->tenant_id, data->is_regular_expression), sql_binding.SerializeAsString()));
    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

SQLBindings MetastoreProxy::getSQLBindings(const String & name_space)
{
    SQLBindings res;
    auto binding_prefix = SQLBindingPrefix(name_space);
    auto it = metastore_ptr->getByPrefix(binding_prefix);
    while (it->next())
    {
        Protos::SQLBinding sql_binding;
        sql_binding.ParseFromString(it->value());
        SQLBindingItemPtr binding = std::make_shared<SQLBindingItem>(RPCHelpers::createUUID(sql_binding.uuid()), sql_binding.pattern(), sql_binding.serialized_ast(), sql_binding.is_regular_expression(), sql_binding.timestamp(), sql_binding.tenant_id());
        res.emplace_back(binding);
    }

    return res;
}

SQLBindings MetastoreProxy::getReSQLBindings(const String & name_space, const bool & is_re_expression)
{
    SQLBindings res;
    auto binding_prefix = SQLBindingRePrefix(name_space, is_re_expression);
    auto it = metastore_ptr->getByPrefix(binding_prefix);
    while (it->next())
    {
        Protos::SQLBinding sql_binding;
        sql_binding.ParseFromString(it->value());
        SQLBindingItemPtr binding = std::make_shared<SQLBindingItem>(RPCHelpers::createUUID(sql_binding.uuid()), sql_binding.pattern(), sql_binding.serialized_ast(), sql_binding.is_regular_expression(), sql_binding.timestamp(), sql_binding.tenant_id());
        res.emplace_back(binding);
    }

    return res;
}

SQLBindingItemPtr MetastoreProxy::getSQLBinding(const String & name_space, const String & uuid, const String & tenant_id, const bool & is_re_expression)
{
    String value;
    auto binding_key = SQLBindingKey(name_space, uuid, tenant_id, is_re_expression);
    metastore_ptr->get(binding_key, value);

    if (value.empty())
        return nullptr;

    Protos::SQLBinding sql_binding;
    sql_binding.ParseFromString(value);
    SQLBindingItemPtr binding = std::make_shared<SQLBindingItem>(RPCHelpers::createUUID(sql_binding.uuid()), sql_binding.pattern(), sql_binding.serialized_ast(), sql_binding.is_regular_expression(), sql_binding.timestamp(), sql_binding.tenant_id());
    return binding;
}

void MetastoreProxy::removeSQLBinding(const String & name_space, const String & uuid, const String & tenant_id, const bool & is_re_expression)
{
    BatchCommitRequest batch_write;
    batch_write.AddDelete(SQLBindingKey(name_space, uuid, tenant_id, is_re_expression));
    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

void MetastoreProxy::updatePreparedStatement(const String & name_space, const PreparedStatementItemPtr & data)
{
    BatchCommitRequest batch_write;

    Protos::PreparedStatementItem prepared_statement;
    prepared_statement.set_name(data->name);
    prepared_statement.set_create_statement(data->create_statement);
    batch_write.AddPut(SinglePutRequest(preparedStatementKey(name_space, data->name), prepared_statement.SerializeAsString()));
    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

PreparedStatements MetastoreProxy::getPreparedStatements(const String & name_space)
{
    PreparedStatements res;
    auto prepared_prefix = preparedStatementPrefix(name_space);
    auto it = metastore_ptr->getByPrefix(prepared_prefix);
    while (it->next())
    {
        Protos::PreparedStatementItem prepared_statement;
        prepared_statement.ParseFromString(it->value());
        PreparedStatementItemPtr statement = std::make_shared<PreparedStatementItem>(prepared_statement.name(), prepared_statement.create_statement());
        res.emplace_back(statement);
    }

    return res;
}
PreparedStatementItemPtr MetastoreProxy::getPreparedStatement(const String & name_space, const String & name)
{
    String value;
    auto prepared_statement_key = preparedStatementKey(name_space, name);
    metastore_ptr->get(prepared_statement_key, value);

    if (value.empty())
        return nullptr;

    Protos::PreparedStatementItem prepared_statement;
    prepared_statement.ParseFromString(value);
    PreparedStatementItemPtr prepared = std::make_shared<PreparedStatementItem>(prepared_statement.name(), prepared_statement.create_statement());
    return prepared;
}

void MetastoreProxy::removePreparedStatement(const String & name_space, const String & name)
{
    BatchCommitRequest batch_write;
    batch_write.AddDelete(preparedStatementKey(name_space, name));
    BatchCommitResponse resp;
    metastore_ptr->batchWrite(batch_write, resp);
}

void MetastoreProxy::createVirtualWarehouse(const String & name_space, const String & vw_name, const VirtualWarehouseData & data)
{
    auto vw_key = VWKey(name_space, vw_name);
    metastore_ptr->put(vw_key, data.serializeAsString(), /*if_not_exists*/ true);
}

void MetastoreProxy::alterVirtualWarehouse(const String & name_space, const String & vw_name, const VirtualWarehouseData & data)
{
    auto vw_key = VWKey(name_space, vw_name);
    metastore_ptr->put(vw_key, data.serializeAsString());
}

bool MetastoreProxy::tryGetVirtualWarehouse(const String & name_space, const String & vw_name, VirtualWarehouseData & data)
{
    auto vw_key = VWKey(name_space, vw_name);
    String value;
    metastore_ptr->get(vw_key, value);

    if (value.empty())
        return false;
    data.parseFromString(value);
    return true;
}

std::vector<VirtualWarehouseData> MetastoreProxy::scanVirtualWarehouses(const String & name_space)
{
    std::vector<VirtualWarehouseData> res;

    auto vw_prefix = VWKey(name_space, String{});
    auto it = metastore_ptr->getByPrefix(vw_prefix);
    while (it->next())
    {
        res.emplace_back();
        res.back().parseFromString(it->value());
    }
    return res;
}

void MetastoreProxy::dropVirtualWarehouse(const String & name_space, const String & vw_name)
{
    VirtualWarehouseData vw_data;
    if (!tryGetVirtualWarehouse(name_space, vw_name, vw_data))
        return;

    auto vw_key = VWKey(name_space, vw_name);
    metastore_ptr->drop(vw_key);
}

void MetastoreProxy::createWorkerGroup(const String & name_space, const String & worker_group_id, const WorkerGroupData & data)
{
    auto worker_group_key = WorkerGroupKey(name_space, worker_group_id);
    metastore_ptr->put(worker_group_key, data.serializeAsString());
}

void MetastoreProxy::updateWorkerGroup(const String & name_space, const String & worker_group_id, const WorkerGroupData & data)
{
    auto worker_group_key = WorkerGroupKey(name_space, worker_group_id);
    metastore_ptr->put(worker_group_key, data.serializeAsString());
}

bool MetastoreProxy::tryGetWorkerGroup(const String & name_space, const String & worker_group_id, WorkerGroupData & data)
{
    auto worker_group_key = WorkerGroupKey(name_space, worker_group_id);
    String value;
    metastore_ptr->get(worker_group_key, value);

    if (value.empty())
        return false;
    data.parseFromString(value);
    return true;
}

std::vector<WorkerGroupData> MetastoreProxy::scanWorkerGroups(const String & name_space)
{
    std::vector<WorkerGroupData> res;

    auto worker_group_prefix = WorkerGroupKey(name_space, String{});
    auto it = metastore_ptr->getByPrefix(worker_group_prefix);
    while (it->next())
    {
        res.emplace_back();
        res.back().parseFromString(it->value());
    }

    return res;
}

void MetastoreProxy::dropWorkerGroup(const String & name_space, const String & worker_group_id)
{
    auto worker_group_key = WorkerGroupKey(name_space, worker_group_id);
    metastore_ptr->drop(worker_group_key);
}

void MetastoreProxy::setMergeMutateThreadStartTime(const String & name_space, const String & uuid, const UInt64 & start_time)
{
    metastore_ptr->put(mergeMutateThreadStartTimeKey(name_space, uuid), toString(start_time));
}

UInt64 MetastoreProxy::getMergeMutateThreadStartTime(const String & name_space, const String & uuid)
{
    String meta_str;
    metastore_ptr->get(mergeMutateThreadStartTimeKey(name_space, uuid), meta_str);
    if (meta_str.empty())
        return 0;
    else
        return std::stoull(meta_str);
}

std::shared_ptr<Protos::MaterializedMySQLManagerMetadata> MetastoreProxy::tryGetMaterializedMySQLManagerMetadata(const String & name_space, const UUID & uuid)
{
    String value;
    auto manager_key = materializedMySQLMetadataKey(name_space, getNameForMaterializedMySQLManager(uuid));
    metastore_ptr->get(manager_key, value);
    if (value.empty())
        return nullptr;

    auto res = std::make_shared<Protos::MaterializedMySQLManagerMetadata>();
    if (!res->ParseFromString(value))
        throw Exception("Failed to parse metadata of MaterializedMySQL manager", ErrorCodes::LOGICAL_ERROR);
    return res;
}

void MetastoreProxy::setMaterializedMySQLManagerMetadata(const String & name_space, const UUID & uuid, const Protos::MaterializedMySQLManagerMetadata & metadata)
{
    String value;
    if (!metadata.SerializeToString(&value))
        throw Exception("Failed to serialize metadata of MaterializedMySQL manager to string", ErrorCodes::LOGICAL_ERROR);

    auto manager_key = materializedMySQLMetadataKey(name_space, getNameForMaterializedMySQLManager(uuid));
    metastore_ptr->put(manager_key, value);
}

void MetastoreProxy::removeMaterializedMySQLManagerMetadata(const String & name_space, const UUID & uuid)
{
    auto manager_key = materializedMySQLMetadataKey(name_space, getNameForMaterializedMySQLManager(uuid));
    metastore_ptr->drop(manager_key);
}

std::shared_ptr<Protos::MaterializedMySQLBinlogMetadata> MetastoreProxy::getMaterializedMySQLBinlogMetadata(const String & name_space, const String & binlog_name)
{
    String value;
    auto log_key = materializedMySQLMetadataKey(name_space, binlog_name);
    metastore_ptr->get(log_key, value);
    if (value.empty())
        return nullptr;

    auto res = std::make_shared<Protos::MaterializedMySQLBinlogMetadata>();
    res->ParseFromString(value);
    return res;
}

void MetastoreProxy::setMaterializedMySQLBinlogMetadata(const String & name_space, const String & binlog_name, const Protos::MaterializedMySQLBinlogMetadata & metadata)
{
    String value;
    if (!metadata.SerializeToString(&value))
        throw Exception("Failed to serialize metadata of Binlog to string", ErrorCodes::LOGICAL_ERROR);

    auto manager_key = materializedMySQLMetadataKey(name_space, binlog_name);
    metastore_ptr->put(manager_key, value);
}

void MetastoreProxy::removeMaterializedMySQLBinlogMetadata(const String & name_space, const String & binlog_name)
{
    auto manager_key = materializedMySQLMetadataKey(name_space, binlog_name);
    metastore_ptr->drop(manager_key);
}

void MetastoreProxy::updateMaterializedMySQLMetadataInBatch(const String & name_space, const Strings &names, const Strings &values, const Strings &delete_names)
{
    if (names.size() != values.size())
        throw Exception("Unmatched keys and values while updating metadata in batch", ErrorCodes::BAD_ARGUMENTS);

    BatchCommitRequest multi_writer;
    BatchCommitResponse resp;

    for (size_t i = 0; i < names.size(); ++i)
        multi_writer.AddPut(SinglePutRequest(materializedMySQLMetadataKey(name_space, names[i]), values[i]));

    for (const auto & delete_key : delete_names)
        multi_writer.AddDelete(materializedMySQLMetadataKey(name_space, delete_key));

    metastore_ptr->batchWrite(multi_writer, resp);
}

// TODO(WangTao): For bytekv we use TTL to expire the async query status, while for other metastore we need a background job to clean the expired status.
void MetastoreProxy::setAsyncQueryStatus(
    const String & name_space, const String & id, const Protos::AsyncQueryStatus & status, UInt64) const
{
    metastore_ptr->put(asyncQueryStatusKey(name_space, id), status.SerializeAsString());
}

void MetastoreProxy::markBatchAsyncQueryStatusFailed(
    const String & name_space, std::vector<Protos::AsyncQueryStatus> & statuses, const String & reason, UInt64) const
{
    for (auto & status : statuses)
    {
        status.set_status(Protos::AsyncQueryStatus::Failed);
        status.set_error_msg(reason);
        status.set_update_time(time(nullptr));
        setAsyncQueryStatus(name_space, status.id(), status);
    }
}

bool MetastoreProxy::tryGetAsyncQueryStatus(const String & name_space, const String & id, Protos::AsyncQueryStatus & status) const
{
    String value;
    metastore_ptr->get(asyncQueryStatusKey(name_space, id), value);
    if (value.empty())
        metastore_ptr->get(finalAsyncQueryStatusKey(name_space, id), value);
    if (value.empty())
        return false;
    status.ParseFromString(value);
    return true;
}

std::vector<Protos::AsyncQueryStatus> MetastoreProxy::getIntermidiateAsyncQueryStatuses(const String & name_space) const
{
    std::vector<Protos::AsyncQueryStatus> res;

    auto status_prefix = asyncQueryStatusKey(name_space, String{});
    auto it = metastore_ptr->getByPrefix(status_prefix);
    while (it->next())
    {
        res.emplace_back();
        res.back().ParseFromString(it->value());
    }
    return res;
}

class MetastoreMultiWriteInBatch
{
public:
    MetastoreMultiWriteInBatch(MetastoreProxy::MetastorePtr & metastore_, size_t max_batch_size_)
        : metastore(metastore_), max_batch_size(max_batch_size_), current_batch_size(0), batch_commit_request()
    {
    }

    void addDelete(const String & key)
    {
        batch_commit_request.AddDelete(key);

        ++current_batch_size;
        flushIfNecessary();
    }

    void addPut(const String& key, const String& value)
    {

        SinglePutRequest put_request(key, value);
        batch_commit_request.AddPut(put_request);

        ++current_batch_size;
        flushIfNecessary();
    }

    void finalize()
    {
        BatchCommitResponse batch_commit_response;
        if (current_batch_size != 0)
        {
            if (!metastore->batchWrite(batch_commit_request, batch_commit_response))
            {
                throw Exception(
                    fmt::format(
                        "Batch Writer batchWrite fail with {} puts, {} deletes.",
                        batch_commit_response.puts.size(),
                        batch_commit_response.deletes.size()),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

private:
    void flushIfNecessary()
    {
        BatchCommitResponse batch_commit_response;
        if (current_batch_size >= max_batch_size)
        {
            current_batch_size = 0;
            if (!metastore->batchWrite(batch_commit_request, batch_commit_response))
            {
                throw Exception(
                    fmt::format(
                        "Batch Writer batchWrite fail with {} puts, {} deletes.",
                        batch_commit_response.puts.size(),
                        batch_commit_response.deletes.size()),
                    ErrorCodes::LOGICAL_ERROR);
            }

            batch_commit_request = BatchCommitRequest();
        }
    }

    MetastoreProxy::MetastorePtr metastore;
    const size_t max_batch_size;

    size_t current_batch_size;
    BatchCommitRequest batch_commit_request;
};

void MetastoreProxy::attachDetachedParts(
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
    size_t batch_delete_size)
{
    if (detached_part_names.size() != static_cast<size_t>(parts.parts_size() + staged_parts.parts_size()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Detached part names' size {} didn't match (parts meta size {} plus staged_parts meta size {})",
            detached_part_names.size(),
            parts.parts_size(),
            staged_parts.parts_size());
    }
    if (detached_part_names.empty())
    {
        return;
    }

    // Write active part meta and corresponding partition list
    {
        std::unordered_set<String> existing_partitions{current_partitions.begin(), current_partitions.end()};
        std::map<String, String> partition_map;
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        // Write partition list
        for (size_t idx = 0, parts_size = parts.parts_size(); idx < parts_size; ++idx)
        {
            auto info_ptr = createPartInfoFromModel(parts.parts(idx).part_info());
            String part_key = dataPartKey(name_space, to_uuid, info_ptr->getPartName());
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[attachDetachedParts] Write part record {}", part_key);

            if (!existing_partitions.contains(info_ptr->partition_id) && !partition_map.contains(info_ptr->partition_id))
            {
                partition_map.emplace(info_ptr->partition_id, parts.parts(idx).partition_minmax());
            }

            batch_writer.addPut(part_key, parts.parts(idx).SerializeAsString());
        }
        for (size_t idx = 0, parts_size = staged_parts.parts_size(); idx < parts_size; ++idx)
        {
            auto info_ptr = createPartInfoFromModel(staged_parts.parts(idx).part_info());
            String staged_part_key = stagedDataPartKey(name_space, to_uuid, info_ptr->getPartName());
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[attachDetachedStagedParts] Write part record {}", staged_part_key);

            if (!existing_partitions.contains(info_ptr->partition_id) && !partition_map.contains(info_ptr->partition_id))
            {
                partition_map.emplace(info_ptr->partition_id, staged_parts.parts(idx).partition_minmax());
            }

            batch_writer.addPut(staged_part_key, staged_parts.parts(idx).SerializeAsString());
        }
        Protos::PartitionMeta partition_model;
        for (const auto& [partition_id, partition_minmax] : partition_map)
        {
            String partition_key = tablePartitionInfoKey(name_space, to_uuid, partition_id);
            partition_model.set_id(partition_id);
            partition_model.set_partition_minmax(partition_minmax);

            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[attachDetachedParts] Write partition record {}",
                partition_key);

            batch_writer.addPut(partition_key, partition_model.SerializeAsString());
        }
        batch_writer.finalize();
    }

    // Delete detached part meta, since multi write is not atomic,
    // it may delete detached part meta but failed to write part meta
    // and lead to meta lost
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (size_t idx = 0; idx < detached_part_names.size(); ++idx)
        {
            if (!detached_part_names[idx].empty())
            {
                String detached_part_key = detachedPartKey(name_space, from_uuid,
                    detached_part_names[idx]);
                LOG_TRACE(&Poco::Logger::get("MetaStore"), "[attachDetachedParts] Delete detached part record {}",
                    detached_part_key);

                batch_writer.addDelete(detached_part_key);
            }
        }
        batch_writer.finalize();
    }

    // Write new meta of delete bitmaps
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        for (auto & bitmap_meta: bitmaps)
        {
            const auto & bitmap_model_meta = bitmap_meta->getModel();
            String detached_bitmap_meta_key = deleteBitmapKey(name_space, to_uuid, *bitmap_model_meta);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[attachDetachedDeleteBitmaps] Write new bitmap meta record {}", detached_bitmap_meta_key);

            batch_writer.addPut(detached_bitmap_meta_key, bitmap_model_meta->SerializeAsString());
        }
        batch_writer.finalize();
    }

    // Delete detached meta of delete bitmaps
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (auto & bitmap_meta: detached_bitmaps)
        {
            String detached_bitmap_meta_key = detachedDeleteBitmapKey(name_space, from_uuid, *bitmap_meta->getModel());
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[detachAttachedDeleteBitmaps] Delete detached bitmap meta record {}", detached_bitmap_meta_key);

            batch_writer.addDelete(detached_bitmap_meta_key);
        }
        batch_writer.finalize();
    }
}

void MetastoreProxy::detachAttachedParts(
    const String & name_space,
    const String & from_uuid,
    const String & to_uuid,
    const std::vector<String> & attached_part_names,
    const std::vector<String> & attached_staged_part_names,
    const std::vector<std::optional<Protos::DataModelPart>> & parts,
    const DeleteBitmapMetaPtrVector & attached_bitmaps,
    const DeleteBitmapMetaPtrVector & committed_bitmaps,
    size_t batch_write_size,
    size_t batch_delete_size)
{
    if (attached_part_names.size() + attached_staged_part_names.size() != parts.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "(Attached part names's count {} plus attached staged part names's size count {}) and parts count {} mismatch",
            attached_part_names.size(),
            attached_staged_part_names.size(),
            parts.size());
    }
    if (parts.empty())
    {
        return;
    }

    // Write detached meta
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        for (size_t idx = 0; idx < parts.size(); ++idx)
        {
            if (parts[idx].has_value())
            {
                auto info_ptr = createPartInfoFromModel(parts[idx].value().part_info());
                String detached_part_key = detachedPartKey(name_space, to_uuid,
                    info_ptr->getPartName());
                LOG_TRACE(&Poco::Logger::get("MetaStore"), "[detachAttachedParts] Write detach part record {}",
                    detached_part_key);

                batch_writer.addPut(detached_part_key, parts[idx].value().SerializeAsString());
            }
        }
        batch_writer.finalize();
    }

    // Delete part metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (size_t idx = 0; idx < attached_part_names.size(); ++idx)
        {
            String part_key = dataPartKey(name_space, from_uuid, attached_part_names[idx]);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[detachAttachedParts] Delete part record {}",
                part_key);

            batch_writer.addDelete(part_key);
        }
        for (size_t idx = 0; idx < attached_staged_part_names.size(); ++idx)
        {
            String part_key = stagedDataPartKey(name_space, from_uuid, attached_staged_part_names[idx]);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[detachAttachedParts] Delete staged part record {}", part_key);

            batch_writer.addDelete(part_key);
        }

        batch_writer.finalize();
    }

    // Write detached meta of delete bitmaps
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        for (auto & bitmap_meta : committed_bitmaps)
        {
            const auto & bitmap_model_meta = bitmap_meta->getModel();
            String detached_bitmap_meta_key = detachedDeleteBitmapKey(name_space, to_uuid, *bitmap_model_meta);
            LOG_TRACE(
                &Poco::Logger::get("MetaStore"),
                "[detachAttachedDeleteBitmaps] Write detach bitmap meta record {}",
                detached_bitmap_meta_key);

            batch_writer.addPut(detached_bitmap_meta_key, bitmap_model_meta->SerializeAsString());
        }
        batch_writer.finalize();
    }

    // Delete part meta of delete bitmaps
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (auto & bitmap_meta : attached_bitmaps)
        {
            String detached_bitmap_meta_key = deleteBitmapKey(name_space, from_uuid, *bitmap_meta->getModel());
            LOG_TRACE(
                &Poco::Logger::get("MetaStore"), "[detachAttachedDeleteBitmaps] Delete bitmap meta record {}", detached_bitmap_meta_key);

            batch_writer.addDelete(detached_bitmap_meta_key);
        }
        batch_writer.finalize();
    }
}

// This method is used only for detach part's rollback, so we won't write partition list here
std::vector<std::pair<String, UInt64>> MetastoreProxy::attachDetachedPartsRaw(
    const String & name_space,
    const String & tbl_uuid,
    const std::vector<String> & part_names,
    size_t detached_visible_part_size,
    size_t detached_staged_part_size,
    const std::vector<String> & bitmap_names,
    size_t batch_write_size,
    size_t batch_delete_size)
{
    if (part_names.size() != detached_visible_part_size + detached_staged_part_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "(Detached visible part size {} plus detached staged part size {}) and parts count {} mismatch",
            detached_visible_part_size,
            detached_staged_part_size,
            part_names.size());

    if (part_names.empty())
    {
        return {};
    }

    std::vector<String> keys;
    keys.reserve(part_names.size());
    std::for_each(part_names.begin(), part_names.end(), [&name_space, &tbl_uuid, &keys](const String& part_name) {
        keys.push_back(detachedPartKey(name_space, tbl_uuid, part_name));
    });
    std::vector<std::pair<String, UInt64>> metas = metastore_ptr->multiGet(keys);

    // Get all written detached part metas, write part metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        for (size_t i = 0; i < part_names.size(); ++i)
        {
            if (!metas[i].first.empty())
            {
                String part_key;
                if (i < detached_visible_part_size)
                    part_key = dataPartKey(name_space, tbl_uuid, part_names[i]);
                else
                    part_key = stagedDataPartKey(name_space, tbl_uuid, part_names[i]);
                LOG_TRACE(
                    &Poco::Logger::get("MetaStore"),
                    "[attachDetachedPartsRaw] Write {} part meta record {}",
                    i < detached_visible_part_size ? "" : "staged ",
                    part_key);

                batch_writer.addPut(part_key, metas[i].first);
            }
        }
        batch_writer.finalize();
    }

    // Delete detached part metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (size_t i = 0; i < part_names.size(); ++i)
        {
            String detached_part_key = detachedPartKey(name_space, tbl_uuid, part_names[i]);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "[attachDetachedPartsRaw] Delete detached part record {}",
                detached_part_key);

            batch_writer.addDelete(detached_part_key);
        }
        batch_writer.finalize();
    }

    // Delete detached bitmap metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (size_t i = 0; i < bitmap_names.size(); ++i)
        {
            String detached_bitmap_meta_key = detachedDeleteBitmapKey(name_space, tbl_uuid, bitmap_names[i]);
            LOG_TRACE(&Poco::Logger::get("MS"), "[attachDetachedPartsRaw] Delete detached bitmap meta record {}", detached_bitmap_meta_key);

            batch_writer.addDelete(detached_bitmap_meta_key);
        }
        batch_writer.finalize();
    }

    return metas;
}

void MetastoreProxy::detachAttachedPartsRaw(
    const String & name_space,
    const String & from_uuid,
    const String & to_uuid,
    const std::vector<String> & attached_part_names,
    const std::vector<std::pair<String, String>> & detached_part_metas,
    const std::vector<String> & attached_bitmap_names,
    const std::vector<std::pair<String, String>> & detached_bitmap_metas,
    size_t batch_write_size,
    size_t batch_delete_size)
{
    // Write detach meta first
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        for (const auto& [detached_part_name, detached_part_meta] : detached_part_metas)
        {
            String detached_part_key = detachedPartKey(name_space, to_uuid, detached_part_name);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "Write detached part record {} in detachAttachedPartsRaw",
                detached_part_key);

            batch_writer.addPut(detached_part_key, detached_part_meta);
        }
        batch_writer.finalize();
    }

    // Delete attached part metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (const String& attached_part_name : attached_part_names)
        {
            /// We don't know whether attach a staged part or normal part, just delete both.
            String attached_part_key = dataPartKey(name_space, from_uuid, attached_part_name);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "Delete part record {} in detachAttachedPartsRaw",
                attached_part_key);

            String attached_staged_part_key = stagedDataPartKey(name_space, from_uuid, attached_part_name);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "Delete staged part record {} in detachAttachedPartsRaw", attached_staged_part_key);

            batch_writer.addDelete(attached_part_key);
            batch_writer.addDelete(attached_staged_part_key);
        }
        batch_writer.finalize();
    }

    // Write detach bitmap metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_write_size);
        for (const auto & [detached_bitmap_name, detached_bitmap_meta] : detached_bitmap_metas)
        {
            String detached_bitmap_key = detachedDeleteBitmapKey(name_space, to_uuid, detached_bitmap_name);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "Write detached bitmap record {} in detachAttachedPartsRaw", detached_bitmap_key);

            batch_writer.addPut(detached_bitmap_key, detached_bitmap_meta);
        }
        batch_writer.finalize();
    }

    // Delete attached bitmap metas
    {
        MetastoreMultiWriteInBatch batch_writer(metastore_ptr, batch_delete_size);
        for (const String & attached_bitmap_name : attached_bitmap_names)
        {
            String attached_bitmap_key = deleteBitmapKey(name_space, from_uuid, attached_bitmap_name);
            LOG_TRACE(&Poco::Logger::get("MetaStore"), "Delete bitmap record {} in detachAttachedPartsRaw", attached_bitmap_key);

            batch_writer.addDelete(attached_bitmap_key);
        }
        batch_writer.finalize();
    }
}

IMetaStore::IteratorPtr MetastoreProxy::getDetachedPartsInRange(
    const String& name_space, const String& tbl_uuid, const String& range_start,
    const String& range_end, bool include_start, bool include_end)
{
    String prefix = detachedPartPrefix(name_space, tbl_uuid);
    return metastore_ptr->getByRange(prefix + range_start, prefix + range_end,
        include_start, include_end);
}

IMetaStore::IteratorPtr MetastoreProxy::getDetachedDeleteBitmapsInRange(
    const String & name_space,
    const String & tbl_uuid,
    const String & range_start,
    const String & range_end,
    bool include_start,
    bool include_end)
{
    String prefix = detachedDeleteBitmapKeyPrefix(name_space, tbl_uuid);
    return metastore_ptr->getByRange(prefix + range_start, prefix + range_end, include_start, include_end);
}

IMetaStore::IteratorPtr MetastoreProxy::getItemsInTrash(const String & name_space, const String & table_uuid, const size_t & limit, const String & start_key)
{
    return metastore_ptr->getByPrefix(trashItemsPrefix(name_space, table_uuid), limit, DEFAULT_SCAN_BATCH_COUNT, start_key);
}

String MetastoreProxy::extractTxnIDFromPartialSchemaKey(const String &partial_schema_key)
{
    auto pos = partial_schema_key.find_last_of('_');
    return partial_schema_key.substr(pos + 1, String::npos);
}

void MetastoreProxy::appendObjectPartialSchema(
    const String & name_space, const String & table_uuid, const UInt64 & txn_id, const SerializedObjectSchema & partial_schema)
{
    BatchCommitRequest batch_write;
    batch_write.AddPut(SinglePutRequest(partialSchemaKey(name_space, table_uuid, txn_id), partial_schema));
    batch_write.AddPut(SinglePutRequest(
        partialSchemaStatusKey(name_space, txn_id),
        ObjectSchemas::serializeObjectPartialSchemaStatus(ObjectPartialSchemaStatus::Running)));

    BatchCommitResponse store_response;
    metastore_ptr->batchWrite(batch_write, store_response);
}

SerializedObjectSchema MetastoreProxy::getObjectPartialSchema(const String &name_space, const String &table_uuid, const UInt64 &txn_id)
{
    SerializedObjectSchema partial_schema;
    metastore_ptr->get(partialSchemaKey(name_space, table_uuid, txn_id), partial_schema);
    if (partial_schema.empty())
        return "";

    return partial_schema;
}

SerializedObjectSchemas MetastoreProxy::scanObjectPartialSchemas(const String &name_space, const String &table_uuid, const UInt64 &limit_size)
{
    auto scan_prefix = partialSchemaPrefix(name_space, table_uuid);
    UInt64 scan_limit = limit_size <= 0 ? 0 : limit_size;

    auto scan_iterator = metastore_ptr->getByPrefix(scan_prefix, scan_limit);
    SerializedObjectSchemas serialized_object_schemas;
    while (scan_iterator->next())
    {
        auto key = scan_iterator->key();
        serialized_object_schemas.emplace(extractTxnIDFromPartialSchemaKey(key), scan_iterator->value());
    }

    return serialized_object_schemas;
}

SerializedObjectSchema MetastoreProxy::getObjectAssembledSchema(const String &name_space, const String &table_uuid)
{
    SerializedObjectSchema assembled_schema;
    metastore_ptr->get(assembledSchemaKey(name_space, table_uuid), assembled_schema);
    if (assembled_schema.empty())
        return "";

    return assembled_schema;
}

bool MetastoreProxy::resetObjectAssembledSchemaAndPurgePartialSchemas(
    const String & name_space,
    const String & table_uuid,
    const SerializedObjectSchema & old_assembled_schema,
    const SerializedObjectSchema & new_assembled_schema,
    const std::vector<TxnTimestamp> & partial_schema_txnids)
{
    Poco::Logger * log = &Poco::Logger::get(__func__);

    BatchCommitRequest batch_write;
    bool if_not_exists = false;
    if (old_assembled_schema.empty())
        if_not_exists = true;

    auto update_request = SinglePutRequest(assembledSchemaKey(name_space, table_uuid), new_assembled_schema, old_assembled_schema);
    update_request.if_not_exists = if_not_exists;
    batch_write.AddPut(update_request);

    for (const auto & txn_id : partial_schema_txnids)
        batch_write.AddDelete(partialSchemaKey(name_space, table_uuid, txn_id.toUInt64()));

    BatchCommitResponse store_response;
    try
    {
        metastore_ptr->batchWrite(batch_write, store_response);
        return true;
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            LOG_WARNING(
                log,
                fmt::format(
                    "Object schema refresh CAS put fail with old schema:{} and new schema:{}", old_assembled_schema, new_assembled_schema));
            return false;
        }
        else
            throw;
    }
}

SerializedObjectSchemaStatuses MetastoreProxy::batchGetObjectPartialSchemaStatuses(const String &name_space, const std::vector<TxnTimestamp> &txn_ids)
{
    Strings keys;
    for (const auto & txn_id : txn_ids)
        keys.emplace_back(partialSchemaStatusKey(name_space, txn_id.toUInt64()));
    auto serialized_statuses_in_metastore = metastore_ptr->multiGet(keys);
    SerializedObjectSchemaStatuses  serialized_statuses;
    serialized_statuses.reserve(serialized_statuses_in_metastore.size());
    for (size_t i = 0; i < serialized_statuses_in_metastore.size(); i++)
    {
        auto txn_id = txn_ids[i];
        auto status = serialized_statuses_in_metastore[i].first;
        if (status.empty())
            serialized_statuses.emplace(txn_id, ObjectSchemas::serializeObjectPartialSchemaStatus(ObjectPartialSchemaStatus::Finished));
        else
            serialized_statuses.emplace(txn_id, status);
    }

    return serialized_statuses;
}

void MetastoreProxy::batchDeletePartialSchemaStatus(const String &name_space, const std::vector<TxnTimestamp> &txn_ids)
{
    BatchCommitRequest batch_delete;
    for (const auto & txn_id : txn_ids)
        batch_delete.AddDelete(partialSchemaStatusKey(name_space, txn_id.toUInt64()));

    BatchCommitResponse delete_result;
    metastore_ptr->batchWrite(batch_delete, delete_result);
}

void MetastoreProxy::updateObjectPartialSchemaStatus(const String &name_space, const TxnTimestamp &txn_id, const ObjectPartialSchemaStatus & status)
{
    metastore_ptr->put(partialSchemaStatusKey(name_space, txn_id), ObjectSchemas::serializeObjectPartialSchemaStatus(status));
}

IMetaStore::IteratorPtr MetastoreProxy::getAllDeleteBitmaps(const String & name_space, const String & table_uuid)
{
    return metastore_ptr->getByPrefix(deleteBitmapPrefix(name_space, table_uuid));
}

void MetastoreProxy::updateTableTrashItemsSnapshot(const String & name_space, const String & table_uuid, const String & snapshot)
{
    metastore_ptr->put(tableTrashItemsMetricsSnapshotPrefix(name_space, table_uuid), snapshot);
}
void MetastoreProxy::updatePartitionMetricsSnapshot(
    const String & name_space, const String & table_uuid, const String & partition_id, const String & snapshot)
{
    metastore_ptr->put(partitionPartsMetricsSnapshotPrefix(name_space, table_uuid, partition_id), snapshot);
}
IMetaStore::IteratorPtr MetastoreProxy::getTablePartitionMetricsSnapshots(const String & name_space, const String & table_uuid)
{
    return metastore_ptr->getByPrefix(partitionPartsMetricsSnapshotPrefix(name_space, table_uuid, ""));
}
String MetastoreProxy::getTableTrashItemsSnapshot(const String & name_space, const String & table_uuid)
{
    String value;
    metastore_ptr->get(tableTrashItemsMetricsSnapshotPrefix(name_space, table_uuid), value);
    return value;
}

Strings MetastoreProxy::removePartitions(const String & name_space, const String & table_uuid, const PartitionWithGCStatus & partitions)
{
    Strings request_keys;
    for (const auto & p : partitions)
        request_keys.emplace_back(tablePartitionInfoKey(name_space, table_uuid, p.first));

    auto partitions_meta = metastore_ptr->multiGet(request_keys);

    Poco::Logger * log = &Poco::Logger::get(__func__);

    Strings res;
    // try commit all partitions with CAS in one batch
    BatchCommitRequest batch_write;
    for (const auto & metadata : partitions_meta)
    {
        Protos::PartitionMeta partition_meta;
        partition_meta.ParseFromString(metadata.first);

        if (auto it = partitions.find(partition_meta.id()); it!=partitions.end() && partition_meta.gctime() == it->second)
        {
            res.push_back(partition_meta.id());
            batch_write.AddDelete(tablePartitionInfoKey(name_space, table_uuid, partition_meta.id()), metadata.first, metadata.second);
        }
    }

    BatchCommitResponse resp;
    auto cas_commit_success = metastore_ptr->batchWrite(batch_write, resp);

    // try again with deleting one by one if fails to commit in batch
    if (!cas_commit_success)
    {
        LOG_WARNING(log, "CAS remove {} partitions failed. Will try to delete them one by one." , res.size());
        res.clear();
        for (const auto & metadata : partitions_meta)
        {
            Protos::PartitionMeta partition_meta;
            partition_meta.ParseFromString(metadata.first);
            try
            {
                if (auto it = partitions.find(partition_meta.id()); it!=partitions.end() && partition_meta.gctime() == it->second)
                {
                    metastore_ptr->drop(tablePartitionInfoKey(name_space, table_uuid, partition_meta.id()), metadata.first);
                }
                res.push_back(partition_meta.id());
            }
            catch (const Exception & e)
            {
                throw e;
            }
        }
    }
    return res;
}

String MetastoreProxy::getByKey(const String & key)
{
    String meta_str;
    metastore_ptr->get(key, meta_str);
    return meta_str;
}

IMetaStore::IteratorPtr MetastoreProxy::getByPrefix(const String & prefix, size_t limit)
{
    return metastore_ptr->getByPrefix(prefix, limit);
}

// Sensitive Resources
void MetastoreProxy::putSensitiveResource(const String & name_space, const String & database, const String & table, const String & column, const String & target, bool value) const
{
    String data;
    auto res = std::make_shared<Protos::DataModelSensitiveDatabase>();

    metastore_ptr->get(sensitiveResourceKey(name_space, database), data);

    if (!data.empty())
        res->ParseFromString(data);

    if (!database.empty())
    {
        res->set_database(database);
        if (target == "DATABASE")
            res->set_is_sensitive(value);
    }

    Protos::DataModelSensitiveTable * current_table = nullptr;
    if (!table.empty())
    {
        // get existing table from database if its already there
        bool found = false;
        for (auto & table_from_meta : *res->mutable_tables())
        {
            if (table_from_meta.table() == table)
            {
                current_table = &table_from_meta;
                found = true;
                break;
            }
        }
        if (!found)
            current_table = res->add_tables();
        current_table->set_table(table);
        if (target == "TABLE")
            current_table->set_is_sensitive(value);
    }

    if (target == "COLUMN" && !column.empty())
    {
        Strings new_columns_for_meta(current_table->sensitive_columns().begin(), current_table->sensitive_columns().end());
        current_table->clear_sensitive_columns();
        for (auto & col : new_columns_for_meta)
        {
            if (col != column)
                current_table->add_sensitive_columns(col);
        }
        if (value)
            current_table->add_sensitive_columns(column);
    }

    metastore_ptr->put(sensitiveResourceKey(name_space, database), res->SerializeAsString());
}

std::shared_ptr<Protos::DataModelSensitiveDatabase> MetastoreProxy::getSensitiveResource(const String & name_space, const String & database) const
{
    String data;
    auto res = std::make_shared<Protos::DataModelSensitiveDatabase>();

    metastore_ptr->get(sensitiveResourceKey(name_space, database), data);

    if (!data.empty())
        res->ParseFromString(data);

    return res;
}

// Access Entities
String MetastoreProxy::getAccessEntity(EntityType type, const String & name_space, const String & name) const
{
    String data;
    String access_entity_key = accessEntityKey(type, name_space, name);
    metastore_ptr->get(access_entity_key, data);
    tryGetLargeValue(metastore_ptr, name_space, access_entity_key, data);
    return data;
}

std::vector<std::pair<String, UInt64>> MetastoreProxy::getEntities(EntityType type, const String & name_space, const std::unordered_set<UUID> & ids) const
{
    Strings requests;

    requests.reserve(ids.size());

    for (const auto & id : ids)
        requests.push_back(accessEntityUUIDNameMappingKey(name_space, UUIDHelpers::UUIDToString(id)));

    auto response = metastore_ptr->multiGet(requests);

    requests.clear();
    for (const auto & [s, version] : response)
    {
        if (s.empty())
            continue;
        requests.push_back(accessEntityKey(type, name_space, s));
    }

    auto res = metastore_ptr->multiGet(requests);

    for (size_t i=0; i<res.size(); i++)
    {
        // try parse large value
        String & value = res[i].first;
        tryGetLargeValue(metastore_ptr, name_space, requests[i], value);
    }

    return res;
}

Strings MetastoreProxy::getAllAccessEntities(EntityType type, const String & name_space) const
{
    Strings models;
    auto it = metastore_ptr->getByPrefix(accessEntityPrefix(type, name_space));
    while (it->next())
    {
        String value = it->value();
        /// NOTE: Too many large KVs will cause severe performance regression.
        tryGetLargeValue(metastore_ptr, name_space, it->key(), value);
        models.push_back(std::move(value));
    }
    return models;
}

String MetastoreProxy::getAccessEntityNameByUUID(const String & name_space, const UUID & id) const
{
    String data;
    String uuid = UUIDHelpers::UUIDToString(id);
    metastore_ptr->get(accessEntityUUIDNameMappingKey(name_space, uuid), data);
    return data;
}

bool MetastoreProxy::dropAccessEntity(EntityType type, const String & name_space, const UUID & id, const String & name) const
{
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;
    String uuid = UUIDHelpers::UUIDToString(id);
    batch_write.AddDelete(accessEntityUUIDNameMappingKey(name_space, uuid));
    batch_write.AddDelete(accessEntityKey(type, name_space, name));
    return metastore_ptr->batchWrite(batch_write, resp);
}

bool MetastoreProxy::putAccessEntity(EntityType type, const String & name_space, const AccessEntityModel & new_access_entity, const AccessEntityModel & old_access_entity, bool replace_if_exists) const
{
    BatchCommitRequest batch_write;
    BatchCommitResponse resp;
    auto is_rename = !old_access_entity.name().empty() && new_access_entity.name() != old_access_entity.name();
    String uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(new_access_entity.uuid()));
    String serialized_old_access_entity = old_access_entity.SerializeAsString();
    if (!serialized_old_access_entity.empty() && !is_rename)
    {
        addPotentialLargeKVToBatchwrite(
            metastore_ptr,
            batch_write,
            name_space,
            accessEntityKey(type, name_space, new_access_entity.name()),
            new_access_entity.SerializeAsString(),
            !replace_if_exists,
            serialized_old_access_entity);
    }
    else
    {
        addPotentialLargeKVToBatchwrite(
            metastore_ptr,
            batch_write,
            name_space,
            accessEntityKey(type, name_space, new_access_entity.name()),
            new_access_entity.SerializeAsString(),
            !replace_if_exists);
    }

    batch_write.AddPut(SinglePutRequest(accessEntityUUIDNameMappingKey(name_space, uuid), new_access_entity.name(), !replace_if_exists));
    if (is_rename)
        batch_write.AddDelete(accessEntityKey(type, name_space, old_access_entity.name())); // delete old one in case of rename
    try
    {
        return metastore_ptr->batchWrite(batch_write, resp);
    }
    catch (Exception & e)
    {
        auto puts_size = batch_write.puts.size();
        if (e.code() == ErrorCodes::METASTORE_COMMIT_CAS_FAILURE)
        {
            if (resp.puts.count(puts_size-2) && replace_if_exists && !serialized_old_access_entity.empty())
            {
                throw Exception(
                    "Access Entity has recently been changed in catalog. Please try the request again.",
                    ErrorCodes::METASTORE_ACCESS_ENTITY_CAS_ERROR);
            }
            else if (resp.puts.count(puts_size-2) && !replace_if_exists)
            {
                throw Exception(
                    "Access Entity with the same name already exists in catalog. Please use another name and try again.",
                    ErrorCodes::METASTORE_ACCESS_ENTITY_EXISTS_ERROR);
            }
            else if (resp.puts.count(puts_size-1) && !replace_if_exists)
            {
                throw Exception(
                    "Access Entity with the same UUID already exists in catalog. Please use another name and try again.",
                    ErrorCodes::METASTORE_ACCESS_ENTITY_EXISTS_ERROR);
            }
        }
        throw;
    }
}

} /// end of namespace DB::Catalog

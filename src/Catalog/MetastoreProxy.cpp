#include <Catalog/MetastoreProxy.h>
#include <Protos/DataModelHelpers.h>
// #include <WAL/CnchLogHelpers.h>
#include <IO/ReadHelpers.h>
#include <cstddef>
#include <sstream>
#include <random>

namespace DB
{

namespace ErrorCodes
{
extern const int METASTORE_DB_UUID_CAS_ERROR;
extern const int METASTORE_TABLE_UUID_CAS_ERROR;
extern const int METASTORE_TABLE_NAME_CAS_ERROR;
extern const int METASTORE_ROOT_PATH_ALREADY_EXISTS;
extern const int METASTORE_ROOT_PATH_ID_NOT_UNIQUE;
extern const int METASTORE_CLEAR_INTENT_CAS_FAILURE;
extern const int VIRTUAL_WAREHOUSE_NOT_FOUND;
extern const int FUNCTION_ALREADY_EXISTS;
}

namespace Catalog
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

void MetastoreProxy::addDatabase(const String & name_space, const Protos::DataModelDB & db_model)
{
    String db_meta;
    db_model.SerializeToString(&db_meta);

    auto multiWrite = metastore_ptr->createMultiWrite();
    if (db_model.has_uuid())
        multiWrite.addPut(dbUUIDUniqueKey(name_space, UUIDHelpers::UUIDToString(RPCHelpers::createUUID(db_model.uuid()))), "", "", true);
    multiWrite.addPut(dbKey(name_space, db_model.name(), db_model.commit_time()), db_meta);

    try
    {
        multiWrite.commit();
    }
    catch (Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
        {
            if (multiWrite.wb_resp.puts_[0].first == bytekv::sdk::Errorcode::CAS_FAILED)
            {
                throw Exception(
                    "Database with the same uuid(" + UUIDHelpers::UUIDToString(RPCHelpers::createUUID(db_model.uuid())) + ") already exists in catalog. Please use another uuid or "
                    "clear old version of this database and try again.",
                    ErrorCodes::METASTORE_DB_UUID_CAS_ERROR);
            }
        }
        throw e;
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

    auto multiWrite = metastore_ptr->createMultiWrite();

    /// get all trashed dictionaries of current db and remove them with db metadata
    auto dic_ptrs = getDictionariesFromTrash(name_space, name + "_" + toString(ts));
    for (auto & dic_ptr : dic_ptrs)
        multiWrite.addDelete(dictionaryTrashKey(name_space, dic_ptr->database(), dic_ptr->name()));

    multiWrite.addDelete(dbKey(name_space, name, ts));
    multiWrite.addDelete(dbTrashKey(name_space, name, ts));
    if (db_model.has_uuid())
        multiWrite.addDelete(dbUUIDUniqueKey(name_space, UUIDHelpers::UUIDToString(RPCHelpers::createUUID(db_model.uuid()))));
    multiWrite.commit();
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

void MetastoreProxy::createTable(const String & name_space, const DB::Protos::DataModelTable & table_data, const Strings & dependencies, const Strings & masking_policy_mapping)
{
    const String & database = table_data.database();
    const String & name = table_data.name();
    const String & uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table_data.uuid()));
    String serialized_meta;
    table_data.SerializeToString(&serialized_meta);

    auto multiWrite = metastore_ptr->createMultiWrite();
    multiWrite.addPut(nonHostUpdateKey(name_space, uuid), "0", "", true);
    // insert table meta
    multiWrite.addPut(tableStoreKey(name_space, uuid, table_data.commit_time()), serialized_meta, "", true);
    /// add dependency mapping if need
    for (const String & dependency : dependencies)
        multiWrite.addPut(viewDependencyKey(name_space, dependency, uuid), uuid);

    for (const String & mask : masking_policy_mapping)
        multiWrite.addPut(maskingPolicyTableMappingKey(name_space, mask, uuid), uuid);

    /// add `table name` ->`uuid` mapping
    Protos::TableIdentifier identifier;
    identifier.set_database(database);
    identifier.set_name(name);
    identifier.set_uuid(uuid);
    multiWrite.addPut(tableUUIDMappingKey(name_space, database, name), identifier.SerializeAsString(), "", true);
    multiWrite.addPut(tableUUIDUniqueKey(name_space, uuid), "", "", true);

    try
    {
        multiWrite.commit();
    }
    catch (Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
        {
            auto size = multiWrite.wb_resp.puts_.size();
            if (multiWrite.wb_resp.puts_[size-1].first == bytekv::sdk::Errorcode::CAS_FAILED)
            {
                throw Exception(
                    "Table with the same uuid already exists in catalog. Please use another uuid or "
                    "clear old version of this table and try again.",
                    ErrorCodes::METASTORE_TABLE_UUID_CAS_ERROR);
            }
            else if (multiWrite.wb_resp.puts_[size-2].first == bytekv::sdk::Errorcode::CAS_FAILED)
            {
                throw Exception(
                    "Table with the same name already exists in catalog. Please use another name and try again.",
                    ErrorCodes::METASTORE_TABLE_NAME_CAS_ERROR);
            }
        }
        throw e;
    }
}

void MetastoreProxy::createUDF(const String & name_space, const DB::Protos::DataModelUDF & udf_data)
{
    const String & database = udf_data.database();
    const String & name = udf_data.function_name();
    String serialized_meta;
    udf_data.SerializeToString(&serialized_meta);

    try
    {
        metastore_ptr->put(udfStoreKey(name_space, database, name), serialized_meta, true);
    }
    catch (Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED) {
            throw Exception("UDF with function name - " + name + " in database - " +  database + " already exists.", ErrorCodes::FUNCTION_ALREADY_EXISTS);
        }
        throw e;
    }
}

void MetastoreProxy::dropUDF(const String & name_space, const String &db_name, const String &function_name)
{
    metastore_ptr->drop(udfStoreKey(name_space, db_name, function_name));
}

void MetastoreProxy::updateTable(const String & name_space, const String & table_uuid, const String & table_info_new, const UInt64 & ts)
{
    metastore_ptr->put(tableStoreKey(name_space, table_uuid, ts), table_info_new);
}

void MetastoreProxy::getTableByUUID(const String & name_space, const String & table_uuid, Strings & tables_info)
{
    auto it = metastore_ptr->getByPrefix(tableStorePrefix(name_space, table_uuid));
    while(it->next())
    {
        tables_info.emplace_back(it->value());
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
    std::vector<std::shared_ptr<Protos::TableIdentifier>> res;
    auto it = metastore_ptr->getByPrefix(tableUUIDMappingPrefix(name_space, db));
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


MetastoreByteKVImpl::IteratorPtr MetastoreProxy::getTrashTableIDIterator(const String & name_space, uint32_t iterator_internal_batch_size)
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

std::unordered_map<String, UInt64> MetastoreProxy::getTrashTableVersions(const String & name_space, const String & database, const String & table)
{
    std::unordered_map<String, UInt64> res;
    auto it = metastore_ptr->getByPrefix(tableTrashPrefix(name_space) + escapeString(database) + "_" + escapeString(table));
    while(it->next())
    {
        const auto & key = it->key();
        auto pos = key.find_last_of('_');
        UInt64 drop_ts = std::stoull(key.substr(pos + 1, String::npos), nullptr);
        Protos::TableIdentifier data_model;
        data_model.ParseFromString(it->value());
        res.emplace(data_model.uuid(), drop_ts);
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

    auto multiWrite = createMultiWrite();

    /// remove all versions of table meta info.
    auto it_t = metastore_ptr->getByPrefix(tableStorePrefix(name_space, uuid));
    while(it_t->next())
    {
        multiWrite.addDelete(it_t->key());
    }

    /// remove table partition list;
    String partition_list_prefix = tablePartitionInfoPrefix(name_space, uuid);
    auto it_p = metastore_ptr->getByPrefix(partition_list_prefix);
    while(it_p->next())
    {
        multiWrite.addDelete(it_p->key());
    }
    /// remove dependency
    for (const String & dependency : dependencies)
        multiWrite.addDelete(viewDependencyKey(name_space, dependency, uuid));

    /// remove all records about memory buffers
    auto log_prefix = cnchLogKey(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(log_prefix); it->next(); )
    {
        multiWrite.addDelete(it->key());
    }

    /// remove trash record if the table marked as deleted before be cleared
    multiWrite.addDelete(tableTrashKey(name_space, database, table, ts));

    /// remove MergeMutateThread meta
    multiWrite.addDelete(mergeMutateThreadStartTimeKey(name_space, uuid));
    /// remove table uuid unique key
    multiWrite.addDelete(tableUUIDUniqueKey(name_space, uuid));
    multiWrite.addDelete(nonHostUpdateKey(name_space, uuid));

    /// remove all statistics
    auto table_statistics_prefix = tableStatisticPrefix(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(table_statistics_prefix); it->next(); )
    {
        multiWrite.addDelete(it->key());
    }
    auto table_statistics_tag_prefix = tableStatisticTagPrefix(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(table_statistics_tag_prefix); it->next(); )
    {
        multiWrite.addDelete(it->key());
    }
    auto column_statistics_prefix = columnStatisticPrefix(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(column_statistics_prefix); it->next(); )
    {
        multiWrite.addDelete(it->key());
    }
    auto column_statistics_tag_prefix = columnStatisticTagPrefixWithoutColumn(name_space, uuid);
    for (auto it = metastore_ptr->getByPrefix(column_statistics_tag_prefix); it->next(); )
    {
        multiWrite.addDelete(it->key());
    }

    multiWrite.commit();
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

void MetastoreProxy::renameTable(const String & name_space,
                                 Protos::DataModelTable & table,
                                 const String & old_db_name,
                                 const String & old_table_name,
                                 const String & uuid,
                                 MetastoreByteKVImpl::MultiWrite & container)
{
    /// update `table`->`uuid` mapping.
    container.addDelete(tableUUIDMappingKey(name_space, old_db_name, old_table_name));
    Protos::TableIdentifier identifier;
    identifier.set_database(table.database());
    identifier.set_name(table.name());
    identifier.set_uuid(uuid);
    container.addPut(tableUUIDMappingKey(name_space, table.database(), table.name()), identifier.SerializeAsString(), "", true);

    String meta_data;
    table.SerializeToString(&meta_data);
    /// add new table meta data with new name
    container.addPut(tableStoreKey(name_space, uuid, table.commit_time()), meta_data, "", true);
}

bool MetastoreProxy::alterTable(const String & name_space, const Protos::DataModelTable & table, const Strings & masks_to_remove, const Strings & masks_to_add)
{
    auto multiWrite = createMultiWrite();

    String table_uuid = UUIDHelpers::UUIDToString(RPCHelpers::createUUID(table.uuid()));
    multiWrite.addPut(tableStoreKey(name_space, table_uuid, table.commit_time()), table.SerializeAsString(), "", true);
    for (const auto & name : masks_to_remove)
        multiWrite.addDelete(maskingPolicyTableMappingKey(name_space, name, table_uuid));

    for (const auto & name : masks_to_add)
        multiWrite.addPut(maskingPolicyTableMappingKey(name_space, name, table_uuid), table_uuid);

    return multiWrite.commit();
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

Strings MetastoreProxy::getPartsByName(const String & name_space, const String & uuid, RepeatedFields & parts_name)
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

void MetastoreProxy::prepareAddDataParts(const String & name_space, const String & table_uuid, const Strings & current_partitions,
        const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts, MetastoreByteKVImpl::MultiWrite & container,
        const std::vector<String> & expected_parts, bool update_sync_list)
{
    if (parts.empty())
        return;

    std::unordered_set<String> existing_partitions{current_partitions.begin(), current_partitions.end()};
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

        container.addPut(dataPartKey(name_space, table_uuid, info_ptr->getPartName()), part_meta, expected_parts[it - parts.begin()]);

        if (!existing_partitions.count(info_ptr->partition_id) && !partition_map.count(info_ptr->partition_id))
            partition_map.emplace(info_ptr->partition_id, it->partition_minmax());
    }

    if (update_sync_list)
        container.addPut(syncListKey(name_space, table_uuid, commit_time), std::to_string(commit_time));

    Protos::PartitionMeta partition_model;
    for (auto it = partition_map.begin(); it != partition_map.end(); it++)
    {
        std::stringstream ss;
        /// To keep the partitions have the same order as data parts in bytekv, we add an extra "_" in the key of partition meta
        ss << tablePartitionInfoPrefix(name_space, table_uuid) << it->first << '_';

        partition_model.set_id(it->first);
        partition_model.set_partition_minmax(it->second);

        container.addPut(ss.str(), partition_model.SerializeAsString());
    }
}

void MetastoreProxy::prepareAddStagedParts(
    const String & name_space, const String & table_uuid,
    const google::protobuf::RepeatedPtrField<Protos::DataModelPart> & parts,
    MetastoreByteKVImpl::MultiWrite & container,
    const std::vector<String> & expected_staged_parts)
{
    if (parts.empty())
        return;

    size_t expected_staged_part_size = expected_staged_parts.size();
    if (expected_staged_part_size != static_cast<size_t>(parts.size()))
        throw Exception("Staged part size wants to write does not match the expected staged part size.", ErrorCodes::LOGICAL_ERROR);

    for (auto it = parts.begin(); it != parts.end(); it++)
    {
        auto info_ptr = createPartInfoFromModel(it->part_info());
        String part_meta = it->SerializeAsString();
        container.addPut(stagedDataPartKey(name_space, table_uuid, info_ptr->getPartName()), part_meta, expected_staged_parts[it - parts.begin()]);
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
    metastore_ptr->clean(tablePartitionInfoPrefix(name_space, uuid));
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
    auto multiWrite = createMultiWrite();

    String path_id = std::to_string(gen());
    /// avoid generate reserved path id
    while (path_id == "0")
        path_id = std::to_string(gen());

    multiWrite.addPut(ROOT_PATH_PREFIX + root_path, path_id, "", true);
    multiWrite.addPut(ROOT_PATH_ID_UNIQUE_PREFIX + path_id, "", "", true);

    try
    {
        multiWrite.commit();
    }
    catch (Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
        {
            if (multiWrite.wb_resp.puts_[0].first == bytekv::sdk::Errorcode::CAS_FAILED)
            {
                throw Exception("Root path " + root_path + " already exists.", ErrorCodes::METASTORE_ROOT_PATH_ALREADY_EXISTS);
            }
            else if (multiWrite.wb_resp.puts_[1].first == bytekv::sdk::Errorcode::CAS_FAILED)
            {
                throw Exception("Failed to create new root path because of path id collision, please try again.", ErrorCodes::METASTORE_ROOT_PATH_ID_NOT_UNIQUE);
            }
        }
        throw e;
    }
}

void MetastoreProxy::deleteRootPath(const String & root_path)
{
    String path_id;
    metastore_ptr->get(ROOT_PATH_PREFIX + root_path, path_id);
    if (!path_id.empty())
    {
        auto multiWrite = createMultiWrite();
        multiWrite.addDelete(ROOT_PATH_PREFIX + root_path);
        multiWrite.addDelete(ROOT_PATH_ID_UNIQUE_PREFIX + path_id);
        multiWrite.commit();
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

    auto multi_write = metastore_ptr->createMultiWrite();

    for (const auto & txn_id : txn_ids)
        multi_write.addDelete(transactionRecordKey(name_space, txn_id.toUInt64()));

    multi_write.commit();
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
    txn_keys.reserve(txn_keys.size());
    for (const auto & txn_id : txn_ids)
        txn_keys.push_back(transactionRecordKey(name_space, txn_id.toUInt64()));

    return metastore_ptr->multiGet(txn_keys);
}

IMetaStore::IteratorPtr MetastoreProxy::getAllTransactionRecord(const String & name_space, const size_t & max_result_number)
{
    return metastore_ptr->getByPrefix(escapeString(name_space) + "_" + TRANSACTION_RECORD_PREFIX, max_result_number);
}

std::pair<bool, String> MetastoreProxy::updateTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data_old, const String & txn_data_new)
{
    return metastore_ptr->putCASWithOldValue(transactionRecordKey(name_space, txn_id), txn_data_new, txn_data_old);
}

bool MetastoreProxy::updateTransactionRecordWithOffsets(const String &name_space, const UInt64 &txn_id,
                                                        const String &txn_data_old, const String &txn_data_new,
                                                        const String & consumer_group,
                                                        const cppkafka::TopicPartitionList & tpl)
{
    auto multi_write = metastore_ptr->createMultiWrite();

    multi_write.addPut(transactionRecordKey(name_space, txn_id), txn_data_new, txn_data_old);
    for (auto & tp : tpl)
        multi_write.addPut(kafkaOffsetsKey(name_space, consumer_group, tp.get_topic(), tp.get_partition()), std::to_string(tp.get_offset()));

    return multi_write.commit();

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

bool MetastoreProxy::updateTransactionRecordWithMemoryBuffer(
    const String & name_space,
    const UInt64 & txn_id,
    const String & txn_data_old,
    String & txn_data_new,
    const Strings & buffer_keys_in_kv,
    const Strings & buffer_values_in_kv,
    const String & consumer_group,
    const cppkafka::TopicPartitionList & tpl)
{
    auto multiWrite = createMultiWrite();

    // Write transaction record
    multiWrite.addPut(transactionRecordKey(name_space, txn_id), txn_data_new, txn_data_old);

    // Write memory buffer info
    for (size_t i = 0; i < buffer_keys_in_kv.size(); i++)
    {
        auto & key = buffer_keys_in_kv[i];
        auto & value = buffer_values_in_kv[i];
        multiWrite.addPut(cnchLogKey(name_space, key), value, value);
    }

    // Write Kafka consume offset
    for (size_t i = 0; i < tpl.size(); ++i)
    {
        auto key = kafkaOffsetsKey(name_space, consumer_group, tpl[i].get_topic(), tpl[i].get_partition());
        auto new_value = std::to_string(tpl[i].get_offset());
        // NOTE: no cas here for offset
        multiWrite.addPut(key, new_value);
    }

    bool res = false;
    try
    {
        // Do not allow cas fail here, so that we can catch cas failed exception and extract content store in kv.
        res = multiWrite.commit(/*allow_cas_fail=*/false);
    }
    catch (const Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
        {
            for (size_t i = 0; i < multiWrite.wb_resp.puts_.size(); i++)
            {
                const auto & [code, resp] = multiWrite.wb_resp.puts_[i];
                if (code == bytekv::sdk::Errorcode::CAS_FAILED)
                {
                    if (i == 0)
                    {
                        LOG_WARNING(&Poco::Logger::get(__func__), "Transaction CAS failed, extracting txn data back from kv.");
                        txn_data_new = resp.current_value;
                    }
                    else if (i < buffer_keys_in_kv.size() + 1)
                    {
                        LOG_WARNING(&Poco::Logger::get(__func__),
                                    "Memory buffer CAS failed, failed key: {}", buffer_keys_in_kv[i - 1]);
                    }
                }
            }
            // DO NOT throw anything here, let the caller handle the cas failed logic.
            return false;
        }
        else
        {
            // Throw in other cases.
            throw;
        }
    }
    return res;
}

std::pair<bool, String> MetastoreProxy::MetastoreProxy::updateTransactionRecordWithRequests(
    const String & name_space, UInt64 txn_id, WriteRequest txn_request, WriteRequests * additional_requests)
{
    if (!additional_requests || additional_requests->empty())
    {
        return metastore_ptr->putCASWithOldValue(
            transactionRecordKey(name_space, txn_id), String(txn_request.new_value), String(*txn_request.old_value));
    }
    else
    {
        String record_key = transactionRecordKey(name_space, txn_id);
        txn_request.key = record_key;
        additional_requests->push_back(txn_request);
        return {metastore_ptr->multiWriteCAS(*additional_requests), String{}};
        /// TODO: set txn_result
    }
}

void MetastoreProxy::setTransactionRecord(const String & name_space, const UInt64 & txn_id, const String & txn_data, UInt64 ttl)
{
    if (ttl)
    {
        return metastore_ptr->putTTL(transactionRecordKey(name_space, txn_id), txn_data, ttl);
    }
    else
    {
        return metastore_ptr->put(transactionRecordKey(name_space, txn_id), txn_data);
    }
}

bool MetastoreProxy::writeIntent(const String & name_space, const String & intent_prefix, const std::vector<WriteIntent> & intents, std::vector<String> & cas_failed_list)
{
    auto multiWrite = createMultiWrite();
    for (auto  & intent : intents)
    {
        multiWrite.addPut(writeIntentKey(name_space, intent_prefix, intent.intent()), intent.serialize(), "", true);
    }
    try
    {
        multiWrite.commit();
        return true;
    }
    catch (Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
        {
            for (size_t i=0; i < multiWrite.wb_resp.puts_.size(); i++)
            {
                if (multiWrite.wb_resp.puts_[i].first == bytekv::sdk::Errorcode::CAS_FAILED)
                    cas_failed_list.push_back(multiWrite.wb_resp.puts_[i].second.current_value);
            }
        }
        else
            throw e;
    }
    return false;
}

bool MetastoreProxy::resetIntent(const String & name_space, const String & intent_prefix, const std::vector<WriteIntent> & intents, const UInt64 & new_txn_id, const String & new_location)
{
    auto multiWrite = createMultiWrite();
    for (const auto & intent : intents)
    {
        WriteIntent new_intent(new_txn_id, new_location, intent.intent());
        multiWrite.addPut(writeIntentKey(name_space, intent_prefix, intent.intent()), new_intent.serialize(), intent.serialize());
    }

    try
    {
        multiWrite.commit();
        return true;
    }
    catch (const Exception & e)
    {
        if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
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
    auto multiWrite = createMultiWrite();

    for (auto idx : matched_intent_index)
        multiWrite.addDelete(intent_names[idx], snapshot[idx].second);

    bool cas_success = multiWrite.commit(true);

    if (!cas_success && intent_names.size() > 1)
    {
        LOG_WARNING(log, "Clear WriteIntent got CAS_FAILED. This happens because other tasks concurrently reset the WriteIntent.");

        // try clean for each intent
        for (auto idx : matched_intent_index)
        {
            try
            {
                metastore_ptr->drop(intent_names[idx], snapshot[idx].second);
            }
            catch (const Exception & e)
            {
                if (e.code() == bytekv::sdk::Errorcode::CAS_FAILED)
                    LOG_WARNING(log, "CAS fail when cleaning the intent: " + intent_names[idx]);
                else
                    throw e;
            }
        }
    }
}

void MetastoreProxy::clearZombieIntent(const String & name_space, const UInt64 & txn_id)
{
    auto it = metastore_ptr->getByPrefix(escapeString(name_space) + "_" + WRITE_INTENT_PREFIX);
    auto multiWrite = createMultiWrite();
    while(it->next())
    {
        Protos::DataModelWriteIntent intent_model;
        intent_model.ParseFromString(it->value());
        if (intent_model.txn_id() == txn_id)
        {
            multiWrite.addDelete(it->value());
        }
    }

    if (!multiWrite.isEmpty())
        multiWrite.commit();
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

// void MetastoreProxy::writeUndoBuffer(const String & name_space, const UInt64 & txnID, const String & uuid, UndoResources & resources)
// {
//     if (resources.empty())
//         return;

//     auto multiWrite = createMultiWrite();
//     for (auto & resource : resources)
//     {
//         resource.setUUID(uuid);
//         multiWrite.addPut(undoBufferStoreKey(name_space, txnID, resource), resource.serialize());
//     }
//     multiWrite.commit();
// }

void MetastoreProxy::clearUndoBuffer(const String & name_space, const UInt64 & txnID)
{
    metastore_ptr->clean(undoBufferKey(name_space, txnID));
}

IMetaStore::IteratorPtr MetastoreProxy::getUndoBuffer(const String & name_space, UInt64 txnID)
{
    return metastore_ptr->getByPrefix(undoBufferKey(name_space, txnID));
}

IMetaStore::IteratorPtr MetastoreProxy::getAllUndoBuffer(const String & name_space)
{
    return metastore_ptr->getByPrefix(escapeString(name_space) + '_' + UNDO_BUFFER_PREFIX);
}

void MetastoreProxy::multiDrop(const Strings & keys)
{
    auto multiWrite = metastore_ptr->createMultiWrite();
    for (const auto & key : keys)
    {
        multiWrite.addDelete(key);
    }
    multiWrite.commit();
}

void MetastoreProxy::writeFilesysLock(const String & name_space, UInt64 txn_id, const String & dir, const String & db, const String & table)
{
    Protos::DataModelFileSystemLock data;
    data.set_txn_id(txn_id);
    data.set_directory(dir);
    data.set_database(db);
    data.set_table(table);

    metastore_ptr->put(filesysLockKey(name_space, dir), data.SerializeAsString());
}

String MetastoreProxy::getFilesysLock(const String & name_space, const String & dir)
{
    String data;
    metastore_ptr->get(filesysLockKey(name_space, dir), data);
    return data;
}

void MetastoreProxy::clearFilesysLock(const String & name_space, const String & dir)
{
    metastore_ptr->drop(filesysLockKey(name_space, dir));
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

void MetastoreProxy::lockPartsInKV(const String & name_space, const String & uuid, const Strings & parts, const String & txnID, std::vector<std::pair<uint32_t, String>> & cas_failed)
{
    Strings keys;
    std::transform(
        parts.begin(),
        parts.end(),
        std::back_inserter(keys),
        std::bind(kvLockKey, std::ref(name_space), std::ref(uuid), std::placeholders::_1)
    );
    metastore_ptr->multiPutCAS(keys, txnID, Strings{}, true, cas_failed);
}

bool MetastoreProxy::unLockPartsInKV(const String & name_space, const String & uuid, const Strings & parts, const String & txnID)
{
    Strings keys;
    Strings expected_values(parts.size(), txnID);

    std::transform(
        parts.begin(),
        parts.end(),
        std::back_inserter(keys),
        std::bind(kvLockKey, std::ref(name_space), std::ref(uuid), std::placeholders::_1)
    );

    /// firstly, make CAS put to check current lock status.
    std::vector<std::pair<uint32_t, String>> cas_failed;
    metastore_ptr->multiPutCAS(keys, txnID, expected_values, false, cas_failed);

    if (!cas_failed.empty())
        return false;

    /// if passed CAS test, unlock parts.
    multiDrop(keys);
    return true;
}

bool MetastoreProxy::resetAndLockConflictPartsInKV(const String & name_space, const String & uuid, const Strings & parts, const Strings & expected_txnID, const String & txnID)
{

    Strings keys;
    std::transform(
        parts.begin(),
        parts.end(),
        std::back_inserter(keys),
        std::bind(kvLockKey, std::ref(name_space), std::ref(uuid), std::placeholders::_1)
    );

    std::vector<std::pair<uint32_t, String>> cas_failed;

    metastore_ptr->multiPutCAS(keys, txnID, expected_txnID, false, cas_failed);

    return cas_failed.empty();
}

IMetaStore::IteratorPtr MetastoreProxy::getPartitionList(const String & name_space, const String & uuid)
{
    return metastore_ptr->getByPrefix(tablePartitionInfoPrefix(name_space, uuid));
}

void MetastoreProxy::updateTopologyMeta(const String & name_space, const String & topology)
{
    metastore_ptr->put(escapeString(name_space) + "_" + SERVERS_TOPOLOGY_KEY, topology);
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
    auto multi_write = createMultiWrite();
    for (auto & ts : sync_list)
        multi_write.addDelete(syncListKey(name_space, uuid, ts));

    multi_write.commit();
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

void MetastoreProxy::setTableClusterStatus(const String & name_space, const String & uuid, const bool & already_clustered)
{
    metastore_ptr->put(clusterStatusKey(name_space, uuid), already_clustered ? "true" : "false");
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

void MetastoreProxy::prepareAddDeleteBitmaps(const String & name_space, const String & table_uuid,
                                             const DeleteBitmapMetaPtrVector & bitmaps,
                                             MetastoreByteKVImpl::MultiWrite & container,
                                             const std::vector<String> & expected_bitmaps)
{
    size_t expected_bitmaps_size = expected_bitmaps.size();
    if (expected_bitmaps_size > 0 && expected_bitmaps_size != static_cast<size_t>(bitmaps.size()))
        throw Exception("The size of deleted bitmaps wants to write does not match the actual size in catalog", ErrorCodes::LOGICAL_ERROR);

    size_t idx {0};
    for (const auto & dlb_ptr : bitmaps)
    {
        const Protos::DataModelDeleteBitmap & model = *(dlb_ptr->getModel());
        if (expected_bitmaps_size == 0)
            container.addPut(deleteBitmapKey(name_space, table_uuid, model), model.SerializeAsString());
        else
            container.addPut(deleteBitmapKey(name_space, table_uuid, model), model.SerializeAsString(), expected_bitmaps[idx]);

        ++idx;
    }
}

void MetastoreProxy::addDeleteBitmaps(const String & name_space, const String & table_uuid, const DeleteBitmapMetaPtrVector & bitmaps)
{
    auto multi_write = createMultiWrite();
    prepareAddDeleteBitmaps(name_space, table_uuid, bitmaps, multi_write);
    multi_write.commit();
}

void MetastoreProxy::removeDeleteBitmaps(const String & name_space, const String & table_uuid, const DeleteBitmapMetaPtrVector & bitmaps)
{
    auto multi_write = createMultiWrite();

    for (const auto & dlb_ptr : bitmaps)
    {
        const Protos::DataModelDeleteBitmap & model = *(dlb_ptr->getModel());
        multi_write.addDelete(deleteBitmapKey(name_space, table_uuid, model));
    }

    multi_write.commit();
}

Strings MetastoreProxy::getDeleteBitmapByKeys(const Strings & keys)
{
    Strings parts_meta;
    auto values = metastore_ptr->multiGet(keys);
    for (auto & ele : values)
        parts_meta.emplace_back(std::move(ele.first));
    return parts_meta;
}

void MetastoreProxy::setCnchLogMetadata(const String & name_space, const String & log_name, const Protos::CnchLogMetadata & metadata)
{
    String value;
    metadata.SerializeToString(&value);

    auto log_key = cnchLogKey(name_space, log_name);
    metastore_ptr->put(log_key, value);
}

void MetastoreProxy::setCnchLogMetadataInBatch(const String &name_space, const Strings &log_names,
                                               const std::vector<Protos::CnchLogMetadata>  &metadata_vec)
{
    if (log_names.size() != metadata_vec.size())
        throw Exception("Unmatched size of keys and values while setCnchLogMetadataInBatch", ErrorCodes::LOGICAL_ERROR);

    auto multi_write = createMultiWrite();
    for (size_t id = 0; id < log_names.size(); ++id)
    {
        String value;
        metadata_vec[id].SerializeToString(&value);
        multi_write.addPut(cnchLogKey(name_space, log_names[id]), value);
    }
    multi_write.commit();
}

std::shared_ptr<Protos::CnchLogMetadata> MetastoreProxy::getCnchLogMetadata(const String & name_space, const String & log_name)
{
    String value;
    auto log_key = cnchLogKey(name_space, log_name);
    metastore_ptr->get(log_key, value);
    if (value.empty())
        return nullptr;

    auto res = std::make_shared<Protos::CnchLogMetadata>();
    res->ParseFromString(value);
    return res;
}

void MetastoreProxy::removeCnchLogMetadata(const String & name_space, const String & log_name)
{
    auto log_key = cnchLogKey(name_space, log_name);
    metastore_ptr->drop(log_key);
}

std::vector<Protos::CnchLogMetadata> MetastoreProxy::getBufferLogMetadataVec([[maybe_unused]]const String & name_space, [[maybe_unused]]const UUID & uuid)
{
    std::vector<Protos::CnchLogMetadata> res;
    ///FIXME: 
    //auto buffer_log_prefix = cnchLogKey(name_space, getCnchLogPrefixForBuffer(uuid));
    String buffer_log_prefix = "mock";
    auto it = metastore_ptr->getByPrefix(buffer_log_prefix);
    while (it->next())
    {
        res.emplace_back();
        res.back().ParseFromString(it->value());
    }

    return res;
}

std::shared_ptr<Protos::BufferManagerMetadata> MetastoreProxy::tryGetBufferManagerMetadata([[maybe_unused]]const String & name_space, [[maybe_unused]]const UUID & uuid)
{
    String value;
    ///FIXME:
    //auto manager_key = cnchLogKey(name_space, getCnchLogNameForBufferManager(uuid));
    String manager_key= "mock";
    metastore_ptr->get(manager_key, value);
    if (value.empty())
        return nullptr;

    auto res = std::make_shared<Protos::BufferManagerMetadata>();
    if (!res->ParseFromString(value))
        throw Exception("Failed to parse metadata of buffer manager", ErrorCodes::LOGICAL_ERROR);
    return res;
}

void MetastoreProxy::setBufferManagerMetadata(
    [[maybe_unused]]const String & name_space, [[maybe_unused]]const UUID & uuid, const Protos::BufferManagerMetadata & metadata)
{
    String value;
    if (!metadata.SerializeToString(&value))
        throw Exception("Failed to serialize metadata of buffer manager to string", ErrorCodes::LOGICAL_ERROR);
    
    ///FIXME:
    //auto manager_key = cnchLogKey(name_space, getCnchLogNameForBufferManager(uuid));
    String manager_key = "mock";
    metastore_ptr->put(manager_key, value);
}

void MetastoreProxy::removeBufferManagerMetadata([[maybe_unused]]const String & name_space, [[maybe_unused]]const UUID & uuid)
{
    ///FIXME:
    //auto manager_key = cnchLogKey(name_space, getCnchLogNameForBufferManager(uuid));
    String manager_key = "mock";
    metastore_ptr->clean(manager_key);
}

void MetastoreProxy::updateResourceGroup(const String & name_space, const String & group_name,  std::shared_ptr<Protos::ResourceGroup> resource_group)
{
    auto group_key = resourceGroupKey(name_space, group_name);
    metastore_ptr->put(group_key, resource_group->SerializeAsString());
}

std::shared_ptr<Protos::ResourceGroup> MetastoreProxy::getResourceGroup(const String & name_space, const String & group_name)
{
    String value;
    auto group_key = resourceGroupKey(name_space, group_name);
    metastore_ptr->get(group_key, value);

    if (value.empty())
        return nullptr;

    auto resource_group = std::make_shared<Protos::ResourceGroup>();

    if (!resource_group->ParseFromString(value))
        throw Exception("Failed to parse resource group from the stored string", ErrorCodes::LOGICAL_ERROR);

    return resource_group;
}

void MetastoreProxy::removeResourceGroup(const String & name_space, const String & group_name)
{
    auto group_key = resourceGroupKey(name_space, group_name);
    metastore_ptr->drop(group_key);
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
    return {version, value};
}

void MetastoreProxy::removeInsertionLabel(const String & name_space, const String & uuid, const String & name, uint64_t expected_version)
{
    auto label_key = insertionLabelKey(name_space, uuid, name);
    metastore_ptr->drop(label_key, expected_version);
}

void MetastoreProxy::removeInsertionLabels(const String & name_space, const std::vector<InsertionLabel> & labels)
{
    auto multi_write = metastore_ptr->createMultiWrite();
    for (auto & label : labels)
        multi_write.addDelete(insertionLabelKey(name_space, toString(label.table_uuid), label.name));
    multi_write.commit();
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

// void MetastoreProxy::updateTableStatistics(const String & name_space, const String & uuid, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data)
// {
//     auto multi_write = createMultiWrite();
//     for (const auto & [tag, statisticPtr] : data)
//     {
//         Protos::TableStatistic table_statistic;
//         table_statistic.set_tag((UInt64)tag);
//         table_statistic.set_timestamp(statisticPtr->getTxnTimestamp().toUInt64());
//         table_statistic.set_blob(statisticPtr->serialize());
//         multi_write.addPut(tableStatisticKey(name_space, uuid, tag), table_statistic.SerializeAsString());
//         multi_write.addPut(tableStatisticTagKey(name_space, uuid, tag), std::to_string((UInt64)tag));
//     }

//     multi_write.commit();
// }

// std::unordered_map<StatisticsTag, StatisticsBasePtr> MetastoreProxy::getTableStatistics(const String & name_space, const String & uuid, const std::unordered_set<StatisticsTag> & tags)
// {
//     Strings keys;
//     keys.reserve(tags.size());
//     for (const auto & tag : tags)
//     {
//         keys.push_back(tableStatisticKey(name_space, uuid, tag));
//     }
//     auto values = metastore_ptr->multiGet(keys);
//     std::unordered_map<StatisticsTag, StatisticsBasePtr> res;
//     for (const auto & value : values)
//     {
//         if (value.first.empty())
//             continue;
//         Protos::TableStatistic table_statistic;
//         table_statistic.ParseFromString(value.first);
//         StatisticsTag tag = static_cast<StatisticsTag>(table_statistic.tag());
//         TxnTimestamp ts(table_statistic.timestamp());
//         auto statisticPtr = createStatisticsBase(tag, ts, table_statistic.blob());
//         res.emplace(tag, statisticPtr);
//     }

//     return res;
// }

// std::unordered_set<StatisticsTag> MetastoreProxy::getAvailableTableStatisticsTags(const String & name_space, const String & uuid)
// {
//     std::unordered_set<StatisticsTag> res;
//     auto it = metastore_ptr->getByPrefix(tableStatisticTagPrefix(name_space, uuid));
//     while (it->next())
//     {
//         res.emplace(static_cast<StatisticsTag>(std::stoull(it->value())));
//     }
//     return res;
// }

// void MetastoreProxy::removeTableStatistics(const String & name_space, const String & uuid, const std::unordered_set<StatisticsTag> & tags)
// {
//     auto multi_write = createMultiWrite();
//     for (const auto & tag : tags)
//     {
//         multi_write.addDelete(tableStatisticKey(name_space, uuid, tag));
//         multi_write.addDelete(tableStatisticTagKey(name_space, uuid, tag));
//     }
//     multi_write.commit();
// }

// void MetastoreProxy::updateColumnStatistics(const String & name_space, const String & uuid, const String & column, const std::unordered_map<StatisticsTag, StatisticsBasePtr> & data)
// {
//     auto multi_write = createMultiWrite();
//     for (const auto & [tag, statisticPtr] : data)
//     {
//         Protos::ColumnStatistic column_statistic;
//         column_statistic.set_tag((UInt64)tag);
//         column_statistic.set_timestamp(statisticPtr->getTxnTimestamp().toUInt64());
//         column_statistic.set_column(column);
//         column_statistic.set_blob(statisticPtr->serialize());
//         multi_write.addPut(columnStatisticKey(name_space, uuid, column, tag), column_statistic.SerializeAsString());
//         multi_write.addPut(columnStatisticTagKey(name_space, uuid, column, tag), std::to_string((UInt64)tag));
//     }

//     multi_write.commit();
// }

// std::unordered_map<StatisticsTag, StatisticsBasePtr> MetastoreProxy::getColumnStatistics(const String & name_space, const String & uuid, const String & column, const std::unordered_set<StatisticsTag> & tags)
// {
//     Strings keys;
//     keys.reserve(tags.size());
//     for (const auto & tag : tags)
//     {
//         keys.push_back(columnStatisticKey(name_space, uuid, column, tag));
//     }
//     auto values = metastore_ptr->multiGet(keys);
//     std::unordered_map<StatisticsTag, StatisticsBasePtr> res;
//     for (const auto & value : values)
//     {
//         if (value.first.empty())
//             continue;
//         Protos::ColumnStatistic column_statistic;
//         column_statistic.ParseFromString(value.first);
//         StatisticsTag tag = static_cast<StatisticsTag>(column_statistic.tag());
//         TxnTimestamp ts(column_statistic.timestamp());
//         auto statisticPtr = createStatisticsBase(tag, ts, column_statistic.blob());
//         res.emplace(tag, statisticPtr);
//     }

//     return res;
// }

// std::unordered_set<StatisticsTag> MetastoreProxy::getAvailableColumnStatisticsTags(const String & name_space, const String & uuid, const String & column)
// {
//     std::unordered_set<StatisticsTag> res;
//     auto it = metastore_ptr->getByPrefix(columnStatisticTagPrefix(name_space, uuid, column));
//     while (it->next())
//     {
//         res.emplace(static_cast<StatisticsTag>(std::stoull(it->value())));
//     }
//     return res;
// }

// void MetastoreProxy::removeColumnStatistics(const String & name_space, const String & uuid, const String & column, const std::unordered_set<StatisticsTag> & tags)
// {
//     auto multi_write = createMultiWrite();
//     for (const auto & tag : tags)
//     {
//         multi_write.addDelete(columnStatisticKey(name_space, uuid, column, tag));
//         multi_write.addDelete(columnStatisticTagKey(name_space, uuid, column, tag));
//     }
//     multi_write.commit();
// }

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

} /// end of namespace Catalog

} /// end of namespace DB

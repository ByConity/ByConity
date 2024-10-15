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

#include <memory>
#include <CloudServices/CnchWorkerResource.h>

#include <CloudServices/CnchCreateQueryHelper.h>
#include <Core/Names.h>
#include <Databases/DatabaseMemory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/formatAST.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Parsers/parseQuery.h>
#include <Storages/ForeignKeysDescription.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/CloudTableDefinitionCache.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/StorageDictCloudMergeTree.h>
#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TABLE_ALREADY_EXISTS;
}

static ASTPtr parseCreateQuery(ContextMutablePtr context, const String & create_query)
{
    const char * begin = create_query.data();
    const char * end = create_query.data() + create_query.size();
    ParserQueryWithOutput parser{end};
    const auto & settings = context->getSettingsRef();
    return parseQuery(parser, begin, end, "CreateCloudTable", settings.max_query_size, settings.max_parser_depth);
}

void CnchWorkerResource::executeCreateQuery(ContextMutablePtr context, const String & create_query, bool skip_if_exists, const ColumnsDescription & object_columns)
{
    LOG_DEBUG(getLogger("WorkerResource"), "start create cloud table {}", create_query);
    auto ast_query = parseCreateQuery(context, create_query);
    auto & ast_create_query = ast_query->as<ASTCreateQuery &>();

    /// set query settings
    /// TODO: can we remove this? i.e., don't rely on create query to pass query setting
    if (ast_create_query.settings_ast)
        InterpreterSetQuery(ast_create_query.settings_ast, context).executeForCurrentContext();

    auto res = createStorageFromQuery(ast_create_query, context);
    if (auto cloud_table = std::dynamic_pointer_cast<StorageCloudMergeTree>(res))
        cloud_table->resetObjectColumns(object_columns);
    res->startup();

    bool throw_if_exists = !ast_create_query.if_not_exists && !skip_if_exists;
    const auto & database_name = ast_create_query.database; // not empty.
    const auto & table_name = ast_create_query.table;
    String tenant_db = formatTenantDatabaseName(database_name);
    insertCloudTable({tenant_db, table_name}, res, context, throw_if_exists);
}

void CnchWorkerResource::executeCacheableCreateQuery(
    ContextMutablePtr context,
    const StorageID & cnch_storage_id,
    const String & definition,
    const String & local_table_name,
    WorkerEngineType engine_type,
    const String & underlying_dictionary_tables,
    const ColumnsDescription & object_columns)
{
    static auto log = getLogger("WorkerResource");

    std::shared_ptr<StorageCloudMergeTree> cached;
    if (auto cache = context->tryGetCloudTableDefinitionCache(); cache)
    {
        auto load = [&]() -> std::shared_ptr<StorageCloudMergeTree>
        {
            auto ast_query = parseCreateQuery(context, definition);
            auto & create_query = ast_query->as<ASTCreateQuery &>();

            replaceCnchWithCloud(
                create_query.storage,
                cnch_storage_id.getDatabaseName(),
                cnch_storage_id.getTableName(),
                engine_type);

            auto table = createStorageFromQuery(create_query, context);
            if (auto cloud_table = std::dynamic_pointer_cast<StorageCloudMergeTree>(table))
                return cloud_table;
            return {};
        };

        cached = cache->getOrSet(CloudTableDefinitionCache::hash(definition), std::move(load)).first;
    }

    StoragePtr res;
    if (cached)
    {
        LOG_DEBUG(log, "Creating cloud table {} from cached template of definition {}", local_table_name, definition);
        StorageID actual_table_id = cached->getStorageID();
        actual_table_id.table_name = local_table_name;

        std::unique_ptr<MergeTreeSettings> new_settings = std::make_unique<MergeTreeSettings>(*cached->getSettings());
        if (!underlying_dictionary_tables.empty())
            new_settings->underlying_dictionary_tables = underlying_dictionary_tables;

        switch (engine_type)
        {
            case WorkerEngineType::CLOUD:
                res = StorageCloudMergeTree::create(
                    actual_table_id,
                    cnch_storage_id.database_name,
                    cnch_storage_id.table_name,
                    *cached->getInMemoryMetadataPtr(),
                    context,
                    /*date_column_name*/ "",
                    cached->getMergingParams(),
                    std::move(new_settings));
                break;
            case WorkerEngineType::DICT:
                /// NOTE: StorageDictCloudMergeTree::create is broken, don't use it
                res = std::make_shared<StorageDictCloudMergeTree>(
                    actual_table_id,
                    cnch_storage_id.database_name,
                    cnch_storage_id.table_name,
                    *cached->getInMemoryMetadataPtr(),
                    context,
                    /*date_column_name*/ "",
                    cached->getMergingParams(),
                    std::move(new_settings));
                break;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown value for engine_type: {}", static_cast<UInt32>(engine_type));
        }

    }
    else /// for cloud table other than CloudMergeTree. e.g., CloudS3, CloudHive, ...
    {
        auto ast_query = parseCreateQuery(context, definition);
        auto & create_query = ast_query->as<ASTCreateQuery &>();

        replaceCnchWithCloud(
            create_query.storage,
            cnch_storage_id.getDatabaseName(),
            cnch_storage_id.getTableName(),
            engine_type);

        create_query.table = local_table_name;
        if (!underlying_dictionary_tables.empty())
            modifyOrAddSetting(create_query, "underlying_dictionary_tables", Field(underlying_dictionary_tables));

        LOG_DEBUG(log, "Creating cloud table {} from rewritted definition {}", local_table_name, serializeAST(create_query));
        res = createStorageFromQuery(create_query, context);
    }

    if (auto cloud_table = std::dynamic_pointer_cast<StorageCloudMergeTree>(res))
        cloud_table->resetObjectColumns(object_columns);
    res->startup();

    auto res_table_id = res->getStorageID();
    insertCloudTable({res_table_id.getDatabaseName(), res_table_id.getTableName()}, res, context, /*throw_if_exists=*/ false);
}

StoragePtr CnchWorkerResource::tryGetTable(const StorageID & table_id, bool load_data_parts) const
{
    String tenant_db = formatTenantDatabaseName(table_id.getDatabaseName());
    StoragePtr res = {};

    {
        auto lock = getLock();
        auto it = cloud_tables.find({tenant_db, table_id.getTableName()});
        if (it != cloud_tables.end())
            res = it->second;
    }

    if (load_data_parts)
    {
        if (auto cloud_table = dynamic_pointer_cast<StorageCloudMergeTree>(res))
            cloud_table->prepareDataPartsForRead();
    }

    return res;
}

DatabasePtr CnchWorkerResource::getDatabase(const String & database_name) const
{
    String tenant_db = formatTenantDatabaseName(database_name);
    auto lock = getLock();

    auto it = memory_databases.find(tenant_db);
    if (it != memory_databases.end())
        return it->second;

    return {};
}

void CnchWorkerResource::insertCloudTable(DatabaseAndTableName key, const StoragePtr & storage, ContextPtr context, bool throw_if_exists)
{
    auto & tenant_db = key.first;
    {
        auto lock = getLock();
        bool inserted = cloud_tables.emplace(key, storage).second;
        if (!inserted && throw_if_exists)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists", storage->getStorageID().getFullTableName());
        auto it = memory_databases.find(tenant_db);
        if (it == memory_databases.end())
        {
            DatabasePtr database = std::make_shared<DatabaseMemory>(tenant_db, context->getGlobalContext());
            memory_databases.insert(std::make_pair(tenant_db, std::move(database)));
        }
    }

    static auto log = getLogger("WorkerResource");
    LOG_DEBUG(log, "Successfully create database {} and table {} {}",
        tenant_db, storage->getName(), storage->getStorageID().getNameForLogs());
}

bool CnchWorkerResource::isCnchTableInWorker(const StorageID & table_id) const
{
    String tenant_db = formatTenantDatabaseName(table_id.getDatabaseName());
    auto lock = getLock();
    return cnch_tables.find({tenant_db, table_id.getTableName()}) != cnch_tables.end();
}

void CnchWorkerResource::clearResource()
{
    auto lock = getLock();
    for (const auto & table : cloud_tables)
        table.second->shutdown();
    cloud_tables.clear();
    memory_databases.clear();
    cnch_tables.clear();
    worker_table_names.clear();
}

}

/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Storages/System/StorageSystemMutations.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Common/Status.h>


namespace DB
{


NamesAndTypesList StorageSystemMutations::getNamesAndTypes()
{
    return {
        { "database",                   std::make_shared<DataTypeString>() },
        { "table",                      std::make_shared<DataTypeString>() },
        { "mutation_id",                std::make_shared<DataTypeString>() },
        { "query_id",                   std::make_shared<DataTypeString>() },
        { "command",                    std::make_shared<DataTypeString>() },
        { "create_time",                std::make_shared<DataTypeDateTime>() },
        { "cnch",                       std::make_shared<DataTypeUInt8>() },
        { "block_numbers.partition_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "block_numbers.number",       std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()) },
        { "parts_to_do_names",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "parts_to_do",                std::make_shared<DataTypeInt64>() },
        { "is_done",                    std::make_shared<DataTypeUInt8>() },
        { "latest_failed_part",         std::make_shared<DataTypeString>() },
        { "latest_fail_time",           std::make_shared<DataTypeDateTime>() },
        { "latest_fail_reason",         std::make_shared<DataTypeString>() },
    };
}


void StorageSystemMutations::fillCnchData(MutableColumns & res_columns, ContextPtr context, const ASTPtr & query)
{
    auto all_tables = context->getCnchCatalog()->getAllTables();

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();
    MutableColumnPtr col_uuid_mut = ColumnUUID::create();

    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);
    const auto tenant_id = getCurrentTenantId();

    for (auto & table: all_tables)
    {
        if (Status::isDeleted(table.status()))
            continue;

        if (!tenant_id.empty() && !isTenantMatchedEntityName(table.database(), tenant_id))
        {
            continue;
        }
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, table.database());
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, table.database(), table.name()))
            continue;

        col_database_mut->insert(getOriginalEntityName(table.database(), tenant_id));
        col_table_mut->insert(table.name());
        col_uuid_mut->insert(RPCHelpers::createUUID(table.uuid()));
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);
    ColumnPtr col_uuid = std::move(col_uuid_mut);

    /// Determine what tables are needed by the conditions in the query.
    Block filtered_block
    {
        { col_database, std::make_shared<DataTypeString>(), "database" },
        { col_table, std::make_shared<DataTypeString>(), "table" },
        { col_uuid, std::make_shared<DataTypeUUID>(), "uuid" },
    };

    VirtualColumnUtils::filterBlockWithQuery(query, filtered_block, context);

    if (!filtered_block.rows())
    {
        LOG_DEBUG(getLogger(__PRETTY_FUNCTION__), "No need to process any tables.");
        return ;
    }

    std::unordered_set<UUID> table_uuids;

    col_uuid = filtered_block.getByName("uuid").column;
    for (size_t i = 0; i < col_uuid->size(); ++i)
    {
        table_uuids.insert((*col_uuid)[i].safeGet<UUID>());
    }

    if (context->getSettingsRef().system_mutations_only_basic_info || !tenant_id.empty())
    {
        std::unordered_map<UUID, std::pair<String, String>> uuid_to_db_table;
        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
        for (size_t i = 0; i < col_uuid->size(); ++i)
        {
            uuid_to_db_table[(*col_uuid)[i].safeGet<UUID>()] = {(*col_database)[i].safeGet<String>(), (*col_table)[i].safeGet<String>()};
        }

        auto all_mutations = context->getCnchCatalog()->getAllMutations();
        for (const auto & [uuid_str, mutation_str] : all_mutations)
        {
            const auto & uuid = UUIDHelpers::toUUID(uuid_str);
            if (!uuid_to_db_table.contains(uuid))
                continue;

            const auto & db = uuid_to_db_table[uuid].first;
            const auto & table = uuid_to_db_table[uuid].second;

            try
            {
                auto mutation = CnchMergeTreeMutationEntry::parse(mutation_str);
                auto command_str = serializeAST(*mutation.commands.ast(), true);

                size_t col_num = 0;
                if (tenant_id.empty())
                    res_columns[col_num++]->insert(db);
                else
                    res_columns[col_num++]->insert(getOriginalEntityName(db, tenant_id));
                res_columns[col_num++]->insert(table);
                res_columns[col_num++]->insert(mutation.txn_id.toString());
                res_columns[col_num++]->insert(mutation.query_id);
                res_columns[col_num++]->insert(command_str);
                res_columns[col_num++]->insert(mutation.commit_time.toSecond());

                res_columns[col_num++]->insert(1); /// cnch = true
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Error when parse mutation: " + mutation_str);
            }
        }

        return;
    }

    auto all_statuses = context->collectMutationStatusesByTables(std::move(table_uuids));

    for (auto & [storage_id, status]: all_statuses)
    {
        size_t col_num = 0;

        if (tenant_id.empty())
            res_columns[col_num++]->insert(storage_id.database_name);
        else
            res_columns[col_num++]->insert(getOriginalEntityName(storage_id.database_name, tenant_id)); // database
        res_columns[col_num++]->insert(storage_id.table_name);      // table
        res_columns[col_num++]->insert(status.id);                  // mutation_id
        res_columns[col_num++]->insert(status.query_id);            // query_id
        res_columns[col_num++]->insert(status.command);             // command
        res_columns[col_num++]->insert(status.create_time);         // create_time

        /// TODO: update mutation status in memory
        res_columns[col_num++]->insert(1);                          // cnch = true
        res_columns[col_num++]->insertDefault();                    // block_numbers.partition_id
        res_columns[col_num++]->insertDefault();                    // block_numbers.number
        res_columns[col_num++]->insertDefault();                    // parts_to_do_names
        res_columns[col_num++]->insert(status.parts_to_do);         // parts_to_do
        res_columns[col_num++]->insert(status.is_done);             // is_done
        res_columns[col_num++]->insertDefault();                    // latest_failed_part
        res_columns[col_num++]->insertDefault();                    // latest_fail_time
        res_columns[col_num++]->insertDefault();                    // latest_fail_reason
    }
}

void StorageSystemMutations::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    if (context->getServerType() == ServerType::cnch_server)
    {
        fillCnchData(res_columns, context, query_info.query);
        return;
    }

    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// Collect a set of *MergeTree tables.
    std::map<String, std::map<String, StoragePtr>> merge_tree_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(context))
    {
        /// Check if database can contain MergeTree tables
        if (!db.second->canContainMergeTreeTables())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const MergeTreeData *>(table.get()))
                continue;

            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;

            merge_tree_tables[db.first][iterator->name()] = table;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : merge_tree_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database = std::move(col_database_mut);
    ColumnPtr col_table = std::move(col_table_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database, std::make_shared<DataTypeString>(), "database" },
            { col_table, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
    }

    for (size_t i_storage = 0; i_storage < col_database->size(); ++i_storage)
    {
        auto database = (*col_database)[i_storage].safeGet<String>();
        auto table = (*col_table)[i_storage].safeGet<String>();

        std::vector<MergeTreeMutationStatus> statuses;
        {
            const IStorage * storage = merge_tree_tables[database][table].get();
            if (const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage))
                statuses = merge_tree->getMutationsStatus();
        }

        for (const MergeTreeMutationStatus & status : statuses)
        {
            Array block_partition_ids;
            block_partition_ids.reserve(status.block_numbers.size());
            Array block_numbers;
            block_numbers.reserve(status.block_numbers.size());
            for (const auto & pair : status.block_numbers)
            {
                block_partition_ids.emplace_back(pair.first);
                block_numbers.emplace_back(pair.second);
            }
            Array parts_to_do_names;
            parts_to_do_names.reserve(status.parts_to_do_names.size());
            for (const String & part_name : status.parts_to_do_names)
                parts_to_do_names.emplace_back(part_name);

            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);

            res_columns[col_num++]->insert(status.id);
            res_columns[col_num++]->insert(status.query_id);
            res_columns[col_num++]->insert(status.command);
            res_columns[col_num++]->insert(static_cast<UInt64>(status.create_time));
            res_columns[col_num++]->insert(0);                          // cnch = false
            res_columns[col_num++]->insert(block_partition_ids);
            res_columns[col_num++]->insert(block_numbers);
            res_columns[col_num++]->insert(parts_to_do_names);
            res_columns[col_num++]->insert(parts_to_do_names.size());
            res_columns[col_num++]->insert(status.is_done);
            res_columns[col_num++]->insert(status.latest_failed_part);
            res_columns[col_num++]->insert(static_cast<UInt64>(status.latest_fail_time));
            res_columns[col_num++]->insert(status.latest_fail_reason);
        }
    }
}

}

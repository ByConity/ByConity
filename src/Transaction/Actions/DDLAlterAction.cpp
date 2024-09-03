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

#include <Transaction/Actions/DDLAlterAction.h>

#include <Catalog/Catalog.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/StorageCnchMergeTree.h>
#include "Storages/MutationCommands.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void DDLAlterAction::setNewSchema(const String & schema_)
{
    new_schema = schema_;
}

void DDLAlterAction::setOldSchema(const String & schema_)
{
    old_schema = schema_;
}

void DDLAlterAction::setMutationCommands(MutationCommands commands)
{
    /// Sanity check. Avoid mixing other commands with recluster command.
    if (commands.size() > 1)
    {
        for (auto & cmd : commands)
        {
            if (cmd.type == MutationCommand::Type::MODIFY_CLUSTER_BY)
                throw Exception("Cannot modify cluster by definition and other table schema together.", ErrorCodes::LOGICAL_ERROR);
        }
    }
    mutation_commands = std::move(commands);
}

void DDLAlterAction::executeV1(TxnTimestamp commit_time)
{
    /// In DDLAlter, we only update schema.
    LOG_DEBUG(log, "Wait for change schema in Catalog.");
    auto catalog = global_context.getCnchCatalog();
    bool is_modify_cluster_by = false;
    /// only used for materialized mysql
    if (params.is_database)
    {
        // updateTsCache(params.storage_id.uuid, commit_time);
        catalog->alterDatabase(params.storage_id.database_name, txn_id, commit_time, params.statement, params.engine_name);
        return;
    }
    try
    {
        if (!mutation_commands.empty())
        {
            final_mutation_entry.emplace();
            final_mutation_entry->txn_id = txn_id;
            final_mutation_entry->query_id = query_id;
            final_mutation_entry->commit_time = commit_time;
            final_mutation_entry->commands = mutation_commands;
            final_mutation_entry->columns_commit_time = mutation_commands.changeSchema() ? commit_time : table->commit_time;

            // Don't create mutation task for reclustering. It will manually triggered by user
            is_modify_cluster_by = final_mutation_entry->isModifyClusterBy();
            if (!is_modify_cluster_by)
                catalog->createMutation(table->getStorageID(), final_mutation_entry->txn_id.toString(), final_mutation_entry->toString());
            LOG_DEBUG(log, "Created mutation entry in Catalog: {}", final_mutation_entry->toString());
        }

        // auto cache = global_context.getMaskingPolicyCache();
        // table->checkMaskingPolicy(*cache);

        // updateTsCache(table->getStorageUUID(), commit_time);
        if (!new_schema.empty() && new_schema!=old_schema)
        {
            catalog->alterTable(*getContext(), query_settings, table, new_schema, table->commit_time, txn_id, commit_time, is_modify_cluster_by);
            LOG_DEBUG(log, "Successfully change schema in catalog.");
        }
        else
        {
            LOG_DEBUG(log, "Skip change table schema because {}",
                new_schema.empty() ? "new schema is empty." : ("new schema is the same as old one : " + old_schema));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        catalog->removeMutation(table->getStorageID(), txn_id.toString());
        throw;
    }
}

void DDLAlterAction::updatePartData(MutableMergeTreeDataPartCNCHPtr part, TxnTimestamp commit_time)
{
    part->commit_time = commit_time;
}

#if 0
void DDLAlterAction::updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time)
{
    auto & ts_cache_manager = global_context.getCnchTransactionCoordinator().getTsCacheManager();
    auto table_guard = ts_cache_manager.getTimestampCacheTableGuard(uuid);
    auto & ts_cache = ts_cache_manager.getTimestampCacheUnlocked(uuid);
    ts_cache->insertOrAssign(UUIDHelpers::UUIDToString(uuid), commit_time);
}
#endif

}

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

#include <Common/Logger.h>
#include <Storages/MergeTree/CnchMergeTreeMutationEntry.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MutationCommands.h>
#include <Transaction/Actions/IAction.h>
#include <Core/Settings.h>

namespace DB
{

struct AlterDatabaseActionParams
{
    StorageID storage_id;
    String statement;
    bool is_database = false;
    String engine_name;
};

class DDLAlterAction : public IAction
{
public:
    DDLAlterAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, StoragePtr table_, const Settings & query_settings_, const String & query_id_)
        : IAction(query_context_, txn_id_),
        log(getLogger("AlterAction")),
        table(std::move(table_)),
        query_settings(query_settings_),
        params{table->getStorageID(), "fake_statement", false, ""},
        query_id(query_id_)
    {
    }

    DDLAlterAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, AlterDatabaseActionParams params_, const Settings & query_settings_)
        : IAction(query_context_, txn_id_),
        log(getLogger("AlterAction")),
        query_settings(query_settings_),
        params(std::move(params_))
    {
    }

    ~DDLAlterAction() override = default;

    /// TODO: versions
    void setOldSchema(const String & shcema_);
    void setNewSchema(const String & schema_);
    String getNewSchema() const { return new_schema; }
    std::optional<CnchMergeTreeMutationEntry> getMutationEntry() const { return final_mutation_entry; }

    void setMutationCommands(MutationCommands commands);
    const MutationCommands & getMutationCommands() const { return mutation_commands; }

    void executeV1(TxnTimestamp commit_time) override;

private:
    // void updateTsCache(const UUID & uuid, const TxnTimestamp & commit_time) override;
    void appendPart(MutableMergeTreeDataPartCNCHPtr part);
    static void updatePartData(MutableMergeTreeDataPartCNCHPtr part, TxnTimestamp commit_time);

    // return if the DDL will change table schema.
    bool changeSchema() const;

    LoggerPtr log;
    const StoragePtr table;

    String old_schema;
    String new_schema;
    MutationCommands mutation_commands;
    std::optional<CnchMergeTreeMutationEntry> final_mutation_entry;
    Settings query_settings;
    AlterDatabaseActionParams params;
    const String query_id;
};

using DDLAlterActionPtr = std::shared_ptr<DDLAlterAction>;

}

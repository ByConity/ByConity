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

#include <Core/UUID.h>
#include <Parsers/ASTRenameQuery.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Transaction/Actions/IAction.h>
#include <variant>

namespace DB
{

struct RenameDatabaseParams
{
    UUID uuid = UUIDHelpers::Nil;
    String old_name;
    String new_name;
};

struct RenameTableParams
{
    String from_database;
    StoragePtr from_storage;
    String to_database;
    String to_table;
    // needed to update db_uuid in TableIdentifier
    UUID to_database_uuid = UUIDHelpers::Nil;
};

using RenameActionParams = std::variant<RenameDatabaseParams, RenameTableParams>;

class DDLRenameAction : public IAction
{
public:
    DDLRenameAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, RenameActionParams params_)
        : IAction(query_context_, txn_id_), params(std::move(params_))
    {}

    ~DDLRenameAction() override = default;

    void executeV1(TxnTimestamp commit_time) override;

private:
    RenameActionParams params;
};

using DDLRenameActionPtr = std::shared_ptr<DDLRenameAction>;

}

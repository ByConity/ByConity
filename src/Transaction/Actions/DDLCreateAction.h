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

#include <variant>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Transaction/Actions/IAction.h>

namespace DB
{
struct CreateDatabaseParams
{
    String name;
    UUID uuid;
    String statement; // used by CnchMaterializedMySQL
    String engine_name;
    enum TextCaseOption text_case_option = TextCaseOption::MIXED;
};

struct CreateSnapshotParams
{
    UUID db_uuid;
    String name;
    int ttl_in_days;
    UUID bind_table;
};

struct CreateDictionaryParams
{
    StorageID storage_id;
    String statement;
    bool attach = false;
};

struct CreateTableParams
{
    UUID db_uuid = UUIDHelpers::Nil;
    StorageID storage_id;
    String statement;
    bool attach = false;
};

using CreateActionParams = std::variant<CreateDatabaseParams, CreateSnapshotParams, CreateDictionaryParams, CreateTableParams>;

class DDLCreateAction : public IAction
{

public:
    DDLCreateAction(const ContextPtr & query_context_, const TxnTimestamp & txn_id_, CreateActionParams params_)
        : IAction(query_context_, txn_id_), params(std::move(params_))
    {}

    ~DDLCreateAction() override = default;

    void executeV1(TxnTimestamp commit_time) override;
    void abort() override;

private:
    CreateActionParams params;
};

using DDLCreateActionPtr = std::shared_ptr<DDLCreateAction>;

}




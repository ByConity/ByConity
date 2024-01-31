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

#include "DDLCreateAction.h"

#include <Catalog/Catalog.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow"
void DDLCreateAction::executeV1(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr catalog = global_context.getCnchCatalog();

    if (auto * p = std::get_if<CreateDatabaseParams>(&params))
    {
        catalog->createDatabase(p->name, p->uuid, txn_id, commit_time, p->statement, p->engine_name, p->text_case_option);
    }
    else if (auto * p = std::get_if<CreateSnapshotParams>(&params))
    {
        catalog->createSnapshot(p->db_uuid, p->name, commit_time, p->ttl_in_days, p->bind_table);
    }
    else if (auto * p = std::get_if<CreateDictionaryParams>(&params))
    {
        if (p->attach)
            catalog->attachDictionary(p->storage_id.database_name, p->storage_id.table_name);
        else
            catalog->createDictionary(p->storage_id, p->statement);
    }
    else if (auto * p = std::get_if<CreateTableParams>(&params))
    {
        if (p->attach)
            catalog->attachTable(p->storage_id.database_name, p->storage_id.table_name, commit_time);
        else
            catalog->createTable(p->db_uuid, p->storage_id, p->statement, /*vw*/ "", txn_id, commit_time);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown create action with index {}", params.index());
    }
}
#pragma clang diagnostic pop

void DDLCreateAction::abort() {}

}

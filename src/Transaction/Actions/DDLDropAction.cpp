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

#include <Catalog/Catalog.h>
#include <Transaction/Actions/DDLDropAction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow"
void DDLDropAction::executeV1(TxnTimestamp commit_time)
{
    Catalog::CatalogPtr catalog = global_context.getCnchCatalog();

    if (auto * p = std::get_if<DropDatabaseParams>(&params))
    {
        catalog->dropDatabase(p->name, p->prev_version, txn_id, commit_time);
    }
    else if (auto * p = std::get_if<DropSnapshotParams>(&params))
    {
        catalog->removeSnapshot(p->db_uuid, p->name);
    }
    else if (auto * p = std::get_if<DropDictionaryParams>(&params))
    {
        if (p->is_detach)
            catalog->detachDictionary(p->database, p->name);
        else
            catalog->dropDictionary(p->database, p->name);
    }
    else if (auto * p = std::get_if<DropTableParams>(&params))
    {
        if (p->is_detach)
            catalog->detachTable(p->storage->getDatabaseName(), p->storage->getTableName(), commit_time);
        else
            catalog->dropTable(p->db_uuid, p->storage, p->prev_version, txn_id, commit_time);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown drop action with index {}", params.index());
    }
}
#pragma clang diagnostic pop

}

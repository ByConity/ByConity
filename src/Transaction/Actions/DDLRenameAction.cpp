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

#include "DDLRenameAction.h"

#include <Catalog/Catalog.h>
// #include <DaemonManager/DaemonManagerClient.h>
#include <Parsers/ASTDropQuery.h>
// #include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
// #include <Storages/StorageCnchMergeTree.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow"
void DDLRenameAction::executeV1(TxnTimestamp commit_time)
{
    auto local_context = getContext();
    auto catalog = local_context->getCnchCatalog();

    if (auto * p = std::get_if<RenameDatabaseParams>(&params))
    {
        catalog->renameDatabase(p->uuid, p->old_name, p->new_name, txn_id, commit_time);
    }
    else if (auto * p = std::get_if<RenameTableParams>(&params))
    {
        if (!dynamic_cast<const StorageCnchMergeTree *>(p->from_storage.get()))
            throw Exception("Only CnchMergeTree are supported to rename now", ErrorCodes::LOGICAL_ERROR);

        catalog->renameTable(p->from_database, p->from_storage->getTableName(), p->to_database, p->to_table, p->to_database_uuid, txn_id, commit_time);

        // check after rename
        auto new_storage = catalog->tryGetTable(*local_context, p->to_database, p->to_table, commit_time);
        if (!new_storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get table {}.{} after renaming", p->to_database, p->to_table);
        if (p->from_storage->getStorageUUID() != new_storage->getStorageUUID())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "UUID mismatch after reame, old={}, new={}",
                UUIDHelpers::UUIDToString(p->from_storage->getStorageUUID()),
                UUIDHelpers::UUIDToString(new_storage->getStorageUUID()));
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown rename action with index {}", params.index());
    }
}
#pragma clang diagnostic pop

}

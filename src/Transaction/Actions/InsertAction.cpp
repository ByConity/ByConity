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

#include "InsertAction.h"

#include <Catalog/Catalog.h>
#include <CloudServices/CnchDedupHelper.h>
#include <Common/Exception.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>
#include <Storages/StorageCnchMergeTree.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void InsertAction::appendPart(MutableMergeTreeDataPartCNCHPtr part)
{
    parts.push_back(std::move(part));
}

void InsertAction::appendDeleteBitmap(DeleteBitmapMetaPtr delete_bitmap)
{
    delete_bitmaps.push_back(std::move(delete_bitmap));
}

void InsertAction::executeV1(TxnTimestamp commit_time)
{
    if (!staged_parts.empty())
        throw Exception("Transaction v1 API is not supported for staged parts", ErrorCodes::LOGICAL_ERROR);

    /// currently we wait for all parts ready before committing to avoid the rename step.
    /// Even support partial commmit, we can still avoid rename, discussion here
    /// https://forums.foundationdb.org/t/way-to-rename-key/2142

    auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!cnch_table)
        throw Exception("CNCH table ptr is null in INSERT Action", ErrorCodes::LOGICAL_ERROR);

    String log_table_name = table->getDatabaseName() + "." + table->getTableName();

    /// Set the commit time of parts and delete_bitmaps must be set, otherwise they are invisible.
    for (auto & part : parts)
        part->commit_time = commit_time;

    for (auto & bitmap : delete_bitmaps)
        bitmap->updateCommitTime(commit_time);

    auto catalog = global_context.getCnchCatalog();
    bool write_manifest = cnch_table->getSettings()->enable_publish_version_on_commit;
    catalog->finishCommit(table, txn_id, commit_time, {parts.begin(), parts.end()}, delete_bitmaps, false, /*preallocate_mode=*/ false, write_manifest);
    ServerPartLog::addNewParts(getContext(), table->getStorageID(), ServerPartLogElement::INSERT_PART, parts, {},
                                txn_id, /*error=*/ false, {}, 0, 0, from_attach);
}

void InsertAction::executeV2()
{
    if (executed)
        return;

    executed = true;
    auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!cnch_table)
        throw Exception("Expected StorageCnchMergeTree, but got: " + table->getName(), ErrorCodes::LOGICAL_ERROR);

    auto catalog = global_context.getCnchCatalog();
    bool write_manifest = cnch_table->getSettings()->enable_publish_version_on_commit;
    catalog->writeParts(table, txn_id, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, {staged_parts.begin(), staged_parts.end()}}, false, /*preallocate_mode=*/ false, write_manifest);

    if (table && table->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
        catalog->appendObjectPartialSchema(table, txn_id, parts);
}

/// Post progressing
void InsertAction::postCommit(TxnTimestamp commit_time)
{
    /// set commit time for part and bitmaps
    global_context.getCnchCatalog()->setCommitTime(
        table, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, {staged_parts.begin(), staged_parts.end()}}, commit_time, txn_id);

    for (auto & part : parts)
        part->commit_time = commit_time;

    /// set commit flag for dynamic object column schema
    if (table && table->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
        global_context.getCnchCatalog()->commitObjectPartialSchema(txn_id);

    ServerPartLog::addNewParts(getContext(), table->getStorageID(), ServerPartLogElement::INSERT_PART, parts, staged_parts,
                                txn_id, /*error=*/ false, {}, 0, 0, from_attach);
}

void InsertAction::abort()
{
    /// clear parts in kv
    /// skip part cache to avoid blocking by write lock of part cache for long time
    global_context.getCnchCatalog()->clearParts(table, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, {staged_parts.begin(), staged_parts.end()}});

    /// set commit flag for dynamic object column schema
    if (table && table->getInMemoryMetadataPtr()->hasDynamicSubcolumns())
        global_context.getCnchCatalog()->abortObjectPartialSchema(txn_id);

    ServerPartLog::addNewParts(getContext(), table->getStorageID(), ServerPartLogElement::INSERT_PART, parts, staged_parts,
                                 txn_id, /*error=*/ true, {}, 0, 0, from_attach);
}

UInt32 InsertAction::collectNewParts() const
{
    return collectNewParts(parts);
}

UInt32 InsertAction::collectNewParts(MutableMergeTreeDataPartsCNCHVector const & parts_)
{
    UInt32 size = 0;
    for (const auto & part : parts_)
    {
        if (part->info.level == 0)
            size += 1;
    }
    return size;
}

std::set<Int64> InsertAction::getBucketNumbers() const
{
    std::set<Int64> res;
    for (const auto & part : parts)
    {
        if (!res.contains(part->bucket_number))
            res.insert(part->bucket_number);
    }
    return res;
}

void InsertAction::checkAndSetDedupMode(CnchDedupHelper::DedupMode dedup_mode_)
{
    if (dedup_mode_ >= CnchDedupHelper::DedupMode::UPSERT)
    {
        if (!table->getInMemoryMetadataPtr()->hasUniqueKey())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Table {} is not unique engine, but dedup mode is {}, it's a bug!",
                table->getCnchStorageID().getNameForLogs(),
                typeToString(dedup_mode_));

        if (!staged_parts.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Dedup mode is {}, but staged parts are not empty for table {}, it's a bug!",
                table->getCnchStorageID().getNameForLogs(),
                typeToString(dedup_mode_));

        LOG_TRACE(log, "Table {} is in {} mode.", table->getCnchStorageID().getNameForLogs(), typeToString(dedup_mode_));
    }
    dedup_mode = dedup_mode_;
}

CnchDedupHelper::DedupTaskPtr InsertAction::getDedupTask() const
{
    /// If parts are empty or dedup mode is append, just skip dedup stage.
    if (dedup_mode == CnchDedupHelper::DedupMode::APPEND || parts.empty())
        return nullptr;

    auto dedup_task =  std::make_shared<CnchDedupHelper::DedupTask>(dedup_mode, table->getCnchStorageID());
    dedup_task->new_parts = parts;
    dedup_task->delete_bitmaps_for_new_parts = delete_bitmaps;
    return dedup_task;
}
}

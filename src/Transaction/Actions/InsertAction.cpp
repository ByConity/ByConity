#include "InsertAction.h"

#include <Catalog/Catalog.h>
// #include <MergeTreeCommon/CnchWorkerClientPools.h>
// #include <MergeTreeCommon/commitCnchParts.h>
// #include <Storages/StorageCnchMergeTree.h>
// #include <Interpreters/ServerPartLog.h>

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

    // auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    // if (!cnch_table)
    //     throw Exception("CNCH table ptr is null in INSERT Action", ErrorCodes::LOGICAL_ERROR);

    String log_table_name = table->getDatabaseName() + "." + table->getTableName();

    // Set the commit time of parts and delete_bitmaps must be set, otherwise they are invisible.
    for (auto & part : parts)
        part->commit_time = commit_time;

    for (auto & bitmap : delete_bitmaps)
        bitmap->updateCommitTime(commit_time);

    auto catalog = context.getCnchCatalog();
    catalog->finishCommit(table, txn_id, commit_time, {parts.begin(), parts.end()}, delete_bitmaps, false/*, !cnch_table->isOnDemandMode()*/);
    // ServerPartLog::addNewParts(context, ServerPartLogElement::INSERT_PART, parts, txn_id, false);
}

void InsertAction::executeV2()
{
    if (executed)
        return;

    executed = true;
    // auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    // if (!cnch_table)
    //     throw Exception("Expected StorageCnchMergeTree, but got: " + table->getName(), ErrorCodes::LOGICAL_ERROR);

    auto catalog = context.getCnchCatalog();
    catalog->writeParts(table, txn_id, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, {staged_parts.begin(), staged_parts.end()}}, false/*, !cnch_table->isOnDemandMode()*/);
}

/// Post progressing
void InsertAction::postCommit(TxnTimestamp commit_time)
{
    /// set commit time for part and bitmaps
    context.getCnchCatalog()->setCommitTime(
        table, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, {staged_parts.begin(), staged_parts.end()}}, commit_time, txn_id);

    for (auto & part : parts)
        part->commit_time = commit_time;

    // ServerPartLog::addNewParts(context, ServerPartLogElement::INSERT_PART, parts, txn_id, false);
}

void InsertAction::abort()
{
    // clear parts in kv
    // skip part cache to avoid blocking by write lock of part cache for long time
    context.getCnchCatalog()->clearParts(table, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, {staged_parts.begin(), staged_parts.end()}}, true);

    // ServerPartLog::addNewParts(context, ServerPartLogElement::INSERT_PART, parts, txn_id, true);
}

UInt32 InsertAction::collectNewParts() const
{
    return collectNewParts(parts);
}

void InsertAction::updatePartData(MutableMergeTreeDataPartCNCHPtr /* part */, bool /*set_column_mutation*/)
{
    // if (set_column_mutation)
    // {
    //     if (part->prepared_checksums)
    //     {
    //         for (auto & file : part->prepared_checksums->files)
    //             file.second.mutation = part->info.mutation;
    //     }
    //     else
    //     {
    //         throw Exception("part " + part->name + " without prepared checksums", ErrorCodes::LOGICAL_ERROR);
    //     }
    // }
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

}

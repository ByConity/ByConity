#include <Transaction/Actions/MergeMutateAction.h>
#include <Catalog/Catalog.h>
#include <CloudServices/commitCnchParts.h>
#include <Storages/StorageCnchMergeTree.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
}

void MergeMutateAction::appendPart(MutableMergeTreeDataPartCNCHPtr part)
{
    parts.emplace_back(std::move(part));
}

void MergeMutateAction::executeV1(TxnTimestamp commit_time)
{
    auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!cnch_table)
        throw Exception("Expected StorageCnchMergeTree, but got: " + table->getName(), ErrorCodes::LOGICAL_ERROR);

    // Set commit time for parts and bitmap, otherwise they are invisible.
    for (MutableMergeTreeDataPartCNCHPtr & part : parts)
    {
        updatePartData(part, commit_time);
    }

    for (auto & bitmap : delete_bitmaps)
        bitmap->updateCommitTime(commit_time);

    getContext()->getCnchCatalog()->finishCommit(table, txn_id, commit_time, {parts.begin(), parts.end()}, delete_bitmaps, true, /*preallocate_mode=*/ false);
}

void MergeMutateAction::executeV2()
{
    auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!cnch_table)
        throw Exception("Expected StorageCnchMergeTree, but got: " + table->getName(), ErrorCodes::LOGICAL_ERROR);

    getContext()->getCnchCatalog()->writeParts(table, txn_id, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, /*staged_parts*/{}}, true,  /*preallocate_mode=*/ false);
}

/// Post processing
void MergeMutateAction::postCommit(TxnTimestamp commit_time)
{
    /// set commit time for part
    getContext()->getCnchCatalog()->setCommitTime(table, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, /*staged_parts*/{}}, commit_time);
    for (auto & part : parts)
        part->commit_time = commit_time;
}

void MergeMutateAction::abort()
{
    // clear parts in kv
    // skip part cache to avoid blocking by write lock of part cache for long time
    getContext()->getCnchCatalog()->clearParts(table, Catalog::CommitItems{{parts.begin(), parts.end()}, delete_bitmaps, /*staged_parts*/{}}, true);
}

void MergeMutateAction::updatePartData(MutableMergeTreeDataPartCNCHPtr part, [[maybe_unused]] TxnTimestamp commit_time)
{
    part->name = part->info.getPartName();
    part->commit_time = commit_time;
}

}

#pragma once

#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/TxnTimestamp.h>
#include <common/logger_useful.h>

namespace DB
{
class Context;
class MergeTreeMetaBase;

class MergeTreeDataDeduper
{
public:
    enum class VersionMode
    {
        ExplicitVersion,
        PartitionValueAsVersion,
        NoVersion,
    };

    MergeTreeDataDeduper(const MergeTreeMetaBase & data_, ContextPtr context_);

    /// Remove duplicate keys among visible, staged, and uncommitted parts.
    /// Assumes that
    /// 1. All parts themselves don't contain duplicate keys
    /// 2. There is no duplicate keys among visible parts with the help of delete bitmaps
    /// Returns all new delete bitmaps to dump in order to remove duplicated keys.
    /// Visible part may or may not have bitmap to dump, but every staged and uncommitted part
    /// should have one bitmap to dump.
    LocalDeleteBitmaps dedupParts(
        TxnTimestamp txn_id,
        const IMergeTreeDataPartsVector & visible_parts,
        const IMergeTreeDataPartsVector & staged_parts,
        const IMergeTreeDataPartsVector & uncommitted_parts = {});

    LocalDeleteBitmaps repairParts(TxnTimestamp txn_id, IMergeTreeDataPartsVector visible_parts);

private:
    /// Low-level interface to dedup `new_parts` with `visible_parts`.
    /// Return delete bitmaps of input parts to remove duplicate keys.
    /// Size of the result vector is `visible_parts.size() + new_parts.size()`.
    /// The result bitmap(could be nullptr) for visible_parts[i] is stored in res[i].
    /// The result bitmap(could be nullptr) for new_parts[j] is stored in res[visible_parts.size() + j].
    DeleteBitmapVector dedupImpl(const IMergeTreeDataPartsVector & visible_parts, const IMergeTreeDataPartsVector & new_parts);

    DeleteBitmapVector repairImpl(const IMergeTreeDataPartsVector & parts);

    const MergeTreeMetaBase & data;
    ContextPtr context;
    Poco::Logger * log;
    VersionMode version_mode;
};

}

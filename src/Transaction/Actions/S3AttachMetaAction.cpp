/*
 * Copyright 2023 Bytedance Ltd. and/or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <Disks/DiskByteS3.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>
#include <Interpreters/ServerPartLog.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/Actions/S3AttachMetaAction.h>
#include <Transaction/TransactionCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

S3AttachPartsInfo::S3AttachPartsInfo(
    const StoragePtr & from_tbl_,
    const IMergeTreeDataPartsVector & former_parts_,
    const MutableMergeTreeDataPartsCNCHVector & parts_,
    const MutableMergeTreeDataPartsCNCHVector & staged_parts_,
    const DeleteBitmapMetaPtrVector & detached_bitmaps_,
    const DeleteBitmapMetaPtrVector & bitmaps_)
    : from_tbl(from_tbl_)
    , former_parts(former_parts_)
    , parts(parts_)
    , staged_parts(staged_parts_)
    , detached_bitmaps(detached_bitmaps_)
    , bitmaps(bitmaps_)
{
    if (former_parts.size() != parts.size() + staged_parts.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Former part's size {} not equals to parts size {} plus staged parts size {}",
            former_parts.size(),
            parts.size(),
            staged_parts.size());
    }

    if (detached_bitmaps.size() != bitmaps.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Detached bitmaps size {} not equals to new bitmaps size {}",
            detached_bitmaps.size(),
            bitmaps.size());
    }
}
void S3AttachMetaAction::executeV1(TxnTimestamp)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void S3AttachMetaAction::executeV2()
{
    if (executed)
        return;

    Strings detached_part_names;
    detached_part_names.reserve(former_parts.size());
    for (const IMergeTreeDataPartPtr & part : former_parts)
    {
        String part_name = part == nullptr ? "" : part->info.getPartName();
        detached_part_names.push_back(part_name);
        LOG_TRACE(log, "Attach Detached part name:{}", part_name);
    }

    global_context.getCnchCatalog()->attachDetachedParts(
        from_tbl,
        to_tbl,
        detached_part_names,
        {parts.begin(), parts.end()},
        {staged_parts.begin(), staged_parts.end()},
        detached_bitmaps,
        bitmaps,
        txn_id);
    executed = true;
}

void S3AttachMetaAction::abort()
{
    global_context.getCnchCatalog()->detachAttachedParts(
        to_tbl,
        from_tbl,
        {parts.begin(), parts.end()},
        {staged_parts.begin(), staged_parts.end()},
        former_parts,
        bitmaps,
        detached_bitmaps,
        txn_id);

    /// Since bitmaps are all new bitmap that have been regenerated, simply delete it
    for (auto & new_bitmap: bitmaps)
        new_bitmap->removeFile();

    ServerPartLog::addNewParts(getContext(), to_tbl->getStorageID(), ServerPartLogElement::INSERT_PART, parts, staged_parts,
                                 txn_id, /*error=*/ true, {}, 0, 0, from_attach);
}

void S3AttachMetaAction::postCommit(TxnTimestamp commit_time)
{
    global_context.getCnchCatalog()->setCommitTime(
        to_tbl, Catalog::CommitItems{{parts.begin(), parts.end()}, bitmaps, {staged_parts.begin(), staged_parts.end()}}, commit_time);

    for (auto & part : parts)
    {
        part->commit_time = commit_time;
    }

    /// After commit, we need to delete original bitmap files
    for (auto & detached_bitmap: detached_bitmaps)
        detached_bitmap->removeFile();

    ServerPartLog::addNewParts(getContext(), to_tbl->getStorageID(), ServerPartLogElement::INSERT_PART, parts, staged_parts,
                                 txn_id, /*error=*/ false, {}, 0, 0, from_attach);
}

void S3AttachMetaAction::collectUndoResourcesForCommit(const UndoResources & resources, UndoResourceNames & resource_names)
{
    // Committed, only set commit time, it required part name with hint mutation.
    // We store both name and name with hint mutation to avoid parse a data model again.
    // From undo resource, we don't know whether a part is belong to commit part or staged part.
    // Because all part name is unique, we add each part name to both part_names and staged_part_names.
    for (const UndoResource & resource : resources)
    {
        if (resource.type() == UndoResourceType::S3AttachPart)
        {
            resource_names.parts.insert(resource.placeholders(1));
            resource_names.staged_parts.insert(resource.placeholders(1));
        }
        else if (resource.type() == UndoResourceType::S3AttachDeleteBitmap)
        {
            resource_names.bitmaps.insert(resource.placeholders(3));
        }
    }
}

void S3AttachMetaAction::abortByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources)
{
    // Map key is from table's uuid
    // First element of map value is former part name and former part meta(The detached part meta)
    // Second element of map value is new part name(The attached part name)
    std::map<String, std::pair<std::vector<std::pair<String, String>>, std::vector<String>>> part_meta_mapping;
    for (const UndoResource & resource : resources)
    {
        if (resource.type() == UndoResourceType::S3AttachPart)
        {
            const String & from_tbl_uuid = resource.placeholders(0);
            // const String& former_part_name_with_hint = resource.placeholders(1);
            const String & former_part_name = resource.placeholders(2);
            const String & former_part_meta = resource.placeholders(3);
            const String & new_part_name = resource.placeholders(4);

            part_meta_mapping[from_tbl_uuid].first.emplace_back(former_part_name, former_part_meta);
            part_meta_mapping[from_tbl_uuid].second.emplace_back(new_part_name);
        }
    }

    // Map key is from table's uuid
    // First element of map value is former bitmap meta name and former bitmap meta(The detached bitmap meta)
    // Second element of map value is new bitmap meta name(The attached bitmap meta name)
    std::map<String, std::pair<std::vector<std::pair<String, String>>, std::vector<String>>> bitmap_meta_mapping;
    for (const UndoResource & resource : resources)
    {
        if (resource.type() == UndoResourceType::S3AttachDeleteBitmap)
        {
            const String & from_tbl_uuid = resource.placeholders(0);
            const String & former_bitmap_name = resource.placeholders(1);
            const String & former_bitmap_meta = resource.placeholders(2);
            const String & new_bitmap_name = resource.placeholders(3);

            bitmap_meta_mapping[from_tbl_uuid].first.emplace_back(former_bitmap_name, former_bitmap_meta);
            bitmap_meta_mapping[from_tbl_uuid].second.emplace_back(new_bitmap_name);
        }
    }

    auto catalog = ctx.getCnchCatalog();
    for (const auto & entry : part_meta_mapping)
    {
        auto bitmaps_pair = bitmap_meta_mapping[entry.first];
        catalog->detachAttachedPartsRaw(tbl, entry.first, entry.second.second, entry.second.first, bitmaps_pair.second, bitmaps_pair.first);
    }
}

}

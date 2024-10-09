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
#include <Interpreters/Context.h>
#include <Transaction/Actions/S3DetachMetaAction.h>
#include <CloudServices/CnchPartsHelper.h>
namespace DB
{
S3DetachMetaAction::S3DetachMetaAction(
    const ContextPtr & context_,
    const TxnTimestamp & txn_id_,
    const StoragePtr & tbl_,
    const MergeTreeDataPartsCNCHVector & parts_,
    const MergeTreeDataPartsCNCHVector & staged_parts_,
    const DeleteBitmapMetaPtrVector & bitmaps_)
    : IAction(context_, txn_id_), tbl(tbl_), bitmaps(bitmaps_)
{
    parts = CnchPartsHelper::toIMergeTreeDataPartsVector(parts_);
    staged_parts = CnchPartsHelper::toIMergeTreeDataPartsVector(staged_parts_);
}

void S3DetachMetaAction::executeV1(TxnTimestamp)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void S3DetachMetaAction::executeV2()
{
    if (executed)
        return;

    /// In current impl, either parts or staged parts is empty.
    global_context.getCnchCatalog()->detachAttachedParts(
        tbl, tbl, parts, staged_parts, parts.empty() ? staged_parts : parts, {}, bitmaps, txn_id);
    executed = true;
}

void S3DetachMetaAction::abort()
{
    std::vector<String> detached_names;
    detached_names.reserve(parts.size());
    for (const auto & part : parts)
    {
        detached_names.push_back(part->info.getPartName());
    }
    for (const auto & part : staged_parts)
    {
        detached_names.push_back(part->info.getPartName());
    }

    global_context.getCnchCatalog()->attachDetachedParts(tbl, tbl, detached_names, parts, staged_parts, bitmaps, {}, txn_id);

    /// Since bitmaps are all new base bitmap that have been regenerated, simply delete it
    for (const auto & bitmap: bitmaps)
        bitmap->removeFile();
}

void S3DetachMetaAction::postCommit(TxnTimestamp)
{
    // Meta already got renamed, nothing to do, skip
}

void S3DetachMetaAction::commitByUndoBuffer(const Context &, const StoragePtr &, const UndoResources &)
{
    // Transaction got execute, all meta must got renamed, nothing to do, skip
}

void S3DetachMetaAction::abortByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources)
{
    std::vector<String> part_names;
    std::for_each(resources.begin(), resources.end(), [&part_names](const UndoResource & resource) {
        if (resource.type() == UndoResourceType::S3DetachPart)
        {
            part_names.push_back(resource.placeholders(0));
        }
    });
    size_t detached_part_size = part_names.size();

    /// We need to obey the rule that all detached visible parts are before detached staged parts
    std::for_each(resources.begin(), resources.end(), [&part_names](const UndoResource & resource) {
        if (resource.type() == UndoResourceType::S3DetachStagedPart)
        {
            part_names.push_back(resource.placeholders(0));
        }
    });
    size_t detached_staged_part_size = part_names.size() - detached_part_size;

    std::vector<String> bitmap_names;
    std::for_each(resources.begin(), resources.end(), [&bitmap_names](const UndoResource & resource) {
        if (resource.type() == UndoResourceType::S3DetachDeleteBitmap)
        {
            bitmap_names.push_back(resource.placeholders(0));
        }
    });

    ctx.getCnchCatalog()->attachDetachedPartsRaw(tbl, part_names, detached_part_size, detached_staged_part_size, bitmap_names);
}

}

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

#pragma once

#include <Common/Logger.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
struct S3AttachPartsInfo
{
public:
    S3AttachPartsInfo(
        const StoragePtr & from_tbl_,
        const IMergeTreeDataPartsVector & former_parts_,
        const MutableMergeTreeDataPartsCNCHVector & parts_,
        const MutableMergeTreeDataPartsCNCHVector & staged_parts_,
        const DeleteBitmapMetaPtrVector & detached_bitmaps_,
        const DeleteBitmapMetaPtrVector & bitmaps_);

    StoragePtr from_tbl;
    const IMergeTreeDataPartsVector & former_parts;
    const MutableMergeTreeDataPartsCNCHVector & parts;
    const MutableMergeTreeDataPartsCNCHVector & staged_parts;
    const DeleteBitmapMetaPtrVector & detached_bitmaps;
    const DeleteBitmapMetaPtrVector & bitmaps;
};

class S3AttachMetaAction : public IAction
{
public:
    S3AttachMetaAction(
        const ContextPtr & context_, const TxnTimestamp & txn_id_, const StoragePtr & target_tbl, const S3AttachPartsInfo & parts_info_)
        : IAction(context_, txn_id_)
        , from_tbl(parts_info_.from_tbl)
        , to_tbl(target_tbl)
        , former_parts(parts_info_.former_parts)
        , parts(parts_info_.parts)
        , staged_parts(parts_info_.staged_parts)
        , detached_bitmaps(parts_info_.detached_bitmaps)
        , bitmaps(parts_info_.bitmaps)
    {
    }

    virtual void executeV1(TxnTimestamp commit_time) override;
    virtual void executeV2() override;
    virtual void abort() override;
    virtual void postCommit(TxnTimestamp commit_time) override;

    static void collectUndoResourcesForCommit(const UndoResources & resources, UndoResourceNames & resource_names);
    static void abortByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources);

    void setFromAttach() { from_attach = true; }

private:
    StoragePtr from_tbl;
    StoragePtr to_tbl;

    // Former parts, for newly generated part, it's nullptr, for example
    // replace will write a new drop range part, which should be removed when rollback
    IMergeTreeDataPartsVector former_parts;
    MutableMergeTreeDataPartsCNCHVector parts;
    MutableMergeTreeDataPartsCNCHVector staged_parts;

    DeleteBitmapMetaPtrVector detached_bitmaps;
    DeleteBitmapMetaPtrVector bitmaps;

    bool executed{false};
    bool from_attach{false};

    LoggerPtr log{getLogger("S3AttachMetaAction")};
};

}

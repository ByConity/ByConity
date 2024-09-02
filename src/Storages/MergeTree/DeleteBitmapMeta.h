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

#pragma once

#include <assert.h>
#include <memory>
#include <optional>
#include <Protos/data_models.pb.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
class MergeTreeMetaBase;

using DataModelDeleteBitmap = Protos::DataModelDeleteBitmap;
using DataModelDeleteBitmapPtr = std::shared_ptr<Protos::DataModelDeleteBitmap>;
using DataModelDeleteBitmapPtrVector = std::vector<std::shared_ptr<Protos::DataModelDeleteBitmap>>;

class DeleteBitmapMeta;
using DeleteBitmapMetaPtr = std::shared_ptr<DeleteBitmapMeta>;
using DeleteBitmapMetaPtrVector = std::vector<DeleteBitmapMetaPtr>;

/// enum value should match Protos::DataModelDeleteBitmap_Type
enum class DeleteBitmapMetaType
{
    Base = 0,
    Delta = 1,
    Tombstone = 2,
    RangeTombstone = 3
};

class LocalDeleteBitmap
{
public:
    static std::shared_ptr<LocalDeleteBitmap>
    createBase(const MergeTreePartInfo & part_info, const DeleteBitmapPtr & bitmap, UInt64 txn_id, int64_t bucket_number)
    {
        return std::make_shared<LocalDeleteBitmap>(part_info, DeleteBitmapMetaType::Base, txn_id, bitmap, bucket_number);
    }

    static std::shared_ptr<LocalDeleteBitmap>
    createDelta(const MergeTreePartInfo & part_info, const DeleteBitmapPtr & bitmap, UInt64 txn_id, int64_t bucket_number)
    {
        return std::make_shared<LocalDeleteBitmap>(part_info, DeleteBitmapMetaType::Delta, txn_id, bitmap, bucket_number);
    }

    /// If the delta part is small, just create a delta bitmap.
    /// Otherwise union `base_bitmap` and `delta_bitmap` to create a new version of base bitmap.
    /// NOTE: `delta_bitmap` will be modified to be the new base bitmap in the latter case.
    /// REQUIRES: both `base_bitmap` and `delta_bitmap` should be not null
    static std::shared_ptr<LocalDeleteBitmap> createBaseOrDelta(
        const MergeTreePartInfo & part_info,
        const ImmutableDeleteBitmapPtr & base_bitmap,
        const DeleteBitmapPtr & delta_bitmap,
        UInt64 txn_id,
        bool force_create_base_bitmap,
        int64_t bucket_number);

    static std::shared_ptr<LocalDeleteBitmap>
    createTombstone(const MergeTreePartInfo & part_info, UInt64 txn_id, int64_t bucket_number)
    {
        return std::make_shared<LocalDeleteBitmap>(part_info, DeleteBitmapMetaType::Tombstone, txn_id, /*bitmap=*/nullptr, bucket_number);
    }

    static std::shared_ptr<LocalDeleteBitmap>
    createRangeTombstone(const String & partition_id, Int64 max_block, UInt64 txn_id, int64_t bucket_number)
    {
        return std::make_shared<LocalDeleteBitmap>(
            partition_id, 0, max_block, DeleteBitmapMetaType::RangeTombstone, txn_id, /*bitmap=*/nullptr, bucket_number);
    }

    /// Clients should prefer the createXxx static factory method above
    LocalDeleteBitmap(
        const MergeTreePartInfo & part_info,
        DeleteBitmapMetaType type,
        UInt64 txn_id,
        DeleteBitmapPtr bitmap,
        int64_t bucket_number);
    LocalDeleteBitmap(
        const String & partition_id,
        Int64 min_block,
        Int64 max_block,
        DeleteBitmapMetaType type,
        UInt64 txn_id,
        DeleteBitmapPtr bitmap,
        int64_t bucket_number);

    UndoResource getUndoResource(const TxnTimestamp & txn_id, UndoResourceType type = UndoResourceType::DeleteBitmap) const;

    bool canInlineStoreInCatalog() const;

    DeleteBitmapMetaPtr dump(const MergeTreeMetaBase & storage, bool check_dir = true) const;

    /// only for merge task to pre-set commit ts for merged part's base bitmap
    void setCommitTs(UInt64 commit_ts) { model->set_commit_time(commit_ts); }

    const DataModelDeleteBitmapPtr & getModel() const { return model; }

private:
    DataModelDeleteBitmapPtr model;
    DeleteBitmapPtr bitmap;
};

using LocalDeleteBitmapPtr = std::shared_ptr<LocalDeleteBitmap>;
using LocalDeleteBitmaps = std::vector<LocalDeleteBitmapPtr>;

DeleteBitmapMetaPtrVector dumpDeleteBitmaps(const MergeTreeMetaBase & storage, const LocalDeleteBitmaps & temp_bitmaps);

class DeleteBitmapMeta
{
public:
    static constexpr auto kInlineBitmapMaxCardinality = 16;
    static constexpr auto delete_files_dir = "DeleteFiles/";
    static constexpr UInt8 delete_file_meta_format_version = 1;

    static String deleteBitmapDirRelativePath(const String & partition_id);

    static String deleteBitmapFileRelativePath(const Protos::DataModelDeleteBitmap & model);

    DeleteBitmapMeta(const MergeTreeMetaBase & storage_, const DataModelDeleteBitmapPtr & model_) : storage(storage_), model(model_) { }

    ~DeleteBitmapMeta();

    const DataModelDeleteBitmapPtr & getModel() const { return model; }

    void updateCommitTime(const TxnTimestamp & commit_time)
    {
        /// do not update commit time if it has been set.
        /// this deals with the special case for merged part, whose delta bitmaps PImay be committed before the base bitmap,
        /// and we explicit set the base bitmap's commit time to be smaller than all delta bitmaps
        if (model->commit_time() == 0)
            model->set_commit_time(commit_time);
    }

    /**
     * @return If it's stored on remote storage, return the relative path to the file. nullopt otherwise (inline).
     */
    std::optional<String> getFullRelativePath() const;

    void removeFile();

    /// PartitionID_MinBlock_MaxBlock
    String getBlockName() const;

    /// Used for data allocation only
    String getNameForAllocation() const;

    const String & getPartitionID() const { return model->partition_id(); }

    bool sameBlock(const DeleteBitmapMeta & rhs) const
    {
        /* clang-format off */
        return model->partition_id() == rhs.model->partition_id()
            && model->part_min_block() == rhs.model->part_min_block()
            && model->part_max_block() == rhs.model->part_max_block();
        /* clang-format on */
    }

    bool sameBlock(const MergeTreePartInfo & part_info) const
    {
        /* clang-format off */
        return model->partition_id() == part_info.partition_id
            && model->part_min_block() == part_info.min_block
            && model->part_max_block() == part_info.max_block;
        /* clang-format on */
    }

    bool operator<=(const MergeTreePartInfo & part_info) const
    {
        return std::forward_as_tuple(model->partition_id(), model->part_min_block(), model->part_max_block())
            <= std::forward_as_tuple(part_info.partition_id, part_info.min_block, part_info.max_block);
    }

    /// only meta of the same storage can be comparable.
    bool operator<(const DeleteBitmapMeta & rhs) const;

    const DeleteBitmapMetaPtr & tryGetPrevious() const { return prev_meta; }
    void setPrevious(DeleteBitmapMetaPtr prev) { prev_meta = std::move(prev); }

    DeleteBitmapMetaType getType() const { return static_cast<DeleteBitmapMetaType>(model->type()); }
    bool isTombstone() const
    {
        return model->type() == Protos::DataModelDeleteBitmap_Type_Tombstone
            || model->type() == Protos::DataModelDeleteBitmap_Type_RangeTombstone;
    }
    bool isRangeTombstone() const { return model->type() == Protos::DataModelDeleteBitmap_Type_RangeTombstone; }
    /// partial means the meta alone is not complete, need to follow the previous chain
    bool isPartial() const { return model->type() == Protos::DataModelDeleteBitmap_Type_Delta; }

    UInt64 getCommitTime() const { return model->commit_time(); }

    UInt64 getTxnId() const { return model->txn_id(); }

    UInt64 getEndTime() const;
    DeleteBitmapMeta & setEndTime(UInt64 end_time);

    String getNameForLogs() const;

    Int64 bucketNumber() const { return model->bucket_number(); }

    // No actual use. Just required in assignParts
    void setHostPort(const String & disk_cache_host_port_, const String & assign_compute_host_port_) const
    {
        disk_cache_host_port = disk_cache_host_port_;
        assign_compute_host_port = assign_compute_host_port_;
    }
    mutable String disk_cache_host_port;
    mutable String assign_compute_host_port;

private:
    const MergeTreeMetaBase & storage;
    DataModelDeleteBitmapPtr model;
    DeleteBitmapMetaPtr prev_meta;
};

struct LessDeleteBitmapMeta
{
    bool operator()(const DeleteBitmapMetaPtr & lhs, const DeleteBitmapMetaPtr & rhs)
    {
        assert(lhs != nullptr);
        assert(rhs != nullptr);
        return *lhs < *rhs;
    }
};

void deserializeDeleteBitmapInfo(const MergeTreeMetaBase & storage, const DataModelDeleteBitmapPtr & meta, DeleteBitmapPtr & to_bitmap);

String dataModelName(const Protos::DataModelDeleteBitmap & model);
}

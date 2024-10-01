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


#include <unordered_set>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/Coding.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB
{
String dataModelName(const Protos::DataModelDeleteBitmap & model)
{
    std::stringstream ss;
    ss << model.partition_id() << "_" << model.part_min_block() << "_" << model.part_max_block() << "_"
       << model.reserved() << "_" << model.type() << "_" << model.txn_id();
    return ss.str();
}

std::shared_ptr<LocalDeleteBitmap> LocalDeleteBitmap::createBaseOrDelta(
    const MergeTreePartInfo & part_info,
    const ImmutableDeleteBitmapPtr & base_bitmap,
    const DeleteBitmapPtr & delta_bitmap,
    UInt64 txn_id,
    bool force_create_base_bitmap,
    int64_t bucket_number)
{
    if (!delta_bitmap)
        throw Exception("base_bitmap and delta_bitmap cannot be null", ErrorCodes::LOGICAL_ERROR);

    /// In repair mode, base delete bitmap may not exists due to some bugs, just return delta bitmap.
    if (!base_bitmap)
        return std::make_shared<LocalDeleteBitmap>(part_info, DeleteBitmapMetaType::Base, txn_id, delta_bitmap, bucket_number);

    if (!force_create_base_bitmap && delta_bitmap->cardinality() <= DeleteBitmapMeta::kInlineBitmapMaxCardinality)
    {
        return std::make_shared<LocalDeleteBitmap>(part_info, DeleteBitmapMetaType::Delta, txn_id, delta_bitmap, bucket_number);
    }
    else
    {
        *delta_bitmap |= *base_bitmap; // change delta_bitmap to the new base bitmap
        return std::make_shared<LocalDeleteBitmap>(part_info, DeleteBitmapMetaType::Base, txn_id, delta_bitmap, bucket_number);
    }
}

LocalDeleteBitmap::LocalDeleteBitmap(
    const MergeTreePartInfo & info, DeleteBitmapMetaType type, UInt64 txn_id, DeleteBitmapPtr bitmap_, int64_t bucket_number)
    : LocalDeleteBitmap(info.partition_id, info.min_block, info.max_block, type, txn_id, std::move(bitmap_), bucket_number)
{
}

LocalDeleteBitmap::LocalDeleteBitmap(
    const String & partition_id,
    Int64 min_block,
    Int64 max_block,
    DeleteBitmapMetaType type,
    UInt64 txn_id,
    DeleteBitmapPtr bitmap_,
    int64_t bucket_number)
    : model(std::make_shared<Protos::DataModelDeleteBitmap>()), bitmap(std::move(bitmap_))
{
    model->set_partition_id(partition_id);
    model->set_part_min_block(min_block);
    model->set_part_max_block(max_block);
    model->set_type(static_cast<Protos::DataModelDeleteBitmap_Type>(type));
    model->set_txn_id(txn_id);
    model->set_cardinality(bitmap ? bitmap->cardinality() : 0);
    if (bucket_number >= 0)
        model->set_bucket_number(bucket_number);
}

UndoResource LocalDeleteBitmap::getUndoResource(const TxnTimestamp & new_txn_id, UndoResourceType type) const
{
    return UndoResource(new_txn_id, type, dataModelName(*model), DeleteBitmapMeta::deleteBitmapFileRelativePath(*model));
}

bool LocalDeleteBitmap::canInlineStoreInCatalog() const
{
    return !bitmap || bitmap->cardinality() <= DeleteBitmapMeta::kInlineBitmapMaxCardinality;
}

DeleteBitmapMetaPtr LocalDeleteBitmap::dump(const MergeTreeMetaBase & storage, bool check_dir) const
{
    if (bitmap)
    {
        if (model->cardinality() <= DeleteBitmapMeta::kInlineBitmapMaxCardinality)
        {
            String value;
            value.reserve(model->cardinality() * sizeof(UInt32));
            for (auto it = bitmap->begin(); it != bitmap->end(); ++it)
                PutFixed32(&value, *it);
            model->set_inlined_value(std::move(value));
        }
        else
        {
            bitmap->runOptimize();
            size_t size = bitmap->getSizeInBytes();
            PODArray<char> buf(size);
            size = bitmap->write(buf.data());
            {
                DiskPtr disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
                String dir_rel_path = fs::path(storage.getRelativeDataPath(IStorage::StorageLocation::MAIN)) / DeleteBitmapMeta::deleteBitmapDirRelativePath(model->partition_id());
                if (check_dir && !disk->exists(dir_rel_path))
                    disk->createDirectories(dir_rel_path);

                String file_rel_path = fs::path(storage.getRelativeDataPath(IStorage::StorageLocation::MAIN)) / DeleteBitmapMeta::deleteBitmapFileRelativePath(*model);
                auto out = disk->writeFile(file_rel_path);

                out->write(buf.data(), size);
                out->finalize();
            }
            model->set_file_size(size);
            LOG_TRACE(storage.getLogger(), "Dumped delete bitmap {}", dataModelName(*model));
        }
    }
    return std::make_shared<DeleteBitmapMeta>(storage, model);
}

DeleteBitmapMetaPtrVector dumpDeleteBitmaps(const MergeTreeMetaBase & storage, const LocalDeleteBitmaps & temp_bitmaps)
{
    Stopwatch watch;
    DeleteBitmapMetaPtrVector res;
    res.resize(temp_bitmaps.size());

    size_t non_inline_bitmaps = 0;
    for (const auto & temp_bitmap : temp_bitmaps)
    {
        non_inline_bitmaps += (!temp_bitmap->canInlineStoreInCatalog());
    }

    {
        /// Check and create delete bitmap dir
        DiskPtr disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
        std::unordered_set<String> delete_bitmap_dirs;
        for (const auto & temp_bitmap : temp_bitmaps)
        {
            if (!temp_bitmap->canInlineStoreInCatalog())
                delete_bitmap_dirs.emplace(
                    fs::path(storage.getRelativeDataPath(IStorage::StorageLocation::MAIN))
                    / DeleteBitmapMeta::deleteBitmapDirRelativePath(temp_bitmap->getModel()->partition_id()));
        }
        for (const auto & delete_bitmap_dir : delete_bitmap_dirs)
        {
            if (!disk->exists(delete_bitmap_dir))
                disk->createDirectories(delete_bitmap_dir);
        }
    }

    const UInt64 max_dumping_threads = storage.getSettings()->cnch_parallel_dumping_threads;
    if (max_dumping_threads == 0 || non_inline_bitmaps <= 4)
    {
        for (size_t i = 0; i < temp_bitmaps.size(); ++i)
            res[i] = temp_bitmaps[i]->dump(storage, /*check_dir=*/false);
    }
    else
    {
        size_t pool_size = std::min(non_inline_bitmaps, max_dumping_threads);
        ThreadPool pool(pool_size);
        for (size_t i = 0; i < temp_bitmaps.size(); ++i)
        {
            if (temp_bitmaps[i]->canInlineStoreInCatalog())
                res[i] = temp_bitmaps[i]->dump(storage, /*check_dir=*/false);
            else
            {
                pool.scheduleOrThrowOnError([i, &res, &temp_bitmaps, &storage] {
                    /// can avoid locking because different threads are writing different elements
                    /// 1. different threads are writing different elements
                    /// 2. vector is resized in advance, hence no reallocation
                    res[i] = temp_bitmaps[i]->dump(storage, /*check_dir=*/false);
                });
            }
        }
        pool.wait(); /// will rethrow exception if task failed
    }

    LOG_TRACE(
        storage.getLogger(),
        "Dumped {} bitmaps ({} not inline) in {} ms",
        temp_bitmaps.size(),
        non_inline_bitmaps,
        watch.elapsedMilliseconds());
    return res;
}

DeleteBitmapMeta::~DeleteBitmapMeta()
{
    DeleteBitmapMetaPtrVector prev_metas;
    DeleteBitmapMetaPtr current_meta = prev_meta;

    /// Avoid destructor stack overflow with deep nested DeleteBitmapMetaPtr
    while (current_meta)
    {
        prev_metas.push_back(current_meta);
        current_meta = current_meta->prev_meta;
        prev_metas.back()->prev_meta.reset();
    }
}

UInt64 DeleteBitmapMeta::getEndTime() const
{
    return model->has_end_time() ? model->end_time() : 0;
}

DeleteBitmapMeta & DeleteBitmapMeta::setEndTime(UInt64 end_time)
{
    model->set_end_time(end_time);
    return *this;
}

String DeleteBitmapMeta::getNameForLogs() const
{
    return dataModelName(*model);
}

String DeleteBitmapMeta::deleteBitmapDirRelativePath(const String & partition_id)
{
    std::stringstream ss;
    ss << delete_files_dir << partition_id <<  "/";
    return ss.str();
}

String DeleteBitmapMeta::deleteBitmapFileRelativePath(const Protos::DataModelDeleteBitmap & model)
{
    std::stringstream ss;
    ss << deleteBitmapDirRelativePath(model.partition_id()) << model.part_min_block() << "_" << model.part_max_block() << "_"
       << model.reserved() << "_" << model.type() << "_" << model.txn_id() << ".bitmap";
    return ss.str();
}

std::optional<String> DeleteBitmapMeta::getFullRelativePath() const
{
    if (model->has_file_size())
    {
        String rel_file_path = fs::path(storage.getRelativeDataPath(IStorage::StorageLocation::MAIN)) / deleteBitmapFileRelativePath(*model);
        return rel_file_path;
    }
    return std::nullopt;
}

void DeleteBitmapMeta::removeFile()
{
    if (auto path = getFullRelativePath(); path.has_value())
    {
        String rel_file_path = path.value();
        DiskPtr disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
        LOG_TRACE(storage.getLogger(), "Remove delete bitmap file {} if exists", rel_file_path);
        disk->removeFileIfExists(rel_file_path);
    }
}

String DeleteBitmapMeta::getBlockName() const
{
    WriteBufferFromOwnString wb;
    writeString(model->partition_id(), wb);
    writeChar('_', wb);
    writeIntText(model->part_min_block(), wb);
    writeChar('_', wb);
    writeIntText(model->part_max_block(), wb);
    return wb.str();
}

String DeleteBitmapMeta::getNameForAllocation() const
{
    WriteBufferFromOwnString wb;
    writeString(model->partition_id(), wb);
    writeChar('_', wb);
    writeIntText(model->part_min_block(), wb);
    writeChar('_', wb);
    writeIntText(model->part_max_block(), wb);
    writeChar('_', wb);
    return wb.str();
}

bool DeleteBitmapMeta::operator<(const DeleteBitmapMeta & rhs) const
{
    return std::forward_as_tuple(model->partition_id(), model->part_min_block(), model->part_max_block(), model->commit_time())
        < std::forward_as_tuple(rhs.model->partition_id(), rhs.model->part_min_block(), rhs.model->part_max_block(), rhs.model->commit_time());
}

void deserializeDeleteBitmapInfo(const MergeTreeMetaBase & storage, const DataModelDeleteBitmapPtr & meta, DeleteBitmapPtr & to_bitmap)
{
    assert(meta != nullptr);
    assert(to_bitmap != nullptr);
    auto cardinality = meta->cardinality();
    if (cardinality == 0)
        return;

    if (meta->has_inlined_value())
    {
        const char * data = meta->inlined_value().data();
        for (UInt32 i = 0; i < cardinality; ++i)
        {
            UInt32 value = DecodeFixed32(data);
            data += sizeof(UInt32);
            to_bitmap->add(value);
        }
    }
    else
    {
        assert(meta->has_file_size());
        /// deserialize bitmap from remote storage
        Roaring bitmap;
        {
            PODArray<char> buf(meta->file_size());
            DiskPtr disk = storage.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
            String rel_path = std::filesystem::path(storage.getRelativeDataPath(IStorage::StorageLocation::MAIN)) / DeleteBitmapMeta::deleteBitmapFileRelativePath(*meta);
            ReadSettings read_settings = storage.getContext()->getReadSettings();
            read_settings.adjustBufferSize(meta->file_size());
            std::unique_ptr<ReadBufferFromFileBase> in = disk->readFile(rel_path, read_settings);
            in->readStrict(buf.data(), meta->file_size());

            bitmap = Roaring::read(buf.data());
            assert(bitmap.cardinality() == cardinality);
        }
        /// union bitmap to result
        *to_bitmap |= bitmap;
    }
}

}

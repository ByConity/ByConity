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

#include <Columns/ListIndex.h>

#include <Compression/CompressedReadBufferFromFile.h>
#include <Storages/DiskCache/BitmapIndexDiskCacheSegment.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include "Storages/MergeTree/MergeTreeSuffix.h"

namespace DB
{
BitmapIndexWriter::BitmapIndexWriter(
    String path, String name, size_t rows, BitmapIndexMode bitmap_index_mode_, const bool & enable_run_optimization_)
{
    bitmap_index_mode = bitmap_index_mode_;
    String ark_suffix = ark_suffix_vec[bitmap_index_mode];
    String adx_suffix = adx_suffix_vec[bitmap_index_mode];

    if (!endsWith(path, "/"))
        path.append("/");

    total_rows = rows;
    enable_run_optimization = enable_run_optimization_;

    String column_name = escapeForFileName(name);

    idx = std::make_unique<WriteBufferFromFile>(path + column_name + adx_suffix, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
    hash_idx = std::make_unique<HashingWriteBuffer>(*idx);
    // use default CompressionSettings
    compressed_idx = std::make_unique<CompressedWriteBuffer>(*hash_idx);
    hash_compressed = std::make_unique<HashingWriteBuffer>(*compressed_idx);

    irk = std::make_unique<WriteBufferFromFile>(path + column_name + ark_suffix, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
    hash_irk = std::make_unique<HashingWriteBuffer>(*irk);
}

BitmapIndexWriter::BitmapIndexWriter(String path, size_t rows, BitmapIndexMode bitmap_index_mode_, const bool & enable_run_optimization_)
{
    bitmap_index_mode = bitmap_index_mode_;
    String ark_suffix = ark_suffix_vec[bitmap_index_mode];
    String adx_suffix = adx_suffix_vec[bitmap_index_mode];

    total_rows = rows;
    enable_run_optimization = enable_run_optimization_;

    idx = std::make_unique<WriteBufferFromFile>(path + adx_suffix, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
    hash_idx = std::make_unique<HashingWriteBuffer>(*idx);
    // use default CompressionSettings
    compressed_idx = std::make_unique<CompressedWriteBuffer>(*hash_idx);
    hash_compressed = std::make_unique<HashingWriteBuffer>(*compressed_idx);

    irk = std::make_unique<WriteBufferFromFile>(path + ark_suffix, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
    hash_irk = std::make_unique<HashingWriteBuffer>(*irk);
}


void BitmapIndexWriter::addToChecksums(MergeTreeData::DataPart::Checksums & checksums, const String & column_name)
{
    String ark_suffix = ark_suffix_vec[bitmap_index_mode];
    String adx_suffix = adx_suffix_vec[bitmap_index_mode];

    checksums.files[column_name + adx_suffix].is_compressed = true;
    checksums.files[column_name + adx_suffix].uncompressed_size = hash_compressed->count();
    checksums.files[column_name + adx_suffix].uncompressed_hash = hash_compressed->getHash();
    checksums.files[column_name + adx_suffix].file_size = hash_idx->count();
    checksums.files[column_name + adx_suffix].file_hash = hash_idx->getHash();

    checksums.files[column_name + ark_suffix].file_size = hash_irk->count();
    checksums.files[column_name + ark_suffix].file_hash = hash_irk->getHash();
}

BitmapIndexReader::BitmapIndexReader(
    const IMergeTreeDataPartPtr & part_, String name, [[maybe_unused]] const HDFSConnectionParams & hdfs_params_)
    : column_name(name)
{
    auto file_column_name = escapeForFileName(name);
    idx_pos = FileOffsetAndSize{
        part_->getFileOffsetOrZero(file_column_name + BITMAP_IDX_EXTENSION),
        part_->getFileSizeOrZero(file_column_name + BITMAP_IDX_EXTENSION)};
    irk_pos = FileOffsetAndSize{
        part_->getFileOffsetOrZero(file_column_name + BITMAP_IRK_EXTENSION),
        part_->getFileSizeOrZero(file_column_name + BITMAP_IRK_EXTENSION)};

    if ((idx_pos.file_offset != 0 && idx_pos.file_size != 0) && (irk_pos.file_offset != 0 && irk_pos.file_size != 0))
        source_part = part_->getMvccDataPart(file_column_name + BITMAP_IDX_EXTENSION);
    else
        source_part = part_;

    init();

    // LOG_TRACE(&Logger::get("BitmapIndexReader"), "BitMapIndex reader inited for column " + column_name);
}

void BitmapIndexReader::init()
{
    try
    {
         /***
         * try to get cache first
         */
         if (source_part->enableDiskCache())
         {
             auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree);
             std::shared_ptr<BitmapIndexDiskCacheSegment> bitmap_index_segment
                 = std::make_shared<BitmapIndexDiskCacheSegment>(source_part, column_name, BITMAP_IDX_EXTENSION);

             String bin_segment_key = BitmapIndexDiskCacheSegment::getSegmentKey(source_part, column_name, 0, BITMAP_IDX_EXTENSION);
             String mrk_segment_key = BitmapIndexDiskCacheSegment::getSegmentKey(source_part, column_name, 0, BITMAP_IRK_EXTENSION);

             auto [index_idx_disk, index_idx_path] = disk_cache->get(bin_segment_key);
             auto [index_irk_disk, index_irk_path] = disk_cache->get(mrk_segment_key);

             if (index_idx_disk && index_irk_disk && !index_idx_path.empty() && !index_irk_path.empty())
             {
                 compressed_idx = std::make_unique<CompressedReadBufferFromFile>(
                     index_idx_disk->readFile(index_idx_path, source_part->storage.getContext()->getReadSettings()));

                 irk_buffer = std::make_unique<ReadBufferFromFile>(index_irk_disk->getPath() + index_irk_path, DBMS_DEFAULT_BUFFER_SIZE);

                 LOG_DEBUG(
                     getLogger("BitmapIndexReader"), "Get BitMapIndex read buffers from local cache for column " + column_name);
                 read_from_local_cache = true;
                 return;
             }
             else
             {
                 disk_cache->cacheBitmapIndexToLocalDisk(bitmap_index_segment);
             }
         }
    }
    catch(...)
    {
        tryLogCurrentException(getLogger("BitmapIndexReader"), "Cache or Get BitMapIndex Failed");
    }

    try
    {
        /**
         * if there is no binary or mark (idx / irk), the reader is invalid so that we should not init buffers.
         */
        if ((idx_pos.file_offset == 0 && idx_pos.file_size == 0) || (irk_pos.file_offset == 0 && irk_pos.file_size == 0))
            return;

        auto data_rel_path = fs::path(source_part->getFullRelativePath()) / "data";
        auto data_disk = source_part->volume->getDisk();

        std::unique_ptr<ReadBufferFromFileBase> raw_idx_buffer = data_disk->readFile(
            data_rel_path,
            ReadSettings{
                .enable_io_scheduler = static_cast<bool>(source_part->storage.getContext()->getSettingsRef().enable_io_scheduler),
                .enable_io_pfra = static_cast<bool>(source_part->storage.getContext()->getSettingsRef().enable_io_pfra),
            });
        compressed_idx = std::make_unique<CompressedReadBufferFromFile>(
            std::move(raw_idx_buffer), false, idx_pos.file_offset, idx_pos.file_size, true);
        irk_buffer = data_disk->readFile(
            data_rel_path,
            ReadSettings{
                .enable_io_scheduler = static_cast<bool>(source_part->storage.getContext()->getSettingsRef().enable_io_scheduler),
                .enable_io_pfra = static_cast<bool>(source_part->storage.getContext()->getSettingsRef().enable_io_pfra),
                .estimated_size = irk_pos.file_size}
                .initializeReadSettings(irk_pos.file_size));
    }
    catch(...)
    {
        tryLogCurrentException(getLogger("BitmapIndexReader"), __PRETTY_FUNCTION__);
        compressed_idx = nullptr;
        irk_buffer = nullptr;
    }
}

}

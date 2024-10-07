/*
 * Copyright 2016-2023 ClickHouse, Inc.
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


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/PartFileDiskCacheSegment.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/DiskCache/IDiskCacheStrategy.h>
#include <Storages/IStorage.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeReaderStreamWithSegmentCache.h>


namespace DB
{

MergeTreeIndexReader::MergeTreeIndexReader(
    MergeTreeIndexPtr index_,
    MergeTreeData::DataPartPtr part_,
    size_t marks_count_,
    const MarkRanges & all_mark_ranges_,
    MergeTreeReaderSettings settings,
    MarkCache * mark_cache,
    const ProgressCallback & internal_progress_cb_)
    : index(index_)
{
    switch(part_->info.storage_type)
    {
        case StorageType::Local:
        case StorageType::RAM:
        {
            stream = std::make_unique<MergeTreeReaderStream>(
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = part_->volume->getDisk(),
                    .rel_path = part_->getFullRelativePath() + index->getFileName() + INDEX_FILE_EXTENSION,
                    .offset = part_->getFileOffsetOrZero(index->getFileName() + INDEX_FILE_EXTENSION),
                    .size = part_->getFileSizeOrZero(index->getFileName() + INDEX_FILE_EXTENSION)
                },
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = part_->volume->getDisk(),
                    .rel_path = part_->index_granularity_info.getMarksFilePath(part_->getFullRelativePath() + index->getFileName()),
                    .offset = part_->getFileOffsetOrZero(index->getFileName() + part_->getMarksFileExtension()),
                    .size = part_->getFileSizeOrZero(index->getFileName() + part_->getMarksFileExtension())
                },
                index->getFileName(),
                marks_count_,
                all_mark_ranges_,
                std::move(settings),
                part_->storage.getContext()->getMarkCache().get(),
                part_->storage.getContext()->getUncompressedCache().get(),
                &part_->index_granularity_info,
                ReadBufferFromFileBase::ProfileCallback{},
                CLOCK_MONOTONIC_COARSE,
                false
            );
            break;
        }
        case StorageType::ByteHDFS:
        case StorageType::HDFS:
        {
            auto path = std::filesystem::path(part_->getFullRelativePath()) / "data";
            const auto & index_name = index->getFileName();
            IDiskCachePtr segment_cache;
            IDiskCacheStrategyPtr segment_cache_strategy;
            MergeTreeDataPartPtr source_data_part = part_->getMvccDataPart(index_name + INDEX_FILE_EXTENSION);
            if (source_data_part->enableDiskCache())
            {
                auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::MergeTree);

                segment_cache_strategy = disk_cache->getStrategy();
                segment_cache = disk_cache;
            }
            String mark_file_name = source_data_part->index_granularity_info.getMarksFilePath(index_name);

            /// data file
            String data_path = std::filesystem::path(source_data_part->getFullRelativePath()) / "data";
            off_t data_file_offset = source_data_part->getFileOffsetOrZero(index_name + INDEX_FILE_EXTENSION);
            size_t data_file_size = source_data_part->getFileSizeOrZero(index_name + INDEX_FILE_EXTENSION);

            /// mark file
            const String & mark_path = data_path;
            off_t mark_file_offset = source_data_part->getFileOffsetOrZero(mark_file_name);
            size_t mark_file_size = source_data_part->getFileSizeOrZero(mark_file_name);
            if (segment_cache_strategy)
            {
                // Cache segment if necessary
                IDiskCacheSegmentsVector segments
                    = segment_cache_strategy->getCacheSegments(segment_cache, segment_cache_strategy->transferRangesToSegments<PartFileDiskCacheSegment>(
                        all_mark_ranges_,
                        source_data_part,
                        PartFileDiskCacheSegment::FileOffsetAndSize{mark_file_offset, mark_file_size},
                        marks_count_,
                        mark_cache,
                        segment_cache->getMetaCache().get(),
                        index_name,
                        INDEX_FILE_EXTENSION,
                        PartFileDiskCacheSegment::FileOffsetAndSize{data_file_offset, data_file_size}));
                segment_cache->cacheSegmentsToLocalDisk(segments);
            }

            PartHostInfo part_host{
            .disk_cache_host_port = source_data_part->disk_cache_host_port,
            .assign_compute_host_port = source_data_part->assign_compute_host_port};

            stream = std::make_unique<MergeTreeReaderStreamWithSegmentCache>(
                source_data_part->storage.getStorageID(),
                source_data_part->name,
                index_name,
                source_data_part->volume->getDisk(),
                marks_count_,
                data_path,
                data_file_offset,
                data_file_size,
                mark_path,
                mark_file_offset,
                mark_file_size,
                all_mark_ranges_,
                settings,
                mark_cache,
                nullptr, /*uncompressed_cache*/
                segment_cache.get(),
                segment_cache_strategy ? segment_cache_strategy->getSegmentSize() : 1,
                part_host,
                &(source_data_part->index_granularity_info),
                ReadBufferFromFileBase::ProfileCallback{},
                internal_progress_cb_,
                CLOCK_MONOTONIC_COARSE,
                false,
                INDEX_FILE_EXTENSION
            );
            break;
        }
        default:
            LOG_DEBUG(getLogger("MergeTreeIndexReader"), "Storage type: {} doesn't support secondary indexes", part_->info.storage_type);
            break;
    }
    if(stream) stream->seekToStart();
}

void MergeTreeIndexReader::seek(size_t mark)
{
    stream->seekToMark(mark);
}

MergeTreeIndexGranulePtr MergeTreeIndexReader::read()
{
    auto granule = index->createIndexGranule();
    granule->deserializeBinary(*stream->data_buffer);
    return granule;
}

}

#include "DiskCacheSegment.h"
#include <memory>

#include <IO/LimitReadBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include "Storages/DiskCache/IDiskCacheSegment.h"

namespace DB
{
DiskCacheSegment::DiskCacheSegment(
    UInt32 segment_number_,
    UInt32 segment_size_,
    const IMergeTreeDataPartPtr & data_part_,
    const String & stream_name_,
    const String & extension_)
    : IDiskCacheSegment(segment_number_, segment_size_), data_part(data_part_), stream_name(stream_name_), extension(extension_)
    , marks_loader(data_part->volume->getDisk(), nullptr,
        data_part->getFullRelativePath() + "data",
        stream_name, data_part->getMarksCount(), data_part->index_granularity_info,
        false, data_part->getFileOffsetOrZero(data_part->index_granularity_info.getMarksFilePath(stream_name)),
        data_part->getFileSizeOrZero(data_part->index_granularity_info.getMarksFilePath(stream_name)))
    , source_buffer(nullptr)
{
}

String DiskCacheSegment::getSegmentKey(
    const StorageID & storage_id, const String & part_name, const String & stream_name, UInt32 segment_index, const String & extension)
{
    return IDiskCacheSegment::formatSegmentName(
        UUIDHelpers::UUIDToString(storage_id.uuid), part_name, stream_name, segment_index, extension);
}

String DiskCacheSegment::getSegmentName() const
{
    return formatSegmentName(
        UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->name, stream_name, segment_number, extension);
}

size_t DiskCacheSegment::getSegmentStartOffset()
{
    const auto & start_mark = marks_loader.getMark(segment_size * segment_number);
    return start_mark.offset_in_compressed_file;
}

size_t DiskCacheSegment::getSegmentEndOffset()
{
    // If this segment is the last segment just return the end offset of this stream file.
    if (segment_size * (segment_number + 1) >= data_part->getMarksCount())
    {
        return data_part->getFileSizeOrZero(stream_name + extension);
    }

    size_t end_mrk_idx = segment_size * (segment_number + 1);
    auto end_mark = marks_loader.getMark(end_mrk_idx);
    off_t end_offset = end_mark.offset_in_compressed_file;
    if (end_mark.offset_in_decompressed_block)
    {
        initSourceBufferIfNecessary();
        size_t base_offset = data_part->getFileOffsetOrZero(stream_name + extension);
        source_buffer->seek(base_offset + end_mark.offset_in_compressed_file, 0);
        end_offset += source_buffer->getSizeCompressed();
    }

    return end_offset;
}

void DiskCacheSegment::cacheToDisk(IDiskCache & disk_cache)
{
    Poco::Logger * log = disk_cache.getLogger();

    try
    {
        String data_path = data_part->getFullRelativePath() + "data";
        auto disk = data_part->volume->getDisk();
        auto data_file = disk->readFile(data_path);

        String segment_key = getSegmentName();
        off_t segment_offset = getSegmentStartOffset();
        size_t segment_length = getSegmentEndOffset() - segment_offset;

        auto checksums = data_part->getChecksums();
        auto data_checksum = checksums->files.find(stream_name + extension);
        if (data_checksum == checksums->files.end())
        {
            LOG_ERROR(log, "Cannot find data file: {} in checkusms.", stream_name + extension);
            return;
        }

        segment_offset += data_checksum->second.file_offset;

        auto marks_checksum = checksums->files.find(stream_name + MARKS_FILE_EXTENSION);
        if (marks_checksum == checksums->files.end())
        {
            LOG_ERROR(log, "Cannot find marks file: {} in checkusms.", stream_name + MARKS_FILE_EXTENSION);
            return;
        }

        /// cache data segment
        data_file->seek(segment_offset);
        LimitReadBuffer segment_value(*data_file, segment_length, false);
        disk_cache.set(segment_key, segment_value, segment_length);

        /// cache mark segment
        off_t marks_offset = marks_checksum->second.file_offset;
        size_t marks_size = marks_checksum->second.file_size;

        data_file->seek(marks_offset);
        LimitReadBuffer marks_value(*data_file, marks_size, false);
        String marks_key = getSegmentKey(data_part->storage.getStorageID(), data_part->name, stream_name, 0, MARKS_FILE_EXTENSION);
        disk_cache.set(marks_key, marks_value, marks_size);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to cache segment to local disk.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void DiskCacheSegment::initSourceBufferIfNecessary()
{
    if (source_buffer != nullptr)
    {
        return;
    }

    source_buffer = std::make_unique<CompressedReadBufferFromFile>(
        data_part->getFullRelativePath() + "data",
        0, 0, 0, nullptr, DBMS_DEFAULT_BUFFER_SIZE, false,
        data_part->getFileOffsetOrZero(stream_name + extension),
        data_part->getFileSizeOrZero(stream_name + extension),
        true
    );
}

}

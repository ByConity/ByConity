#include "DiskCacheSegment.h"

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

MergeTreeReaderStream & DiskCacheSegment::getStream()
{
    if (!reader_stream)
    {
        reader_stream = std::make_unique<MergeTreeReaderStream>(
            data_part->volume->getDisk(),
            data_part->getFullRelativePath() + "data",
            stream_name,
            extension,
            data_part->getMarksCount(),
            MarkRanges{},
            MergeTreeReaderSettings{},
            nullptr, // mark cache
            nullptr, // uncompressed_cache
            &data_part->index_granularity_info,
            ReadBufferFromFileBase::ProfileCallback{},
            CLOCK_MONOTONIC_COARSE,
            data_part->getFileOffsetOrZero(stream_name + extension),
            data_part->getFileSizeOrZero(stream_name + extension),
            data_part->getFileOffsetOrZero(stream_name + MARKS_FILE_EXTENSION),
            data_part->getFileSizeOrZero(stream_name + MARKS_FILE_EXTENSION));
    }
    return *reader_stream;
}

size_t DiskCacheSegment::getSegmentStartOffset()
{
    auto & stream = getStream();
    const auto & start_mark = stream.marks_loader.getMark(segment_size * segment_number);
    return start_mark.offset_in_compressed_file;
}

size_t DiskCacheSegment::getSegmentEndOffset()
{
    // If this segment is the last segment just return the end offset of this stream file.
    if (segment_size * (segment_number + 1) >= data_part->getMarksCount())
    {
        return data_part->getFileSizeOrZero(stream_name + extension);
    }

    auto & stream = getStream();
    size_t end_mrk_idx = segment_size * (segment_number + 1);
    auto end_mark = stream.marks_loader.getMark(end_mrk_idx);
    off_t end_offset = end_mark.offset_in_compressed_file;
    if (end_mark.offset_in_decompressed_block)
    {
        stream.seekToMark(end_mrk_idx);
        end_offset += stream.getCurrentBlockCompressedSize();
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

}

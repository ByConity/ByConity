#include "DiskCacheSegment.h"

#include <IO/LimitReadBuffer.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include "Storages/DiskCache/IDiskCacheSegment.h"

namespace DB
{
DiskCacheSegment::DiskCacheSegment(
    IMergeTreeDataPartPtr data_part_,
    const String & stream_name_,
    const String & extension_,
    String full_path_,
    UInt32 segment_number_,
    UInt32 segment_size_)
    : IDiskCacheSegment(segment_number_, segment_size_)
    , data_part(std::move(data_part_))
    , uuid(UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()))
    , part_name(data_part->name)
    , stream_name(stream_name_)
    , extension(extension_)
    , full_path(full_path_)
    // , uncompressed_cache(data_part->storage.global_context.getUncompressedCache())
    // , mark_cache(data_part->storage.global_context.getMarkCache())
    // , hdfs_params(data_part->storage.global_context.getHdfsConnectionParams())
{
}

String DiskCacheSegment::getSegmentKey(const IMergeTreeDataPartPtr & data_part, const String & column_name, UInt32 segment_number_, const String & extension)
{
    return IDiskCacheSegment::formatSegmentName(
        UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->name, column_name, segment_number_, extension);
}

String DiskCacheSegment::getSegmentKey(const StorageID& storage_id,
    const String& part_name, const String& stream_name, UInt32 segment_index,
    const String& extension)
{
    return IDiskCacheSegment::formatSegmentName(
        UUIDHelpers::UUIDToString(storage_id.uuid), part_name, stream_name,
        segment_index, extension);
}

String DiskCacheSegment::getSegmentName() const
{
    return formatSegmentName(uuid, part_name, stream_name, segment_number, extension);
}

MergeTreeReaderStream & DiskCacheSegment::getStream() const
{
    if (remote_stream)
        return *remote_stream;

    // stream = std::make_unique<MergeTreeReaderStreamRemote>(
    //     data_part,
    //     MergeTreeReaderStream::Position{
    //         full_path + "data",
    //         data_part->getFileOffsetOrZero(stream_name + extension),
    //         data_part->getFileSizeOrZero(stream_name + extension)},
    //     MergeTreeReaderStream::Position{
    //         full_path + "data",
    //         data_part->getFileOffsetOrZero(stream_name + MARKS_FILE_EXTENSION),
    //         data_part->getFileSizeOrZero(stream_name + MARKS_FILE_EXTENSION)},
    //     stream_name,
    //     marks_count,
    //     MarkRanges{},
    //     mark_cache.get(),
    //     false,
    //     uncompressed_cache.get(),
    //     DBMS_DEFAULT_BUFFER_SIZE,
    //     ReadBufferFromFileBase::ProfileCallback{},
    //     CLOCK_MONOTONIC_COARSE,
    //     hdfs_params,
    //     data_part->getAesEncrypter());

    return *remote_stream;
}

off_t DiskCacheSegment::getSegmentStartOffset() const
{
    // auto & stream = getStream();
    // auto start_mark = stream.getMark(segment_size * segment_number);
    // return start_mark.offset_in_compressed_file;
    return 0;
}

size_t DiskCacheSegment::getSegmentEndOffset() const
{
    return 0;
    /// If this segment is the last segment just return the end offset of this stream file.
    // if (segment_size * (segment_number + 1) >= data_part->marks_count)
    // {
    //     String data_file_name = stream_name + extension;
    //     auto checksums = data_part->getChecksums();
    //     auto checksum = checksums->files.find(data_file_name);
    //     if (checksum == checksums->files.end())
    //     {
    //         LOG_ERROR((&Logger::get("DiskCache")), "Cannot find file: " << data_file_name << " in checkusms.");
    //         return 0;
    //     }
    //     return checksum->second.file_size;
    // }

    // auto & stream = getStream();
    // size_t end_index = segment_size * (segment_number + 1);
    // auto end_mark = stream.getMark(end_index);
    // off_t end_offset = end_mark.offset_in_compressed_file;
    // if (end_mark.offset_in_decompressed_block)
    // {
    //     stream.seekToMark(end_index);
    //     end_offset += stream.getCurrentBlockCompressedSize();
    // }

    // return end_offset;
}

void DiskCacheSegment::cacheToDisk(IDiskCache &)
{
    // Poco::Logger * log = disk_cache.getLogger();

    // try
    // {
    //     String data_path = data_part->getMvccFullPath(stream_name + extension) + "/data";
    //     ReadBufferFromByteHDFS segment_file(
    //         data_path, true, hdfs_params, DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0, false, disk_cache.getDiskCacheThrottler());

    //     String segment_key = getSegmentName();
    //     off_t segment_offset = getSegmentStartOffset();
    //     size_t segment_length = getSegmentEndOffset() - segment_offset;

    //     String data_file_name = stream_name + extension;
    //     auto checksums = data_part->getChecksums();
    //     auto data_checksum = checksums->files.find(data_file_name);
    //     if (data_checksum == checksums->files.end())
    //     {
    //         LOG_ERROR(log, "Cannot find data file: " << data_file_name << " in checkusms.");
    //         return;
    //     }

    //     segment_offset += data_checksum->second.file_offset;

    //     String marks_file_name = stream_name + MARKS_FILE_EXTENSION;
    //     auto marks_checksum = checksums->files.find(marks_file_name);
    //     if (marks_checksum == checksums->files.end())
    //     {
    //         LOG_ERROR(log, "Cannot find marks file: " << marks_file_name << " in checkusms.");
    //         return;
    //     }

    //     /// cache data segment
    //     segment_file.seek(segment_offset);
    //     LimitReadBuffer segment_value(segment_file, segment_length, false);
    //     disk_cache.set(segment_key, segment_value, segment_length);

    //     /// cache mark segment
    //     off_t marks_offset = marks_checksum->second.file_offset;
    //     size_t marks_size = marks_checksum->second.file_size;

    //     segment_file.seek(marks_offset);
    //     LimitReadBuffer marks_value(segment_file, marks_size, false);
    //     String marks_key = formatSegmentName(uuid, part_name, stream_name, 0, MARKS_FILE_EXTENSION);
    //     disk_cache.set(marks_key, marks_value, marks_size);
    // }
    // catch (...)
    // {
    //     LOG_ERROR(log, "Failed to cache segment to local disk.");
    //     tryLogCurrentException(log, __PRETTY_FUNCTION__);
    // }
}

}

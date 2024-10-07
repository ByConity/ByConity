#include <Storages/DiskCache/BitmapIndexDiskCacheSegment.h>

#include <IO/LimitReadBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Core/Defines.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>

namespace DB
{

BitmapIndexDiskCacheSegment::BitmapIndexDiskCacheSegment(
        IMergeTreeDataPartPtr data_part_,
        const String & stream_name_,
        const String & extension_)
        : IDiskCacheSegment(0, 0, SegmentType::BITMAP_INDEX)
        , data_part(std::move(data_part_))
        , storage(data_part->storage.shared_from_this()) /// Need to extend the lifetime of storage because disk cache can run async
        , uuid(UUIDHelpers::UUIDToString(data_part->get_uuid()))
        , part_name(data_part->name)
        , data_path(data_part->getFullRelativePath() + "/data")
        , stream_name(stream_name_)
        , extension(extension_)
        , hdfs_params(data_part->storage.getContext()->getHdfsConnectionParams())
        , read_settings(data_part->storage.getContext()->getReadSettings())
    {
    }

String BitmapIndexDiskCacheSegment::getSegmentName() const
{
    return formatSegmentName(uuid, part_name, stream_name, 0, extension);
}

String BitmapIndexDiskCacheSegment::getSegmentKey(const IMergeTreeDataPartPtr & data_part_, const String & column_name_, UInt32 segment_number_, const String & extension)
{
    return IDiskCacheSegment::formatSegmentName(
        UUIDHelpers::UUIDToString(data_part_->get_uuid()), data_part_->name, column_name_, segment_number_, extension);
}

void BitmapIndexDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache, bool)
{
    LoggerPtr log = disk_cache.getLogger();
    auto disk = data_part->volume->getDisk();
    std::unique_ptr<ReadBufferFromFileBase> segment_file = disk->readFile(
        data_path, read_settings
    );

    // try cache idx file
    String index_key = getSegmentName();
    String index_name = stream_name + extension;
    auto checksum = data_part->getChecksums()->files.find(index_name);
    if (checksum != data_part->getChecksums()->files.end())
    {
        off_t index_offset = checksum->second.file_offset;
        size_t index_size = checksum->second.file_size;

        segment_file->seek(index_offset);
        LimitReadBuffer index_value(*segment_file, index_size, false);
        disk_cache.set(index_key, index_value, index_size, false);

        LOG_TRACE(log, "Cached local cache for BitmapIndex {}#{}{} of column ", part_name, stream_name, extension);
    }

    /// try cache ird file
    String mark_key = formatSegmentName(uuid, part_name, stream_name, 0, BITMAP_IRK_EXTENSION);
    String mark_name = stream_name + BITMAP_IRK_EXTENSION;
    auto mark_checksum = data_part->getChecksums()->files.find(mark_name);
    if (mark_checksum != data_part->getChecksums()->files.end())
    {
        off_t mark_offset = mark_checksum->second.file_offset;
        size_t mark_size = mark_checksum->second.file_size;

        segment_file->seek(mark_offset);
        LimitReadBuffer mark_value(*segment_file, mark_size, false);
        disk_cache.set(mark_key, mark_value, mark_size, false);

        LOG_TRACE(log, "Cached local cache for BitmapIndex {}#{}{} of column ", part_name, stream_name, BITMAP_IRK_EXTENSION);
    }
}


}


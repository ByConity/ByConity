#include "MetaFileDiskCacheSegment.h"

#include <Core/UUIDHelpers.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

namespace DB
{
ChecksumsDiskCacheSegment::ChecksumsDiskCacheSegment(IMergeTreeDataPartPtr data_part_)
    : IDiskCacheSegment(0, 0)
    , data_part(std::move(data_part_))
    , segment_name(formatSegmentName(
          UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->name, "", segment_number, "checksums.txt"))
{
}

String ChecksumsDiskCacheSegment::getSegmentName() const
{
    return segment_name;
}

void ChecksumsDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache)
{
    auto checksums = data_part->getChecksums();
    if (!checksums)
        return;

    MemoryWriteBuffer write_buffer;
    checksums->write(write_buffer);
    size_t file_size = write_buffer.count();
    if (auto read_buffer = write_buffer.tryGetReadBuffer())
    {
        disk_cache.set(getSegmentName(), *read_buffer, file_size);
    }
}

PrimaryIndexDiskCacheSegment::PrimaryIndexDiskCacheSegment(IMergeTreeDataPartPtr data_part_)
    : IDiskCacheSegment(0, 0)
    , data_part(std::move(data_part_))
    , segment_name(formatSegmentName(
          UUIDHelpers::UUIDToString(data_part->storage.getStorageUUID()), data_part->name, "", segment_number, "primary.idx"))
{
}

String PrimaryIndexDiskCacheSegment::getSegmentName() const
{
    return segment_name;
}

void PrimaryIndexDiskCacheSegment::cacheToDisk(IDiskCache &)
{
    // auto metadata_snapshot = data_part->storage.getInMemoryMetadataPtr();
    // const auto & primary_key = metadata_snapshot->getPrimaryKey();
    // size_t key_size = primary_key.column_names.size();

    // if (key_size == 0)
    //     return;

    // auto index = data_part->getIndex();
    // size_t marks_count = data_part->getMarksCount();
    // MemoryWriteBuffer write_buffer;
    // for (size_t i = 0; i < marks_count; ++i)
    // {
    //     for (size_t j = 0; j < index->size(); ++j)
    //         data_part->storage.primary_key_data_types[j]->serializeBinary(*index->at(j), i, write_buffer);
    // }

    // size_t file_size = write_buffer.count();
    // if (auto read_buffer = write_buffer.tryGetReadBuffer())
    // {
    //     disk_cache.set(getSegmentName(), *read_buffer, file_size);
    // }
}

}

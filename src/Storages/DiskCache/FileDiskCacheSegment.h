#pragma once

#include <Core/UUID.h>
#include <IO/UncompressedCache.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MarkCache.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{
class IDiskCache;

using FileRanges = MarkRanges;

class FileRange : public MarkRange
{
public:
    FileRange() : MarkRange()
    {
        begin = 0;
        end = 1;
    }
};

class FileDiskCacheSegment : public IDiskCacheSegment
{
public:
    FileDiskCacheSegment(
        UInt32 segment_number_,
        UInt32 segment_size_,
        const String & uuid_, /// Note: pass `part_name` in old version, cached data will be `not-reload` and lost if upgraded cur version
        const String & path_ /* it's also part_name */,
        const HDFSConnectionParams & params_)
        : IDiskCacheSegment(segment_number_, segment_size_), uuid(uuid_), path(path_), hdfs_params(params_)
    {
    }

    static String getSegmentKey(const String & uuid_, const String & path_)
    {
        return formatSegmentName(uuid_, path_, "", 0, "");
    }

    String getSegmentName() const override { return formatSegmentName(uuid, path, "", 0, ""); }

    void cacheToDisk(IDiskCache & cache) override;

private:
    String uuid;
    String path;
    HDFSConnectionParams hdfs_params;
};

using ParquetRanges = FileRanges;
using ParquetFileDiskCacheSegment = FileDiskCacheSegment;

}

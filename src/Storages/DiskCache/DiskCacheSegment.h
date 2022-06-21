#pragma once

#include <Core/UUID.h>
#include <Core/UUIDHelpers.h>
#include <IO/UncompressedCache.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include "Storages/HDFS/HDFSCommon.h"

namespace DB
{
class IDiskCache;
class MergeTreeReaderStream;

class DiskCacheSegment : public IDiskCacheSegment
{
public:
    DiskCacheSegment(
        IMergeTreeDataPartPtr data_part_,
        const String & stream_name_,
        const String & extension_,
        String full_path_,
        UInt32 segment_number_,
        UInt32 segment_size_);

    static String
    getSegmentKey(const IMergeTreeDataPartPtr & data_part, const String & column_name, UInt32 segment_number, const String & extension);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & cache) override;

private:
    off_t getSegmentStartOffset() const;
    size_t getSegmentEndOffset() const;
    MergeTreeReaderStream & getStream() const;

    IMergeTreeDataPartPtr data_part;
    String uuid;
    String part_name;

    String stream_name;
    String extension;
    String full_path;
    UncompressedCachePtr uncompressed_cache;
    MarkCachePtr mark_cache;
    HDFSConnectionParams hdfs_params;
    mutable std::unique_ptr<MergeTreeReaderStream> remote_stream;
};

}

#pragma once

#include <IO/UncompressedCache.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include "common/types.h"
#include "Interpreters/StorageID.h"

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

    static String
    getSegmentKey(const StorageID& storage_id, const String& part_name,
        const String& stream_name, UInt32 segment_index, const String& extension);

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

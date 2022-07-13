#pragma once

#include <IO/UncompressedCache.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Interpreters/StorageID.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>

namespace DB
{
class IDiskCache;
class MergeTreeReaderStream;

class DiskCacheSegment : public IDiskCacheSegment
{
public:
    DiskCacheSegment(
        UInt32 segment_number_,
        UInt32 segment_size_,
        const IMergeTreeDataPartPtr & data_part_,
        const String & stream_name_,
        const String & extension_);

    static String
    getSegmentKey(const StorageID& storage_id, const String& part_name,
        const String& stream_name, UInt32 segment_index, const String& extension);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & cache) override;

private:
    size_t getSegmentStartOffset();
    size_t getSegmentEndOffset();

    void initSourceBufferIfNecessary();

    IMergeTreeDataPartPtr data_part;
    String stream_name;
    String extension;

    MergeTreeMarksLoader marks_loader;
    std::unique_ptr<CompressedReadBufferFromFile> source_buffer;
};

}

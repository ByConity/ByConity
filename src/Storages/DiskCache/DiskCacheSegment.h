#pragma once

#include <IO/UncompressedCache.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Interpreters/StorageID.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
class IDiskCache;
class MergeTreeReaderStream;

class DiskCacheSegment : public IDiskCacheSegment
{
public:
    struct FileOffsetAndSize
    {
        off_t file_offset;
        size_t file_size;
    };
    DiskCacheSegment(
        UInt32 segment_number_,
        UInt32 segment_size_,
        const IMergeTreeDataPartPtr & data_part_,
        const FileOffsetAndSize & mrk_file_pos,
        size_t marks_count_,
        const String & stream_name_,
        const String & extension_,
        const FileOffsetAndSize & stream_file_pos);

    static String
    getSegmentKey(const StorageID& storage_id, const String& part_name,
        const String& stream_name, UInt32 segment_index, const String& extension);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & cache) override;

private:
    IMergeTreeDataPartPtr data_part;
    ConstStoragePtr storage;
    FileOffsetAndSize mrk_file_pos;
    size_t marks_count;

    String stream_name;
    String extension;
    FileOffsetAndSize stream_file_pos;

    MergeTreeMarksLoader marks_loader;
};

}

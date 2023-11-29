#pragma once

#include <Core/UUID.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/IStorage_fwd.h>
#include "IO/ReadSettings.h"
#include "Interpreters/StorageID.h"

namespace DB
{
class IDiskCache;

class BitmapIndexDiskCacheSegment : public IDiskCacheSegment
{
public:
    BitmapIndexDiskCacheSegment(
        IMergeTreeDataPartPtr data_part_,
        const String & stream_name_,
        const String & extension_);

    static String getSegmentKey(const IMergeTreeDataPartPtr & data_part, const String & column_name, UInt32 segment_number, const String & extension);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & cache, bool throw_exception) override;

private:
    IMergeTreeDataPartPtr data_part;
    ConstStoragePtr storage;

    String uuid;
    String part_name;
    String data_path;

    String stream_name;
    String extension;
    HDFSConnectionParams hdfs_params;
    ReadSettings read_settings;
};

}

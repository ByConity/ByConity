#pragma once

#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>

namespace DB
{
class IDiskCache;

class ChecksumsDiskCacheSegment : public IDiskCacheSegment
{
public:
    explicit ChecksumsDiskCacheSegment(IMergeTreeDataPartPtr data_part_);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & diskcache) override;

private:
    IMergeTreeDataPartPtr data_part;
    String segment_name;
};

class PrimaryIndexDiskCacheSegment : public IDiskCacheSegment
{
public:
    explicit PrimaryIndexDiskCacheSegment(IMergeTreeDataPartPtr data_part_);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & diskcache) override;

private:
    IMergeTreeDataPartPtr data_part;
    String segment_name;
};

}

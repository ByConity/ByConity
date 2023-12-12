#pragma once

#include <Disks/IDisk.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>

namespace DB
{
class IDiskCache;

class FileDiskCacheSegment : public IDiskCacheSegment
{
public:
    FileDiskCacheSegment(DiskPtr remote_disk_, const String & path_,
        const ReadSettings & read_settings_, const std::optional<std::pair<size_t, size_t>>& data_range_ = std::nullopt)
        : IDiskCacheSegment(0, 0), data_range(data_range_),
        remote_disk(std::move(remote_disk_)), path(path_), read_settings(read_settings_)
    {
    }

    String getSegmentName() const override;

    void cacheToDisk(IDiskCache & cache, bool throw_exception) override;

private:
    std::optional<std::pair<size_t, size_t>> data_range;

    DiskPtr remote_disk;
    String path;
    ReadSettings read_settings;
};
}

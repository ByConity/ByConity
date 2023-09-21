#pragma once

#include <Disks/IDisk.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>

namespace DB
{
class IDiskCache;

class FileDiskCacheSegment : public IDiskCacheSegment
{
public:
    FileDiskCacheSegment(DiskPtr remote_disk_, const String & path_, const ReadSettings & read_settings_)
        : IDiskCacheSegment(0, 0), remote_disk(std::move(remote_disk_)), path(path_), read_settings(read_settings_)
    {
    }

    String getSegmentName() const override { return std::filesystem::path(remote_disk->getPath()) / path; }

    void cacheToDisk(IDiskCache & cache, bool throw_exception) override;

private:
    DiskPtr remote_disk;
    String path;
    ReadSettings read_settings;
};
}

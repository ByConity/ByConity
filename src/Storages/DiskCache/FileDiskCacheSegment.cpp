#include "FileDiskCacheSegment.h"

#include <Storages/DiskCache/IDiskCache.h>

namespace DB
{

void FileDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache)
{
    Poco::Logger * log = disk_cache.getLogger();

    try
    {
        read_settings.throttler = disk_cache.getDiskCacheThrottler();
        auto buf = remote_disk->readFile(path, read_settings);
        size_t segment_length = remote_disk->getFileSize(path);
        String segment_key = getSegmentName();
        disk_cache.set(segment_key, *buf, segment_length);
        LOG_TRACE(log, "Cached {} to disk, length = {}", path, segment_length);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to cache File segment to local disk.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

}

#include "FileDiskCacheSegment.h"

#include <IO/LimitSeekableReadBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>

namespace DB
{

String FileDiskCacheSegment::getSegmentName() const
{
    if (data_range.has_value())
    {
        return std::filesystem::path(remote_disk->getPath()) / path
            / fmt::format("{}_{}", data_range.value().first, data_range.value().second);
    }
    else
    {
        return std::filesystem::path(remote_disk->getPath()) / path;
    }
}

void FileDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache, bool)
{
    Poco::Logger * log = disk_cache.getLogger();

    try
    {
        size_t begin = data_range.has_value() ? data_range.value().first : 0;
        size_t end = data_range.has_value() ? data_range.value().second : remote_disk->getFileSize(path);

        read_settings.remote_throttler = disk_cache.getDiskCacheThrottler();
        auto buf = remote_disk->readFile(path, read_settings);
        LimitSeekableReadBuffer limit_reader(*buf, begin, end);

        String segment_key = getSegmentName();
        disk_cache.set(segment_key, limit_reader, end - begin, false);
        LOG_TRACE(log, "Cached file data range {}-{} segment={}, stream={} to disk",
            begin, end, getSegmentName(), stream_name);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to cache File segment to local disk.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

}

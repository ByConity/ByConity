#include "FileDiskCacheSegment.h"

#include <IO/LimitReadBuffer.h>
#include <Storages/DiskCache/IDiskCache.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>

namespace DB
{

void FileDiskCacheSegment::cacheToDisk(IDiskCache & disk_cache)
{
    Poco::Logger * log = disk_cache.getLogger();

    try
    {
        ReadBufferFromByteHDFS file(
            path, false, hdfs_params, DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0, false, disk_cache.getDiskCacheThrottler());

        String segment_key = getSegmentName();
        ssize_t segment_length = HDFSCommon::getSize(path);
        disk_cache.set(segment_key, file, segment_length);
        LOG_TRACE(log, "Cached {}  to disk, length = {}", path, segment_length);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to cache File segment to local disk.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

}

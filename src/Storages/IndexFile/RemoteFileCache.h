#pragma once

#include <Core/Types.h>
#include <memory>

namespace DB::IndexFile
{
struct RemoteFileInfo
{
    String hdfs_user;
    String hdfs_nn_proxy;
    String path;            /// full path to the remote file containing the logical file
    UInt64 start_offset;    /// offset to the beginning of the logical file
    size_t size;            /// size of the logical file
    String cache_key;       /// unique identifier for the file in the cache
};

class RemoteFileCache
{
public:
    virtual ~RemoteFileCache() = default;

    /// If file is cached locally, return opened fd to it.
    /// Client must call `release(fd)` after reading the file.
    /// Otherwise return -1 and cache the file in the background.
    virtual int get(const RemoteFileInfo & file) = 0;

    /// Release the fd returned by `get` so that the file could be deleted
    /// if the cache decides to evicts it in order to cache new files.
    virtual void release(int fd) = 0;

    virtual String cacheFileName(const RemoteFileInfo & file) const = 0;
};

using RemoteFileCachePtr = std::shared_ptr<RemoteFileCache>;

}


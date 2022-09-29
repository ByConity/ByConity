#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/IndexFile/RemoteFileCache.h>
#include <common/logger_useful.h>

namespace DB
{
class KeyIndexFileCache : public IndexFile::RemoteFileCache
{
public:
    KeyIndexFileCache(Context & context, UInt64 max_size);
    ~KeyIndexFileCache() override;

    /// If file is cached locally, return opened fd to it.
    /// Client must call `release(fd)` after reading the file.
    /// Otherwise return -1 and cache the file in the background.
    int get(const IndexFile::RemoteFileInfo & file) override;

    /// Release the fd returned by `get` so that the file could be deleted
    /// if the cache decides to evicts it in order to cache new files.
    void release(int fd) override;

    String cacheFileName(const IndexFile::RemoteFileInfo & file) const override;

private:
    void initCacheFromFileSystem();

    Poco::Logger * log;
    struct Rep;
    std::shared_ptr<Rep> rep;
};

}

#pragma once

#include <memory>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/IndexFile/IndexFileReader.h>

namespace DB
{
class UniqueKeyIndex
{
public:
    /// empty index
    UniqueKeyIndex() = default;

    /// created from local file located at "file_path".
    UniqueKeyIndex(const String & file_path, UniqueKeyIndexBlockCachePtr block_cache);

    /// created from remote file
    UniqueKeyIndex(
        const IndexFile::RemoteFileInfo & remote_file, UniqueKeyIndexFileCachePtr file_cache, DB::UniqueKeyIndexBlockCachePtr block_cache);

    /// return true and set rowid if found.
    /// return false if not found.
    /// throws exception if error.
    bool lookup(const String & key, UInt32 & rowid);

    /// Return an iterator over KVs in this file.
    /// Note: client should make sure the UniqueKeyIndex object lives longer than the returned iterator.
    std::unique_ptr<IndexFile::Iterator> new_iterator(const IndexFile::ReadOptions & options);

    size_t residentMemoryUsage() const;

private:
    /// nullptr if the index contains no entries
    std::unique_ptr<IndexFile::IndexFileReader> index_reader;
};

using UniqueKeyIndexPtr = std::shared_ptr<UniqueKeyIndex>;

}

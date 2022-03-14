#pragma once

#include <memory>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/IndexFile/IndexFileReader.h>

namespace DB
{

class UniqueRowStore
{
public:
    using Roaring = roaring::Roaring;
    using DeleteBitmapPtr = std::shared_ptr<const Roaring>;
    
    NamesAndTypesList columns;
    DeleteBitmapPtr columns_delete_bitmap;

    /// created from local file located at "file_path".
    UniqueRowStore(
        const String & file_path, IndexFileBlockCachePtr block_cache, NamesAndTypesList columns, DeleteBitmapPtr columns_delete_bitmap);

    /// Return an iterator over KVs in this file.
    /// Note: client should make sure the UniqueRowStore object lives longer than the returned iterator.
    std::unique_ptr<IndexFile::Iterator> new_iterator(const IndexFile::ReadOptions & options);

    size_t residentMemoryUsage() const;

private:

    void loadColumns(const String & columns_path);

    /// nullptr if the index contains no entries
    std::unique_ptr<IndexFile::IndexFileReader> index_reader;
};

using UniqueRowStorePtr = std::shared_ptr<UniqueRowStore>;
using UniqueRowStoreVector = std::vector<UniqueRowStorePtr>;

}

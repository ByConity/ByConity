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
    /// created from local file located at "file_path".
    UniqueRowStore(const String & file_path, IndexFileBlockCachePtr block_cache);

    /// Return an iterator over KVs in this file.
    /// Note: client should make sure the UniqueRowStore object lives longer than the returned iterator.
    std::unique_ptr<IndexFile::Iterator> new_iterator(const IndexFile::ReadOptions & options);

    size_t residentMemoryUsage() const;

private:
    /// nullptr if the index contains no entries
    std::unique_ptr<IndexFile::IndexFileReader> index_reader;
};

/// TODO(lta): add comments
struct UniqueRowStoreMeta
{
    NamesAndTypesList columns;
    
    NameSet removed_columns;

    UniqueRowStoreMeta() { }

    UniqueRowStoreMeta(NamesAndTypesList columns_, NameSet removed_column_) : columns(columns_), removed_columns(removed_column_) { }

    UniqueRowStoreMeta(const UniqueRowStoreMeta & meta) : columns(meta.columns), removed_columns(meta.removed_columns) { }

    void write(WriteBuffer & to);

    void read(ReadBuffer & in);
};

using UniqueRowStorePtr = std::shared_ptr<UniqueRowStore>;
using UniqueRowStoreVector = std::vector<UniqueRowStorePtr>;
using UniqueRowStoreMetaPtr = std::shared_ptr<UniqueRowStoreMeta>;
using UniqueRowStoreMetaVector = std::vector<UniqueRowStoreMetaPtr>;

}

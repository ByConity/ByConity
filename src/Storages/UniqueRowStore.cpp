#include <Storages/IndexFile/FilterPolicy.h>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/UniqueRowStore.h>
#include <Common/Coding.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

UniqueRowStore::UniqueRowStore(
    const String & file_path, IndexFileBlockCachePtr block_cache, NamesAndTypesList columns_, DeleteBitmapPtr columns_delete_bitmap_)
    : columns(columns_), columns_delete_bitmap(columns_delete_bitmap_)
{
    IndexFile::Options options;
    options.block_cache = std::move(block_cache);
    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
    auto local_reader = std::make_unique<IndexFile::IndexFileReader>(options);
    auto status = local_reader->Open(file_path);
    if (!status.ok())
        throw Exception("Failed to open index file " + file_path + ": " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    index_reader = std::move(local_reader);
}

std::unique_ptr<IndexFile::Iterator> UniqueRowStore::new_iterator(const IndexFile::ReadOptions & options)
{
    if (!index_reader)
        return std::unique_ptr<IndexFile::Iterator>(IndexFile::NewEmptyIterator());
    std::unique_ptr<IndexFile::Iterator> res;
    auto st = index_reader->NewIterator(options, &res);
    if (!st.ok())
        throw Exception("Failed to get iterator: " + st.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    return res;
}

size_t UniqueRowStore::residentMemoryUsage() const
{
    return index_reader ? index_reader->ResidentMemoryUsage() : sizeof(UniqueRowStore);
}

}

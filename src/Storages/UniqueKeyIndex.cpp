#include <Storages/IndexFile/FilterPolicy.h>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/UniqueKeyIndex.h>
#include <Common/Coding.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

bool MemoryUniqueKeyIndex::lookup(const String & key, UInt32 & rowid, String & version, size_t version_size)
{
    if (!unique_index)
        throw Exception("Unique index in memory unique key index is nullptr.", ErrorCodes::LOGICAL_ERROR);

    auto it = unique_index->find(StringRef(key));
    if (it == unique_index->end())
        return false;
    rowid = it->getMapped();
    /// In memory uki, the version data is at the end of the key
    version.assign(it->getKey().data + key.size(), version_size);
    return true;
}

DiskUniqueKeyIndex::DiskUniqueKeyIndex(const String & file_path, DiskUniqueKeyIndexBlockCachePtr block_cache)
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

bool DiskUniqueKeyIndex::lookup(const String & key, UInt32 & rowid, String & version, size_t version_size)
{
    if (!index_reader)
        throw Exception("Index reader in disk unique key index is nullptr.", ErrorCodes::LOGICAL_ERROR);

    String value;
    auto status = index_reader->Get(IndexFile::ReadOptions(), key, &value);
    if (status.ok())
    {
        Slice input(value);
        if (GetVarint32(&input, &rowid))
        {
            /// In disk uki, the version data is at the end of the value
            version.assign(input.data(), version_size);
            return true;
        }
        throw Exception("Failed to decode rowid", ErrorCodes::UNKNOWN_EXCEPTION);
    }
    else if (status.IsNotFound())
    {
        return false;
    }
    else
    {
        throw Exception("Failed to lookup key: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    }
}

size_t DiskUniqueKeyIndex::residentMemoryUsage() const
{
    return index_reader ? index_reader->ResidentMemoryUsage() : sizeof(DiskUniqueKeyIndex);
}

}

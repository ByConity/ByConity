#pragma once

#include <memory>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
class UniqueKeyIndex
{
public:
    enum class Type
    {
        MEMORY, /// In the old version, the unique key indices are stored in memory whose amount is limited by memory.
        DISK, /// In the new version, the unique key indices are stored in disk which support on-demand loading.
        UNKNOWN /// Initial state. If "unique_key.idx" file exists, change state to "DISK". Otherwise, change state to "MEMORY".
    };

    /// return true and set rowid and version if found.
    /// return false if not found.
    /// throws exception if error.
    virtual bool lookup(
        [[maybe_unused]] const String & key,
        [[maybe_unused]] UInt32 & rowid,
        [[maybe_unused]] String & version,
        [[maybe_unused]] size_t version_size)
    {
        return false;
    }
    virtual ~UniqueKeyIndex() = default;
};

/// Unique key indices are stored in memory. It maintains the size and access time of unique key indices.
class MemoryUniqueKeyIndex : public UniqueKeyIndex
{
    using MemoryIndex = HashMapWithSavedHash<StringRef, UInt32, StringRefHash, HashTableGrower<2>>;
    using MemoryIndexPtr = std::unique_ptr<MemoryIndex>;

public:
    MemoryUniqueKeyIndex(size_t num_element, size_t arena_initial_size)
    {
        unique_index_arena = std::make_shared<Arena>(arena_initial_size);
        unique_index = std::make_unique<MemoryIndex>(num_element);
    }

    bool lookup(const String & key, UInt32 & rowid, String & version, size_t version_size) override;

    size_t size() { return unique_index->size(); }

    size_t getBufferSizeInBytes() { return unique_index->getBufferSizeInBytes(); }

    ArenaPtr getArena() { return unique_index_arena; }

    UInt32 & operator[](const StringRef & key) { return (*unique_index)[key]; }

    void setMemoryBytes(size_t memory_bytes) { unique_index_memory_bytes.store(memory_bytes, std::memory_order_relaxed); }

    size_t getMemorySize() { return unique_index_memory_bytes.load(std::memory_order_relaxed); }

    void setAccessTime(time_t time) { unique_index_access_time.store(time, std::memory_order_relaxed); }

    time_t getAccessTime() { return unique_index_access_time.load(std::memory_order_relaxed); }

    MemoryIndex::iterator begin() { return unique_index->begin(); }

    MemoryIndex::iterator end() { return unique_index->end(); }

private:
    std::atomic<UInt64> unique_index_memory_bytes{0};
    std::atomic<time_t> unique_index_access_time{0}; /// last access time

    /// access to following fields should be protected by `unique_index_mutex'
    /// arena for unique key and version storage
    ArenaPtr unique_index_arena;

    /// Auxiliary index for locating unique key's rowid.
    /// serialized unique key -> rowid.
    MemoryIndexPtr unique_index;
};

/// Unique key indices are stored in disk.
class DiskUniqueKeyIndex : public UniqueKeyIndex
{
public:
    /// created from local file located at "file_path".
    DiskUniqueKeyIndex(const String & file_path, DiskUniqueKeyIndexBlockCachePtr block_cache);

    bool lookup(const String & key, UInt32 & rowid, String & version, size_t version_size) override;

    size_t residentMemoryUsage() const;

private:
    /// nullptr if the index contains no entries
    std::unique_ptr<IndexFile::IndexFileReader> index_reader;
};

using UniqueKeyIndexPtr = std::shared_ptr<UniqueKeyIndex>;
using UniqueKeyIndicesVector = std::vector<UniqueKeyIndexPtr>;
using MemoryUniqueKeyIndexPtr = std::shared_ptr<MemoryUniqueKeyIndex>;
using DiskUniqueKeyIndexPtr = std::shared_ptr<DiskUniqueKeyIndex>;
using UkiType = UniqueKeyIndex::Type;

}

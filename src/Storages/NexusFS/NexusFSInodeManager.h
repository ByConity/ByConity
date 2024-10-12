#pragma once

#include <chrono>
#include <memory>
#include <shared_mutex>
#include <stdint.h>

#include <google/protobuf/io/coded_stream.h>
#include <tsl/sparse_map.h>
#include <Poco/AtomicCounter.h>

#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <common/defines.h>
#include <common/types.h>
#include "IO/BufferWithOwnMemory.h"
#include "Storages/DiskCache/Buffer.h"
#include "Storages/DiskCache/Types.h"
#include "Storages/NexusFS/NexusFSBuffer.h"

#include <folly/concurrency/ConcurrentHashMap.h>


namespace DB::NexusFSComponents
{

struct FileCachedState
{
    String file_path;
    UInt64 total_segments;
    UInt64 cached_segments;
    UInt64 total_size;
    UInt64 cached_size;
};

class FileMeta
{
public:
    explicit FileMeta(size_t file_size_, UInt32 segment_size) : file_size(file_size_)
    {
        chassert(segment_size > 0);
        size_t segments_count = (file_size + segment_size - 1) / segment_size;
        segments.resize(segments_count);
    }
    FileMeta(const FileMeta &) = delete;
    FileMeta & operator=(const FileMeta &) = delete;

    size_t getFileSize() const { return file_size; }
    std::pair<UInt64, UInt64> getTotalSizeAndSegments() const { return {file_size, segments.size()}; }
    std::pair<UInt64, UInt64> getCachedSizeAndSegments();

    std::shared_ptr<BlockHandle> getHandle(UInt64 segment_id);
    void setHandle(UInt64 segment_id, std::shared_ptr<BlockHandle> & handle);

    void toProto(Protos::NexusFSFileMeta * proto);

    bool canBeRemoved();

private:
    folly::fibers::TimedMutex mutex;
    const size_t file_size;
    std::vector<std::shared_ptr<BlockHandle>> segments;
};

class Inode
{
public:
    explicit Inode(UInt64 id_) : id(id_) { }
    Inode(const Inode &) = delete;
    Inode & operator=(const Inode &) = delete;

    UInt64 getId() const { return id; }

    std::shared_ptr<BlockHandle> getHandle(String & file, UInt64 segment_id);
    void setHandle(
        const String & file,
        UInt64 segment_id,
        std::shared_ptr<BlockHandle> & handle,
        const std::function<std::pair<size_t, UInt32>()> & get_file_and_segment_size,
        std::atomic<UInt64> & num_file_metas);
    void setHandle(const String & file, std::shared_ptr<FileMeta> & file_meta);

    void toProto(Protos::NexusFSInode * node);

    void cleanInvalidFiles(std::atomic<UInt64> & num_file_metas);

    void getFileCachedStates(std::vector<FileCachedState> & result);

private:
    UInt64 id;
    folly::ConcurrentHashMap<String, std::shared_ptr<FileMeta>> files;
};

class InodeManager
{
public:
    explicit InodeManager(const String & prefix_, const UInt32 segment_size_)
        : prefix(prefix_), segment_size(segment_size_), root_inode(std::make_shared<Inode>(0))
    {
        num_inodes++;
    }
    InodeManager(const InodeManager &) = delete;
    InodeManager & operator=(const InodeManager &) = delete;


    void persist(google::protobuf::io::CodedOutputStream * stream) const;
    void recover(
        google::protobuf::io::CodedInputStream * stream, HybridCache::RegionManager & region_manager, std::atomic<UInt64> & num_segments);

    // Gets value and update tracking counters
    std::shared_ptr<BlockHandle> lookup(const String & path, UInt64 segment_id) const;

    // Gets value without updating tracking counters
    std::shared_ptr<BlockHandle> peek(const String & path, UInt64 segment_id) const;

    // Overwrites existing key if exists with new address adn size. If the entry was successfully overwritten,
    // LookupResult returns <true, OldRecord>.
    void insert(
        const String & path,
        UInt64 segment_id,
        std::shared_ptr<BlockHandle> & handle,
        const std::function<std::pair<size_t, UInt32>()> & get_file_and_segment_size);

    // Resets all the buckets to the initial state.
    void reset();

    void cleanInvalidFiles();

    UInt64 getNumInodes() const { return num_inodes.load(); }
    UInt64 getNumFileMetas() const { return num_file_metas.load(); }

    std::vector<FileCachedState> getFileCachedStates();

private:
    String extractValidPath(const String & path) const;
    void resolvePath(const String & path, std::vector<String> & resolved_dirs) const;

    LoggerPtr log = getLogger("NexusFSInodeManager");

    const String prefix;
    const UInt32 segment_size;
    std::atomic<UInt64> inode_id{1};
    std::shared_ptr<Inode> root_inode;
    folly::ConcurrentHashMap<String, std::shared_ptr<Inode>> inodes;

    std::atomic<UInt64> num_inodes{0};
    std::atomic<UInt64> num_file_metas{0};
};

}

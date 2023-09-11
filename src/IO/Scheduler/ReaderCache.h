#pragma once

#include <cstdint>
#include <map>
#include <list>
#include <mutex>
#include <memory>
#include <Core/Types.h>
#include <IO/RemoteFSReader.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::IO::Scheduler {

class SingleReaderCache {
public:
    struct Options {
        size_t max_cache_elements_ = 256;
    };

    struct CacheKey {
        CacheKey(const String& file_name, uint64_t offset, uint64_t size = 0):
            file_name_(file_name), offset_(offset), size_(size) {}

        bool operator<(const CacheKey& rhs) const {
            return file_name_ < rhs.file_name_ && offset_ < rhs.offset_
                && size_ < rhs.size_;
        }

        String file_name_;
        uint64_t offset_;
        uint64_t size_;
    };

    SingleReaderCache(const Options& opts);

    void insert(const String& file_name, std::unique_ptr<RemoteFSReader> reader);
    std::unique_ptr<RemoteFSReader> retrieve(const String& file_name, uint64_t offset);

private:
    struct CacheEntry;

    using MapType = std::multimap<CacheKey, CacheEntry>;
    using ListType = std::list<MapType::iterator>;

    struct CacheEntry {
        CacheEntry(ListType::iterator iter, std::unique_ptr<RemoteFSReader> reader):
            ddl_iter_(iter), reader_(std::move(reader)) {}

        ListType::iterator ddl_iter_;
        std::unique_ptr<RemoteFSReader> reader_;
    };

    Options opts_;

    std::mutex mu_;

    ListType ddl_queue_;
    MapType container_;
};

class ReaderCache {
public:
    struct Options {
        static Options parseFromConfig(const Poco::Util::AbstractConfiguration& cfg,
            const String& prefix);

        size_t shard_size_ = 8;
        size_t max_cache_size_ = 1024;
    };

    static ReaderCache& instance();

    void initialize(const Options& opts);

    SingleReaderCache& shard(const String& file_name);

private:
    Options opts_;

    std::hash<String> hasher_;

    std::vector<std::unique_ptr<SingleReaderCache>> caches_;
};

}

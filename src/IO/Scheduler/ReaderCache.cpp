#include <memory>
#include <mutex>
#include <IO/Scheduler/ReaderCache.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB::IO::Scheduler {

SingleReaderCache::SingleReaderCache(const Options& opts): opts_(opts) {}

void SingleReaderCache::insert(const String& file_name,
        std::unique_ptr<RemoteFSReader> reader) {
    CacheKey cache_key(file_name, reader->offset(), reader->remain());

    std::lock_guard<std::mutex> lock(mu_);

    {
        // Insert new reader
        ddl_queue_.emplace_back();
        auto ddl_queue_iter = std::prev(ddl_queue_.end());

        auto container_iter = container_.emplace(std::pair<CacheKey, CacheEntry>(
            cache_key, CacheEntry(ddl_queue_iter, std::move(reader))));

        *ddl_queue_iter = container_iter;
    }

    {
        // Evict if necessary
        if (container_.size() > opts_.max_cache_elements_) {
            auto iter = ddl_queue_.front();
            ddl_queue_.pop_front();

            container_.erase(iter);
        }
    }
}

std::unique_ptr<RemoteFSReader> SingleReaderCache::retrieve(const String& file_name,
        uint64_t offset) {
    std::lock_guard<std::mutex> lock(mu_);

    if (container_.empty()) {
        return nullptr;
    }

    // Find a possible reader
    auto iter = container_.lower_bound(CacheKey(file_name, offset));
    if (iter != container_.begin()) {
        if (iter != container_.end()) {
            if (iter->first.offset_ != offset
                    || iter->first.file_name_ != file_name) {
                iter = std::prev(iter);
            }
        } else {
            iter = std::prev(iter);
        }
    }

    if (iter != container_.end()) {
        if (iter->first.file_name_ == file_name) {
            std::unique_ptr<RemoteFSReader> reader = std::move(iter->second.reader_);

            ddl_queue_.erase(iter->second.ddl_iter_);
            container_.erase(iter);

            return reader;
        }
    }

    return nullptr;
}

ReaderCache::Options ReaderCache::Options::parseFromConfig(const Poco::Util::AbstractConfiguration& cfg,
        const String& prefix) {
    return ReaderCache::Options {
        .shard_size_ = cfg.getUInt(prefix + ".shard_size", 8),
        .max_cache_size_ = cfg.getUInt(prefix + ".cache_size", 1024),
    };
}

ReaderCache& ReaderCache::instance() {
    static ReaderCache cache;
    return cache;
}

void ReaderCache::initialize(const Options& opts) {
    opts_ = opts;

    size_t cache_per_shard = std::max(1ul,
        (opts.max_cache_size_ + opts.shard_size_ - 1) / opts.shard_size_);
    for (size_t i = 0; i < opts.shard_size_; ++i) {
        caches_.emplace_back(std::make_unique<SingleReaderCache>(
            SingleReaderCache::Options {
                .max_cache_elements_ = cache_per_shard
            }
        ));
    }
}

SingleReaderCache& ReaderCache::shard(const String& file_name) {
    return *caches_[hasher_(file_name) % opts_.shard_size_];
}

}

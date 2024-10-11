#pragma once

#include <mutex>
#include <set>
#include <unordered_map>
#include <boost/core/noncopyable.hpp>
#include <common/types.h>
#include <Common/FST.h>
#include <Common/BucketLRUCache.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/IndexFile/Cache.h>
#include <Storages/IndexFile/Env.h>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/IndexFile/RemappingEnv.h>
#include <Storages/MergeTree/GINStoreCommon.h>
#include <Storages/MergeTree/GinIdxFilterResultCache.h>

namespace DB
{

using GinIndexPostingsList = roaring::Roaring;
using GinIndexPostingsListPtr = std::shared_ptr<GinIndexPostingsList>;

/// Container for postings lists for each segment
using GinSegmentedPostingsListContainer = std::unordered_map<UInt32, GinIndexPostingsListPtr>;

/// Postings lists and terms built from query string
using GinPostingsCache = std::unordered_map<std::string, GinSegmentedPostingsListContainer>;
using GinPostingsCachePtr = std::shared_ptr<GinPostingsCache>;

/// GinIndexReader may shared between multiple threads, so all
/// member functions should be thread safe
class GINStoreReader
{
public:
    static std::shared_ptr<GINStoreReader> open(const String& name_,
        GinDataPartHelperPtr&& storage_info_, const std::shared_ptr<IndexFile::Cache>& sst_block_cache_);

    explicit GINStoreReader(const String& name_, GinDataPartHelperPtr&& storage_info_);

    virtual ~GINStoreReader() = default;

    /// Read postings lists for terms (which are created by tokenzing query string)
    virtual GinPostingsCachePtr createPostingsCacheFromTerms(
        const std::set<String> & terms) = 0;

    virtual size_t residentMemory() const = 0;

    String partID() const;
    const String& name() const;

protected:
    GinDataPartHelperPtr storage_info;
    const String idx_name;
};

class GINStoreReaderV0V1V2Base: public GINStoreReader
{
public:
    GINStoreReaderV0V1V2Base(const String& idx_name_,
        GinDataPartHelperPtr&& storage_info_, UInt32 segment_num_);

protected:
    std::vector<GinIndexSegmentV0V1V2> loadSegmentMetas();

    const UInt32 segment_num;
};

class GINStoreReaderV0: public GINStoreReaderV0V1V2Base
{
public:
    GINStoreReaderV0(const String& idx_name_,
        GinDataPartHelperPtr&& storage_info_, UInt32 segment_num_);

    virtual GinPostingsCachePtr createPostingsCacheFromTerms(
        const std::set<String> & terms) override;

    virtual size_t residentMemory() const override;

private:
    struct SegmentMeta
    {
        explicit SegmentMeta(UInt64 postings_offset_):
            postings_start_offset(postings_offset_) {}

        UInt64 postings_start_offset;

        FST::FiniteStateTransducer dict;
    };

    void loadDictionaries(
        const std::vector<GinIndexSegmentV0V1V2>& segment_metas_);

    /// Mapping from segment id to segment meta and segment fst
    std::map<UInt32, std::unique_ptr<SegmentMeta>> segments;
};

class GINStoreReaderV2: public GINStoreReaderV0V1V2Base
{
public:
    GINStoreReaderV2(const String& idx_name_,
        GinDataPartHelperPtr&& storage_info_, UInt32 segment_num_,
        const std::shared_ptr<IndexFile::Cache>& sst_block_cache_);

    virtual GinPostingsCachePtr createPostingsCacheFromTerms(
        const std::set<String> & terms) override;

    virtual size_t residentMemory() const override;

private:
    struct SegmentMeta
    {
        SegmentMeta(UInt64 posting_offset_,
            std::unique_ptr<IndexFile::IndexFileReader>&& sst_reader_):
                postings_start_offset(posting_offset_), sst_reader(std::move(sst_reader_)) {}

        const UInt64 postings_start_offset;
        std::once_flag sst_reader_init_flag;
        std::unique_ptr<IndexFile::IndexFileReader> sst_reader;
    };

    void loadDictionaries(
        const std::vector<GinIndexSegmentV0V1V2>& segment_metas_);

    std::shared_ptr<IndexFile::Cache> sst_block_cache;
    std::unique_ptr<IndexFile::Env> env;
    /// Mapping from segment id to segment meta and segment sst
    std::map<UInt32, std::unique_ptr<SegmentMeta>> segments;
};

/// PostingsCacheForStore contains postings lists from 'store' which are retrieved from Gin index files for the terms in query strings
/// GinPostingsCache is per query string (one query can have multiple query strings): when skipping index (row ID ranges) is used for the part during the
/// query, the postings cache is created and associated with the store where postings lists are read
/// for the tokenized query string. The postings caches are released automatically when the query is done.
struct PostingsCacheForStore
{
    /// Which store to retrieve postings lists
    std::shared_ptr<GINStoreReader> store = nullptr;

    /// map of <query, postings lists>
    std::unordered_map<String, GinPostingsCachePtr> cache;

    GinIdxFilterResultCache* filter_result_cache = nullptr;

    /// Get postings lists for query string, return nullptr if not found
    GinPostingsCachePtr getPostings(const String & query_string) const;
};

struct GinIndexReaderCacheWeighter
{
    size_t operator()(const GINStoreReader& reader) const
    {
        return reader.residentMemory();
    }
};

struct GINStoreReaderFactorySettings
{
    size_t lru_max_size {5368709120}; /// Reader cache size, for GINStoreReader, default to 5GB
    size_t mapping_bucket_size {1000};
    size_t lru_update_interval {60};
    size_t cache_shard_num {2};
    size_t cache_ttl {60};
    size_t sst_block_cache_size {5368709120}; /// Block cache size, for sst, default to 5GB
};

class GINStoreReaderFactory: private boost::noncopyable
{
public:
    explicit GINStoreReaderFactory(const GINStoreReaderFactorySettings& settings_);

    std::shared_ptr<GINStoreReader> get(const String& name_,
        GinDataPartHelperPtr&& storage_info_);

    size_t residentMemory() const;
    size_t sstBlockCacheSize() const;

private:
    std::shared_ptr<IndexFile::Cache> sst_block_cache;

    using CacheContainer = BucketLRUCache<String, GINStoreReader, size_t,
        GinIndexReaderCacheWeighter>;
    ShardCache<String, std::hash<String>, CacheContainer> reader_cache;
};

}

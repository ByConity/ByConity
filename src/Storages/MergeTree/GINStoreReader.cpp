#include <Storages/MergeTree/GINStoreReader.h>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/VarInt.h>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/IndexFile/Options.h>
#include <Storages/IndexFile/RemappingEnv.h>
#include <Storages/IndexFile/Cache.h>
#include <Storages/MergeTree/GINStoreCommon.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int UNKNOWN_FORMAT_VERSION;
}

std::shared_ptr<GINStoreReader> GINStoreReader::open(const String& name_,
    GinDataPartHelperPtr&& storage_info_, const std::shared_ptr<IndexFile::Cache>& sst_block_cache_)
{
    GINStoreVersion version = GINStoreVersion::v0;
    UInt32 segment_num = 0;

    /// Read gin store version and segment num
    {
        String version_file_name = name_ + GIN_SEGMENT_ID_FILE_EXTENSION;
        if (!storage_info_->exists(version_file_name))
        {
            return nullptr;
        }
        auto version_file_stream = storage_info_->readFile(version_file_name, 4096);

        uint8_t raw_version = 0;
        readBinary(raw_version, *version_file_stream);
        version = static_cast<GINStoreVersion>(raw_version);

        if (version != GINStoreVersion::v0 && version != GINStoreVersion::v1
            && version != GINStoreVersion::v2)
        {
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported "
                "inverted index version {}", raw_version);
        }

        readVarUInt(segment_num, *version_file_stream);
        /// NOTE: In gin store version it actually record next segment id
        --segment_num;
    }

    switch (version)
    {
        case GINStoreVersion::v0:
        case GINStoreVersion::v1:
        {
            return std::make_shared<GINStoreReaderV0>(name_, std::move(storage_info_),
                segment_num);
        }
        case GINStoreVersion::v2:
        {
            return std::make_shared<GINStoreReaderV2>(name_, std::move(storage_info_),
                segment_num, sst_block_cache_);
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported "
                "inverted index version {}", static_cast<uint8_t>(version));
    }
}

GINStoreReader::GINStoreReader(const String& name_, GinDataPartHelperPtr&& storage_info_):
    storage_info(std::move(storage_info_)), idx_name(name_)
{
}

String GINStoreReader::partID() const
{
    return storage_info->getPartUniqueID();
}

const String& GINStoreReader::name() const
{
    return idx_name;
}

GINStoreReaderV0V1V2Base::GINStoreReaderV0V1V2Base(const String& idx_name_,
    GinDataPartHelperPtr&& storage_info_, UInt32 segment_num_):
        GINStoreReader(idx_name_, std::move(storage_info_)),
        segment_num(segment_num_)
{
}

std::vector<GinIndexSegmentV0V1V2> GINStoreReaderV0V1V2Base::loadSegmentMetas()
{
    if (segment_num == 0)
    {
        return {};
    }

    /// Load segment metadata
    std::vector<GinIndexSegmentV0V1V2> segments(segment_num);

    size_t total_segment_bytes = segment_num * sizeof(GinIndexSegmentV0V1V2);
    auto metadata_file_stream = storage_info->readFile(
        idx_name + GIN_SEGMENT_METADATA_FILE_EXTENSION, total_segment_bytes);
    metadata_file_stream->readStrict(reinterpret_cast<char*>(segments.data()),
        total_segment_bytes);

    return segments;
}

GINStoreReaderV0::GINStoreReaderV0(const String& idx_name_,
    GinDataPartHelperPtr&& storage_info_, UInt32 segment_num_):
        GINStoreReaderV0V1V2Base(idx_name_, std::move(storage_info_), segment_num_)
{
    std::vector<GinIndexSegmentV0V1V2> segment_metas = loadSegmentMetas();
    loadDictionaries(segment_metas);
}

GinPostingsCachePtr GINStoreReaderV0::createPostingsCacheFromTerms(
    const std::set<String> & terms)
{
    auto postings_cache = std::make_shared<GinPostingsCache>();
    if (terms.empty() || segments.empty())
    {
        return postings_cache;
    }

    String posting_file_name = idx_name + GIN_POSTINGS_FILE_EXTENSION;
    auto posting_file_stream = storage_info->readFile(posting_file_name,
        DBMS_DEFAULT_BUFFER_SIZE);
    for (const auto& segment : segments)
    {
        for (const auto& term : terms)
        {
            GinSegmentedPostingsListContainer& posting_container = (*postings_cache)[term];

            auto [offset, found] = segment.second->dict.getOutput(term);
            if (found)
            {
                posting_container[segment.first] = GinIndexPostingsBuilder::deserialize(
                    *posting_file_stream, segment.second->postings_start_offset + offset);
            }
        }
    }
    return postings_cache;
}

size_t GINStoreReaderV0::residentMemory() const
{
    size_t total_mem = 0;
    for (const auto & pair : segments)
    {
        total_mem += (pair.second->dict.getData().capacity()) * sizeof (UInt8);
    }
    return total_mem;
}

void GINStoreReaderV0::loadDictionaries(
    const std::vector<GinIndexSegmentV0V1V2>& segment_metas_)
{
    String dict_file_name = idx_name + GIN_DICTIONARY_FILE_EXTENSION;
    std::unique_ptr<SeekableReadBuffer> dict_file_stream = storage_info->readFile(
        dict_file_name, DBMS_DEFAULT_BUFFER_SIZE);
    for (const auto& segment_meta : segment_metas_)
    {
        auto segment = std::make_unique<SegmentMeta>(segment_meta.postings_start_offset);

        dict_file_stream->seek(segment_meta.dict_start_offset);
        /// Read FST size
        size_t fst_size = 0;
        readVarUInt(fst_size, *dict_file_stream);
        /// Read FST blob
        segment->dict.getData().resize(fst_size);
        dict_file_stream->readStrict(
            reinterpret_cast<char*>(segment->dict.getData().data()), fst_size);

        segments.emplace(segment_meta.segment_id, std::move(segment));
    }
}

GINStoreReaderV2::GINStoreReaderV2(const String& idx_name_,
    GinDataPartHelperPtr&& storage_info_, UInt32 segment_num_,
    const std::shared_ptr<IndexFile::Cache>& sst_block_cache_):
        GINStoreReaderV0V1V2Base(idx_name_, std::move(storage_info_), segment_num_),
        sst_block_cache(sst_block_cache_)
{
    std::vector<GinIndexSegmentV0V1V2> segment_metas = loadSegmentMetas();
    loadDictionaries(segment_metas);
}

GinPostingsCachePtr GINStoreReaderV2::createPostingsCacheFromTerms(
    const std::set<String>& terms)
{
    auto postings_cache = std::make_shared<GinPostingsCache>();
    if (terms.empty() || segments.empty())
    {
        return postings_cache;
    }

    String posting_file_name = idx_name + GIN_POSTINGS_FILE_EXTENSION;
    auto posting_file_stream = storage_info->readFile(posting_file_name,
        DBMS_DEFAULT_BUFFER_SIZE);
    String value;
    for (const auto& segment : segments)
    {
        std::call_once(segment.second->sst_reader_init_flag,
            [&sst_reader = segment.second->sst_reader, segment_id = segment.first]() {
                String segment_sst_name = v2StoreSegmentDictName(segment_id);
                if (IndexFile::Status s = sst_reader->Open(segment_sst_name); !s.ok())
                {
                    throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to open sst {} "
                        ", error {}", segment_sst_name, s.ToString());
                }
            }
        );

        for (const auto& term : terms)
        {
            /// Read posting offset from sstable
            Slice key(term.data(), term.size());
            IndexFile::Status status = segment.second->sst_reader->Get(
                IndexFile::ReadOptions(), key, &value);
            if (status.IsNotFound())
            {
                continue;
            }
            else if (!status.ok())
            {
                throw Exception(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Failed "
                    "to read sst {}", status.ToString());
            }

            ReadBufferFromString buf(value);
            UInt64 posting_offset;
            readIntBinary(posting_offset, buf);

            /// Read posting list
            GinSegmentedPostingsListContainer& posting_container = (*postings_cache)[term];
            posting_container[segment.first] = GinIndexPostingsBuilder::deserialize(
                *posting_file_stream, segment.second->postings_start_offset + posting_offset);
        }
    }
    return postings_cache;
}

size_t GINStoreReaderV2::residentMemory() const
{
    size_t total_mem = 0;
    for (const auto & pair : segments)
    {
        total_mem += pair.second->sst_reader->ResidentMemoryUsage();
    }
    return total_mem;
}

void GINStoreReaderV2::loadDictionaries(
    const std::vector<GinIndexSegmentV0V1V2>& segment_metas_)
{
    String dict_file_name = idx_name + GIN_DICTIONARY_FILE_EXTENSION;
    size_t dict_file_size = storage_info->getFileSize(dict_file_name);

    env = std::make_unique<IndexFile::ReadRemappingEnv>([this, dict_file_name](size_t buf_size) {
        return storage_info->readFile(dict_file_name, buf_size);
    }, dict_file_size);

    for (const auto& segment_meta : segment_metas_)
    {
        auto sst_reader = std::make_unique<IndexFile::IndexFileReader>(
            IndexFile::Options {
                .env = env.get(),
                .block_cache = sst_block_cache
            }
        );
        auto segment = std::make_unique<SegmentMeta>(segment_meta.postings_start_offset,
            std::move(sst_reader));

        segments.emplace(segment_meta.segment_id, std::move(segment));
    }
}

GinPostingsCachePtr PostingsCacheForStore::getPostings(const String & query_string) const
{
    auto it = cache.find(query_string);
    if (it == cache.end())
        return nullptr;
    return it->second;
}

GINStoreReaderFactory::GINStoreReaderFactory(const GINStoreReaderFactorySettings& settings_)
    : reader_cache(
        settings_.cache_shard_num,
        CacheContainer::Options{
            .lru_update_interval = static_cast<UInt32>(settings_.lru_update_interval),
            .mapping_bucket_size = static_cast<UInt32>(std::max(1UL, settings_.mapping_bucket_size / settings_.cache_shard_num)),
            .max_weight = std::max(static_cast<size_t>(1), static_cast<size_t>(settings_.lru_max_size / settings_.cache_shard_num)),
            .cache_ttl = static_cast<UInt32>(settings_.cache_ttl),
        }
    )
{
    if (settings_.sst_block_cache_size > 0)
    {
        sst_block_cache = IndexFile::NewLRUCache(settings_.sst_block_cache_size);
    }
}

std::shared_ptr<GINStoreReader> GINStoreReaderFactory::get(const String& name_,
    GinDataPartHelperPtr&& storage_info_)
{
    String key = fmt::format("{}:{}", storage_info_->getPartUniqueID(), name_);

    auto& shard = reader_cache.shard(key);
    if (auto cached_store = shard.get(key); cached_store != nullptr)
    {
        return cached_store;
    }
    return shard.getOrSet(key, [&]() -> std::shared_ptr<GINStoreReader> {
        return GINStoreReader::open(name_, std::move(storage_info_), sst_block_cache);
    });
}

size_t GINStoreReaderFactory::residentMemory() const
{
    return reader_cache.weight();
}

size_t GINStoreReaderFactory::sstBlockCacheSize() const
{
    return sst_block_cache == nullptr ? 0 : sst_block_cache->TotalCharge();
}

}

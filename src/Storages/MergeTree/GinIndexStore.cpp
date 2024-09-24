#include <Storages/MergeTree/GinIndexStore.h>
#include <Columns/ColumnString.h>
#include <common/logger_useful.h>
#include <Common/FST.h>
#include <common/find_symbols.h>
#include <Compression/CompressionFactory.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <IO/ConsecutiveReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <fmt/core.h>
#include <Poco/Logger.h>
#include <cmath>
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <numeric>
#include <algorithm>

namespace ProfileEvents
{
    extern const Event PostingReadBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int CANNOT_READ_ALL_DATA;
};

GinIndexPostingsBuilder::GinIndexPostingsBuilder()
    : rowid_lst{}
{}

bool GinIndexPostingsBuilder::contains(UInt32 row_id) const
{
    if (useRoaring())
        return rowid_bitmap.contains(row_id);
    else
    {
        const auto * const it = std::find(rowid_lst.begin(), rowid_lst.begin()+rowid_lst_length, row_id);
        return it != rowid_lst.begin() + rowid_lst_length;
    }
}

String GinIndexPostingsBuilder::toString() const
{
    if (useRoaring())
    {
        return "Roaring bitmap:" + rowid_bitmap.toString();
    }
    else 
    {
        String result = "rowid_lst: ";
        for(const auto & val : rowid_lst)
        {   
            result = fmt::format("{} {}", result, val);
        }
        return result;
    }
}

void GinIndexPostingsBuilder::add(UInt32 row_id)
{
    if (containsAllRows())
        return;

    if (useRoaring())
    {
        rowid_bitmap.add(row_id);
    }
    else
    {
        assert(rowid_lst_length < MIN_SIZE_FOR_ROARING_ENCODING);
        rowid_lst[rowid_lst_length] = row_id;
        rowid_lst_length++;

        if (rowid_lst_length == MIN_SIZE_FOR_ROARING_ENCODING)
        {
            for (size_t i = 0; i < rowid_lst_length; i++)
                rowid_bitmap.add(rowid_lst[i]);

            rowid_lst_length = USES_BIT_MAP;
        }
    }
}

UInt64 GinIndexPostingsBuilder::serialize(WriteBuffer & buffer, size_t rows_limit)
{
    UInt64 written_bytes = 0;

    if (useRoaring() && rowid_bitmap.cardinality() > rows_limit)
    {
        buffer.write(1);
        writeVarUInt(CONTAINS_ALL, buffer);
        return getLengthOfVarUInt(CONTAINS_ALL) + 1;
    }
    else
    {
        buffer.write(rowid_lst_length);
        written_bytes += 1;
    }

    if (useRoaring())
    {
        rowid_bitmap.runOptimize();

        auto size = rowid_bitmap.getSizeInBytes();
        auto buf = std::make_unique<char[]>(size);
        rowid_bitmap.write(buf.get());

        auto codec = CompressionCodecFactory::instance().get(GIN_COMPRESSION_CODEC, GIN_COMPRESSION_LEVEL);
        Memory<> memory;
        memory.resize(codec->getCompressedReserveSize(static_cast<UInt32>(size)));
        auto compressed_size = codec->compress(buf.get(), static_cast<UInt32>(size), memory.data());

        writeVarUInt(size, buffer);
        written_bytes += getLengthOfVarUInt(size);

        writeVarUInt(compressed_size, buffer);
        written_bytes += getLengthOfVarUInt(compressed_size);
        
        buffer.write(memory.data(), compressed_size);
        written_bytes += compressed_size;
    }
    else
    {
        for (size_t i = 0; i <  rowid_lst_length; ++i)
        {
            writeVarUInt(rowid_lst[i], buffer);
            written_bytes += getLengthOfVarUInt(rowid_lst[i]);
        }
    }

    return written_bytes;
}

GinIndexPostingsListPtr GinIndexPostingsBuilder::deserialize(SeekableReadBuffer& buffer,
    size_t posting_offset)
{
    size_t read_big_readed = 0;
    size_t init_buffer_count = buffer.count();
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::PostingReadBytes, buffer.supportsReadAt() ?
            read_big_readed : buffer.count() - init_buffer_count);
    });

    /// Speculate read some data, to bypass buffered read, since we found it's read
    /// amplification is quiet large. We need two IO for each posting list
    /// One byte for posting list size and 18 bytes for two VarUInt64
    size_t speculate_prefix_size = 1 + 2 * 9;
    String speculate_prefix_data(speculate_prefix_size, '\0');

    size_t speculate_prefix_readed = 0;
    if (buffer.supportsReadAt())
    {
        speculate_prefix_readed = buffer.readBigAt(speculate_prefix_data.data(), speculate_prefix_size,
            posting_offset);
        read_big_readed += speculate_prefix_readed;
    }
    else
    {
        buffer.seek(posting_offset, SEEK_SET);
        speculate_prefix_readed = buffer.readBig(speculate_prefix_data.data(), speculate_prefix_size);
    }
    if (speculate_prefix_readed == 0)
    {
        throw Exception("Unexpected EOF when deserialize posting list",
            ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    ReadBufferFromString speculate_prefix_buffer(speculate_prefix_data);

    UInt8 postings_list_size = 0;
    speculate_prefix_buffer.readStrict(reinterpret_cast<char &>(postings_list_size));

    if (postings_list_size == USES_BIT_MAP)
    {
        /// Following two read will processed by prefix buffer and won't trigger a buffer read
        size_t size = 0;
        size_t compressed_size = 0;
        readVarUInt(size, speculate_prefix_buffer);
        readVarUInt(compressed_size, speculate_prefix_buffer);

        auto buf = std::make_unique<char []>(compressed_size);

        size_t copied = speculate_prefix_buffer.readBig(reinterpret_cast<char*>(buf.get()),
            compressed_size);
        /// Still some data to read
        if (copied < compressed_size)
        {
            size_t bytes_remain = compressed_size - copied;
            size_t bytes_readed = 0;
            if (buffer.supportsReadAt())
            {
                bytes_readed = buffer.readBigAt(reinterpret_cast<char*>(buf.get()) + copied,
                    bytes_remain, posting_offset + speculate_prefix_readed);
                read_big_readed += bytes_readed;
            }
            else
            {
                bytes_readed = buffer.readBig(reinterpret_cast<char*>(buf.get()) + copied,
                    bytes_remain);
            }
            if (bytes_readed < bytes_remain)
            {
                throw Exception(fmt::format("Cannot read all data when deserialize posting list, expect {}, readed {}",
                    bytes_remain, bytes_readed), ErrorCodes::CANNOT_READ_ALL_DATA);
            }
        }

        Memory<> memory;
        memory.resize(size);
        auto codec = CompressionCodecFactory::instance().get(GIN_COMPRESSION_CODEC, GIN_COMPRESSION_LEVEL);
        codec->decompress(buf.get(), static_cast<UInt32>(compressed_size), memory.data());

        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>(GinIndexPostingsList::read(memory.data()));

        return postings_list;
    }
    else
    {
        assert(postings_list_size < MIN_SIZE_FOR_ROARING_ENCODING);
        GinIndexPostingsListPtr postings_list = std::make_shared<GinIndexPostingsList>();
        UInt32 row_ids[MIN_SIZE_FOR_ROARING_ENCODING];

        String speculate_posting_data;
        if (postings_list_size > 2)
        {
            size_t speculate_posting_size = (postings_list_size - 2) * 9;
            speculate_posting_data.resize(speculate_posting_size);

            size_t readed = 0;
            if (buffer.supportsReadAt())
            {
                readed = buffer.readBigAt(speculate_posting_data.data(),
                    speculate_posting_size, posting_offset + speculate_prefix_readed);
                read_big_readed += readed;
            }
            else
            {
                readed = buffer.readBig(speculate_posting_data.data(), speculate_posting_size);
            }
            speculate_posting_data.resize(readed);
        }

        auto read_posting_list = [&](ReadBuffer& input_buffer) {
            for (auto i = 0; i < postings_list_size; ++i)
                readVarUInt(row_ids[i], input_buffer);
            postings_list->addMany(postings_list_size, row_ids);
        };

        if (speculate_posting_data.empty())
        {
            read_posting_list(speculate_prefix_buffer);
        }
        else
        {
            ReadBufferFromString speculate_posting_buffer(speculate_posting_data);
            ConsecutiveReadBuffer merged_buffer({&speculate_prefix_buffer, &speculate_posting_buffer});
            read_posting_list(merged_buffer);
        }
        return postings_list;
    }
}

GinIndexStore::GinIndexStore(const String & name_, GinDataPartHelperPtr && storage_info_)
    : name(name_)
    , storage_info(std::move(storage_info_))
{
}

GinIndexStore::GinIndexStore(const String & name_, GinDataPartHelperPtr && storage_info_, UInt64 max_digestion_size_)
    : name(name_)
    , storage_info(std::move(storage_info_))
    , max_digestion_size(max_digestion_size_)
{
}

bool GinIndexStore::exists() const
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_EXTENSION;
    return storage_info->exists(segment_id_file_name);
}

UInt32 GinIndexStore::getNextSegmentIDRange(const String & file_name, size_t n)
{
    std::lock_guard guard(mutex);

    /// When the method is called for the first time, the file doesn't exist yet, need to create it and write segment ID 1.
    if (!this->storage_info->exists(file_name))
    {
        /// Create file
        std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->storage_info->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);

        /// Write version
        writeChar(static_cast<char>(CURRENT_GIN_FILE_FORMAT_VERSION), *ostr);

        /// Write segment ID 1
        writeVarUInt(1, *ostr);
        ostr->sync();
        ostr->finalize();
    }

    /// Read id in file
    UInt32 result = 0;
    {
        std::unique_ptr<SeekableReadBuffer> istr = this->storage_info->readFile(file_name);

        /// Skip version
        istr->seek(1, SEEK_SET);

        readVarUInt(result, *istr);
    }

    /// Save result + n
    {
        std::unique_ptr<DB::WriteBufferFromFileBase> ostr = this->storage_info->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);

        /// Write version
        writeChar(static_cast<char>(CURRENT_GIN_FILE_FORMAT_VERSION), *ostr);

        writeVarUInt(result + n, *ostr);
        ostr->sync();
        ostr->finalize();
    }
    return result;
}

UInt32 GinIndexStore::getNextRowIDRange(size_t numIDs)
{
    UInt32 result = current_segment.next_row_id;
    current_segment.next_row_id += numIDs;
    current_segment.total_row_size += numIDs;
    return result;
}

UInt32 GinIndexStore::getNextSegmentID()
{
    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_EXTENSION;
    return getNextSegmentIDRange(segment_id_file_name, 1);
}

UInt32 GinIndexStore::getNumOfSegments()
{
    if (cached_segment_num)
        return cached_segment_num;

    String segment_id_file_name = getName() + GIN_SEGMENT_ID_FILE_EXTENSION;
    if (!storage_info->exists(segment_id_file_name))
        return 0;

    UInt32 result = 0;
    {
        std::unique_ptr<SeekableReadBuffer> istr = this->storage_info->readFile(segment_id_file_name);

        uint8_t version = 0;
        readBinary(version, *istr);

        if (version > static_cast<std::underlying_type_t<Format>>(CURRENT_GIN_FILE_FORMAT_VERSION))
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported inverted index version {}", version);

        readVarUInt(result, *istr);
    }

    cached_segment_num = result - 1;
    return cached_segment_num;
}

void GinIndexStore::setPostingsBuilder(const StringRef & term, GinIndexPostingsBuilderPtr builder)
{
    StringHashMap<GinIndexPostingsBuilderPtr>::LookupResult result;
    bool inserted = false;

    current_postings.builders.emplace(ArenaKeyHolder{term, *current_postings.term_pool}, result, inserted);
    assert(inserted && "Trying to add posting builder which it already exist");

    result.getMapped() = builder;
}

bool GinIndexStore::needToWrite() const
{
    assert(max_digestion_size > 0);
    return current_size > max_digestion_size;
}

void GinIndexStore::finalize()
{
    if (!current_postings.builders.empty())
        writeSegment();

    if (metadata_file_stream != nullptr)
        metadata_file_stream->finalize();

    if (dict_file_stream != nullptr)
        dict_file_stream->finalize();

    if (postings_file_stream != nullptr)
        postings_file_stream->finalize();
}

void GinIndexStore::initFileStreams()
{
    String metadata_file_name = getName() + GIN_SEGMENT_METADATA_FILE_EXTENSION;
    String dict_file_name = getName() + GIN_DICTIONARY_FILE_EXTENSION;
    String postings_file_name = getName() + GIN_POSTINGS_FILE_EXTENSION;

    metadata_file_stream = storage_info->writeFile(metadata_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    dict_file_stream =  storage_info->writeFile(dict_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    postings_file_stream =  storage_info->writeFile(postings_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
}

void GinIndexStore::writeSegment()
{
    if (metadata_file_stream == nullptr)
        initFileStreams();

    using TokenPostingsBuilderPair = std::pair<std::string_view, GinIndexPostingsBuilderPtr>;
    using TokenPostingsBuilderPairs = std::vector<TokenPostingsBuilderPair>;

    size_t rows_limit = std::lround(density * current_segment.total_row_size);

    LOG_DEBUG(&Poco::Logger::get("gin info"), 
        "density {} total_size {} next_row_id {} limit {} ", 
        density, current_segment.total_row_size, current_segment.next_row_id, rows_limit);


    /// Write segment
    metadata_file_stream->write(reinterpret_cast<char *>(&current_segment), sizeof(GinIndexSegment));
    TokenPostingsBuilderPairs token_postings_list_pairs;
    token_postings_list_pairs.reserve(current_postings.builders.size());

    current_postings.builders.forEachValue([&token_postings_list_pairs](const StringRef& term, const GinIndexPostingsBuilderPtr& builder) {
        token_postings_list_pairs.push_back({term.toView(), builder});
    });

    /// Sort token-postings list pairs since all tokens have to be added in FST in sorted order
    std::sort(token_postings_list_pairs.begin(), token_postings_list_pairs.end(),
                    [](const TokenPostingsBuilderPair & x, const TokenPostingsBuilderPair & y)
                    {
                        return x.first < y.first;
                    });

    /// Write postings
    std::vector<UInt64> posting_list_byte_sizes(current_postings.builders.size(), 0);

    for (size_t i = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        auto posting_list_byte_size = postings_list->serialize(*postings_file_stream, rows_limit);

        posting_list_byte_sizes[i] = posting_list_byte_size;
        i++;
        current_segment.postings_start_offset += posting_list_byte_size;
    }
    ///write item dictionary
    std::vector<UInt8> buffer;
    WriteBufferFromVector<std::vector<UInt8>> write_buf(buffer);
    FST::FstBuilder fst_builder(write_buf);

    UInt64 offset = 0;
    for (size_t i = 0; const auto & [token, postings_list] : token_postings_list_pairs)
    {
        fst_builder.add(token, offset);
        offset += posting_list_byte_sizes[i];
        i++;
    }

    fst_builder.build();
    write_buf.finalize();

    /// Write FST size
    writeVarUInt(buffer.size(), *dict_file_stream);
    current_segment.dict_start_offset += getLengthOfVarUInt(buffer.size());

    /// Write FST blob
    dict_file_stream->write(reinterpret_cast<char *>(buffer.data()), buffer.size());
    current_segment.dict_start_offset += buffer.size();

    current_size = 0;
    current_postings.reset();
    current_segment.segment_id = getNextSegmentID();
    
    metadata_file_stream->next();
    metadata_file_stream->sync();
    
    dict_file_stream->next();
    dict_file_stream->sync();
    
    postings_file_stream->next();
    postings_file_stream->sync();
    
}

void GinIndexStore::addToChecksums(MergeTreeDataPartChecksums & checksums)
{
    CityHash_v1_0_2::uint128 file_hash(0, 0);

    std::vector<String> file_names = {
        name + GIN_SEGMENT_ID_FILE_EXTENSION,
        name + GIN_SEGMENT_METADATA_FILE_EXTENSION,
        name + GIN_DICTIONARY_FILE_EXTENSION,
        name + GIN_POSTINGS_FILE_EXTENSION
    };

    for (const String& file_name : file_names)
    {
        if (storage_info->exists(file_name))
        {
            checksums.addFile(file_name, storage_info->getFileSize(file_name), file_hash);
        }
    }
}

GinSegmentDictionaryPtr GinIndexStore::getDictionary(UInt32 segment_id_) const
{
    auto iter = segment_dictionaries.find(segment_id_);
    if (iter == segment_dictionaries.end())
    {
        throw Exception("Didn't find dictionariy for segment", ErrorCodes::LOGICAL_ERROR);
    }
    return iter->second;
}

void GinIndexStore::setStoreDensity(const Float32 & density_)
{
    density = density_;
}

size_t GinIndexStore::cacheWeight() const
{
    size_t weight = 0;
    for (const auto & pair : segment_dictionaries)
    {
        weight += sizeof(UInt32); //key
        weight += 2 * sizeof(UInt64); //postings_start_offset + dict_start_offset
        weight += (pair.second->offsets.getData().capacity()) * sizeof(UInt8); //offsets
    }
    return weight;
}

GinIndexStoreDeserializer::GinIndexStoreDeserializer(const GinIndexStorePtr & store_)
    : store(store_)
{
    initFileStreams();
}

void GinIndexStoreDeserializer::initFileStreams()
{
    String metadata_file_name = store->getName() + GIN_SEGMENT_METADATA_FILE_EXTENSION;
    String dict_file_name = store->getName() + GIN_DICTIONARY_FILE_EXTENSION;
    String postings_file_name = store->getName() + GIN_POSTINGS_FILE_EXTENSION;

    metadata_file_stream = store->storage_info->readFile(metadata_file_name);
    dict_file_stream = store->storage_info->readFile(dict_file_name);
    postings_file_stream = store->storage_info->readFile(postings_file_name);
}

void GinIndexStoreDeserializer::readSegments()
{
    UInt32 num_segments = store->getNumOfSegments();

    //LOG_TRACE(&Poco::Logger::get("GinIndexStoreDeserializer"), "Get {} Gin segments ", num_segments);
    
    if (num_segments == 0)
        return;

    using GinIndexSegments = std::vector<GinIndexSegment>;
    GinIndexSegments segments (num_segments);

    assert(metadata_file_stream != nullptr);

    metadata_file_stream->readStrict(reinterpret_cast<char *>(segments.data()), num_segments * sizeof(GinIndexSegment));

    for (UInt32 i = 0; i < num_segments; ++i)
    {
        auto seg_id = segments[i].segment_id;
        auto seg_dict = std::make_shared<GinSegmentDictionary>();
        seg_dict->postings_start_offset = segments[i].postings_start_offset;
        seg_dict->dict_start_offset = segments[i].dict_start_offset;
        store->segment_dictionaries[seg_id] = seg_dict;
    }
}

void GinIndexStoreDeserializer::readSegmentDictionaries()
{
    for (UInt32 seg_index = 0; seg_index < store->getNumOfSegments(); ++seg_index)
        readSegmentDictionary(seg_index);
}

void GinIndexStoreDeserializer::readSegmentDictionary(UInt32 segment_id)
{
    /// Check validity of segment_id
    auto it = store->segment_dictionaries.find(segment_id);
    if (it == store->segment_dictionaries.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid segment id {}", segment_id);

    assert(dict_file_stream != nullptr);

    /// Set file pointer of dictionary file
    dict_file_stream->seek(it->second->dict_start_offset, SEEK_SET);

    it->second->offsets.getData().clear();
    /// Read FST size
    size_t fst_size = 0;
    readVarUInt(fst_size, *dict_file_stream);

    /// Read FST blob
    it->second->offsets.getData().resize(fst_size);
    dict_file_stream->readStrict(reinterpret_cast<char *>(it->second->offsets.getData().data()), fst_size);

    //LOG_TRACE(&Poco::Logger::get("GinIndexStoreDeserializer"), "Read Gin FST Dict {} size", fst_size);
}

GinPostingsCachePtr GinIndexStoreDeserializer::createPostingsCacheFromTerms(const std::set<String> & terms)
{
    auto postings_cache = std::make_shared<GinPostingsCache>();
    if (terms.empty() || store->segment_dictionaries.empty())
    {
        return postings_cache;
    }

    assert(postings_file_stream != nullptr);

    UInt32 min_seg_id = store->segment_dictionaries.begin()->first;
    UInt32 max_seg_id = min_seg_id;

    std::for_each(store->segment_dictionaries.begin(), store->segment_dictionaries.end(), [&](const std::pair<UInt32, GinSegmentDictionaryPtr>& entry) {
        min_seg_id = std::min(min_seg_id, entry.first);
        max_seg_id = std::max(max_seg_id, entry.first);
    });

    for (UInt32 seg_id = min_seg_id; seg_id <= max_seg_id; ++seg_id)
    {
        auto iter = store->segment_dictionaries.find(seg_id);
        if (iter != store->segment_dictionaries.end())
        {
            for (const auto& term : terms)
            {
                readSegmentPostingList(term, seg_id, iter->second, *postings_cache);
            }
        }
    }

    return postings_cache;
}

void GinIndexStoreDeserializer::readSegmentPostingList(const String& term,
    UInt32 segment_id, const GinSegmentDictionaryPtr& dict, GinPostingsCache& cache)
{
    GinSegmentedPostingsListContainer& posting_container = cache[term];

    auto [offset, found] = dict->offsets.getOutput(term);
    if (!found)
    {
        return;
    }

    posting_container[segment_id] = GinIndexPostingsBuilder::deserialize(
        *postings_file_stream, dict->postings_start_offset + offset);
}

GinPostingsCachePtr PostingsCacheForStore::getPostings(const String & query_string) const
{
    auto it = cache.find(query_string);
    if (it == cache.end())
        return nullptr;
    return it->second;
}

GinIndexStoreFactory::GinIndexStoreFactory(const GinIndexStoreCacheSettings & settings)
    : stores_lru_cache(
        settings.cache_shard_num,
        BucketLRUCache<String, GinIndexStore, size_t, GinIndexStoreWeightFunction>::Options{
            .lru_update_interval = static_cast<UInt32>(settings.lru_update_interval),
            .mapping_bucket_size = static_cast<UInt32>(std::max(1UL, settings.mapping_bucket_size / settings.cache_shard_num)),
            .max_weight = std::max(static_cast<size_t>(1), static_cast<size_t>(settings.lru_max_size / settings.cache_shard_num)),
            .cache_ttl = static_cast<UInt32>(settings.cache_ttl),
        })
{
}

GinIndexStorePtr GinIndexStoreFactory::get(const String & name, GinDataPartHelperPtr && storage_info)
{
    const String & part_path = storage_info->getPartUniqueID();
    String key = part_path + ":" + name;

    auto& shard = stores_lru_cache.shard(key);
    GinIndexStorePtr cache_result = shard.get(key);
    if (cache_result)
        return cache_result;

    return shard.getOrSet(key, [&]() -> GinIndexStorePtr {
        GinIndexStorePtr store = std::make_shared<GinIndexStore>(name, std::move(storage_info));
        if (!store->exists())
            return nullptr;

        GinIndexStoreDeserializer deserializer(store);
        deserializer.readSegments();
        deserializer.readSegmentDictionaries();
        return store;
    });
}

}

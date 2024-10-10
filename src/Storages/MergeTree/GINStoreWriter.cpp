#include <string_view>
#include <Storages/MergeTree/GINStoreWriter.h>
#include <Core/Defines.h>
#include <Common/FST.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConsecutiveReadBuffer.h>
#include <Storages/IndexFile/RemappingEnv.h>
#include <Storages/IndexFile/IndexFileWriter.h>
#include <Storages/MergeTree/GINStoreCommon.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>

namespace ProfileEvents
{
    extern const Event PostingReadBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int UNKNOWN_FORMAT_VERSION;
}

std::unique_ptr<GINStoreWriter> GINStoreWriter::open(GINStoreVersion version_,
    const String& name_, GinDataPartHelperPtr&& storage_info_,
    UInt64 max_digestion_size_, Float32 density_)
{
    switch (version_)
    {
        case GINStoreVersion::v0:
        case GINStoreVersion::v1:
        {
            return std::make_unique<GINStoreWriterV0>(name_, std::move(storage_info_),
                max_digestion_size_, density_);
        }
        case GINStoreVersion::v2:
        {
            return std::make_unique<GINStoreWriterV2>(name_, std::move(storage_info_),
                max_digestion_size_, density_);
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported "
                "inverted index version {}", static_cast<uint8_t>(version_));
    }
}

GINStoreWriter::GINStoreWriter(const String& name_, GinDataPartHelperPtr&& storage_info_,
    GINStoreVersion version_, UInt64 max_digestion_size_, Float32 density_):
        version(version_), density(density_), max_digestion_size(max_digestion_size_),
        name(name_), storage_info(std::move(storage_info_))
{
}

GinIndexPostingsBuilder::GinIndexPostingsBuilder()
    : rowid_lst{}
{}

bool GinIndexPostingsBuilder::contains(UInt32 row_id_) const
{
    if (useRoaring())
    {
        return rowid_bitmap.contains(row_id_);
    }
    else
    {
        const auto * const it = std::find(rowid_lst.begin(), rowid_lst.begin()+rowid_lst_length, row_id_);
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

void GinIndexPostingsBuilder::add(UInt32 row_id_)
{
    if (containsAllRows())
        return;

    if (useRoaring())
    {
        rowid_bitmap.add(row_id_);
    }
    else
    {
        assert(rowid_lst_length < MIN_SIZE_FOR_ROARING_ENCODING);
        rowid_lst[rowid_lst_length] = row_id_;
        rowid_lst_length++;

        if (rowid_lst_length == MIN_SIZE_FOR_ROARING_ENCODING)
        {
            for (size_t i = 0; i < rowid_lst_length; i++)
                rowid_bitmap.add(rowid_lst[i]);

            rowid_lst_length = USES_BIT_MAP;
        }
    }
}

UInt64 GinIndexPostingsBuilder::serialize(WriteBuffer& buffer_, size_t rows_limit_)
{
    UInt64 written_bytes = 0;

    if (useRoaring() && rowid_bitmap.cardinality() > rows_limit_)
    {
        buffer_.write(1);
        writeVarUInt(CONTAINS_ALL, buffer_);
        return getLengthOfVarUInt(CONTAINS_ALL) + 1;
    }
    else
    {
        buffer_.write(rowid_lst_length);
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

        writeVarUInt(size, buffer_);
        written_bytes += getLengthOfVarUInt(size);

        writeVarUInt(compressed_size, buffer_);
        written_bytes += getLengthOfVarUInt(compressed_size);
        
        buffer_.write(memory.data(), compressed_size);
        written_bytes += compressed_size;
    }
    else
    {
        for (size_t i = 0; i <  rowid_lst_length; ++i)
        {
            writeVarUInt(rowid_lst[i], buffer_);
            written_bytes += getLengthOfVarUInt(rowid_lst[i]);
        }
    }

    return written_bytes;
}

GinIndexPostingsListPtr GinIndexPostingsBuilder::deserialize(SeekableReadBuffer& buffer_,
    size_t posting_offset_)
{
    size_t read_big_readed = 0;
    size_t init_buffer_count = buffer_.count();
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::PostingReadBytes, buffer_.supportsReadAt() ?
            read_big_readed : buffer_.count() - init_buffer_count);
    });

    /// Speculate read some data, to bypass buffered read, since we found it's read
    /// amplification is quiet large. We need two IO for each posting list
    /// One byte for posting list size and 18 bytes for two VarUInt64
    size_t speculate_prefix_size = 1 + 2 * 9;
    String speculate_prefix_data(speculate_prefix_size, '\0');

    size_t speculate_prefix_readed = 0;
    if (buffer_.supportsReadAt())
    {
        speculate_prefix_readed = buffer_.readBigAt(speculate_prefix_data.data(), speculate_prefix_size,
            posting_offset_);
        read_big_readed += speculate_prefix_readed;
    }
    else
    {
        buffer_.seek(posting_offset_, SEEK_SET);
        speculate_prefix_readed = buffer_.readBig(speculate_prefix_data.data(), speculate_prefix_size);
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
            if (buffer_.supportsReadAt())
            {
                bytes_readed = buffer_.readBigAt(reinterpret_cast<char*>(buf.get()) + copied,
                    bytes_remain, posting_offset_ + speculate_prefix_readed);
                read_big_readed += bytes_readed;
            }
            else
            {
                bytes_readed = buffer_.readBig(reinterpret_cast<char*>(buf.get()) + copied,
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
            if (buffer_.supportsReadAt())
            {
                readed = buffer_.readBigAt(speculate_posting_data.data(),
                    speculate_posting_size, posting_offset_ + speculate_prefix_readed);
                read_big_readed += readed;
            }
            else
            {
                readed = buffer_.readBig(speculate_posting_data.data(), speculate_posting_size);
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

GINStoreWriterV0V1V2Base::GINStoreWriterV0V1V2Base(const String& name_,
    GinDataPartHelperPtr&& storage_info_, GINStoreVersion version_,
    UInt64 max_digestion_size_, Float32 density_):
        GINStoreWriter(name_, std::move(storage_info_), version_,
            max_digestion_size_, density_)
{
}

UInt32 GINStoreWriterV0V1V2Base::segmentID() const
{
    return current_segment.segment_id;
}

UInt32 GINStoreWriterV0V1V2Base::rowID() const
{
    return current_segment.next_row_id;
}

void GINStoreWriterV0V1V2Base::incrementRows(UInt64 rows_)
{
    current_segment.next_row_id += rows_;
    current_segment.total_row_size += rows_;
}

void GINStoreWriterV0V1V2Base::incrementProcessedBytes(UInt64 sz_)
{
    current_size += sz_;
}

void GINStoreWriterV0V1V2Base::appendPostingEntry(const StringRef& term_,
    UInt32 row_id_)
{
    auto iter = current_postings.builders.find(term_);
    if (iter)
    {
        const GinIndexPostingsBuilderPtr& builder = iter.getMapped();
        if (builder->last_row_id != row_id_)
        {
            builder->add(row_id_);
            builder->last_row_id = row_id_;
        }
    }
    else
    {
        auto builder = std::make_shared<GinIndexPostingsBuilder>();
        builder->add(row_id_);

        bool inserted = false;
        StringHashMap<GinIndexPostingsBuilderPtr>::LookupResult result;
        current_postings.builders.emplace(ArenaKeyHolder{term_, *current_postings.term_pool},
            result, inserted);
        assert(inserted && "Trying to add posting builder which already exist");

        result.getMapped() = builder;
    }
}

bool GINStoreWriterV0V1V2Base::needToWrite() const
{
    assert(max_digestion_size > 0);
    return current_size > max_digestion_size;
}

void GINStoreWriterV0V1V2Base::finalize()
{
    if (!current_postings.builders.empty())
        writeSegment();

    if (metadata_file_stream != nullptr)
        metadata_file_stream->finalize();

    if (postings_file_stream != nullptr)
        postings_file_stream->finalize();

    finalizeDictStream();

    /// Write version & segment id file
    if (current_segment.segment_id > 0)
    {
        String version_file_name = name + GIN_SEGMENT_ID_FILE_EXTENSION;
        auto version_file = storage_info->writeFile(version_file_name,
            DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        
        /// Write version
        writeChar(static_cast<uint8_t>(version), *version_file);

        /// Write segment id
        /// NOTE: it write segment + 1... no idea why
        writeVarUInt(current_segment.segment_id + 1, *version_file);

        version_file->sync();
        version_file->finalize();
    }
}

void GINStoreWriterV0V1V2Base::writeSegment()
{
    if (metadata_file_stream == nullptr)
        initFileStreams();

    size_t rows_limit = std::lround(density * current_segment.total_row_size);

    LOG_TRACE(getLogger("GINStoreWriterV0V1V2Base"), "Write segment {}, total size {}, "
        "next row id {}, density {}, limit {}", current_segment.segment_id,
        current_segment.total_row_size, current_segment.next_row_id,
        density, rows_limit);

    /// Write segment
    metadata_file_stream->write(reinterpret_cast<char *>(&current_segment),
        sizeof(GinIndexSegmentV0V1V2));
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
    std::vector<UInt64> posting_list_byte_sizes(token_postings_list_pairs.size(), 0);
    for (size_t i = 0, sz = posting_list_byte_sizes.size(); i < sz; ++i)
    {
        auto posting_list_byte_size = token_postings_list_pairs[i].second->serialize(
            *postings_file_stream, rows_limit);
        posting_list_byte_sizes[i] = posting_list_byte_size;
        current_segment.postings_start_offset += posting_list_byte_size;
    }

    /// Write dict
    current_segment.dict_start_offset += writeDict(token_postings_list_pairs,
        posting_list_byte_sizes, current_segment.segment_id);

    current_size = 0;
    current_postings.reset();
    ++current_segment.segment_id;
    
    metadata_file_stream->next();
    metadata_file_stream->sync();
 
    postings_file_stream->next();
    postings_file_stream->sync();

    flushDictStream();
}

void GINStoreWriterV0V1V2Base::addToChecksums(MergeTreeDataPartChecksums& checksums_)
{
    CityHash_v1_0_2::uint128 file_hash(0, 0);

    std::vector<String> file_names = {
        name + GIN_SEGMENT_ID_FILE_EXTENSION,
        name + GIN_SEGMENT_METADATA_FILE_EXTENSION,
        name + GIN_POSTINGS_FILE_EXTENSION
    };

    for (const String& file_name : file_names)
    {
        if (storage_info->exists(file_name))
        {
            checksums_.addFile(file_name, storage_info->getFileSize(file_name), file_hash);
        }
    }

    addDictToChecksums(checksums_);
}

void GINStoreWriterV0V1V2Base::initFileStreams()
{
    String metadata_file_name = name + GIN_SEGMENT_METADATA_FILE_EXTENSION;
    String postings_file_name = name + GIN_POSTINGS_FILE_EXTENSION;

    metadata_file_stream = storage_info->writeFile(metadata_file_name,
        DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    postings_file_stream =  storage_info->writeFile(postings_file_name,
        DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);

    initDictStream();
}

GINStoreWriterV0::GINStoreWriterV0(const String& name_,
    GinDataPartHelperPtr&& storage_info_, UInt64 max_digestion_size_,
    Float32 density_):
        GINStoreWriterV0V1V2Base(name_, std::move(storage_info_),
            GINStoreVersion::v0, max_digestion_size_, density_)
{
}

void GINStoreWriterV0::initDictStream()
{
    if (dict_file_stream == nullptr)
    {
        String dict_file_name = name + GIN_DICTIONARY_FILE_EXTENSION;
        dict_file_stream = storage_info->writeFile(dict_file_name,
            DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    }
}

void GINStoreWriterV0::flushDictStream()
{
    if (dict_file_stream != nullptr)
    {
        dict_file_stream->next();
        dict_file_stream->sync();
    }
}

void GINStoreWriterV0::finalizeDictStream()
{
    if (dict_file_stream != nullptr)
    {
        dict_file_stream->finalize();
    }
}

UInt64 GINStoreWriterV0::writeDict(const TokenPostingsBuilderPairs& postings_builders_,
    const std::vector<UInt64>& posting_sizes_, size_t)
{
    std::vector<UInt8> buffer;
    WriteBufferFromVector<std::vector<UInt8>> write_buf(buffer);
    FST::FstBuilder fst_builder(write_buf);

    UInt64 offset = 0;
    for (size_t i = 0, sz = postings_builders_.size(); i < sz; ++i)
    {
        fst_builder.add(postings_builders_[i].first, offset);
        offset += posting_sizes_[i];
    }

    fst_builder.build();
    write_buf.finalize();

    /// Write FST size
    UInt64 fst_written = 0;
    writeVarUInt(buffer.size(), *dict_file_stream);
    fst_written += getLengthOfVarUInt(buffer.size());

    /// Write FST blob
    dict_file_stream->write(reinterpret_cast<char*>(buffer.data()), buffer.size());
    fst_written += buffer.size();

    return fst_written;
}

void GINStoreWriterV0::addDictToChecksums(MergeTreeDataPartChecksums& checksums_)
{
    if (String file_name = name + GIN_DICTIONARY_FILE_EXTENSION;
        storage_info->exists(file_name))
    {
        checksums_.addFile(file_name, storage_info->getFileSize(file_name),
            CityHash_v1_0_2::uint128(0, 0));
    }
}

GINStoreWriterV2::GINStoreWriterV2(const String& name_,
    GinDataPartHelperPtr&& storage_info_, UInt64 max_digestion_size_, Float32 density_):
        GINStoreWriterV0V1V2Base(name_, std::move(storage_info_),
            GINStoreVersion::v2, max_digestion_size_, density_)
{
}

void GINStoreWriterV2::initDictStream()
{
    if (env == nullptr)
    {
        String dict_file_name = name + GIN_DICTIONARY_FILE_EXTENSION;
        env = std::make_unique<IndexFile::WriteRemappingEnv>(
            storage_info->writeFile(dict_file_name, DBMS_DEFAULT_BUFFER_SIZE,
                WriteMode::Rewrite));
    }
}

void GINStoreWriterV2::flushDictStream()
{
}

void GINStoreWriterV2::finalizeDictStream()
{
    if (env != nullptr)
    {
        if (IndexFile::Status s = env->finalize(); !s.ok())
        {
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                "Failed to finalize WriteRemappingEnv, error {}", s.ToString());
        }
    }
}

UInt64 GINStoreWriterV2::writeDict(const TokenPostingsBuilderPairs& postings_builders_,
    const std::vector<UInt64>& posting_sizes_, size_t segment_id_)
{
    if (unlikely(env == nullptr))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "initDictStream is not invoked "
            "before write dictionary");
    }

    size_t file_base_offset = 0;
    if (IndexFile::Status s = env->underlyingFileOffset(&file_base_offset); !s.ok())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get buffer offset of "
            "WriteRemappingEnv {}", s.ToString());
    }

    {
        IndexFile::Options sst_opts {
            .env = env.get(),
        };
        IndexFile::IndexFileWriter sst_writer(sst_opts);
        sst_writer.Open(v2StoreSegmentDictName(segment_id_));

        UInt64 offset = 0;
        WriteBufferFromOwnString value_buf;
        for (size_t i = 0, sz = posting_sizes_.size(); i < sz; ++i)
        {
            value_buf.restart();
            writeIntBinary(offset, value_buf);

            const std::string_view& key_view = postings_builders_[i].first;
            StringRef data = value_buf.stringRef();
            sst_writer.Add(Slice(key_view.data(), key_view.size()),
                Slice(data.data, data.size));

            offset += posting_sizes_[i];
        }

        sst_writer.Finish(nullptr);
    }

    size_t file_final_offset = 0;
    if (IndexFile::Status s = env->underlyingFileOffset(&file_final_offset); !s.ok())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get buffer offset of "
            "WriteRemappingEnv {}", s.ToString());
    }
    return file_final_offset - file_base_offset;
}

void GINStoreWriterV2::addDictToChecksums(MergeTreeDataPartChecksums& checksums_)
{
    if (String file_name = name + GIN_DICTIONARY_FILE_EXTENSION;
        storage_info->exists(file_name))
    {
        checksums_.addFile(file_name, storage_info->getFileSize(file_name),
            CityHash_v1_0_2::uint128(0, 0));
    }
}

}

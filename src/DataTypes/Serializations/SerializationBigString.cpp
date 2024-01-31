#include <memory>
#include <typeinfo>
#include <DataTypes/Serializations/SerializationBigString.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Compression/CompressedDataIndex.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationNumber.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

namespace DB
{

/// We need 16byte for each compressed block(which default contains 1MB uncompressed data),
/// so, for a 100GB raw data, we need 1.56MB of compressed index, seems not worth
/// to record compressed index data on granule basis
class DeserializeBinaryBulkStateBigString: public ISerialization::DeserializeBinaryBulkState
{
public:
    explicit DeserializeBinaryBulkStateBigString(
        std::unique_ptr<CompressedDataIndex> idx):
            compressed_idx(std::move(idx)) {}

    std::unique_ptr<CompressedDataIndex> compressed_idx;
};

namespace
{

void deserializeOffsets(UInt64 data_offset, ColumnString::Offsets& offsets,
    ReadBuffer& offset_stream, size_t limit)
{
    for (size_t i = 0; i < limit; ++i)
    {
        if (offset_stream.eof())
        {
            break;
        }

        UInt64 size;
        readVarUInt(size, offset_stream);

        data_offset += size + 1;
        offsets.push_back(data_offset);
    }
}

void deserializeChars(ColumnString::Chars& data, const ColumnString::Offsets& offsets,
    ReadBuffer& data_stream, size_t start_row, size_t limit)
{
    if (limit == 0)
    {
        return;
    }

    size_t data_start_offset = start_row == 0 ? 0 : offsets[start_row - 1];
    size_t data_end_offset = offsets[start_row + limit - 1];
    size_t total_size_to_read = data_end_offset - data_start_offset;

    size_t data_init_size = data.size();
    data.resize(data_init_size + total_size_to_read);

#ifdef __SSE2__
    {
        for (size_t readed = 0; readed < total_size_to_read; )
        {
            if (unlikely(data_stream.eof()))
            {
                throw Exception("Attempt to read after eof",
                    ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
            }

            size_t size_to_copy = std::min(data_stream.available(),
                total_size_to_read - readed);
            size_t sse_copy_size = size_to_copy - (size_to_copy % 16);
            size_t non_sse_copy_size = size_to_copy - sse_copy_size;

            const __m128i* sse_src_pos = reinterpret_cast<const __m128i*>(
                data_stream.position());
            const __m128i* sse_src_end = sse_src_pos + sse_copy_size / 16;
            __m128i* sse_dst_pos = reinterpret_cast<__m128i*>(&data[data_init_size + readed]);
            while (sse_src_pos < sse_src_end)
            {
                _mm_storeu_si128(sse_dst_pos, _mm_loadu_si128(sse_src_pos));

                ++sse_src_pos;
                ++sse_dst_pos;
            }
            data_stream.position() += sse_copy_size;
            readed += sse_copy_size;

            data_stream.readStrict(reinterpret_cast<char*>(&data[data_init_size + readed]),
                non_sse_copy_size);
            readed += non_sse_copy_size;
        }
    }
#else
    {
        data_stream.readStrict(reinterpret_cast<char*>(&data[data_start_offset]),
            total_size_to_read);
    }
#endif
}

DeserializeBinaryBulkStateBigString * checkAndGetBigStringDeserializeState(
    ISerialization::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for SerializationBigString.", ErrorCodes::LOGICAL_ERROR);

    auto * big_str_state = typeid_cast<DeserializeBinaryBulkStateBigString *>(state.get());
    if (!big_str_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid DeserializeBinaryBulkState for SerializationBigString. Expected: "
                        + demangle(typeid(DeserializeBinaryBulkStateBigString).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return big_str_state;
}

}

void SerializationBigString::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData &  /*data*/) const
{
    settings.path.push_back(Substream::StringElements);
    callback(settings.path);
    settings.path.back() = Substream::StringOffsets;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationBigString::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit,
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr &) const
{
    const ColumnString& col = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars & data = col.getChars();
    const IColumn::Offsets& offsets = col.getOffsets();

    settings.path.push_back(Substream::StringOffsets);
    WriteBuffer* offset_stream = settings.getter(settings.path);
    settings.path.back() = Substream::StringElements;
    WriteBuffer* data_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (offset_stream == nullptr || data_stream == nullptr)
    {
        throw Exception("StringOffsets or StringElements substream for String didn't exist",
            ErrorCodes::LOGICAL_ERROR);
    }

    size_t end = limit && (offset + limit < col.size()) ? offset + limit : col.size();
    if (offset == end)
    {
        /// Nothing to serialize
        return;
    }

    /// Serialize string's offsets, don't use multiple streams at same time,
    /// since it may write to same buffer at NativeBlockOutputStream
    IColumn::Offset prev_offset = offset == 0 ? 0 : offsets[offset - 1];
    for (size_t i = offset; i < end; ++i)
    {
        /// One for string's last \0
        UInt64 str_size = offsets[i] - prev_offset - 1;
        writeVarUInt(str_size, *offset_stream);
        prev_offset = offsets[i];
    }

    size_t data_start_offset = offset == 0 ? 0 : offsets[offset - 1];
    size_t data_end_offset = offsets[end - 1];
    data_stream->write(reinterpret_cast<const char *>(&data[data_start_offset]), data_end_offset - data_start_offset);
}

void SerializationBigString::deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    if (!settings.compressed_idx_getter)
        return;

    settings.path.push_back(Substream::StringElements);
    std::unique_ptr<CompressedDataIndex> index = settings.compressed_idx_getter(
        settings.path);
    settings.path.pop_back();

    if (index == nullptr)
        return;

    state = std::make_shared<DeserializeBinaryBulkStateBigString>(std::move(index));
}

void SerializationBigString::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column, size_t limit, DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr &, SubstreamsCache *) const
{
    auto mutable_column = column->assumeMutable();
    ColumnString& column_string = assert_cast<ColumnString&>(*mutable_column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    settings.path.push_back(Substream::StringOffsets);
    ReadBuffer* offset_stream = settings.getter(settings.path);
    settings.path.back() = Substream::StringElements;
    ReadBuffer* data_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (offset_stream == nullptr)
    {
        return;
    }
    if (data_stream == nullptr)
    {
        throw Exception("StringOffsets or StringElements substream for String didn't exist",
            ErrorCodes::LOGICAL_ERROR);
    }

    size_t init_row_count = offsets.size();

    offsets.reserve(offsets.size() + limit);
    deserializeOffsets(data.size(), offsets, *offset_stream, limit);

    deserializeChars(data, offsets, *data_stream, init_row_count, std::min(limit, offsets.size() - init_row_count));
}

size_t SerializationBigString::skipBinaryBulkWithMultipleStreams(
    const NameAndTypePair & , size_t limit, DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state, SubstreamsCache * cache) const
{
    auto cached_column = getFromSubstreamsCache(cache, settings.path);
    if (cached_column)
    {
        return cached_column->size();
    }

    settings.path.push_back(Substream::StringElements);
    ReadBuffer* data_stream = settings.getter(settings.path);
    settings.path.back() = Substream::StringOffsets;
    ReadBuffer* offset_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (offset_stream == nullptr)
    {
        return 0;
    }
    if (data_stream == nullptr)
    {
        throw Exception("StringOffsets or StringElements substream for String didn't exist",
            ErrorCodes::LOGICAL_ERROR);
    }

    size_t skipped_rows = 0;
    UInt64 data_skip_size = 0;
    for (; skipped_rows < limit; ++skipped_rows)
    {
        if (offset_stream->eof())
        {
            break;
        }

        UInt64 size = 0;
        readVarUInt(size, *offset_stream);
        data_skip_size += size;
    }

    /// TODO: This assume the corresponding buffer is CompressedReadBuffer/CachedCompressedReadBuffer,
    /// if it's wrapped within something like hashing buffer, this will fail, need a better way to
    /// to do this
    CompressedReadBufferFromFile* comp_file_buf =
        dynamic_cast<CompressedReadBufferFromFile*>(data_stream);
    CachedCompressedReadBuffer* comp_cache_buf =
        dynamic_cast<CachedCompressedReadBuffer*>(data_stream);

    if ((comp_file_buf != nullptr || comp_cache_buf != nullptr)
        && state != nullptr)
    {
        DeserializeBinaryBulkStateBigString* big_str_state = checkAndGetBigStringDeserializeState(state);

        /// Current offset of compressed block and uncompressed offset within the
        /// compress block
        size_t compressed_offset = 0;
        size_t uncompressed_offset = 0;
        size_t current_uncompressed_block_size = 0;

        if (comp_file_buf)
        {
            std::tie(compressed_offset, uncompressed_offset) = comp_file_buf->position();
            current_uncompressed_block_size = comp_file_buf->currentBlockUncompressedSize();
        }
        else
        {
            std::tie(compressed_offset, uncompressed_offset) = comp_cache_buf->position();
            current_uncompressed_block_size = comp_cache_buf->currentBlockUncompressedSize();
        }

        size_t block_remain = current_uncompressed_block_size == 0 ? 0
            : current_uncompressed_block_size - uncompressed_offset;
        if (data_skip_size < block_remain)
        {
            data_stream->ignore(data_skip_size);
        }
        else
        {
            /// Not in current compressed block, worth a seek
            UInt64 uncompressed_offset_to_skip = data_skip_size + uncompressed_offset;
            UInt64 target_compressed_offset = 0;
            UInt64 target_uncompressed_offset = 0;
            big_str_state->compressed_idx->searchCompressBlock(compressed_offset,
                uncompressed_offset_to_skip, &target_compressed_offset,
                &target_uncompressed_offset);

            if (comp_file_buf != nullptr)
            {
                comp_file_buf->seek(target_compressed_offset, target_uncompressed_offset);
            }
            else
            {
                comp_cache_buf->seek(target_compressed_offset, target_uncompressed_offset);
            }
        }
    }
    else
    {
        data_stream->ignore(data_skip_size);
    }

    /// Add a mocked column into substream cache to record the size
    MutableColumnPtr base_col = ColumnString::create();
    base_col->insert(Field(""));
    addToSubstreamsCache(cache, settings.path, ColumnConst::create(std::move(base_col), skipped_rows));

    return skipped_rows;
}

void SerializationBigString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}

void SerializationBigString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    String & s = get<String &>(field);
    s.resize(size);
    istr.readStrict(s.data(), size);
}

void SerializationBigString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & s = assert_cast<const ColumnString &>(column).getDataAt(row_num);
    writeVarUInt(s.size, ostr);
    writeString(s, ostr);
}

void SerializationBigString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    UInt64 size;
    readVarUInt(size, istr);

    size_t old_chars_size = data.size();
    size_t offset = old_chars_size + size + 1;
    offsets.push_back(offset);

    try
    {
        data.resize(offset);
        istr.readStrict(reinterpret_cast<char*>(&data[offset - size - 1]), size);
        data.back() = 0;
    }
    catch (...)
    {
        offsets.pop_back();
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

void SerializationBigString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}

void SerializationBigString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}

template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

void SerializationBigString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readStringInto(data, istr); });
}

void SerializationBigString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readEscapedStringInto(data, istr); });
}

void SerializationBigString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}

void SerializationBigString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}

void SerializationBigString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr, settings);
}

void SerializationBigString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readJSONStringInto(data, istr); });
}

void SerializationBigString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}

void SerializationBigString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString<>(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}

void SerializationBigString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(column, [&](ColumnString::Chars & data) { readCSVStringInto(data, istr, settings.csv); });
}

// serializeMemComparable guarantees the encoded value is in ascending order for comparison,
/// encoding with the following rule:
///  [group1][marker1]...[groupN][markerN]
///  group is 8 bytes slice which is padding with 0.
///  marker is `0xFF - padding 0 count`
/// for example:
///   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
///   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
///   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
///   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
void SerializationBigString::serializeMemComparable(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & value = static_cast<const ColumnString &>(column).getDataAt(row_num);
    size_t len = value.size;
    for (size_t i = 0; i <= len; i += enc_group_size)
    {
        auto remain = len - i;
        char pad_count = 0;
        if (remain >= enc_group_size)
            ostr.write(&value.data[i], enc_group_size);
        else
        {
            pad_count = enc_group_size - remain;
            ostr.write(&value.data[i], remain);
            for (size_t j = pad_count; j > 0; j--)
                writeChar(enc_pad, ostr);
        }

        char mark = enc_marker - pad_count;
        writeChar(mark, ostr);
    }
}

void SerializationBigString::deserializeMemComparable(IColumn & column, ReadBuffer & istr) const
{
    ColumnString & column_string = static_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t old_chars_size = data.size();
    size_t offset = 0, tmp_size = 0;
    String tmp;
    try
    {
        while (true)
        {
            tmp.resize(tmp_size + enc_group_size + 1);
            istr.readStrict(&(tmp.data()[tmp_size]), enc_group_size + 1);
            char marker = tmp.data()[tmp_size + enc_group_size];
            if (marker == enc_marker)
                tmp_size += enc_group_size;
            else
            {
                auto pad_count = enc_marker - marker;
                tmp_size += enc_group_size - pad_count;
                break;
            }
        }

        offset = old_chars_size + tmp_size + 1;
        data.resize(offset);
        memcpy(reinterpret_cast<char *>(&data[old_chars_size]), tmp.data(), tmp_size);
        data.back() = 0;
    }
    catch (...)
    {
        data.resize_assume_reserved(old_chars_size);
        throw;
    }

    offsets.push_back(offset);
}

}

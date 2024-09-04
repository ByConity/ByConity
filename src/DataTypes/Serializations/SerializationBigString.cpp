#include <DataTypes/Serializations/SerializationBigString.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <typeinfo>
#include <vector>
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
        std::shared_ptr<CompressedDataIndex> idx):
            compressed_idx(std::move(idx)) {}

    std::shared_ptr<CompressedDataIndex> compressed_idx;
};

namespace
{

/// Returns
/// 1. Total processed rows
/// 2. Total processed bytes
/// 3. Data ranges to copy from DataStream's current position
std::tuple<size_t, size_t, std::vector<std::pair<size_t, size_t>>> deserializeOffsets(
    ColumnString::Offsets& offsets, size_t res_data_offset, ReadBuffer& offset_stream,
    size_t limit, const UInt8* filter)
{
    /// First element is relative offset of DataStream's current position,
    /// second element is range size
    std::vector<std::pair<size_t, size_t>> copy_ranges;
    size_t data_range_offset = 0;
    size_t data_range_size = 0;
    size_t data_offset = 0;
    size_t processed_rows = 0;
    size_t res_col_offset = res_data_offset;

    for (size_t i = 0; i < limit; ++i)
    {
        if (offset_stream.eof())
        {
            break;
        }

        ++processed_rows;

        UInt64 size;
        readVarUInt(size, offset_stream);
        /// Add one byte for tailing \0
        ++size;

        if (filter == nullptr || *(filter + i) != 0)
        {
            if (data_offset == data_range_offset + data_range_size)
            {
                data_range_size += size;
            }
            else
            {
                if (data_range_size != 0)
                {
                    copy_ranges.push_back({data_range_offset, data_range_size});
                }
                data_range_offset = data_offset;
                data_range_size = size;
            }

            res_col_offset += size;
            offsets.push_back(res_col_offset);
        }

        data_offset += size;
    }
    if (data_range_size != 0)
    {
        copy_ranges.push_back({data_range_offset, data_range_size});
    }
    return {processed_rows, data_offset, copy_ranges};
}

void deserializeChars(ColumnString::Chars& chars, ReadBuffer& data_stream,
    const std::vector<std::pair<size_t, size_t>>& ranges, size_t final_offset,
    CompressedDataIndex* compressed_idx)
{
    size_t total_size = 0;
    std::for_each(ranges.begin(), ranges.end(), [&total_size](const std::pair<size_t, size_t>& range) {
        total_size += range.second;
    });

    size_t result_offset = chars.size();
    chars.resize(chars.size() + total_size);

    IndexedCompressedBufferReader reader(data_stream, compressed_idx);

    size_t source_offset = 0;
    for (const auto& range : ranges)
    {
        if (size_t offset_to_ignore = range.first - source_offset; offset_to_ignore > 0)
        {
            reader.ignore(offset_to_ignore);
        }

        data_stream.readStrict(reinterpret_cast<char*>(chars.data()) + result_offset,
            range.second);

        result_offset += range.second;
        source_offset = range.first + range.second;
    }

    if (size_t offset_to_ignore = final_offset - source_offset; offset_to_ignore > 0)
    {
        reader.ignore(offset_to_ignore);
    }
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

void SerializationBigString::enumerateStreams(EnumerateStreamsSettings & settings, 
        const StreamCallback & callback, 
        const SubstreamData & data) const
{
    settings.path.push_back(Substream::StringElements);
    settings.path.back().data = data;
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
    /// since it may write to same buffer at NativeWriter
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
    std::shared_ptr<CompressedDataIndex> index = settings.compressed_idx_getter(
        settings.path);
    settings.path.pop_back();

    if (index == nullptr)
        return;

    state = std::make_shared<DeserializeBinaryBulkStateBigString>(std::move(index));
}

size_t SerializationBigString::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr& column, size_t limit, DeserializeBinaryBulkSettings& settings,
    DeserializeBinaryBulkStatePtr& state, SubstreamsCache* cache) const
{
    /// Although SerializationBigString have multiple streams, but we won't cache
    /// each substream in substream cache seperately
    if (auto cache_entry = getFromSubstreamsCache(cache, settings.path); cache_entry.has_value())
    {
        column = cache_entry->column;
        return cache_entry->rows_before_filter;
    }

    auto mutable_column = column->assumeMutable();
    ColumnString& column_string = assert_cast<ColumnString&>(*mutable_column);

    settings.path.push_back(Substream::StringOffsets);
    ReadBuffer* offset_stream = settings.getter(settings.path);
    settings.path.back() = Substream::StringElements;
    ReadBuffer* data_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (offset_stream == nullptr)
    {
        return 0;
    }
    if (data_stream == nullptr)
    {
        throw Exception("StringOffset substream exists while StringElements substream "
            "didn't", ErrorCodes::LOGICAL_ERROR);
    }

    ColumnString::Chars& chars = column_string.getChars();
    ColumnString::Offsets& offsets = column_string.getOffsets();

    offsets.reserve(offsets.size() + limit);

    auto [processed_rows, processed_bytes, copy_ranges] = deserializeOffsets(offsets,
        chars.size(), *offset_stream, limit, settings.filter);
    deserializeChars(chars, *data_stream, copy_ranges, processed_bytes,
        state == nullptr ? nullptr : checkAndGetBigStringDeserializeState(state)->compressed_idx.get());

    addToSubstreamsCache(cache, settings.path, processed_rows, column);

    return processed_rows;
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

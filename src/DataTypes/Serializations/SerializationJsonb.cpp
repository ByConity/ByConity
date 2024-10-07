#include <string_view>
#include <DataTypes/Serializations/SerializationJsonb.h>
#include <DataTypes/Serializations/JSONBValue.h>
#include <Common/JSONParsers/jsonb/JSONBUtils.h>
#include <Columns/ColumnJsonb.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

namespace DB
{
void SerializationJsonb::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    JsonbField jsonb_field = get<JsonbField>(field);
    JsonBinaryValue binary_val;
    binary_val.fromJsonString(jsonb_field.getValue(), jsonb_field.getSize());
    writeVarUInt(binary_val.size(), ostr);
    ostr.write(binary_val.value(), binary_val.size());
}

void SerializationJsonb::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    String jsonb_str;
    readStringBinary(jsonb_str, istr);
    std::string json_str = JsonbToJson::jsonb_to_json_string(jsonb_str.c_str(), jsonb_str.size());
    field = JsonbField(json_str.c_str(), json_str.size());
}

void SerializationJsonb::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const auto & column_jsonb = assert_cast<const ColumnJsonb &>(column);
    std::string_view string_ref = column_jsonb.getDataAt(row_num).toView();
    JsonBinaryValue binary_val;
    binary_val.fromJsonString(string_ref.data(), string_ref.size());
    writeVarUInt(binary_val.size(), ostr);
    ostr.write(binary_val.value(), binary_val.size());
}

void SerializationJsonb::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    String jsonb_str;
    readStringBinary(jsonb_str, istr);
    std::string json_str = JsonbToJson::jsonb_to_json_string(jsonb_str.c_str(), jsonb_str.size());
    auto & column_jsonb = assert_cast<ColumnJsonb &>(column);
    column_jsonb.insert(json_str);
}

void SerializationJsonb::serializeBinaryBulk(const IColumn &column, WriteBuffer &ostr, size_t offset, size_t limit) const
{
    const ColumnJsonb & column_jsonb = typeid_cast<const ColumnJsonb &>(column);
    const ColumnJsonb::Chars & data = column_jsonb.getChars();
    const ColumnJsonb::Offsets & offsets = column_jsonb.getOffsets();

    size_t size = column.size();
    if (!size)
        return;

    size_t end = limit && offset + limit < size
        ? offset + limit
        : size;

    if (offset == 0)
    {
        UInt64 str_size = offsets[0] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(data.data()), str_size);

        ++offset;
    }

    for (size_t i = offset; i < end; ++i)
    {
        UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
    }
}

template <int UNROLL_TIMES>
static NO_INLINE size_t deserializeBinarySSE2(ColumnJsonb::Chars & data, ColumnJsonb::Offsets & offsets, ReadBuffer & istr, size_t limit, const UInt8* filter)
{
    size_t offset = data.size();
    size_t processed_rows = 0;
    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;
        ++processed_rows;
        UInt64 size;
        readVarUInt(size, istr);
        bool keep_element = filter == nullptr || *(filter + i) != 0;
        if (keep_element)
        {
            offset += size + 1;
            offsets.push_back(offset);
            data.resize(offset);
            if (size)
            {
#ifdef __SSE2__
                /// An optimistic branch in which more efficient copying is possible.
                if (offset + 16 * UNROLL_TIMES <= data.capacity() && istr.position() + size + 16 * UNROLL_TIMES <= istr.buffer().end())
                {
                    const __m128i * sse_src_pos = reinterpret_cast<const __m128i *>(istr.position());
                    const __m128i * sse_src_end = sse_src_pos + (size + (16 * UNROLL_TIMES - 1)) / 16 / UNROLL_TIMES * UNROLL_TIMES;
                    __m128i * sse_dst_pos = reinterpret_cast<__m128i *>(&data[offset - size - 1]);
                    while (sse_src_pos < sse_src_end)
                    {
                        for (size_t j = 0; j < UNROLL_TIMES; ++j)
                            _mm_storeu_si128(sse_dst_pos + j, _mm_loadu_si128(sse_src_pos + j));
                        sse_src_pos += UNROLL_TIMES;
                        sse_dst_pos += UNROLL_TIMES;
                    }
                    istr.position() += size;
                }
                else
#endif
                {
                    istr.readStrict(reinterpret_cast<char*>(&data[offset - size - 1]), size);
                }
            }
            data[offset - 1] = 0;
        }
        else
        {
            istr.ignore(size);
        }
    }
    return processed_rows;
}

size_t SerializationJsonb::deserializeBinaryBulk(IColumn &column, ReadBuffer &istr, size_t limit, double avg_value_size_hint, bool /*zero_copy_cache_read*/, const UInt8* filter) const
{
    ColumnJsonb & column_string = typeid_cast<ColumnJsonb &>(column);
    ColumnJsonb::Chars & data = column_string.getChars();
    ColumnJsonb::Offsets & offsets = column_string.getOffsets();

    double avg_chars_size = 1; /// By default reserve only for empty strings.

    if (avg_value_size_hint && avg_value_size_hint > sizeof(offsets[0]))
    {
        /// Randomly selected.
        constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

        avg_chars_size = (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier;
    }

    size_t size_to_reserve = data.size() + std::ceil(limit * avg_chars_size);

    /// Never reserve for too big size.
    if (size_to_reserve < 256 * 1024 * 1024)
    {
        try
        {
            data.reserve(size_to_reserve);
        }
        catch (Exception & e)
        {
            e.addMessage(
                "(avg_value_size_hint = " + toString(avg_value_size_hint)
                + ", avg_chars_size = " + toString(avg_chars_size)
                + ", limit = " + toString(limit) + ")");
            throw;
        }
    }

    offsets.reserve(offsets.size() + limit);

    if (avg_chars_size >= 64)
        return deserializeBinarySSE2<4>(data, offsets, istr, limit, filter);
    else if (avg_chars_size >= 48)
        return deserializeBinarySSE2<3>(data, offsets, istr, limit, filter);
    else if (avg_chars_size >= 32)
        return deserializeBinarySSE2<2>(data, offsets, istr, limit, filter);
    else
        return deserializeBinarySSE2<1>(data, offsets, istr, limit, filter);
}

template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    ColumnJsonb & column_jsonb = assert_cast<ColumnJsonb &>(column);
    ColumnJsonb::Chars & data = column_jsonb.getChars();
    ColumnJsonb::Offsets & offsets = column_jsonb.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        std::string text_json;
        reader(text_json);
        JsonBinaryValue binary_json;
        if (!(binary_json.fromJsonString(text_json.c_str(), text_json.size())))
        {
            //TODO:@lianwenlong don't use {} as default value for some scalar value
            binary_json.fromJsonString("{}", 2);
        }

        //TODO:@lianwenlong use memcpy ?
        for (size_t i = 0; i < binary_json.size(); ++i)
        {
            data.push_back(binary_json.value()[i]);
        }

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

void SerializationJsonb::serializeText(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &) const
{
    const auto & binary_json = assert_cast<const ColumnJsonb &>(column).getDataAt(row_num);
    auto text_json = JsonbToJson::jsonb_to_json_string(binary_json.data, binary_json.size);
    writeString(text_json, ostr);
}

void SerializationJsonb::deserializeWholeText(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    read(column, [&](std::string & text_json) { readStringInto(text_json, istr); });
}

void SerializationJsonb::serializeTextJSON(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings & settings) const
{
    const auto & binary_json = assert_cast<const ColumnJsonb &>(column).getDataAt(row_num);
    auto text_json = JsonbToJson::jsonb_to_json_string(binary_json.data, binary_json.size);
    writeJSONString(text_json, ostr, settings);
}

void SerializationJsonb::deserializeTextJSON(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    read(column, [&](std::string & text_json) { readJSONStringInto(text_json, istr); });
}

void SerializationJsonb::serializeTextCSV(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &) const
{
    const auto & binary_json = assert_cast<const ColumnJsonb &>(column).getDataAt(row_num);
    auto text_json = JsonbToJson::jsonb_to_json_string(binary_json.data, binary_json.size);
    writeCSVString<>(text_json, ostr);
}

void SerializationJsonb::deserializeTextCSV(IColumn &column, ReadBuffer &istr, const FormatSettings & settings) const
{
    read(column, [&](std::string & text_json) { readCSVStringInto(text_json, istr, settings.csv); });
}

void SerializationJsonb::serializeTextEscaped(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &) const
{
    const auto & binary_json = assert_cast<const ColumnJsonb &>(column).getDataAt(row_num);
    auto text_json = JsonbToJson::jsonb_to_json_string(binary_json.data, binary_json.size);
    writeEscapedString(text_json, ostr);
}

void SerializationJsonb::deserializeTextEscaped(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    read(column, [&](std::string & text_json) { readEscapedString(text_json, istr); });
}

void SerializationJsonb::serializeTextQuoted(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &) const
{
    const auto & binary_json = assert_cast<const ColumnJsonb &>(column).getDataAt(row_num);
    auto text_json = JsonbToJson::jsonb_to_json_string(binary_json.data, binary_json.size);
    writeQuotedString(text_json, ostr);
}

void SerializationJsonb::deserializeTextQuoted(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    read(column, [&](std::string & text_json) { readQuotedStringInto<true>(text_json, istr); });
}

} //namespace DB

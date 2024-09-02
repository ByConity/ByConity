#include <DataTypes/Serializations/SerializationSketch.h>

#include <Columns/ColumnSketchBinary.h>
#include <Columns/ColumnConst.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/Field.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

namespace DB
{

void SerializationSketch::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void SerializationSketch::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    String & s = get<String &>(field);
    s.resize(size);
    istr.readStrict(s.data(), size);
}


void SerializationSketch::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & s = assert_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    writeVarUInt(s.size, ostr);
    writeString(s, ostr);
}


void SerializationSketch::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnSketchBinary & column_string = assert_cast<ColumnSketchBinary &>(column);
    ColumnSketchBinary::Chars & data = column_string.getChars();
    ColumnSketchBinary::Offsets & offsets = column_string.getOffsets();

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


void SerializationSketch::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnSketchBinary & column_string = typeid_cast<const ColumnSketchBinary &>(column);
    const ColumnSketchBinary::Chars & data = column_string.getChars();
    const ColumnSketchBinary::Offsets & offsets = column_string.getOffsets();

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
static NO_INLINE void deserializeBinarySSE2(ColumnSketchBinary::Chars & data, ColumnSketchBinary::Offsets & offsets, ReadBuffer & istr, size_t limit)
{
    size_t offset = data.size();
    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size;
        readVarUInt(size, istr);

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
}


void SerializationSketch::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint, bool /*zero_copy_cache_read*/) const
{
    ColumnSketchBinary & column_string = typeid_cast<ColumnSketchBinary &>(column);
    ColumnSketchBinary::Chars & data = column_string.getChars();
    ColumnSketchBinary::Offsets & offsets = column_string.getOffsets();

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
        deserializeBinarySSE2<4>(data, offsets, istr, limit);
    else if (avg_chars_size >= 48)
        deserializeBinarySSE2<3>(data, offsets, istr, limit);
    else if (avg_chars_size >= 32)
        deserializeBinarySSE2<2>(data, offsets, istr, limit);
    else
        deserializeBinarySSE2<1>(data, offsets, istr, limit);
}

String decodeBase64(const StringRef& base64)
{
    Poco::MemoryInputStream istr(base64.data, base64.size);
    Poco::Base64Decoder decoder(istr);
    std::string result;
    Poco::StreamCopier::copyToString(decoder, result);
    return result;
}

String encodeBase64(const StringRef& data)
{
    std::ostringstream ostr;
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << data;
    encoder.close();
    return ostr.str();
}

void SerializationSketch::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto & binary = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    String encoded = encodeBase64(binary);

    writeString(encoded, ostr);
}


void SerializationSketch::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto & binary = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    String encoded = encodeBase64(binary);
    writeEscapedString(encoded, ostr);
}


template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    ColumnSketchBinary & column_string = assert_cast<ColumnSketchBinary &>(column);
    ColumnSketchBinary::Chars & data = column_string.getChars();
    ColumnSketchBinary::Offsets & offsets = column_string.getOffsets();
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


void SerializationSketch::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnSketchBinary::Chars & data) {
        String encoded_string;
        readString(encoded_string, istr);
        String decodeString = decodeBase64(encoded_string);
        data.insert(decodeString.begin(), decodeString.end());
    });
}


void SerializationSketch::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnSketchBinary::Chars & data) {
        String encoded_string;
        readEscapedString(encoded_string, istr);
        String decodeString = decodeBase64(encoded_string);
        data.insert(decodeString.begin(), decodeString.end());
    });
}


void SerializationSketch::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto & binary = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    String encoded = encodeBase64(binary);
    writeQuotedString(encoded, ostr);
}


void SerializationSketch::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnSketchBinary::Chars & data) {
        String encoded_string;
        readQuotedStringInto<true>(encoded_string, istr);
        String decodeString = decodeBase64(encoded_string);
        data.insert(decodeString.begin(), decodeString.end());
    });
}


void SerializationSketch::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & binary = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    String encoded = encodeBase64(binary);
    writeJSONString(encoded, ostr, settings);
}


void SerializationSketch::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnSketchBinary::Chars & data) {
        String encoded_string;
        readJSONStringInto(encoded_string, istr);
        String decodeString = decodeBase64(encoded_string);
        data.insert(decodeString.begin(), decodeString.end());
    });
}


void SerializationSketch::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto & binary = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    String encoded = encodeBase64(binary);
    writeXMLStringForTextElement(encoded, ostr);
}


void SerializationSketch::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto & binary = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
    String encoded = encodeBase64(binary);
    writeCSVString<>(encoded, ostr);
}


void SerializationSketch::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(column, [&](ColumnSketchBinary::Chars & data) {
        String encoded_string;
        readCSVStringInto(encoded_string, istr, settings.csv);
        String decodeString = decodeBase64(encoded_string);
        data.insert(decodeString.begin(), decodeString.end());
    });
}

/// serializeMemComparable guarantees the encoded value is in ascending order for comparison,
/// encoding with the following rule:
///  [group1][marker1]...[groupN][markerN]
///  group is 8 bytes slice which is padding with 0.
///  marker is `0xFF - padding 0 count`
/// for example:
///   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
///   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
///   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
///   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
void SerializationSketch::serializeMemComparable(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & value = static_cast<const ColumnSketchBinary &>(column).getDataAt(row_num);
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

void SerializationSketch::deserializeMemComparable(IColumn & column, ReadBuffer & istr) const
{
    ColumnSketchBinary & column_string = static_cast<ColumnSketchBinary &>(column);
    ColumnSketchBinary::Chars & data = column_string.getChars();
    ColumnSketchBinary::Offsets & offsets = column_string.getOffsets();

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

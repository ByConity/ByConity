#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Poco/UTF8Encoding.h>

#include <string_view>

#ifdef __SSE2__
#    include <emmintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

extern const UInt8 length_of_utf8_sequence[256];

namespace
{

    struct FromUTF8Impl
    {
        static void convertHexToCharArray(const char * begin, const char * end, char * output)
        {
            begin += 2; // Skip "0x" prefix
            size_t index = 0;

            for (; begin < end; begin += 2, ++index)
            {
                unsigned int value = 0;
                sscanf(begin, "%2x", &value);
                output[index] = static_cast<char>(value);
            }
            output[index] = '\0'; // Add null terminator
        }


        static void toValidUTF8One(const char * begin, const char * end, WriteBuffer & write_buffer)
        {
            static constexpr std::string_view replacement = "\xEF\xBF\xBD";

            const char * p = begin;
            const char * valid_start = begin;

            /// The last recorded character was `replacement`.
            bool just_put_replacement = false;

            auto put_valid = [&write_buffer, &just_put_replacement](const char * data, size_t len) {
                if (len == 0)
                    return;
                just_put_replacement = false;
                write_buffer.write(data, len);
            };

            auto put_replacement = [&write_buffer, &just_put_replacement]() {
                if (just_put_replacement)
                    return;
                just_put_replacement = true;
                write_buffer.write(replacement.data(), replacement.size());
            };

            while (p < end)
            {
#ifdef __SSE2__
                /// Fast skip of ASCII
                static constexpr size_t SIMD_BYTES = 16;
                const char * simd_end = p + (end - p) / SIMD_BYTES * SIMD_BYTES;

                while (p < simd_end && !_mm_movemask_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(p))))
                    p += SIMD_BYTES;

                if (!(p < end))
                    break;
#endif

                size_t len = length_of_utf8_sequence[static_cast<unsigned char>(*p)];

                if (len > 4)
                {
                    /// Invalid start of sequence. Skip one byte.
                    put_valid(valid_start, p - valid_start);
                    put_replacement();
                    ++p;
                    valid_start = p;
                }
                else if (p + len > end)
                {
                    /// Sequence was not fully written to this buffer.
                    break;
                }
                else if (Poco::UTF8Encoding::isLegal(reinterpret_cast<const unsigned char *>(p), len))
                {
                    /// Valid sequence.
                    p += len;
                }
                else
                {
                    /// Invalid sequence. Skip just first byte.
                    put_valid(valid_start, p - valid_start);
                    put_replacement();
                    ++p;
                    valid_start = p;
                }
            }

            put_valid(valid_start, p - valid_start);

            if (p != end)
                put_replacement();
        }

        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets)
        {
            const size_t offsets_size = offsets.size();
            /// It can be larger than that, but we believe it is unlikely to happen.
            res_data.resize(data.size());
            res_offsets.resize(offsets_size);

            size_t prev_offset = 0;
            WriteBufferFromVector<ColumnString::Chars> write_buffer(res_data);
            for (size_t i = 0; i < offsets_size; ++i)
            {
                const char * haystack_data = reinterpret_cast<const char *>(&data[prev_offset]);
                const size_t haystack_size = offsets[i] - prev_offset - 1;
                if (haystack_size >= 2 && haystack_data[0] == '0' && (haystack_data[1] == 'x' || haystack_data[1] == 'X'))
                {
                    size_t converted_size = (haystack_size - 2) / 2;
                    char converted[converted_size + 1];
                    convertHexToCharArray(haystack_data, haystack_data + haystack_size, converted);
                    toValidUTF8One(converted, converted + converted_size, write_buffer);
                }
                else
                {
                    toValidUTF8One(haystack_data, haystack_data + haystack_size, write_buffer);
                }
                writeChar(0, write_buffer);
                res_offsets[i] = write_buffer.count();
                prev_offset = offsets[i];
            }
            write_buffer.finalize();
        }

        [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
        {
            throw Exception("Column of type FixedString is not supported by from_UTF8 function", ErrorCodes::ILLEGAL_COLUMN);
        }
    };

    struct NameFromUTF8
    {
        static constexpr auto name = "FROM_UTF8";
    };
    using FunctionFromUTF8 = FunctionStringToString<FromUTF8Impl, NameFromUTF8>;

}

void registerFunctionFromUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFromUTF8>(FunctionFactory::CaseInsensitive);
}


}

#pragma once

#include <Columns/ColumnString.h>
#include <Core/Types.h>

#include <arrow/util/bit_stream_utils.h>
#include <arrow/util/decimal.h>
#include <parquet/types.h>
#include "Common/Endian.h"
#include "common/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int PARQUET_EXCEPTION;
}

template <typename T> struct ToArrowDecimal;
template <> struct ToArrowDecimal<Decimal<wide::integer<128, signed>>>
{
    using ArrowDecimal = arrow::Decimal128;
};
template <> struct ToArrowDecimal<Decimal<wide::integer<256, signed>>>
{
    using ArrowDecimal = arrow::Decimal256;
};


/// TODO: @cao.liu refactor this
class ParquetDataBuffer
{
private:

public:
    ParquetDataBuffer(const uint8_t * data_, UInt64 available_, UInt8 datetime64_scale_ = DataTypeDateTime64::default_scale)
        : data(reinterpret_cast<const Int8 *>(data_))
        , available(available_)
        , datetime64_scale(datetime64_scale_)
        , bit_reader(std::make_unique<::arrow::bit_util::BitReader>(data_, available_))
    {
    }

    template <typename TValue>
    void ALWAYS_INLINE readValue(TValue & dst)
    {
        readBytes(&dst, sizeof(TValue));
    }

    void ALWAYS_INLINE readBytes(void * dst, size_t bytes)
    {
        checkAvaible(bytes);
        std::copy(data, data + bytes, reinterpret_cast<Int8 *>(dst));
        consume(bytes);
    }

    void ALWAYS_INLINE skipBytes(size_t bytes)
    {
        checkAvaible(bytes);
        consume(bytes);
    }

    void ALWAYS_INLINE readBit(UInt8 & dst)
    {
        bool val;
        if (!bit_reader->GetValue(1, &val))
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unexpected eof when reading bit");

        dst = val;
    }

    void ALWAYS_INLINE readBits(UInt8 * dst, int cnt)
    {
        for (int i = 0; i < cnt; i++) {
            readBit(*(dst++));
        }
    }

    void ALWAYS_INLINE readDateTime64(DateTime64 & dst)
    {
        static const int max_scale_num = 9;
        static const UInt64 pow10[max_scale_num + 1]
            = {1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};
        static const UInt64 spd = 60 * 60 * 24;
        static const UInt64 scaled_day[max_scale_num + 1]
            = {spd,
               10 * spd,
               100 * spd,
               1000 * spd,
               10000 * spd,
               100000 * spd,
               1000000 * spd,
               10000000 * spd,
               100000000 * spd,
               1000000000 * spd};

        parquet::Int96 tmp;
        readValue(tmp);
        auto decoded = parquet::DecodeInt96Timestamp(tmp);

        uint64_t scaled_nano = decoded.nanoseconds / pow10[datetime64_scale];
        dst = static_cast<Int64>(decoded.days_since_epoch * scaled_day[datetime64_scale] + scaled_nano);
    }

    /**
     * This method should only be used to read string whose elements size is small.
     * Because memcpySmallAllowReadWriteOverflow15 instead of memcpy is used according to ColumnString::indexImpl
     */
    void ALWAYS_INLINE readString(ColumnString & column, size_t cursor)
    {
        // refer to: PlainByteArrayDecoder::DecodeArrowDense in encoding.cc
        //           deserializeBinarySSE2 in SerializationString.cpp
        checkAvaible(4);
        auto value_len = ::arrow::util::SafeLoadAs<Int32>(getArrowData());
        if (unlikely(value_len < 0 || value_len > INT32_MAX - 4))
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Invalid or corrupted value_len '{}'", value_len);
        }
        consume(4);
        checkAvaible(value_len);

        ColumnString::Chars & chars = column.getChars();
        ColumnString::Offsets & offsets = column.getOffsets();

        const size_t old_char_size = chars.size();
        const size_t new_char_size = old_char_size + value_len + 1;
        chars.resize(new_char_size);
        // TODO: memcpySmallAllowReadWriteOverflow15 coredump
        // memcpySmallAllowReadWriteOverflow15(chars.data() + old_char_size, data, value_len);
        memcpy(&chars[old_char_size], data, value_len);
        chars[old_char_size + value_len] = 0;
        offsets[cursor] = new_char_size;

        consume(value_len);
    }

    /**
     * peek the length of the next `n` strings
     *
     */
    size_t peekStringSize(size_t n)
    {
        const Int8 * old_data = data;
        size_t old_avail = available;
        size_t res = 0;

        while (n--)
        {
            checkAvaible(4);
            Int32 sz = ::arrow::util::SafeLoadAs<Int32>(getArrowData());
            consume(4);
            checkAvaible(sz);
            consume(sz);
            res += sz;
        }

        /// revert cursor
        data = old_data;
        available = old_avail;
        return res;
    }

    void ALWAYS_INLINE skipString()
    {
        checkAvaible(4);
        auto value_len = ::arrow::util::SafeLoadAs<Int32>(getArrowData());
        consume(4);
        checkAvaible(value_len);
        consume(value_len);
    }

    template <is_decimal TDecimal>
    void ALWAYS_INLINE readBigEndianDecimal(TDecimal * out, Int32 elem_bytes_num)
    {
        checkAvaible(elem_bytes_num);

        if constexpr (is_over_big_decimal<TDecimal>)
        {
            using TArrowDecimal = typename ToArrowDecimal<TDecimal>::ArrowDecimal;
            // refer to: RawBytesToDecimalBytes in reader_internal.cc, Decimal128::FromBigEndian in decimal.cc
            auto status = TArrowDecimal::FromBigEndian(getArrowData(), elem_bytes_num);
            if (unlikely(!status.ok()))
            {
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Read parquet decimal failed: {}", status.status().ToString());
            }
            status.ValueUnsafe().ToBytes(reinterpret_cast<uint8_t *>(out));
        }
        else
        {
            chassert(elem_bytes_num <= 8);
            UInt64 val = 0;
            memcpy(reinterpret_cast<uint8_t*>(&val) + 8 - elem_bytes_num, getArrowData(), elem_bytes_num);
            val = Endian::swap(val);
            if ((val >> (elem_bytes_num * 8 - 1)) != 0)
                val |= 0 - (1ul << (elem_bytes_num * 8));

            *out = static_cast<Int64>(val);
        }

        consume(elem_bytes_num);
    }

private:
    const Int8 * data;
    UInt64 available;
    const UInt8 datetime64_scale;
    std::unique_ptr<::arrow::bit_util::BitReader> bit_reader;

    void ALWAYS_INLINE checkAvaible(UInt64 num)
    {
        if (unlikely(available < num))
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Consuming {} bytes while {} available", num, available);
        }
    }

    const uint8_t * ALWAYS_INLINE getArrowData() { return reinterpret_cast<const uint8_t *>(data); }

    void ALWAYS_INLINE consume(UInt64 num)
    {
        data += num;
        available -= num;
    }
};


class LazyNullMap
{
public:
    LazyNullMap(UInt64 size_) : size(size_), col_nullable(nullptr) {}

    template <typename T>
    requires std::is_integral_v<T>
    void setNull(T cursor)
    {
        initialize();
        null_map[cursor] = 1;
    }

    template <typename T>
    requires std::is_integral_v<T>
    void setNull(T cursor, UInt32 count)
    {
        initialize();
        memset(null_map + cursor, 1, count);
    }

    ColumnPtr getNullableCol() { return col_nullable; }

private:
    UInt64 size;
    UInt8 * null_map;
    ColumnPtr col_nullable;

    void initialize()
    {
        if (likely(col_nullable))
        {
            return;
        }
        auto col = ColumnVector<UInt8>::create(size);
        null_map = col->getData().data();
        col_nullable = std::move(col);
        memset(null_map, 0, size);
    }
};

}

#include <Core/Types.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/FieldVisitorCompatibleBinary.h>
#include "Core/Field.h"

#include <gtest/gtest.h>

TEST(TestFieldCompatibleBinary, UInt64Binary)
{
/// UInt64;
{
    DB::WriteBufferFromOwnString write_buffer;

    DB::Field uint64_field_write(100000UL);
    DB::writeFieldBinary(uint64_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field uint64_field_read;
    DB::readFieldBinary(uint64_field_read, read_buffer);

    ASSERT_EQ(uint64_field_write, uint64_field_read);
}
};

TEST(TestFieldCompatibleBinary, Int128Binary)
{
/// Int128;
{
    DB::WriteBufferFromOwnString write_buffer;

    DB::Field int128_field_write(static_cast<Int128>(100000LL));
    DB::writeFieldBinary(int128_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field int128_field_read;
    DB::readFieldBinary(int128_field_read, read_buffer);

    ASSERT_EQ(int128_field_write, int128_field_read);
}
};

TEST(TestFieldCompatibleBinary, Float64Binary)
{
/// Float64;
{
    DB::WriteBufferFromOwnString write_buffer;

    DB::Field float64_field_write(100000.00);
    DB::writeFieldBinary(float64_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field float64_field_read;
    DB::readFieldBinary(float64_field_read, read_buffer);

    ASSERT_EQ(float64_field_write, float64_field_read);
}
};

TEST(TestFieldCompatibleBinary, StringBinary)
{
/// String;
{
    DB::WriteBufferFromOwnString write_buffer;

    DB::Field string_field_write(String(100, 'a'));
    DB::writeFieldBinary(string_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field string_field_read;
    DB::readFieldBinary(string_field_read, read_buffer);

    ASSERT_EQ(string_field_write, string_field_read);
}
};

TEST(TestFieldCompatibleBinary, Decimal64Binary)
{
/// Decimal64;
{
    DB::WriteBufferFromOwnString write_buffer;

    DB::Field decimal64_field_write(DB::DecimalField<DB::Decimal64>(DB::Decimal64(100000UL), 3));
    DB::writeFieldBinary(decimal64_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field decimal64_field_read;
    DB::readFieldBinary(decimal64_field_read, read_buffer);

    ASSERT_EQ(decimal64_field_write, decimal64_field_read);
}
}
;

TEST(TestFieldCompatibleBinary, ArrayBinary)
{
/// Array (UInt64);
{
    DB::Array array;
    for (size_t i = 0; i < 100; ++i)
    {
        array.push_back(i);
    }

    DB::WriteBufferFromOwnString write_buffer;

    DB::Field array_uint_field_write(array);
    DB::writeFieldBinary(array_uint_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field array_uint_field_read;
    DB::readFieldBinary(array_uint_field_read, read_buffer);

    ASSERT_EQ(array_uint_field_write, array_uint_field_read);
}

/// Array (String);
{
    DB::Array array;
    for (size_t i = 0; i < 100; ++i)
    {
        array.push_back(std::to_string(i));
    }

    DB::WriteBufferFromOwnString write_buffer;

    DB::Field array_str_field_write(array);
    DB::writeFieldBinary(array_str_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field array_str_field_read;
    DB::readFieldBinary(array_str_field_read, read_buffer);

    ASSERT_EQ(array_str_field_write, array_str_field_read);
}
}
;

TEST(TestFieldCompatibleBinary, TupleBinary)
{
/// Tuple;
{
    DB::Tuple tuple;

    /// write UInt64
    tuple.push_back(DB::Field(100UL));
    tuple.push_back(DB::Field(String(100, 'a')));
    tuple.push_back(DB::Field(DB::DecimalField<DB::Decimal64>(DB::Decimal64(100000UL), 3)));


    DB::WriteBufferFromOwnString write_buffer;

    DB::Field tuple_field_write(tuple);
    DB::writeFieldBinary(tuple_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field tuple_field_read;
    DB::readFieldBinary(tuple_field_read, read_buffer);

    ASSERT_EQ(tuple_field_write, tuple_field_read);
}
};

TEST(TestFieldCompatibleBinary, MapBinary)
{
/// Map;
{
    DB::Map map; /// key type is String, and value type is Array(UInt64)

    DB::Array v1;
    for (size_t i = 0; i < 10; ++i)
        v1.push_back(i);
    map.push_back({DB::Field("k1"), v1});

    DB::Array v2;
    for (size_t i = 0; i < 100; ++i)
        v2.push_back(i + 100000UL);
    map.push_back({DB::Field("k2"), v2});

    DB::Array v3;
    for (size_t i = 0; i < 1000; ++i)
        v3.push_back(i);
    map.push_back({DB::Field("k3"), v3});

    DB::WriteBufferFromOwnString write_buffer;

    DB::Field map_field_write(map);
    DB::writeFieldBinary(map_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field map_field_read;
    DB::readFieldBinary(map_field_read, read_buffer);

    ASSERT_EQ(map_field_write, map_field_read);
}
};

TEST(TestFieldCompatibleBinary, Bitmap64Binary)
{
    /// Bitmap64;
{
    auto bitmap64 = roaring::Roaring64Map::bitmapOf(1U, 100000U, 100000000U);

    DB::WriteBufferFromOwnString write_buffer;

    DB::Field bitmap64_field_write(bitmap64);
    DB::writeFieldBinary(bitmap64_field_write, write_buffer);

    DB::ReadBufferFromOwnString read_buffer(write_buffer.str());

    DB::Field bitmap64_field_read;
    DB::readFieldBinary(bitmap64_field_read, read_buffer);

    ASSERT_EQ(bitmap64_field_write, bitmap64_field_read);
}
};

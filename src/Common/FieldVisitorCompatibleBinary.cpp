#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorCompatibleBinary.h>
#include <Common/PODArray.h>

namespace DB
{
void FieldVisitorCompatibleWriteBinary::operator()(const Null &, WriteBuffer &) const
{
}
void FieldVisitorCompatibleWriteBinary::operator()(const NegativeInfinity &, WriteBuffer &) const
{
}
void FieldVisitorCompatibleWriteBinary::operator()(const PositiveInfinity &, WriteBuffer &) const
{
}
void FieldVisitorCompatibleWriteBinary::operator()(const UInt64 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const Int64 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const Float64 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const String & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const UInt128 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const Int128 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const UInt256 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const Int256 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}
void FieldVisitorCompatibleWriteBinary::operator()(const UUID & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}

void FieldVisitorCompatibleWriteBinary::operator()(const IPv4 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}

void FieldVisitorCompatibleWriteBinary::operator()(const IPv6 & x, WriteBuffer & buf) const
{
    writeBinary(x, buf);
}


void FieldVisitorCompatibleWriteBinary::operator()(const DecimalField<Decimal32> & x, WriteBuffer & buf) const
{
    writeBinary(x.getValue(), buf);
    writeBinary(x.getScale(), buf);
}

void FieldVisitorCompatibleWriteBinary::operator()(const DecimalField<Decimal64> & x, WriteBuffer & buf) const
{
    writeBinary(x.getValue(), buf);
    writeBinary(x.getScale(), buf);
}

void FieldVisitorCompatibleWriteBinary::operator()(const DecimalField<Decimal128> & x, WriteBuffer & buf) const
{
    writeBinary(x.getValue(), buf);
    writeBinary(x.getScale(), buf);
}

void FieldVisitorCompatibleWriteBinary::operator()(const DecimalField<Decimal256> & x, WriteBuffer & buf) const
{
    writeBinary(x.getValue(), buf);
    writeBinary(x.getScale(), buf);
}


void FieldVisitorCompatibleWriteBinary::operator()(const AggregateFunctionStateData & x, WriteBuffer & buf) const
{
    writeStringBinary(x.name, buf);
    writeStringBinary(x.data, buf);
}

/// similar with writeBinary(const Array & x, WriteBuffer & buf)
/// but use FieldVisitorCompatibleWriteBinary for sub-field
void FieldVisitorCompatibleWriteBinary::operator()(const Array & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf](const auto & value) { FieldVisitorCompatibleWriteBinary()(value, buf); }, x[i]);
    }
}

void FieldVisitorCompatibleWriteBinary::operator()(const Tuple & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf](const auto & value) { FieldVisitorCompatibleWriteBinary()(value, buf); }, x[i]);
    }
}

void FieldVisitorCompatibleWriteBinary::operator()(const Map & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        /// key
        UInt8 type = x[i].first.getType();
        writeBinary(type, buf);
        Field::dispatch([&buf](const auto & value) { FieldVisitorCompatibleWriteBinary()(value, buf); }, x[i].first);

        /// value
        type = x[i].second.getType();
        writeBinary(type, buf);
        Field::dispatch([&buf](const auto & value) { FieldVisitorCompatibleWriteBinary()(value, buf); }, x[i].second);
    }
}

void FieldVisitorCompatibleWriteBinary::operator()(const BitMap64 & x, WriteBuffer & buf) const
{
    const size_t bytes = x.getSizeInBytes();
    writeBinary(bytes, buf);
    PODArray<char> buffer(bytes);
    x.write(buffer.data());
    writeString(buffer.data(), bytes, buf);
}

void FieldVisitorCompatibleWriteBinary::operator()(const Object & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & [key, value] : x)
    {
        const UInt8 type = value.getType();
        writeBinary(type, buf);
        writeBinary(key, buf);
        Field::dispatch([&buf] (const auto & val) { FieldVisitorCompatibleWriteBinary()(val, buf); }, value);
    }
}

void FieldVisitorCompatibleWriteBinary::operator()(const JsonbField & x, WriteBuffer & buf) const
{
    writeStringBinary(std::string(x.getValue(), x.getSize()), buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(UInt64 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(UInt128 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(UInt256 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(Int64 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(Int128 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(Int256 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(UUID & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(Float64 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(String & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(IPv4 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(IPv6 & value, ReadBuffer & buf)
{
    readBinary(value, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(DecimalField<Decimal32> & value, ReadBuffer & buf)
{
    Decimal32 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);
    value = DecimalField<Decimal32>(data, scale);
}

void FieldVisitorCompatibleReadBinary::deserialize(DecimalField<Decimal64> & value, ReadBuffer & buf)
{
    Decimal64 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);

    value = DecimalField<Decimal64>(data, scale);
}

void FieldVisitorCompatibleReadBinary::deserialize(DecimalField<Decimal128> & value, ReadBuffer & buf)
{
    Decimal128 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);
    value = DecimalField<Decimal128>(data, scale);
}

void FieldVisitorCompatibleReadBinary::deserialize(DecimalField<Decimal256> & value, ReadBuffer & buf)
{
    Decimal256 data;
    UInt32 scale;
    readBinary(data, buf);
    readBinary(scale, buf);
    value = DecimalField<Decimal256>(data, scale);
}

void FieldVisitorCompatibleReadBinary::deserialize(AggregateFunctionStateData & value, ReadBuffer & buf)
{
    readStringBinary(value.name, buf);
    readStringBinary(value.data, buf);
}

void FieldVisitorCompatibleReadBinary::deserialize(Array & value, ReadBuffer & buf)
{
    size_t size{0};
    readBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        UInt8 type{0};
        readBinary(type, buf);
        auto f = Field::dispatch(FieldVisitorCompatibleReadBinary(buf), static_cast<Field::Types::Which>(type));
        value.push_back(f);
    }
}

void FieldVisitorCompatibleReadBinary::deserialize(Tuple & value, ReadBuffer & buf)
{
    size_t size{0};
    readBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        UInt8 type{0};
        readBinary(type, buf);
        auto f = Field::dispatch(FieldVisitorCompatibleReadBinary(buf), static_cast<Field::Types::Which>(type));
        value.push_back(f);
    }
}

void FieldVisitorCompatibleReadBinary::deserialize(Map & value, ReadBuffer & buf)
{
    size_t size{0};
    readBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        UInt8 type{0};

        /// key
        readBinary(type, buf);
        auto key_field = Field::dispatch(FieldVisitorCompatibleReadBinary(buf), static_cast<Field::Types::Which>(type));

        /// value
        readBinary(type, buf);
        auto value_field = Field::dispatch(FieldVisitorCompatibleReadBinary(buf), static_cast<Field::Types::Which>(type));

        value.push_back(std::make_pair(key_field, value_field));
    }
}

void FieldVisitorCompatibleReadBinary::deserialize(BitMap64 & value, ReadBuffer & buf)
{
    size_t bytes{0};
    readBinary(bytes, buf);
    PODArray<char> bitmap_buffer(bytes);
    buf.readStrict(bitmap_buffer.data(), bytes);
    value = BitMap64::readSafe(bitmap_buffer.data(), bytes);
}

void FieldVisitorCompatibleReadBinary::deserialize(Object & value, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        String key;
        readBinary(type, buf);
        readBinary(key, buf);
        value[key] = Field::dispatch(FieldVisitorCompatibleReadBinary(buf), static_cast<Field::Types::Which>(type));
    }
}

void FieldVisitorCompatibleReadBinary::deserialize(JsonbField & value, ReadBuffer & buf)
{
    std::string data;
    readStringBinary(data, buf);
    value = JsonbField(data.c_str(), data.size());
}

}

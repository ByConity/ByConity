#include <Common/FieldVisitorWriteBinary.h>

#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
namespace DB
{

void FieldVisitorWriteBinary::operator() (const Null &, WriteBuffer &) const { }
void FieldVisitorWriteBinary::operator() (const NegativeInfinity &, WriteBuffer &) const { }
void FieldVisitorWriteBinary::operator() (const PositiveInfinity &, WriteBuffer &) const { }
void FieldVisitorWriteBinary::operator() (const UInt64 & x, WriteBuffer & buf) const { writeVarUInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int64 & x, WriteBuffer & buf) const { writeVarInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const Float64 & x, WriteBuffer & buf) const { writeFloatBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const String & x, WriteBuffer & buf) const { writeStringBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const UInt128 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int128 & x, WriteBuffer & buf) const { writeVarInt(x, buf); }
void FieldVisitorWriteBinary::operator() (const UInt256 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const Int256 & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const UUID & x, WriteBuffer & buf) const { writeBinary(x, buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal32> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal64> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal128> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); }
void FieldVisitorWriteBinary::operator() (const DecimalField<Decimal256> & x, WriteBuffer & buf) const { writeBinary(x.getValue(), buf); }
void FieldVisitorWriteBinary::operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const
{
    writeStringBinary(x.name, buf);
    writeStringBinary(x.data, buf);
}

void FieldVisitorWriteBinary::operator() (const Array & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}

void FieldVisitorWriteBinary::operator() (const Tuple & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}


void FieldVisitorWriteBinary::operator() (const Map & x, WriteBuffer & buf) const
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (size_t i = 0; i < size; ++i)
    {
        const UInt8 type = x[i].getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, x[i]);
    }
}

// FIXME
void FieldVisitorWriteBinary::operator() (const ByteMap & x, WriteBuffer & buf) const
{
    UInt8 ktype = Field::Types::Null;
    UInt8 vtype = Field::Types::Null;
 
    const size_t size = x.size();
    if (size)
    {
        ktype = x.front().first.getType();
        vtype = x.front().second.getType();
    }
    writeBinary(ktype, buf);
    writeBinary(vtype, buf);
    writeBinary(size, buf);
    for (ByteMap::const_iterator it = x.begin(); it != x.end(); ++it)
    {
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, it->first);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, it->second);
    }
}

void FieldVisitorWriteBinary::operator() (const BitMap64 & x, WriteBuffer & buf) const
{
    const size_t bytes = x.getSizeInBytes();
    writeBinary(bytes, buf);
    PODArray<char> buffer(bytes);
    x.write(buffer.data());
    writeString(buffer.data(), bytes, buf);
}

}

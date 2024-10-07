#pragma once

#include <cstddef>
#include <type_traits>
#include <Core/Types.h>
#include <Common/FieldVisitors.h>
#include <common/types.h>

namespace DB
{

/// Different with class FieldVisitorBinary that the binary data can be
/// deserialized into a Field
class FieldVisitorCompatibleWriteBinary
{
public:
    void operator()(const Null & x, WriteBuffer & buf) const;
    void operator()(const NegativeInfinity & x, WriteBuffer & buf) const;
    void operator()(const PositiveInfinity & x, WriteBuffer & buf) const;
    void operator()(const UInt64 & x, WriteBuffer & buf) const;
    void operator()(const UInt128 & x, WriteBuffer & buf) const;
    void operator()(const UInt256 & x, WriteBuffer & buf) const;
    void operator()(const Int64 & x, WriteBuffer & buf) const;
    void operator()(const Int128 & x, WriteBuffer & buf) const;
    void operator()(const Int256 & x, WriteBuffer & buf) const;
    void operator()(const UUID & x, WriteBuffer & buf) const;
    void operator()(const Float64 & x, WriteBuffer & buf) const;
    void operator()(const String & x, WriteBuffer & buf) const;
    void operator()(const Array & x, WriteBuffer & buf) const;
    void operator()(const Tuple & x, WriteBuffer & buf) const;
    void operator()(const Map & x, WriteBuffer & buf) const;
    void operator()(const DecimalField<Decimal32> & x, WriteBuffer & buf) const;
    void operator()(const DecimalField<Decimal64> & x, WriteBuffer & buf) const;
    void operator()(const DecimalField<Decimal128> & x, WriteBuffer & buf) const;
    void operator()(const DecimalField<Decimal256> & x, WriteBuffer & buf) const;
    void operator()(const AggregateFunctionStateData & x, WriteBuffer & buf) const;
    void operator()(const BitMap64 & x, WriteBuffer & buf) const;
    void operator()(const Object & x, WriteBuffer & buf) const;
    void operator()(const IPv4 & x, WriteBuffer & buf) const;
    void operator()(const IPv6 & x, WriteBuffer & buf) const;
    void operator()(const JsonbField & x, WriteBuffer & buf) const;
};

class FieldVisitorCompatibleReadBinary
{
private:
    ReadBuffer & buf;

public:
    explicit FieldVisitorCompatibleReadBinary(ReadBuffer & buf_) : buf(buf_)
    {
    }

    template <typename RealType>
    Field operator()()
    {
        if constexpr (
            std::is_same_v<RealType, Null> || std::is_same_v<RealType, NegativeInfinity> || std::is_same_v<RealType, PositiveInfinity>)
        {
            return Field();
        }
        else if constexpr (
            std::is_same_v<
                RealType,
                Decimal32> || std::is_same_v<RealType, Decimal64> || std::is_same_v<RealType, Decimal128> || std::is_same_v<RealType, Decimal256>)
        {
            DecimalField<RealType> value;
            deserialize(value, buf);
            return Field(value);
        }
        else
        {
            RealType value;
            deserialize(value, buf);
            return Field(value);
        }
    }

private:
    static void deserialize(UInt64 & value, ReadBuffer & buf);
    static void deserialize(UInt128 & value, ReadBuffer & buf);
    static void deserialize(UInt256 & value, ReadBuffer & buf);
    static void deserialize(Int64 & value, ReadBuffer & buf);
    static void deserialize(Int128 & value, ReadBuffer & buf);
    static void deserialize(Int256 & value, ReadBuffer & buf);
    static void deserialize(UUID & value, ReadBuffer & buf);
    static void deserialize(Float64 & value, ReadBuffer & buf);
    static void deserialize(String & value, ReadBuffer & buf);
    static void deserialize(IPv4 & value, ReadBuffer & buf);
    static void deserialize(IPv6 & value, ReadBuffer & buf);

    static void deserialize(DecimalField<Decimal32> & value, ReadBuffer & buf);
    static void deserialize(DecimalField<Decimal64> & value, ReadBuffer & buf);
    static void deserialize(DecimalField<Decimal128> & value, ReadBuffer & buf);
    static void deserialize(DecimalField<Decimal256> & value, ReadBuffer & buf);

    static void deserialize(AggregateFunctionStateData & value, ReadBuffer & buf);
    static void deserialize(Array & value, ReadBuffer & buf);
    static void deserialize(Tuple & value, ReadBuffer & buf);
    static void deserialize(Map & value, ReadBuffer & buf);
    static void deserialize(BitMap64 & value, ReadBuffer & buf);
    static void deserialize(Object & value, ReadBuffer & buf);
    static void deserialize(JsonbField & value, ReadBuffer & buf);
};

}

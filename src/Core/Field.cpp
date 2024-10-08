/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>
#include <Protos/plan_node_utils.pb.h>
#include <Common/FieldVisitorCompatibleBinary.h>
#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorWriteBinary.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
    extern const int DECIMAL_OVERFLOW;
}

const char * Field::Types::toString(Which which)
{
    switch (which)
    {
        case NegativeInfinity:
            return "-Inf";
        case PositiveInfinity:
            return "+Inf";

        default: {
            // this API returns a reference to String, so data() is safe
            const char * name = WhichConverter::toString(which).data();
            if (!name)
            {
                throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
            }
            return name;
        }
    }
}

inline Field getBinaryValue(UInt8 type, ReadBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null: {
            return Field();
        }
        case Field::Types::UInt64: {
            UInt64 value;
            readVarUInt(value, buf);
            return value;
        }
        case Field::Types::UInt128: {
            UInt128 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::UInt256:
        {
            UInt256 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Int64: {
            Int64 value;
            readVarInt(value, buf);
            return value;
        }
        case Field::Types::Int128:
        {
            Int128 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Int256:
        {
            Int256 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Float64: {
            Float64 value;
            readFloatBinary(value, buf);
            return value;
        }
        case Field::Types::String: {
            std::string value;
            readStringBinary(value, buf);
            return value;
        }
        case Field::Types::Array: {
            Array value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Tuple: {
            Tuple value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Map: {
            Map value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::AggregateFunctionState: {
            AggregateFunctionStateData value;
            readStringBinary(value.name, buf);
            readStringBinary(value.data, buf);
            return value;
        }
        case Field::Types::UUID: {
            UUID value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::IPv4:
        {
            IPv4 value;
            DB::readBinary(value, buf);
            return value;
        }
        case Field::Types::IPv6:
        {
            IPv6 value;
            DB::readBinary(value.toUnderType(), buf);
            return value;
        }
        case Field::Types::Object:
        {
            Object value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::JSONB:
        {
            JsonbField value;
            readBinary(value, buf);
            return value;
        }
    }
    return Field();
}

void readBinary(Array & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        readBinary(type, buf);
        x.push_back(getBinaryValue(type, buf));
    }
}

void writeBinary(const Array & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, elem);
    }
}

void writeText(const Array & x, WriteBuffer & buf)
{
    String res = applyVisitor(FieldVisitorToString(), Field(x));
    buf.write(res.data(), res.size());
}

void readBinary(Tuple & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        readBinary(type, buf);
        x.push_back(getBinaryValue(type, buf));
    }
}

void writeBinary(const Tuple & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, elem);
    }
}

void writeText(const Tuple & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(Map & x, ReadBuffer & buf)
{
    size_t size;
    DB::readBinary(size, buf);

    if (size > 0)
    {
        x.reserve(size);

        for (size_t index = 0; index < size; ++index)
        {
            Field key, value;
            readFieldBinary(key, buf);
            readFieldBinary(value, buf);
            x.push_back(std::make_pair(key, value));
        }
    }
}

void writeBinary(const Map & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    DB::writeBinary(size, buf);

    if (size > 0)
    {
        for (const auto & elem: x)
        {
            writeFieldBinary(elem.first, buf);
            writeFieldBinary(elem.second, buf);
        }
    }
}

void writeText(const Map & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(BitMap64 & x, ReadBuffer & buf)
{
    size_t bytes{0};
    readVarUInt(bytes, buf);
    PODArray<char> tmp_buf(bytes);
    buf.readStrict(tmp_buf.data(), bytes);
    x = roaring::Roaring64Map::readSafe(tmp_buf.data(), bytes);
}

void writeBinary(const BitMap64 & x, WriteBuffer & buf)
{
    const size_t bytes = x.getSizeInBytes();
    writeVarUInt(bytes, buf);
    PODArray<char> tmp_buf(bytes);
    x.write(tmp_buf.data());
    writeString(tmp_buf.data(), bytes, buf);
}

void readBinary(Object & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        String key;
        readBinary(type, buf);
        readBinary(key, buf);
        x[key] = getBinaryValue(type, buf);
    }
}

void writeBinary(const Object & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & [key, value] : x)
    {
        const UInt8 type = value.getType();
        writeBinary(type, buf);
        writeBinary(key, buf);
        Field::dispatch([&buf] (const auto & val) { FieldVisitorWriteBinary()(val, buf); }, value);
    }
}

void writeText(const Object & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(JsonbField & x, ReadBuffer & buf)
{
    std::string value;
    readStringBinary(value, buf);
    x = JsonbField(value.data(), value.size());
}

void writeBinary(const JsonbField & x, WriteBuffer & buf)
{
    writeStringBinary(std::string(x.getValue(), x.getSize()), buf);
}

void writeText(const JsonbField & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

template <typename T>
void readQuoted(DecimalField<T> & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    T value;
    UInt32 scale;
    int32_t exponent;
    uint32_t max_digits = static_cast<uint32_t>(-1);
    readDigits<true>(buf, value, max_digits, exponent, true);
    if (exponent > 0)
    {
        scale = 0;
        if (common::mulOverflow(value.value, DecimalUtils::scaleMultiplier<T>(exponent), value.value))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }
    else
        scale = -exponent;
    assertChar('\'', buf);
    x = DecimalField<T>{value, scale};
}

template void readQuoted<Decimal32>(DecimalField<Decimal32> & x, ReadBuffer & buf);
template void readQuoted<Decimal64>(DecimalField<Decimal64> & x, ReadBuffer & buf);
template void readQuoted<Decimal128>(DecimalField<Decimal128> & x, ReadBuffer & buf);
template void readQuoted<Decimal256>(DecimalField<Decimal256> & x, ReadBuffer & buf);

void writeFieldText(const Field & x, WriteBuffer & buf)
{
    String res = Field::dispatch(FieldVisitorToString(), x);
    buf.write(res.data(), res.size());
}

// used for both protobuf and original serde
void writeFieldBinaryBlobImpl(const Field & field, Field::Types::Which type, WriteBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null:
        {
            return;
        }
        case Field::Types::UInt64:
        {
            writeBinary(field.get<UInt64>(), buf);
            return;
        }
        case Field::Types::Int64:
        {
            writeBinary(field.get<Int64>(), buf);
            return;
        }
        case Field::Types::Float64:
        {
            writeBinary(field.get<Float64>(), buf);
            return;
        }
        case Field::Types::UInt128:
        {
            writeBinary(field.get<UInt128>(), buf);
            return;
        }
        case Field::Types::Int128:
        {
            writeBinary(field.get<Int128>(), buf);
            return;
        }
        case Field::Types::UInt256:
        {
            writeBinary(field.get<UInt256>(), buf);
            return;
        }
        case Field::Types::Int256:
        {
            writeBinary(field.get<Int256>(), buf);
            return;
        }
        case Field::Types::String:
        {
            writeBinary(field.get<String>(), buf);
            return;
        }
        case Field::Types::Array:
        {
            writeBinary(field.get<Array>(), buf);
            return;
        }
        case Field::Types::Tuple:
        {
            writeBinary(field.get<Tuple>(), buf);
            return;
        }
        case Field::Types::IPv4:
        {
            writeBinary(field.get<IPv4>(), buf);
            return;
        }
        case Field::Types::IPv6:
        {
            writeBinary(field.get<IPv6>(), buf);
            return;
        }
        case Field::Types::Decimal32:
        {
            auto df = field.get<DecimalField<Decimal32>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Decimal64:
        {
            auto df = field.get<DecimalField<Decimal64>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Decimal128:
        {
            auto df = field.get<DecimalField<Decimal128>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Decimal256:
        {
            auto df = field.get<DecimalField<Decimal256>>();
            writeBinary(df.getValue(), buf);
            writeBinary(df.getScale(), buf);
            return;
        }
        case Field::Types::Map:
        {
            auto df = field.get<Map>();
            writeBinary(df, buf);
            return;
        }
        case Field::Types::UUID:
        {
            writeBinary(field.get<UUID>(), buf);
            return;
        }
        case Field::Types::AggregateFunctionState:
        {
            writeStringBinary(field.get<AggregateFunctionStateData>().name, buf);
            writeStringBinary(field.get<AggregateFunctionStateData>().data, buf);
            return;
        }
        case Field::Types::JSONB:
        {
            writeBinary(field.get<JsonbField>(), buf);
            return;
        }
        default:
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field {} when serializing.", type);
    }
}

// used for both protobuf and original serde
void readFieldBinaryBlobImpl(Field & field, Field::Types::Which type, ReadBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null:
        {
            field = Field();
            return;
        }
        case Field::Types::UInt64:
        {
            UInt64 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Int64:
        {
            Int64 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Float64:
        {
            Float64 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::UInt128:
        {
            UInt128 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Int128:
        {
            Int128 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::UInt256:
        {
            UInt256 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Int256:
        {
            Int256 value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::String:
        {
            String value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Array:
        {
            Array value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Tuple:
        {
            Tuple value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::Decimal32:
        {
            Decimal32 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal32>(value, scale);
            return;
        }
        case Field::Types::Decimal64:
        {
            Decimal64 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal64>(value, scale);
            return;
        }
        case Field::Types::Decimal128:
        {
            Decimal128 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal128>(value, scale);
            return;
        }
        case Field::Types::Decimal256:
        {
            Decimal256 value;
            UInt32 scale;
            readBinary(value, buf);
            readBinary(scale, buf);
            field = DecimalField<Decimal256>(value, scale);
            return;
        }
        case Field::Types::Map:
        {
            Map value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::UUID:
        {
            UUID value;
            readBinary(value, buf);
            field = value;
            return;
        }
        case Field::Types::AggregateFunctionState:
        {
            AggregateFunctionStateData value;
            readStringBinary(value.name, buf);
            readStringBinary(value.data, buf);
            field = value;
            return;
        }
        case Field::Types::JSONB:
        {
            JsonbField value;
            readBinary(value, buf);
            field = value;
            return;
        }
        default:
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field {} when deserializing.", type);
    }
}

void writeFieldBinary(const Field & field, WriteBuffer & buf)
{
    auto type = field.getType();
    writeBinary(static_cast<UInt8>(type), buf);

    Field::dispatch([&buf](const auto & value) { FieldVisitorCompatibleWriteBinary()(value, buf); }, field);
}

void readFieldBinary(Field & field, ReadBuffer & buf)
{
    UInt8 read_type = 0;
    readBinary(read_type, buf);
    auto type = static_cast<Field::Types::Which>(read_type);

    field = Field::dispatch(FieldVisitorCompatibleReadBinary(buf), type);
}

void Field::toProto(Protos::Field & proto) const
{
    auto type = this->getType();
    auto proto_type = Field::Types::WhichConverter::toProto(type);
    proto.set_type(proto_type);
    WriteBufferFromOwnString buf;
    writeFieldBinaryBlobImpl(*this, type, buf);
    proto.set_blob(std::move(buf.str()));
}

void Field::fillFromProto(const Protos::Field & proto)
{
    auto type = Field::Types::WhichConverter::fromProto(proto.type());
    ReadBufferFromString buf(proto.blob());
    readFieldBinaryBlobImpl(*this, type, buf);
}

String Field::dump() const
{
    return applyVisitor(FieldVisitorDump(), *this);
}

Field Field::restoreFromDump(const std::string_view & dump_)
{
    auto show_error = [&dump_]
    {
        throw Exception("Couldn't restore Field from dump: " + String{dump_}, ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP);
    };

    std::string_view dump = dump_;
    trim(dump);

    if (dump == "NULL")
        return {};

    std::string_view prefix = std::string_view{"Int64_"};
    if (dump.starts_with(prefix))
    {
        Int64 value = parseFromString<Int64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt64_"};
    if (dump.starts_with(prefix))
    {
        UInt64 value = parseFromString<UInt64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Int128_"};
    if (dump.starts_with(prefix))
    {
        Int128 value = parseFromString<Int128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt128_"};
    if (dump.starts_with(prefix))
    {
        UInt128 value = parseFromString<UInt128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Int256_"};
    if (dump.starts_with(prefix))
    {
        Int256 value = parseFromString<Int256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"UInt256_"};
    if (dump.starts_with(prefix))
    {
        UInt256 value = parseFromString<UInt256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Float64_"};
    if (dump.starts_with(prefix))
    {
        Float64 value = parseFromString<Float64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"Decimal32_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal32> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"Decimal64_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal64> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"Decimal128_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal128> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"Decimal256_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal256> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    if (dump.starts_with("\'"))
    {
        String str;
        ReadBufferFromString buf{dump};
        readQuoted(str, buf);
        return str;
    }

    prefix = std::string_view{"Array_["};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Array array;
        while (tail != "]")
        {
            size_t separator = tail.find_first_of(",]");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != "]")
                show_error();
            array.push_back(Field::restoreFromDump(element));
        }
        return array;
    }

    prefix = std::string_view{"Tuple_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Tuple tuple;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            tuple.push_back(Field::restoreFromDump(element));
        }
        return tuple;
    }

    prefix = std::string_view{"Map_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Map map;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();

            Tuple tuple = Field::restoreFromDump(element).safeGet<Tuple>();
            map.emplace_back(tuple[0], tuple[1]);
        }
        return map;
    }

    prefix = std::string_view{"AggregateFunctionState_("};
    if (dump.starts_with(prefix))
    {
        std::string_view after_prefix = dump.substr(prefix.length());
        size_t comma = after_prefix.find(',');
        size_t end = after_prefix.find(')', comma + 1);
        if ((comma == std::string_view::npos) || (end != after_prefix.length() - 1))
            show_error();
        std::string_view name_view = after_prefix.substr(0, comma);
        std::string_view data_view = after_prefix.substr(comma + 1, end - comma - 1);
        trim(name_view);
        trim(data_view);
        ReadBufferFromString name_buf{name_view};
        ReadBufferFromString data_buf{data_view};
        AggregateFunctionStateData res;
        readQuotedString(res.name, name_buf);
        readQuotedString(res.data, data_buf);
        return res;
    }

    prefix = std::string_view{"BitMap64_{"};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        BitMap64 bitmap;
        while (tail != "}")
        {
            size_t separator = tail.find_first_of(",}");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != "}")
                show_error();
            bitmap.add(parseFromString<UInt64>(element));
        }
    }

    show_error();
    __builtin_unreachable();
}


template <typename T>
bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}


template bool decimalEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLess<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLessOrEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);


inline void writeText(const Null &, WriteBuffer & buf)
{
    writeText(std::string("NULL"), buf);
}

inline void writeText(const NegativeInfinity &, WriteBuffer & buf)
{
    writeText(std::string("-Inf"), buf);
}

inline void writeText(const PositiveInfinity &, WriteBuffer & buf)
{
    writeText(std::string("+Inf"), buf);
}

String toString(const Field & x)
{
    return Field::dispatch(
        [] (const auto & value)
        {
            // Use explicit type to prevent implicit construction of Field and
            // infinite recursion into toString<Field>.
            return toString<decltype(value)>(value);
        },
        x);
}

String fieldTypeToString(Field::Types::Which type)
{
    switch (type)
    {
        case Field::Types::Which::Null: return "Null";
        case Field::Types::Which::Array: return "Array";
        case Field::Types::Which::Tuple: return "Tuple";
        case Field::Types::Which::Map: return "Map";
        case Field::Types::Which::Object: return "Object";
        case Field::Types::Which::AggregateFunctionState: return "AggregateFunctionState";
        case Field::Types::Which::String: return "String";
        case Field::Types::Which::Decimal32: return "Decimal32";
        case Field::Types::Which::Decimal64: return "Decimal64";
        case Field::Types::Which::Decimal128: return "Decimal128";
        case Field::Types::Which::Decimal256: return "Decimal256";
        case Field::Types::Which::Float64: return "Float64";
        case Field::Types::Which::Int64: return "Int64";
        case Field::Types::Which::Int128: return "Int128";
        case Field::Types::Which::Int256: return "Int256";
        case Field::Types::Which::UInt64: return "UInt64";
        case Field::Types::Which::UInt128: return "UInt128";
        case Field::Types::Which::UInt256: return "UInt256";
        case Field::Types::Which::UUID: return "UUID";
        case Field::Types::Which::IPv4: return "IPv4";
        case Field::Types::Which::IPv6: return "IPv6";
        case Field::Types::Which::BitMap64: return "BitMap64";
        case Field::Types::Which::SketchBinary: return "SketchBinary";
        case Field::Types::Which::NegativeInfinity: return "NegativeInfinity";
        case Field::Types::Which::PositiveInfinity: return "PositiveInfinity";
        case Field::Types::Which::JSONB: return "JSONB";
    }

    __builtin_unreachable();
}

}

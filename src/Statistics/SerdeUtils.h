#pragma once
#include <string_view>
#include <tuple>
#include <DataTypes/IDataType.h>
#include <Statistics/SerdeDataType.h>
#include <fmt/format.h>

namespace DB::Statistics
{
template <typename HeaderType = SerdeDataType>
inline std::tuple<HeaderType, std::string_view> parseBlobWithHeader(std::string_view raw_blob)
{
    static_assert(std::is_trivial_v<HeaderType>);
    if (raw_blob.size() <= sizeof(HeaderType))
    {
        throw Exception("corrupted blob", ErrorCodes::LOGICAL_ERROR);
    }

    HeaderType header;
    memcpy(&header, raw_blob.data(), sizeof(header));
    auto blob = raw_blob.substr(sizeof(header), raw_blob.size() - sizeof(header));
    return {header, blob};
}

inline SerdeDataType dataTypeToSerde(const IDataType & data_type)
{
    auto type_index = data_type.getTypeId();
    switch (type_index)
    {
#define CASE(RAW_TYPE, SERDE_TYPE) \
    case TypeIndex::RAW_TYPE: \
        return SerdeDataType::SERDE_TYPE;
#define CASE_SAME(TYPE) CASE(TYPE, TYPE)
        ALL_TYPE_ITERATE(CASE_SAME)
        CASE(Date, UInt16)
        CASE(DateTime, UInt32)
        default:
            throw Exception(fmt::format(FMT_STRING("unknown type index {}"), type_index), ErrorCodes::LOGICAL_ERROR);
    }
}

template <typename T>
constexpr bool IsWideInteger = wide::IsWideInteger<T>::value;
template <typename T>
void checkSerdeDataType(SerdeDataType serde_data_type)
{
    if (std::is_same_v<T, String> && serde_data_type == SerdeDataType::StringOldVersion)
        return;

    if (serde_data_type != SerdeDataTypeFrom<T>)
    {
        throw Exception("mismatched type", ErrorCodes::LOGICAL_ERROR);
    }
}


}

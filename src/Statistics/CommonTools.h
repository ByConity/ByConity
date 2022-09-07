#pragma once
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Statistics/CommonErrorCodes.h>

// use namespace to avoid name pollution
namespace DB::Statistics
{

struct DecayVerboseResult
{
    DataTypePtr type;
    bool is_nullable = false;
    bool is_low_cardinality = false;
};

namespace impl
{
    template <class>
    inline constexpr bool always_false_v = false;
}


inline DecayVerboseResult decayDataTypeVerbose(DataTypePtr type)
{
    DecayVerboseResult result;

    while (true)
    {
        if (type->lowCardinality())
        {
            type = dynamic_cast<const DataTypeLowCardinality *>(type.get())->getDictionaryType();
            result.is_low_cardinality = true;
        }

        else if (type->isNullable())
        {
            type = dynamic_cast<const DataTypeNullable *>(type.get())->getNestedType();
            result.is_nullable = true;
        }
        else
        {
            break;
        }
    }
    result.type = type;
    return result;
}

inline DataTypePtr decayDataType(const DataTypePtr & type)
{
    return decayDataTypeVerbose(type).type;
}

inline bool isCollectableType(const DataTypePtr & raw_type)
{
    auto res = decayDataTypeVerbose(raw_type);
    auto type = res.type;

    if (isColumnedAsNumber(type))
    {
        return true;
    }
    else if (isStringOrFixedString(type))
    {
        return true;
    }
    else if (isDecimal(type))
    {
        return true;
    }
    else
    {
        return false;
    }
}

}

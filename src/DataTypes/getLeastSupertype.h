#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

template <typename T,
typename = std::enable_if_t<std::is_same_v<T, DataTypeDateTime64> || std::is_same_v<T, DataTypeTime>, T>>
size_t getMaxScaleIndex(const DataTypes &types)
{
    UInt8 max_scale = 0;
    size_t max_scale_date_time_index = 0;

    for (size_t i = 0; i < types.size(); ++i)
    {
        const auto & type = types[i];

        if (const auto * datetime64_or_time_type = typeid_cast<const T *>(type.get()))
        {
            const auto scale = datetime64_or_time_type->getScale();
            if (scale >= max_scale)
            {
                max_scale_date_time_index = i;
                max_scale = scale;
            }
        }
    }
    return max_scale_date_time_index;
}

/** Get data type that covers all possible values of passed data types.
  * If there is no such data type, throws an exception.
  *
  * Examples: least common supertype for UInt8, Int8 - Int16.
  * Examples: there is no least common supertype for Array(UInt8), Int8.
  */
DataTypePtr getLeastSupertype(const DataTypes & types);

}

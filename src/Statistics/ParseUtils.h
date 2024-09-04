/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <type_traits>
#include <DataTypes/IDataType.h>
#include <DataTypes/MapHelpers.h>
#include <Statistics/BucketBounds.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/StatisticsCommon.h>
#include <Statistics/StatsHllSketch.h>
#include <Statistics/StatsNdvBucketsResult.h>
#include <Statistics/TableHandler.h>
#include <Statistics/TypeUtils.h>

namespace DB::Statistics
{

enum class WrapperKind
{
    Invalid = 0,
    None = 1,
    StringToHash64 = 2, // apply "cityHash64" in sql
    DecimalToFloat64 = 3, // apply "toFloat64" in sql
    FixedStringToHash64 = 4, // apply "cityHash64 . toString" in sql
    UUIDToUInt128 = 5, // apply "toUInt128" in sql
};

struct ColumnCollectConfig
{
    WrapperKind wrapper_kind = WrapperKind::Invalid;
    bool need_count = true;
    bool need_ndv = true;
    bool need_histogram = true;
    bool need_minmax = false;
    bool need_length = false;
    bool is_nullable = false;
    
    String getByteSizeSql(const String& quote_col_name)
    {
        if (is_nullable) 
        {
            return fmt::format(FMT_STRING("if(isNotNull({0}), byteSize({0}), 0)"), quote_col_name);
        }
        else 
        {
            return fmt::format(FMT_STRING("byteSize({})"), quote_col_name);
        }
    }
};

inline ColumnCollectConfig getColumnConfig(const CollectorSettings & settings, const DataTypePtr & raw_type)
{
    auto type_verbose = decayDataTypeVerbose(raw_type);
    auto type = type_verbose.type;
    ColumnCollectConfig config;
    config.need_count = true;
    config.need_ndv = true;
    config.need_histogram = true;
    config.need_minmax = false;
    config.need_length = false;
    config.is_nullable = type_verbose.is_nullable;

    if (!settings.collect_histogram() || settings.collect_in_partitions())
    {
        config.need_histogram = false;
        config.need_minmax = true;
    }

    if (isString(type))
    {
        config.wrapper_kind = WrapperKind::StringToHash64;
        config.need_length = true;
    }
    else if (isFixedString(type))
    {
        config.wrapper_kind = WrapperKind::FixedStringToHash64;
        // NOTE: we don't need length for fixed string
        // just return the fixed size
    }
    else if (isColumnedAsDecimal(type))
    {
        // Note: DateTime64 is a Decimal, convert it to float64
        config.wrapper_kind = WrapperKind::DecimalToFloat64;
    }
    else if (isUUID(type))
    {
        config.wrapper_kind = WrapperKind::UUIDToUInt128;
    }
    else
    {
        config.wrapper_kind = WrapperKind::None;
    }

    return config;
}


template <typename T>
inline T getSingleValue(const Block & block, size_t index)
{
    auto col = block.getByPosition(index).column;
    if constexpr (std::is_same_v<T, std::string_view>)
    {
        return static_cast<std::string_view>(col->getDataAt(0));
    }
    else if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T>)
    {
        return col->getUInt(0);
    }
    else if constexpr (std::is_same_v<T, double>)
    {
        if (col->isNullAt(0))
        {
            return std::numeric_limits<double>::quiet_NaN();
        }
        union
        {
            Float64 f64;
            UInt64 u64;
        } x;
        x.u64 = col->get64(0);
        return x.f64;
    }
    else
    {
        static_assert(impl::always_false_v<T>, "not implemented");
    }
}

inline Float64 getNdvFromSketchBinary(std::string_view blob)
{
    if (blob.empty())
    {
        return 0;
    }
    auto hll = createStatisticsUntyped<StatsHllSketch>(StatisticsTag::HllSketch, blob);
    return hll->getEstimate();
}

inline String getWrappedColumnName(const ColumnCollectConfig & config, const String & col_name)
{
    auto wrapper_kind = config.wrapper_kind;
    switch (wrapper_kind)
    {
        case WrapperKind::None:
            return col_name;
        case WrapperKind::StringToHash64:
            return fmt::format(FMT_STRING("cityHash64({})"), col_name);
        case WrapperKind::FixedStringToHash64:
            return fmt::format(FMT_STRING("cityHash64(toString({}))"), col_name);
        case WrapperKind::DecimalToFloat64:
            return fmt::format(FMT_STRING("toFloat64({})"), col_name);
        case WrapperKind::UUIDToUInt128:
            return fmt::format(FMT_STRING("reinterpret({}, 'UInt128')"), col_name);
        default:
            throw Exception("Unknown wrapper mode", ErrorCodes::LOGICAL_ERROR);
    }
}

inline String getKllFuncNameWithConfig(UInt64 kll_log_k)
{
    if (kll_log_k == DEFAULT_KLL_SKETCH_LOG_K)
    {
        return "kll";
    }
    else
    {
        return fmt::format("kll({})", kll_log_k);
    }
}


inline std::vector<String> toQuotedNames(const ColumnDescVector & cols_desc)
{
    std::vector<String> result;
    for (const auto & col_desc : cols_desc)
    {
        result.push_back(quoteString(col_desc.name));
    }
    return result;
}

// return col name that suitable to use in sql
// 1. backQuoted when needed
// 2. convert to map{'...'} when needed
inline String colNameForSql(const String & col_name)
{
    // optimizer don't support use __map__'key' directly
    // so workaround it with map{'key'}
    if (isMapImplicitKey(col_name))
    {
        auto map_col = parseMapNameFromImplicitColName(col_name);
        auto key_name = parseKeyNameFromImplicitColName(col_name, map_col);
        /// Attention: key_name has been quoted, no need to add more quote
        return fmt::format("{}{{{}}}", backQuoteIfNeed(map_col), key_name);
    }
    else
    {
        return backQuoteIfNeed(col_name);
    }
}

inline ColumnPtr getNestedColumn(ColumnPtr column)
{
    if (column->lowCardinality())
    {
        column = column->convertToFullColumnIfLowCardinality();
    }

    if (column->isNullable())
    {
        return checkAndGetColumn<ColumnNullable>(*column)->getNestedColumnPtr();
    }

    return column;
}

}


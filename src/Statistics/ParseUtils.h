#pragma once
#include <Statistics/BucketBounds.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CommonTools.h>
#include <Statistics/StatisticsCommon.h>
#include <Statistics/StatsCpcSketch.h>
#include <Statistics/StatsNdvBucketsResult.h>
#include <Statistics/TableHandler.h>

namespace DB::Statistics
{

enum class WrapperKind
{
    Invalid = 0,
    None = 1,
    StringToHash64 = 2, // when necessary, apply "cityHash64" in sql
    DecimalToFloat64 = 3, // when necessary, apply "toFloat64" in sql
};


struct ColumnCollectConfig
{
    WrapperKind wrapper_kind = WrapperKind::Invalid;
    bool need_count = true;
    bool need_ndv = true;
    bool need_histogram = true;
    bool need_minmax = false;
};

inline ColumnCollectConfig get_column_config(const Settings & settings, const DataTypePtr & type)
{
    ColumnCollectConfig config;
    config.need_count = true;
    config.need_ndv = true;
    config.need_histogram = true;
    config.need_minmax = true;

    if (!settings.statistics_collect_histogram)
    {
        config.need_histogram = false;
    }
    else if (!settings.statistics_collect_floating_histogram)
    {
        if (isFloat(type) || isDecimal(type))
        {
            config.need_histogram = false;
        }
    }

    if (isStringOrFixedString(type))
    {
        config.wrapper_kind = WrapperKind::StringToHash64;
    }
    else if (isDecimal(type))
    {
        config.wrapper_kind = WrapperKind::DecimalToFloat64;
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
    else
    {
        static_assert(impl::always_false_v<T>, "not implemented");
    }
}

inline Float64 getNdvFromBase64(std::string_view b64_blob)
{
    if (b64_blob.empty())
    {
        return 0;
    }
    auto blob = base64Decode(b64_blob);
    auto cpc = createStatisticsUntyped<StatsCpcSketch>(StatisticsTag::CpcSketch, blob);
    return cpc->get_estimate();
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
        case WrapperKind::DecimalToFloat64:
            return fmt::format(FMT_STRING("toFloat64({})"), col_name);
        default:
            throw Exception("Unknown wrapper mode", ErrorCodes::LOGICAL_ERROR);
    }
}


}

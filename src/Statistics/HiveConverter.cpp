#include <hive_metastore_types.h>
#include <Statistics/HiveConverter.h>
#include <Statistics/StatsColumnBasic.h>
#include <Statistics/StatsTableBasic.h>
#include <Statistics/TypeUtils.h>


namespace DB::Statistics::StatisticsImpl
{

template <class T>
static UInt64 getPatchNdv(const T & stats)
{
    return stats.numDVs - (stats.numNulls > 0 ? 1 : 0);
}

static ColumnStats parse(UInt64 row_count, const ApacheHive::LongColumnStatsData & stats)
{
    auto basic = std::make_shared<StatsColumnBasic>();
    basic->mutableProto().set_min_as_double(stats.lowValue);
    basic->mutableProto().set_max_as_double(stats.highValue);
    basic->mutableProto().set_ndv_value(getPatchNdv(stats));
    basic->mutableProto().set_nonnull_count(row_count - stats.numNulls);

    return ColumnStats{.basic = std::move(basic)};
}

static ColumnStats parse(UInt64 row_count, const ApacheHive::DoubleColumnStatsData & stats)
{
    auto basic = std::make_shared<StatsColumnBasic>();
    basic->mutableProto().set_min_as_double(stats.lowValue);
    basic->mutableProto().set_max_as_double(stats.highValue);
    basic->mutableProto().set_ndv_value(getPatchNdv(stats));
    basic->mutableProto().set_nonnull_count(row_count - stats.numNulls);

    return ColumnStats{.basic = std::move(basic)};
}

static ColumnStats parse(UInt64 row_count, const ApacheHive::DateColumnStatsData & stats)
{
    auto basic = std::make_shared<StatsColumnBasic>();
    basic->mutableProto().set_min_as_double(stats.lowValue.daysSinceEpoch);
    basic->mutableProto().set_max_as_double(stats.highValue.daysSinceEpoch);
    basic->mutableProto().set_ndv_value(getPatchNdv(stats));
    basic->mutableProto().set_nonnull_count(row_count - stats.numNulls);

    return ColumnStats{.basic = std::move(basic)};
}

static ColumnStats parse(UInt64 row_count, const ApacheHive::StringColumnStatsData & stats)
{
    auto basic = std::make_shared<StatsColumnBasic>();
    basic->mutableProto().set_min_as_double(0);
    basic->mutableProto().set_max_as_double(std::numeric_limits<UInt64>::max());
    basic->mutableProto().set_ndv_value(getPatchNdv(stats));
    basic->mutableProto().set_nonnull_count(row_count - stats.numNulls);

    return ColumnStats{.basic = std::move(basic)};
}

static double parse(const ApacheHive::Decimal & decimal)
{
    if (decimal.unscaled.empty())
        return 0;

    bool is_first = true;
    double value = 0;
    for (auto byte : decimal.unscaled)
    {
        if (is_first)
        {
            value = static_cast<Int8>(byte);
            is_first = false;
            continue;
        }
        value = value * 256 + static_cast<UInt8>(byte);
    }
    value *= std::pow(10, -decimal.scale);
    return value;
}

static ColumnStats parse(UInt64 row_count, const ApacheHive::DecimalColumnStatsData & stats)
{
    auto basic = std::make_shared<StatsColumnBasic>();
    basic->mutableProto().set_min_as_double(parse(stats.lowValue));
    basic->mutableProto().set_max_as_double(parse(stats.highValue));
    basic->mutableProto().set_ndv_value(getPatchNdv(stats));
    basic->mutableProto().set_nonnull_count(row_count - stats.numNulls);

    return ColumnStats{.basic = std::move(basic)};
}

static ColumnStats parse(UInt64 row_count, const ApacheHive::BooleanColumnStatsData & stats)
{
    auto basic = std::make_shared<StatsColumnBasic>();

    int ndv = 0;
    double min = 0;
    double max = 0;
    if (stats.numFalses > 0)
    {
        min = 0;
        if (stats.numTrues > 0)
        {
            ndv = 2;
            max = 1;
        }
        else
        {
            ndv = 1;
            max = 0;
        }
    }
    else if (stats.numTrues > 0)
    {
        ndv = 1;
        min = max = 1;
    }
    else
    {
        ndv = 0;
        min = max = std::numeric_limits<double>::quiet_NaN();
    }

    basic->mutableProto().set_min_as_double(min);
    basic->mutableProto().set_max_as_double(max);
    basic->mutableProto().set_ndv_value(ndv);
    basic->mutableProto().set_nonnull_count(row_count - stats.numNulls);

    return ColumnStats{.basic = std::move(basic)};
}



std::pair<TableStats, ColumnStatsMap>
convertHiveToStats(UInt64 row_count, const NameToType & name_to_type, const ApacheHive::TableStatsResult & hive_stats)
{
    using namespace StatisticsImpl;
    TableStats table_stats;
    table_stats.basic = std::make_shared<StatsTableBasic>();
    table_stats.basic->setRowCount(row_count);
    // TODO support column stats

    ColumnStatsMap stats_map;

    for (const auto & col_obj : hive_stats.tableStats)
    {
        if (!name_to_type.contains(col_obj.colName))
            continue;
        auto type = decayDataType(name_to_type.at(col_obj.colName));

        ColumnStats col_stats;

        switch (type->getTypeId())
        {
            case TypeIndex::Int8:
            case TypeIndex::UInt8:

                if (col_obj.statsData.__isset.booleanStats)
                {
                    // hack when use UInt8 to represent hive boolean
                    col_stats = parse(row_count, col_obj.statsData.booleanStats);
                }
                else
                {
                    col_stats = parse(row_count, col_obj.statsData.longStats);
                }
                break;

            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Int64:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
                col_stats = parse(row_count, col_obj.statsData.longStats);
                break;

            case TypeIndex::Float32:
            case TypeIndex::Float64:
                col_stats = parse(row_count, col_obj.statsData.doubleStats);
                break;

            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128:
            case TypeIndex::Decimal256:
                col_stats = parse(row_count, col_obj.statsData.decimalStats);
                break;

            case TypeIndex::Date:
            case TypeIndex::Date32:
                col_stats = parse(row_count, col_obj.statsData.dateStats);
                break;

            case TypeIndex::String:
            case TypeIndex::FixedString:
                col_stats = parse(row_count, col_obj.statsData.stringStats);
                break;

            case TypeIndex::DateTime:
            case TypeIndex::DateTime64:
                col_stats = parse(row_count, col_obj.statsData.longStats);
                break;
            default:
                // unsupported type
                continue;
        }
        stats_map[col_obj.colName] = std::move(col_stats);
    }


    return {std::move(table_stats), std::move(stats_map)};
}

}

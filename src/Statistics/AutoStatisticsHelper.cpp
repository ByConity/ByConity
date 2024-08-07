
#include <chrono>
#include <Core/UUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/now64.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InterpreterCreateStatsQuery.h>
#include <Interpreters/executeQuery.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTStatsQuery.h>
#include <Protos/cnch_server_rpc.pb.h>
#include <Protos/optimizer_statistics.pb.h>
#include <ResourceManagement/VirtualWarehouseType.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/AutoStatisticsMemoryRecord.h>
#include <Statistics/StatisticsCollector.h>
#include <fmt/format.h>


namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TABLE;
extern const int BAD_ARGUMENTS;
}


namespace DB::Statistics::AutoStats
{


std::tuple<UInt64, DateTime64> getTableStatsFromCatalog(CatalogAdaptorPtr catalog, const StatsTableIdentifier & table)
{
    // read table stats only
    auto stats_collection = catalog->readSingleStats(table, std::nullopt);
    StatisticsImpl::TableStats table_stats;
    table_stats.readFromCollection(stats_collection);

    if (const auto & basic = table_stats.basic)
    {
        auto count = basic->getRowCount();
        auto timestamp = basic->getTimestamp();
        return std::make_tuple(count, timestamp);
    }
    else
    {
        return std::make_tuple(0, DateTime64(0));
    }
}

bool betweenTime(Time target, Time beg, Time end)
{
    if (beg == end)
    {
        return false;
    }

    if (beg < end)
    {
        return beg <= target && target < end;
    }
    else
    {
        return beg <= target || target < end;
    }
}

DateTime64 convertToDateTime64(TimePoint time)
{
    static_assert(DataTypeDateTime64::default_scale == 3); // milliseconds
    auto ms = (time - TimePoint()) / std::chrono::milliseconds(1);
    DateTime64 result;
    result.value = ms;
    return result;
}

TimePoint nowTimePoint()
{
    return std::chrono::system_clock::now();
}

ExtendedDayNum convertToDate(DateTime64 time)
{
    time_t ts = time.value / DecimalUtils::scaleMultiplier<time_t>(DataTypeDateTime64::default_scale);
    auto date = DateLUT::serverTimezoneInstance().toDayNum(ts);
    return date;
}


TimePoint convertToTimePoint(DateTime64 time)
{
    static_assert(DataTypeDateTime64::default_scale == 3); // milliseconds
    auto t = TimePoint() + std::chrono::milliseconds(time.value);
    return t;
}

std::optional<Time> getTimeFromString(const String & text)
{
    struct std::tm tmp;
    std::istringstream ss(text);
    ss >> std::get_time(&tmp, "%H:%M");
    if (ss.fail())
    {
        return std::nullopt;
    }
    return DateLUT::serverTimezoneInstance().toTime(DateLUT::serverTimezoneInstance().makeDateTime(1971, 1, 1, tmp.tm_hour, tmp.tm_min, 0));
}


std::optional<double> calcPriority(const InternalConfig & cfg, UInt64 total_udi, UInt64 table_row_count)
{
    if (total_udi == 0)
    {
        // DO NOTHING
        return std::nullopt;
    }

    if (table_row_count == 0)
    {
        // range (3, 4)
        // when stats is empty, it must be collected first.
        // and smaller table has a larger priority
        // use log(x + \e) to make the mapping more user-friendly
        auto priority = 3 + 1.0 / std::log(static_cast<double>(total_udi) + M_E);
        return priority;
    }
    else if (total_udi > table_row_count * cfg.update_ratio_threshold || total_udi > cfg.update_row_count_threshold)
    {
        // range (1, 2)
        // when stats is not empty, it will be collected later
        // and higher change of data, i.e. ratio=total_udi/table_row_count
        // will have higher priority
        // use fake_ratio to make the range better
        auto fake_ratio = 1.0 * total_udi / (table_row_count + total_udi);
        auto priority = 1.0 + fake_ratio;
        return priority;
    }
    else
    {
        // DO NOTHING
        return std::nullopt;
    }
}

String serializeToText(DateTime64 time)
{
    WriteBufferFromOwnString buffer;
    writeDateTimeText(time, DataTypeDateTime64::default_scale, buffer, DateLUT::serverTimezoneInstance());
    return buffer.str();
}

String serializeToText(ExtendedDayNum date)
{
    WriteBufferFromOwnString buffer;
    writeDateText(date, buffer);
    return buffer.str();
}

} // namespace DB::Statistics

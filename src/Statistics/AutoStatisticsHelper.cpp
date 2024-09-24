
#include <chrono>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <Functions/now64.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InterpreterCreateStatsQuery.h>
#include <Interpreters/executeQuery.h>
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
extern const int UNKNOWN_DATABASE;
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
    return DateLUT::serverTimezoneInstance().toTime(DateLUT::serverTimezoneInstance().makeDateTime(1970, 1, 1, tmp.tm_hour, tmp.tm_min, 0));
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
    else if (total_udi > table_row_count * cfg.update_ratio_threshold() || total_udi > cfg.update_row_count_threshold())
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

DataTypePtr enumDataTypeForTaskType()
{
    // TODO(gouguilin): change this using reflection of Protobuf
    // Protos::AutoStats::Status
    auto enum_values = DataTypeEnum8::Values({{"Manual", 1}, {"Auto", 2}});
    auto enum_type = std::make_shared<DataTypeEnum8>(enum_values);
    return enum_type;
}
DataTypePtr enumDataTypeForStatus()
{
    // TODO(gouguilin): change this using reflection of Protobuf
    // Protos::AutoStats::Status
    auto enum_values = DataTypeEnum8::Values(
        {{"NotExists", 1}, {"Created", 2}, {"Pending", 3}, {"Running", 4}, {"Error", 5}, {"Failed", 6}, {"Success", 7}, {"Cancelled", 8}});
    auto enum_type = std::make_shared<DataTypeEnum8>(enum_values);
    return enum_type;
}

std::vector<StatisticsScope> getChildrenScope(ContextPtr context, const StatisticsScope & scope)
{
    auto catalog = createCatalogAdaptor(context);
    std::vector<StatisticsScope> result;
    auto check_and_add_table = [&](const String & db_name, const String & table_name) {
        auto table_opt = catalog->getTableIdByName(db_name, table_name);
        if (!table_opt)
            return false;

        if (!catalog->isTableAutoUpdatable(table_opt.value()))
            return false;

        result.emplace_back(StatisticsScope{db_name, table_name});
        return true;
    };

    if (!scope.database)
    {
        result.emplace_back(scope);
        for (auto & [db_name, db] : DatabaseCatalog::instance().getDatabases(context))
        {
            if (!CatalogAdaptor::isDatabaseCollectable(db_name))
                continue;
            if (db->getEngineName() == "Memory")
                continue;

            result.emplace_back(StatisticsScope{db_name, std::nullopt});
            for (auto iter = db->getTablesIterator(context); iter->isValid(); iter->next())
            {
                check_and_add_table(db_name, iter->name());
            }
        }
    }
    else if (!scope.table)
    {
        auto db_name = *scope.database;
        auto db = DatabaseCatalog::instance().getDatabase(db_name, context);
        if (!db)
            throw Exception("database is dropped", ErrorCodes::UNKNOWN_DATABASE);

        result.emplace_back(scope);
        for (auto iter = db->getTablesIterator(context); iter->isValid(); iter->next())
        {
            check_and_add_table(db_name, iter->name());
        }
    }
    else
    {
        auto succ = check_and_add_table(scope.database.value(), scope.table.value());
        if (!succ)
        {
            throw Exception("unknown or unsupported table", ErrorCodes::UNKNOWN_TABLE);
        }
    }
    return result;
}

std::vector<StatsTableIdentifier> getTablesInScope(ContextPtr context, const StatisticsScope & statistics_scope)
{
    auto catalog = createCatalogAdaptor(context);
    auto scopes = getChildrenScope(context, statistics_scope);
    std::vector<StatsTableIdentifier> identifiers;
    for (const auto & scope : scopes)
    {
        if (!scope.table.has_value())
            continue;
        auto table_opt = catalog->getTableIdByName(scope.database.value(), scope.table.value());
        if (!table_opt)
            throw Exception("invalid tables", ErrorCodes::LOGICAL_ERROR);
        identifiers.emplace_back(table_opt.value());
    }
    return identifiers;
}

void extractPredicatesImpl(const IAST & elem, PredicatesHint & result, ContextPtr context)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            extractPredicatesImpl(*child, result, context);
        return;
    }

    const auto & args = function->arguments->as<ASTExpressionList &>();

    if (function->name == "in")
    {
        // TODO: support in
        return;
    }
    else if (function->name == "equals")
    {
        if (args.children.size() != 2)
            return;
        ASTPtr value;
        auto left_ast = args.children.at(0);
        auto right_ast = args.children.at(1);
        if (right_ast->getType() == ASTType::ASTIdentifier)
        {
            std::swap(left_ast, right_ast);
        }

        auto * ident = left_ast->as<ASTIdentifier>();
        auto * literal = right_ast->as<ASTLiteral>();
        if (!ident || !literal)
        {
            return;
        }
        if (ident->name() == "database")
        {
            result.database_opt = literal->value.toString();
        }
        else if (ident->name() == "table")
        {
            result.table_opt = literal->value.toString();
        }
    }
}

PredicatesHint extractPredicates(const IAST & query, ContextPtr context)
{
    PredicatesHint res;
    const auto & select = query.as<ASTSelectQuery &>();
    if (!select.where())
        return res;

    extractPredicatesImpl(*select.where(), res, context);
    return res;
}


} // namespace DB::Statistics

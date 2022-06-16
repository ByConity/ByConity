#include <DataStreams/IBlockInputStream.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Statistics/Base64.h>
#include <Statistics/BlockParser.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatsDataSource.h>
#include <Statistics/SubqueryHelper.h>
#include <Storages/IStorage.h>
#include <fmt/core.h>
#include <Common/CurrentThread.h>
#include "Interpreters/getClusterName.h"

namespace DB::Statistics
{
namespace
{
    const String DATABASE = "system";
    const String TABLE = "optimizer_statistics";
    const String LOCAL_TABLE = "optimizer_statistics_local";
    const String DB_TABLE = fmt::format(FMT_STRING("{}.{}"), DATABASE, TABLE);
    const String DB_LOCAL_TABLE = fmt::format(FMT_STRING("{}.{}"), DATABASE, LOCAL_TABLE);
}

static String fetchClusterName(ContextPtr context)
{
    auto logger = &Poco::Logger::get("StatsDataSource::fetchClusterName");
    auto cluster_name = context->getConfigRef().getString("optimizer.primary_cluster_name", "");
    if (!cluster_name.empty())
    {
        LOG_INFO(logger, fmt::format("using cluster name {} from xml config optimizer.primary_cluster_name ", cluster_name));
        return cluster_name;
    }
    auto clusters = context->getClusters()->getContainer();
    if (clusters.empty())
    {
        throw Exception("no cluster found in remote_servers", ErrorCodes::LOGICAL_ERROR);
    }

    if (clusters.size() > 1)
    {
        LOG_WARNING(logger, "multiple clusters found in remote_servers, choosing random cluster from cluster list");
    }
    cluster_name = clusters.begin()->first;

    LOG_INFO(logger, fmt::format("using cluster name {}", cluster_name));
    return cluster_name;
}

void StatsDataSource::initialize() const
{
    String zookeeper_path = "/clickhouse/optimizer_statistics_local/{shard}";
    String replica_name = "{replica}";
    String create_local_sql = fmt::format(
        FMT_STRING("create table if not exists {} (table_uuid UUID, column_name String, tag UInt64, value String) "
                   "Engine=HaUniqueMergeTree('{}', '{}') "
                   "partition by table_uuid "
                   "order by table_uuid "
                   "unique key (sipHash64(table_uuid), sipHash64(column_name), tag) "),
        DB_LOCAL_TABLE,
        zookeeper_path,
        replica_name);

    // TODO fetch from config
    String cluster_name = fetchClusterName(context);
    String create_distributed_sql = fmt::format( //
        FMT_STRING("create table if not exists {} "
                   "(table_uuid UUID, column_name String, tag UInt64, value String, _delete_flag_ UInt8) "
                   "Engine=Distributed('{}', '{}', '{}', sipHash64(table_uuid))"),
        DB_TABLE,
        cluster_name,
        DATABASE,
        LOCAL_TABLE);

    executeSubQuery(context, create_local_sql);
    executeSubQuery(context, create_distributed_sql);
}


bool StatsDataSource::has(const StatsTableIdentifier & table_identifier) const
{
    if (!isInitialized())
    {
        initialize();
    }

    auto sql_key = toString(table_identifier.getUUID());
    String sql = fmt::format(
        FMT_STRING("select count(*) "
                   "from {} "
                   "where table_uuid='{}' "),
        DB_TABLE,
        sql_key);
    auto helper = SubqueryHelper::create(context, sql);
    auto block = getOnlyRowFrom(helper);
    auto column = block.getByPosition(0).column;

    if (column->size() != 1)
    {
        throw Exception("Unexpected result", ErrorCodes::LOGICAL_ERROR);
    }

    return column->getUInt(0) > 0;
}

StatsCollection StatsDataSource::getSingle(const StatsTableIdentifier & table_identifier, const std::optional<String> & column_name) const
{
    if (!isInitialized())
    {
        initialize();
    }

    auto timestamp = getUpdateTime();
    auto sql_key = toString(table_identifier.getUUID());
    String sql = fmt::format(
        FMT_STRING("select column_name, tag, value "
                   "from {} "
                   "where table_uuid='{}' and column_name='{}' "),
        DB_TABLE,
        sql_key,
        column_name.value_or(""));

    auto helper = SubqueryHelper::create(context, sql);

    StatsCollection stats_collection;
    Block block;
    while ((block = helper.getNextBlock()))
    {
        if (block.rows() == 0)
        {
            continue;
        }

        assert(block.columns() == 3);
        auto cols = block.getColumns();
        for (size_t row_id = 0; row_id < block.rows(); ++row_id)
        {
            auto tag = static_cast<StatisticsTag>(cols[1]->get64(row_id));
            auto value_blob_b64 = cols[2]->getDataAt(row_id);
            auto value_ptr = createStatisticsBase(tag, timestamp, base64Decode(static_cast<std::string_view>(value_blob_b64)));
            // no need to check duplicate
            // if conflicts, just consider both are ok
            stats_collection[tag] = std::move(value_ptr);
        }
    }
    return stats_collection;
}

StatsData StatsDataSource::get(const StatsTableIdentifier & table_identifier) const
{
    if (!isInitialized())
    {
        initialize();
    }

    auto timestamp = getUpdateTime();
    auto sql_key = toString(table_identifier.getUUID());
    String sql = fmt::format(
        FMT_STRING("select column_name, tag, value "
                   "from {} "
                   "where table_uuid='{}' "),
        DB_TABLE,
        sql_key);

    auto helper = SubqueryHelper::create(context, sql);

    StatsData stats_data;
    Block block;
    while ((block = helper.getNextBlock()))
    {
        if (block.rows() == 0)
        {
            continue;
        }

        assert(block.columns() == 3);
        auto cols = block.getColumns();
        for (size_t row_id = 0; row_id < block.rows(); ++row_id)
        {
            auto column_name = cols[0]->getDataAt(row_id).toString();
            auto tag = static_cast<StatisticsTag>(cols[1]->get64(row_id));
            auto value_blob_b64 = cols[2]->getDataAt(row_id);
            auto & stats_collection = [&]() -> StatsCollection & {
                if (column_name.empty())
                {
                    // when column_name is empty
                    // this is table stats
                    return stats_data.table_stats;
                }
                else
                {
                    return stats_data.column_stats[column_name];
                }
            }();
            auto value_ptr = createStatisticsBase(tag, timestamp, base64Decode(static_cast<std::string_view>(value_blob_b64)));
            // no need to check duplicate
            // if conflicts, just consider both are ok
            stats_collection[tag] = std::move(value_ptr);
        }
    }
    return stats_data;
}


void StatsDataSource::set(const StatsTableIdentifier & table_identifier, const StatsData & stats_data)
{
    if (!isInitialized())
    {
        initialize();
    }

    if (stats_data.table_stats.empty() && stats_data.column_stats.empty())
    {
        return;
    }

    auto sql_key = toString(table_identifier.getUUID());
    auto insert_sql = fmt::format(
        FMT_STRING("insert into table {} (table_uuid, column_name, tag, value, _delete_flag_)"
                   "values "),
        DB_TABLE);
    auto sql_line = [&](std::string_view column_name, StatisticsTag tag, const StatisticsBasePtr & ptr) {
        auto blob = ptr->serialize();
        auto b64_blob = base64Encode(blob);
        return fmt::format(FMT_STRING("('{}','{}',{},'{}',0)"), sql_key, column_name, static_cast<UInt64>(tag), b64_blob);
    };

    for (const auto & [tag, ptr] : stats_data.table_stats)
    {
        insert_sql += sql_line("", tag, ptr);
    }
    for (const auto & [col_name, column_data] : stats_data.column_stats)
    {
        for (const auto & [tag, ptr] : column_data)
        {
            insert_sql += sql_line(col_name, tag, ptr);
        }
    }

    executeSubQuery(context, insert_sql);
}

void StatsDataSource::drop(const StatsTableIdentifier & table_identifier)
{
    if (!isInitialized())
    {
        initialize();
    }

    auto sql_key = toString(table_identifier.getUUID());
    auto logger = &Poco::Logger::get("StatsDataSource::drop");
    if (!has(table_identifier))
    {
        LOG_INFO(logger, fmt::format(FMT_STRING("table {} ({}) has no stats, skip it"), table_identifier.getDbTableName(), sql_key));
        return;
    }

    LOG_INFO(logger, fmt::format(FMT_STRING("dropping stats on table {} ({})"), table_identifier.getDbTableName(), sql_key));

    // TODO load table and delete them
    std::string delete_sql = fmt::format(
        FMT_STRING("insert into {0}(table_uuid, column_name, tag, value, _delete_flag_) "
                   "select table_uuid, column_name, tag, '', 1 "
                   "from {0} where table_uuid='{1}';"),
        DB_TABLE,
        sql_key);
    executeSubQuery(context, delete_sql);
}
void StatsDataSource::invalidateClusterCache(const StatsTableIdentifier & table_identifier) const
{
    auto cluster_name = fetchClusterName(context);
    auto sql = fmt::format(
        FMT_STRING("select host(), invalidateStatsCache('{}', '{}') from clusterAllReplicas('{}', system.one)"),
        table_identifier.getDatabaseName(),
        table_identifier.getTableName(),
        cluster_name);
    executeSubQuery(context, sql);
}

bool StatsDataSource::isInitialized() const
{
    auto db_storage = DatabaseCatalog::instance().getDatabase(DATABASE);
    return db_storage->isTableExist(TABLE, context);
}

UInt64 StatsDataSource::getUpdateTime() const
{
    // TODO: use LRU Cache to elimate
    return 0;
}

void StatsDataSource::resetAllStats()
{
    std::string drop_sql = fmt::format(FMT_STRING("drop table if exists {0};"), DB_TABLE);
    executeSubQuery(context, drop_sql);
    auto drop_local_sql = fmt::format(FMT_STRING("drop table if exists {0};"), DB_LOCAL_TABLE);
    executeSubQuery(context, drop_local_sql);
    initialize();
}

}

#include <Optimizer/Dump/StatsLoader.h>

#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>
#include <Optimizer/Dump/ProtoEnumUtils.h>
#include <Optimizer/Dump/ReproduceUtils.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatisticsCollector.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <common/logger_useful.h>
#include "Statistics/CollectorSettings.h"

using namespace DB::Statistics;

namespace DB
{

std::unordered_set<QualifiedTableName> StatsLoader::loadStats(bool load_all, const std::unordered_set<QualifiedTableName> & tables_to_load)
{
    Poco::JSON::Object::Ptr tables = stats_json ? stats_json : ReproduceUtils::readJsonFromAbsolutePath(json_file_path);

    if (!tables)
    {
        LOG_WARNING(log, "there is no table to load in file {}", json_file_path);
        return {};
    }

    std::unordered_set<QualifiedTableName> tables_loaded;

    for (const auto & [database_table, stats] : *tables)
    {
        auto optional_name = QualifiedTableName::tryParseFromString(database_table);
        if (!optional_name)
        {
            LOG_WARNING(log, "invalid database table {}", database_table);
            continue;
        }
        if (!load_all && !tables_to_load.contains(optional_name.value()))
        {
            LOG_INFO(log, "skipping table {}", database_table);
            continue;
        }
        const std::string & database_name = optional_name.value().database;
        const std::string & table_name = optional_name.value().table;
        auto stats_collector = readStatsFromJson(database_name, table_name, stats.extract<Poco::JSON::Object::Ptr>());
        if (!stats_collector)
        {
            LOG_WARNING(log, "failed to load stats for {}", database_table);
            continue;
        }
        stats_collector->writeToCatalog();
        LOG_DEBUG(log, "loaded stats for table {}, rows={}", database_table, stats_collector->getTableStats().basic->getRowCount());
        tables_loaded.emplace(optional_name.value());
    }
    return tables_loaded;
}

std::shared_ptr<StatisticsCollector> StatsLoader::readStatsFromJson(
    const std::string & database_name, const std::string & table_name, Poco::JSON::Object::Ptr json_ptr)
{
    if (!json_ptr || json_ptr->size() == 0)
    {
        LOG_WARNING(log, "stats for table {}.{} is empty", database_name, table_name);
        return nullptr;
    }

    auto table_id_opt = stats_catalog->getTableIdByName(database_name, table_name);
    if (!table_id_opt)
    {
        LOG_WARNING(log, "table {}.{} does not exist", database_name, table_name);
        return nullptr;
    }

    std::shared_ptr<StatisticsCollector> collector
        = std::make_shared<StatisticsCollector>(getContext(), stats_catalog, table_id_opt.value(), CollectorSettings{});

    {
        StatsCollection table_basic_collection;
        StatisticsTag tag = StatisticsTag::TableBasic;
        auto obj = createStatisticsBaseFromJson(tag, json_ptr->getValue<String>("TableBasic"));
        table_basic_collection.emplace(tag, std::move(obj));
        StatisticsCollector::TableStats table_stats;
        table_stats.readFromCollection(table_basic_collection);
        collector->setTableStats(std::move(table_stats));
    }

    auto column_jsons = json_ptr->getObject("Columns");
    for (auto & column : *column_jsons)
    {
        auto column_json = column.second.extract<Poco::JSON::Object::Ptr>();
        StatsCollection column_collection;
        for (auto & entry : *column_json)
        {
            auto column_tag = ProtoEnumUtils::statisticsTagFromString(entry.first);
            auto column_obj = createStatisticsBaseFromJson(column_tag, entry.second.toString());
            column_collection.emplace(column_tag, std::move(column_obj));
        }
        StatisticsCollector::ColumnStats column_stats;
        column_stats.readFromCollection(column_collection);
        collector->setColumnStats(column.first, std::move(column_stats));
    }
    return collector;
}

}

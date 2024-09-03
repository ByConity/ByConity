#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Protos/auto_statistics.pb.h>
#include <Statistics/AutoStatisticsCommand.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsTaskQueue.h>
#include <Statistics/AutoStatsTaskLogHelper.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CollectTarget.h>
#include <Statistics/SettingsMap.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/VersionHelper.h>
#include <boost/lexical_cast.hpp>
#include <fmt/format.h>
#include <Common/SettingsChanges.h>
#include <Statistics/VersionHelper.h>


namespace DB::ErrorCodes
{
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_TABLE;
}

namespace DB::Statistics::AutoStats
{


void AutoStatisticsCommand::create(const StatisticsScope & scope, SettingsChanges settings_changes)
{
    // TODO
    (void) scope;
    (void) settings_changes;
    throw Exception("not implemented, plz change xml config", ErrorCodes::NOT_IMPLEMENTED);
    // auto identifiers = getTablesInScope(context, scope);
    // manager->settings_manager.enableAutoStatsTasks(scope, std::move(settings_changes));
    // manager->markCollectableCandidates(identifiers, true);
}

Block AutoStatisticsCommand::show(const StatisticsScope & target_scope)
{
    NamesAndTypes names_and_types = {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"enable_auto_stats", std::make_shared<DataTypeUInt8>()},
        {"settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"status", std::make_shared<DataTypeNullable>(enumDataTypeForStatus())},
        {"rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"version", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale))},
    };
    std::vector<MutableColumnPtr> columns;
    auto str_type = std::make_shared<DataTypeString>();

    for (const auto & pr : names_and_types)
    {
        columns.emplace_back(pr.type->createColumn());
    }

    auto full_settings = manager->settings_manager.getFullStatisticsSettings();

    auto scopes = getChildrenScope(context, target_scope);

    // TODO: default value for all tables should be configured somewhere
    auto default_enable_auto_stats = false;
    auto & task_queue = manager->task_queue;

    auto curr_catalog = createCatalogAdaptor(context);
    for (const auto & scope : scopes)
    {
        // TODO add settings
        auto task_settings = full_settings.getByScope(scope);

        auto i = 0;
        columns[i++]->insert(scope.database.value_or("*"));
        columns[i++]->insert(scope.table.value_or("*"));

        if (task_settings)
        {
            Settings settings;
            applyStatisticsSettingsChanges(settings, task_settings->settings_changes);
            columns[i++]->insert(task_settings->enable_auto_stats);
            auto * map_column = columns[i++].get();
            settings.dumpToMapColumn(map_column, true);
        }
        else
        {
            columns[i++]->insert(default_enable_auto_stats);
            columns[i++]->insertDefault();
        }

        if (!scope.table)
        {
            columns[i++]->insertDefault();
            columns[i++]->insertDefault();
            columns[i++]->insertDefault();
        }
        else if (auto table_opt = curr_catalog->getTableIdByName(scope.database.value(), scope.table.value()))
        {
            auto table = table_opt.value();
            auto table_stats = getTableStatistics(context, table);
            auto task = task_queue.tryGetTaskInfo(table.getUniqueKey());
            if (task)
            {
                auto status = task->getStatus();
                columns[i++]->insert(status);
            }
            else
            {
                // since failed task will be deleted from task_queue
                // we cannot distinguish a failed recent task and a successful ancient one
                // TODO: check log for this purpose
                auto status = table_stats ? Status::Success : Status::NotExists;
                columns[i++]->insert(status);
            }

            if (table_stats)
            {
                columns[i++]->insert(table_stats->getRowCount());
                columns[i++]->insert(table_stats->getTimestamp());
            }
            else
            {
                columns[i++]->insertDefault();
                columns[i++]->insertDefault();
            }
        }
        else
        {
            columns[i++]->insert(Status::Error);
            columns[i++]->insertDefault();
            columns[i++]->insertDefault();
        }
    }

    Block block;
    for (size_t i = 0; i < names_and_types.size(); ++i)
    {
        ColumnWithTypeAndName tuple;
        tuple.name = names_and_types[i].name;
        tuple.type = names_and_types[i].type;
        tuple.column = std::move(columns[i]);
        block.insert(std::move(tuple));
    }
    return block;
}

void AutoStatisticsCommand::drop(const StatisticsScope & scope)
{
    (void) scope;
    throw Exception("not implemented, plz change xml config", ErrorCodes::NOT_IMPLEMENTED);
    // manager->settings_manager.disableAutoStatsTasks(scope);
}

void AutoStatisticsCommand::alter(const SettingsChanges & changes)
{
    (void) changes;
    throw Exception("not implemented, plz change xml config", ErrorCodes::NOT_IMPLEMENTED);
    // manager->settings_manager.alterManagerSettings(changes);
}
}

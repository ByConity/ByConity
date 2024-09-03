#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Storages/System/StorageSystemAutoStatsScopeSettings.h>
#include <Statistics/AutoStatisticsManager.h>
#include "DataTypes/DataTypeMap.h"
#include "Statistics/SettingsMap.h"
#include <Statistics/AutoStatisticsHelper.h>

namespace DB
{
using namespace Statistics;
using namespace Statistics::AutoStats;
NamesAndTypesList StorageSystemAutoStatsScopeSettings::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"enable_auto_stats", std::make_shared<DataTypeUInt8>()},
        {"settings", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
    };
}

void StorageSystemAutoStatsScopeSettings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & select_query_info) const
{
    PredicatesHint hint = extractPredicates(*select_query_info.query, context);
    StatisticsScope target_scope;
    if (hint.database_opt)
    {
        target_scope.database = hint.database_opt.value();
        if (hint.table_opt)
        {
            target_scope.table = hint.table_opt.value();
        }
    }

    auto * manager = context->getAutoStatisticsManager();
    if (!manager)
        throw Exception("auto stats manager is not initialized", ErrorCodes::LOGICAL_ERROR);
    auto full_settings = manager->getSettingsManager().getFullStatisticsSettings();
    auto & columns = res_columns;

    auto scopes = getChildrenScope(context, target_scope);

    // TODO: default value for all tables should be configured somewhere
    auto default_enable_auto_stats = false;
    // auto & task_queue = context->getAutoStatisticsManager()->task_queue;

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
    }
}
}

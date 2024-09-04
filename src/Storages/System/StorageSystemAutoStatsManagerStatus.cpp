#include <memory>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Storages/System/StorageSystemAutoStatsManagerStatus.h>
#include "DataTypes/DataTypeMap.h"
#include "Statistics/SettingsMap.h"

namespace DB
{
using namespace Statistics;
using namespace Statistics::AutoStats;
NamesAndTypesList StorageSystemAutoStatsManagerStatus::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"task_uuid", std::make_shared<DataTypeUUID>()},
        {"task_type", std::make_shared<DataTypeNullable>(enumDataTypeForTaskType())},
        {"status", std::make_shared<DataTypeNullable>(enumDataTypeForStatus())},
        {"priority", std::make_shared<DataTypeFloat64>()},
        {"retry_times", std::make_shared<DataTypeUInt32>()},
    };
}

void StorageSystemAutoStatsManagerStatus::fillData(
    MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & select_query_info) const
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

    auto all_tasks = manager->getAllTasks();
    for (const auto & task : all_tasks)
    {
        auto table = task.table;
        if (target_scope.database && target_scope.database.value() != table.getDatabaseName())
            continue;
        if (target_scope.table && target_scope.table.value() != table.getTableName())
            continue;

        auto i = 0;
        res_columns[i++]->insert(table.getDatabaseName());
        res_columns[i++]->insert(table.getTableName());
        res_columns[i++]->insert(table.getUUID());
        res_columns[i++]->insert(task.task_uuid);
        res_columns[i++]->insert(task.task_type);
        res_columns[i++]->insert(task.status);
        res_columns[i++]->insert(task.priority);
        res_columns[i++]->insert(task.retry_times);
    }
}
}

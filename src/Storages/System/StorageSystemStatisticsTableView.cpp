#include <memory>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Statistics/VersionHelper.h>
#include <Storages/System/StorageSystemStatisticsTableView.h>
#include "DataTypes/DataTypeDateTime64.h"
#include "DataTypes/DataTypeMap.h"
#include "Statistics/SettingsMap.h"

namespace DB
{
using namespace Statistics;
using namespace Statistics::AutoStats;
NamesAndTypesList StorageSystemStatisticsTableView::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"row_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale))},
    };
}

void StorageSystemStatisticsTableView::fillData(
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

    auto tables = getTablesFromScope(context, target_scope);
    auto catalog = createCatalogAdaptor(context);

    // data.append("database", table.getDatabaseName());
    // data.append("table", table.getTableName());
    // data.append("engine", storage->getName());
    // data.append("unique_key", UUIDHelpers::UUIDToString(table.getUniqueKey()));
    // data.append("row_count", obj ? std::to_string(obj->getRowCount()) : "");
    // data.append("timestamp", obj ? AutoStats::serializeToText(obj->getTimestamp()) : "");

    for (const auto & table : tables)
    {
        auto obj = getTableStatistics(context, table);
        auto storage = catalog->getStorageByTableId(table);

        auto i = 0;
        res_columns[i++]->insert(table.getDatabaseName());
        res_columns[i++]->insert(table.getTableName());
        res_columns[i++]->insert(storage->getName());
        res_columns[i++]->insert(table.getUUID());
        if (obj)
        {
            res_columns[i++]->insert(obj->getRowCount());
            res_columns[i++]->insert(obj->getTimestamp());
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }
    }
}
}

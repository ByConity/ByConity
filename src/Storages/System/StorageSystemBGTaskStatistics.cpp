#include <Storages/System/StorageSystemBGTaskStatistics.h>

#include <Access/ContextAccess.h>
#include <Storages/MergeTree/MergeTreeBgTaskStatistics.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>

namespace DB
{
NamesAndTypesList StorageSystemBGTaskStatistics::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"inserted_parts", std::make_shared<DataTypeUInt64>()},
        {"merged_parts", std::make_shared<DataTypeUInt64>()},
        {"removed_parts", std::make_shared<DataTypeUInt64>()},
        {"last_insert_time", std::make_shared<DataTypeDateTime>()},
        {"last_hour_inserted_bytes", std::make_shared<DataTypeUInt64>()},
        {"last_hour_merged_bytes", std::make_shared<DataTypeUInt64>()},
        {"last_six_hours_inserted_bytes", std::make_shared<DataTypeUInt64>()},
        {"last_six_hours_merged_bytes", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemBGTaskStatistics::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);
    const auto tenant_id = getCurrentTenantId();

    auto curr_time = time(nullptr);
    auto all_table_stats = MergeTreeBgTaskStatisticsInitializer::instance().getAllTableStats();
    for (auto & table_stats: all_table_stats)
    {
        if (!table_stats)
            continue;

        auto storage_id = table_stats->getStorageID();
        const String & db = storage_id.getDatabaseName();
        const String & table = storage_id.getTableName();
        const UUID uuid = storage_id.uuid;

        if (!tenant_id.empty() && !isTenantMatchedEntityName(db, tenant_id))
            continue;
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db) && !access->isGranted(AccessType::SHOW_TABLES, db, table))
            continue;

        /// Call of lastIntervalStats will rollup stats, so we must use executeWithWriteLock
        table_stats->executeWithWriteLock(
            [&res_columns, &db, &table, &uuid, &curr_time](MergeTreeBgTaskStatistics::PartitionStatsMap & partition_stats_map)
            {
                for (auto & [partition_id, p_stats]: partition_stats_map)
                {
                    size_t col_num = 0;
                    res_columns[col_num++]->insert(db);
                    res_columns[col_num++]->insert(table);
                    res_columns[col_num++]->insert(uuid);
                    res_columns[col_num++]->insert(partition_id);
                    res_columns[col_num++]->insert(p_stats.inserted_parts);
                    res_columns[col_num++]->insert(p_stats.merged_parts);
                    res_columns[col_num++]->insert(p_stats.removed_parts);
                    res_columns[col_num++]->insert(p_stats.last_insert_time);

                    auto last_hour_stats = p_stats.last_hour_stats.lastIntervalStats(curr_time);
                    auto last_6hour_stats = p_stats.last_6hour_stats.lastIntervalStats(curr_time);
                    res_columns[col_num++]->insert(last_hour_stats ? last_hour_stats->first : 0);
                    res_columns[col_num++]->insert(last_hour_stats ? last_hour_stats->second : 0);
                    res_columns[col_num++]->insert(last_6hour_stats ? last_6hour_stats->first : 0);
                    res_columns[col_num++]->insert(last_6hour_stats ? last_6hour_stats->second : 0);
                }
            });
    }
}

}

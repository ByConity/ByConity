#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Storages/System/StorageSystemAutoStatsManagerSettings.h>
#include <Statistics/AutoStatisticsManager.h>

namespace DB
{
NamesAndTypesList StorageSystemAutoStatsManagerSettings::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
        {"changed", std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemAutoStatsManagerSettings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto * manager = context->getAutoStatisticsManager();
    if (!manager)
        throw Exception("auto stats manager is not initialized", ErrorCodes::LOGICAL_ERROR);
    auto settings = manager->getSettingsManager().getManagerSettings();

    auto insert_key = [&]<typename SettingFieldType, typename T>(
                          const String & key, const T & default_value, const String & explain, const String & type) {
        auto i = 0;
        res_columns[i++]->insert(key);
        if (settings.count(key))
        {
            res_columns[i++]->insert(settings[key]);
            res_columns[i++]->insert(true);
        }
        else
        {
            SettingFieldType tmp(default_value);
            res_columns[i++]->insert(tmp.toString());
            res_columns[i++]->insert(false);
        }
        res_columns[i++]->insert(explain);
        res_columns[i++]->insert(type);
    };

#define INSERT_KEY(TYPE, NAME, DEFAULT_VALUE, EXPLAIN, INTERNAL) \
    if constexpr (!(INTERNAL)) \
        insert_key.operator()<SettingField##TYPE>(#NAME, DEFAULT_VALUE, EXPLAIN, #TYPE);

    AUTO_STATS_MANAGER_SETTINGS(INSERT_KEY)
}

}

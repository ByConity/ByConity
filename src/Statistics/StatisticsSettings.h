#pragma once
#include <unordered_map>
#include <Statistics/SettingsMap.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Poco/Logger.h>
#include <Common/SettingsChanges.h>
#include <Statistics/ASTHelpers.h>

namespace DB::Protos
{
class StatisticsSettings;
}

namespace DB::Statistics
{
// this settings is guaranteed to be the same for the whole cluster
// manager_settings can be modified by 'alter auto stats'
// scope_settings_list can be modified by 'create/drop auto stats'
// in ce, it will be stored in zookeeper
// in cnch, we may store it in catalog
struct StatisticsSettings
{
    struct TableSettings
    {
        bool enable_auto_stats = false;
        SettingsChanges settings_changes;
    };
    // table_name -> SettingsChanges
    using TableEntries = std::unordered_map<std::optional<String>, TableSettings>;

    // database_name -> table_name -> TableSettings
    std::unordered_map<std::optional<String>, TableEntries> scope_settings_list;
    AutoStatsManagerSettings manager_settings;


    std::optional<TableSettings> getByScope(const StatisticsScope & scope) const
    {
        std::optional<TableSettings> result = getByScopeImpl(scope);
        if (!result && scope.table)
            result = getByScopeImpl(StatisticsScope{scope.database, std::nullopt});
        if (!result && scope.database)
            result = getByScopeImpl(StatisticsScope{});
        return result;
    }

    void toProto(Protos::StatisticsSettings & proto) const;
    void fillFromProto(const Protos::StatisticsSettings & proto);

private:
    std::optional<TableSettings> getByScopeImpl(const StatisticsScope & scope) const
    {
        if (auto iter = scope_settings_list.find(scope.database); iter != scope_settings_list.end())
        {
            const auto & table_entries = iter->second;
            if (auto iter2 = table_entries.find(scope.table); iter2 != table_entries.end())
            {
                return iter2->second;
            }
        }
        return std::nullopt;
    }
};
}

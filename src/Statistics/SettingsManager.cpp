#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/SettingsManager.h>
#include <Statistics/SettingsMap.h>
#include <Statistics/StatisticsSettings.h>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include "common/logger_useful.h"
#include <Common/SettingsChanges.h>

namespace DB::Statistics
{

// void SettingsManager::enableAutoStatsTasks(const StatisticsScope & scope, SettingsChanges settings_changes)
// {
//     initialize();
//     // std::unique_lock lck(mutex);
//     if (!scope.database.has_value())
//     {
//         data.scope_settings_list.clear();
//     }
//     else if (!scope.table.has_value())
//     {
//         auto db = scope.database.value();
//         data.scope_settings_list[db].clear();
//     }
//     data.scope_settings_list[scope.database][scope.table] = TableSettings{true, std::move(settings_changes)};
//     data.manager_settings.set_enable_auto_stats(true);
//     // flushSettings();
// }

// void SettingsManager::disableAutoStatsTasks(const StatisticsScope & scope)
// {
//     initialize();
//     std::unique_lock lck(mutex);
//     if (!scope.database.has_value())
//     {
//         data.scope_settings_list.clear();
//         // when create stats ALL_DATABASES, disable enable_auto_stats flag
//         data.manager_settings.set_enable_auto_stats(false);
//     }
//     else if (!scope.table.has_value())
//     {
//         auto db = scope.database.value();
//         data.scope_settings_list[db].clear();
//     }
//     data.scope_settings_list[scope.database][scope.table] = TableSettings{.enable_auto_stats = false};
//     flushSettings();
// }

// void SettingsManager::alterManagerSettings(const SettingsChanges & settings_changes)
// {
//     initialize();
//     std::unique_lock lck(mutex);
//     auto copy = data.manager_settings;
//     // here may throw exception, won't affect correctness of manager_settings
//     copy.applySettingsChanges(settings_changes);

//     std::swap(this->data.manager_settings, copy);
//     flushSettings();
// }

AutoStatsManagerSettings SettingsManager::getManagerSettings()
{
    std::shared_lock lck(mutex);
    return data.manager_settings;
}

auto SettingsManager::getTableSettings(const StatsTableIdentifier & identifier) -> TableSettings
{
    std::shared_lock lck(mutex);
    auto settings_opt = data.getByScope(StatisticsScope{identifier.getDatabaseName(), identifier.getTableName()});

    return settings_opt.value_or(TableSettings{.enable_auto_stats = false});
}

std::unordered_map<String, String> parseAllKeyValue(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    std::unordered_map<String, String> result;
    std::vector<String> keys;
    config.keys(prefix, keys);

    for (const auto & key : keys)
    {
        String full_key = prefix + "." + key;
        result[key] = config.getString(full_key);
    }

    return result;
}

SettingsChanges mapToChanges(const std::unordered_map<String, String> & kv_map, LoggerPtr logger)
{
    SettingsChanges changes;
    for (const auto & [k, v] : kv_map)
    {
        auto dup = changes.insertSetting(k, v);
        if (dup)
            LOG_WARNING(logger, "duplicated settings {}={}", k, v);
    }
    return changes;
}

bool stringToBool(const std::string& str) {
    std::string trimmed_str = boost::algorithm::trim_copy(str);
    boost::algorithm::to_lower(trimmed_str);
    if (trimmed_str == "true" || trimmed_str == "1") {
        return true;
    } else if (trimmed_str == "false" || trimmed_str == "0") {
        return false;
    } else {
        throw std::invalid_argument("Invalid boolean value: " + str);
    }
}


void SettingsManager::loadSettingsFromXml(const Poco::Util::AbstractConfiguration & config)
{
    auto logger = getLogger("Statistics::AutoStats::SettingsManager");
    try
    {
        bool any_is_enabled = false;

        StatisticsSettings new_data;

        String overall_prefix = "auto_statistics";
        String scope_prefix = overall_prefix + ".scopes";
        Poco::Util::AbstractConfiguration::Keys scope_keys;
        config.keys(scope_prefix, scope_keys);
        auto move_out_opt = [](std::unordered_map<String, String> & kv_map, const String & key) -> std::optional<String> {
            if (kv_map.count(key))
            {
                auto result = kv_map.at(key);
                kv_map.erase(key);
                if (result.empty())
                {
                    return std::nullopt;
                }
                return result;
            }
            return std::nullopt;
        };
        for (const auto & scope_key : scope_keys)
        {
            if (scope_key.starts_with("scope"))
            {
                auto item_prefix = scope_prefix + "." + scope_key;
                auto kv_map = parseAllKeyValue(config, item_prefix);
                auto database_opt = move_out_opt(kv_map, "database");
                auto table_opt = move_out_opt(kv_map, "table");
                auto is_enable = stringToBool(move_out_opt(kv_map, "enable").value_or("1"));


                auto & entry = new_data.scope_settings_list[database_opt][table_opt];
                entry.enable_auto_stats = is_enable;
                entry.settings_changes = mapToChanges(kv_map, logger);
                any_is_enabled = any_is_enabled || is_enable;
            }
        }

        auto manager_kv_map = parseAllKeyValue(config, overall_prefix + ".manager_settings");
        auto manager_kv_changes = mapToChanges(manager_kv_map, logger);
        new_data.manager_settings.applySettingsChanges(manager_kv_changes);

        new_data.manager_settings.set_enable_auto_stats(any_is_enabled);

        std::unique_lock lck(mutex);
        this->data = std::move(new_data);
    }
    catch (...)
    {
        auto err_msg = getCurrentExceptionMessage(true);
        LOG_ERROR(logger, "failed to update, due to {}", err_msg);
    }
}

}

#include <memory>
#include <optional>
#include <shared_mutex>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/CollectorSettings.h>
#include <Statistics/SettingsManager.h>
#include <Statistics/SettingsMap.h>
#include <Statistics/StatisticsSettings.h>
#include <boost/lexical_cast.hpp>
#include <Common/SettingsChanges.h>

namespace DB::Statistics
{

void SettingsManager::enableAutoStatsTasks(const StatisticsScope & scope, SettingsChanges settings_changes)
{
    initialize();
    std::unique_lock lck(mutex);
    if (!scope.database.has_value())
    {
        data.scope_settings_list.clear();
    }
    else if (!scope.table.has_value())
    {
        auto db = scope.database.value();
        data.scope_settings_list[db].clear();
    }
    data.scope_settings_list[scope.database][scope.table] = TableSettings{true, std::move(settings_changes)};
    data.manager_settings.set_enable_auto_stats(true);
    flushSettings();
}

void SettingsManager::disableAutoStatsTasks(const StatisticsScope & scope)
{
    initialize();
    std::unique_lock lck(mutex);
    if (!scope.database.has_value())
    {
        data.scope_settings_list.clear();
        // when create stats ALL_DATABASES, disable enable_auto_stats flag
        data.manager_settings.set_enable_auto_stats(false);
    }
    else if (!scope.table.has_value())
    {
        auto db = scope.database.value();
        data.scope_settings_list[db].clear();
    }
    data.scope_settings_list[scope.database][scope.table] = TableSettings{.enable_auto_stats = false};
    flushSettings();
}

void SettingsManager::alterManagerSettings(const SettingsChanges & settings_changes)
{
    initialize();
    std::unique_lock lck(mutex);
    auto copy = data.manager_settings;
    // here may throw exception, won't affect correctness of manager_settings
    copy.applySettingsChanges(settings_changes);

    std::swap(this->data.manager_settings, copy);
    flushSettings();
}

AutoStatsManagerSettings SettingsManager::getManagerSettings()
{
    initialize();
    std::shared_lock lck(mutex);
    return data.manager_settings;
}

auto SettingsManager::getTableSettings(const StatsTableIdentifier & identifier) -> TableSettings
{
    initialize();
    std::shared_lock lck(mutex);
    auto settings_opt = data.getByScope(StatisticsScope{identifier.getDatabaseName(), identifier.getTableName()});

    return settings_opt.value_or(TableSettings{.enable_auto_stats = false});
}

// this can be failed due to zookeeper disconnection
// so use initialize flag to check it in every API
void SettingsManager::initialize()
{
    // load from zk
    if (is_initialized)
        return;

    std::unique_lock lck(mutex);
    if (is_initialized)
        return;

    loadSettings();
    is_initialized = true;
}

void SettingsManager::flushSettings()
{
#if 0
    Protos::StatisticsSettings proto;
    data.toProto(proto);
    auto_stats_zk.saveSettings(proto);
#endif
}

void SettingsManager::loadSettings()
{
#if 0
    Protos::StatisticsSettings proto;
    auto_stats_zk.loadSettings(proto);
    data.fillFromProto(proto);
#endif
}
}

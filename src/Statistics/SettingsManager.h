#pragma once
#include <map>
#include <shared_mutex>
#include <Statistics/SettingsMap.h>
#include <Statistics/StatisticsBase.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatisticsSettings.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/TypeUtils.h>
#include <fmt/format.h>
#include <Common/SettingsChanges.h>

namespace DB::Statistics
{

// In Memory Settings and its zookeeper mirror
class SettingsManager
{
public:
    using TableSettings = StatisticsSettings::TableSettings;
    // TODO: change it to catalog
    explicit SettingsManager(ContextPtr context)
    {
        (void) context;
    }

    void enableAutoStatsTasks(const StatisticsScope & scope, SettingsChanges settings_changes);
    void disableAutoStatsTasks(const StatisticsScope & scope);

    void alterManagerSettings(const SettingsChanges & settings);

    TableSettings getTableSettings(const StatsTableIdentifier & identifier);

    AutoStatsManagerSettings getManagerSettings();

    // only for show auto_stats command, always copy all to avoid data race
    auto getFullStatisticsSettings()
    {
        initialize();
        std::shared_lock lck(mutex);
        return data;
    }

    void initialize();

private:
    // TODO: make it incremental
    void flushSettings();
    void loadSettings();

    std::atomic_bool is_initialized = false;
    // AutoStats::AutoStatisticsZk & auto_stats_zk;
    StatisticsSettings data;
    mutable std::shared_mutex mutex;
};
}

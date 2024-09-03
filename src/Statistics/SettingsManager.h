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
#include "Interpreters/Context_fwd.h"

namespace DB::Statistics
{

// In Memory Settings and its zookeeper mirror
class SettingsManager: WithContext
{
public:
    using TableSettings = StatisticsSettings::TableSettings;
    // TODO: change it to catalog
    explicit SettingsManager(ContextPtr context_): WithContext(context_)
    {
    }

    // void enableAutoStatsTasks(const StatisticsScope & scope, SettingsChanges settings_changes);
    // void disableAutoStatsTasks(const StatisticsScope & scope);
    // void alterManagerSettings(const SettingsChanges & settings);

    TableSettings getTableSettings(const StatsTableIdentifier & identifier);

    AutoStatsManagerSettings getManagerSettings();

    // only for show auto_stats command, always copy all to avoid data race
    auto getFullStatisticsSettings()
    {
        std::shared_lock lck(mutex);
        return data;
    }

    void loadSettingsFromXml(const Poco::Util::AbstractConfiguration & config);

private:

    StatisticsSettings data;
    mutable std::shared_mutex mutex;
};
}

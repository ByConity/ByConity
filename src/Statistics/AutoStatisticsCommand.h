#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/SettingsManager.h>

namespace DB::Statistics::AutoStats
{
class AutoStatisticsCommand
{
public:
    explicit AutoStatisticsCommand(ContextPtr context_) : context(context_)
    {
        manager = context_->getAutoStatisticsManager();
        if (!manager)
            throw Exception("auto stats manager not initialized", ErrorCodes::LOGICAL_ERROR);
        catalog = createCatalogAdaptor(context);
    }

    void create(const StatisticsScope & scope, SettingsChanges settings_changes);

    Block show(const StatisticsScope & scope);
    void drop(const StatisticsScope & scope);
    void alter(const SettingsChanges & changes);

private:
    AutoStats::AutoStatisticsManager * manager;
    ContextPtr context;
    CatalogAdaptorPtr catalog;
};

}

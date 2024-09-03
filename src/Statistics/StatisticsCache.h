#include <chrono>
#include <shared_mutex>
#include <Statistics/StatsTableIdentifier.h>
#include "Statistics/StatisticsBase.h"
#include "Statistics/StatisticsCollectorObjects.h"
#include "Statistics/StatsTableIdentifier.h"

namespace DB::Statistics
{
namespace chrono = std::chrono;

class StatisticsCache
{
public:
    explicit StatisticsCache(const chrono::nanoseconds & expire_time_) : expire_time(expire_time_) { }

    struct CacheEntry
    {
        // use shared ptr, nullptr
        std::shared_ptr<StatsData> data;
        chrono::time_point<chrono::steady_clock, chrono::nanoseconds> expire_time_point;
    };

    void invalidate(const UUID & table);
    std::shared_ptr<StatsData> get(const UUID & table);
    void update(const UUID & table, std::shared_ptr<StatsData> data);
    void clear();

private:
    chrono::nanoseconds expire_time;
    std::unordered_map<UUID, CacheEntry> impl;
    std::shared_mutex mutex;
};
}

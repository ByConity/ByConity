#include <chrono>
#include <shared_mutex>
#include <Statistics/StatsTableIdentifier.h>
#include "Statistics/StatisticsBase.h"
#include "Statistics/StatsTableIdentifier.h"
#include "Transaction/TxnTimestamp.h"

namespace DB::Statistics
{
namespace chrono = std::chrono;

class StatisticsCache
{
public:
    explicit StatisticsCache(const chrono::nanoseconds & expire_time_) : expire_time(expire_time_)
    {
    }

    struct CacheEntry
    {
        // use shared ptr, nullptr
        std::shared_ptr<StatsCollection> data;
        chrono::time_point<chrono::steady_clock, chrono::nanoseconds> expire_time_point;
    };

    void invalidate(const UUID & table);

    // column=nullopt for table stats
    // return nullptr as no point exception
    std::shared_ptr<StatsCollection> get(const UUID & table, const std::optional<String> & column);

    // column=nullopt for table stats
    void update(const UUID & table, const std::optional<String> column, std::shared_ptr<StatsCollection> data);

    void clear();

private:
    chrono::nanoseconds expire_time;
    std::unordered_map<UUID, std::unordered_map<String, CacheEntry>> impl;
    std::shared_mutex mutex;
};
}

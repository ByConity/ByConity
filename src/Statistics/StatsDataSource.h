#pragma once
#include <Statistics/StatisticsBaseImpl.h>
#include <Statistics/StatsTableIdentifier.h>
#include <fmt/format.h>
#include "Statistics/StatisticsBase.h"
namespace DB::Statistics
{
// a short lived adaptor for table meta info
class StatsDataSource
{
public:
    StatsDataSource(ContextPtr context_) : context(context_) { }

    StatsData get(const StatsTableIdentifier & table_identifier) const;
    StatsCollection getSingle(const StatsTableIdentifier & table_identifier, const std::optional<String> & column_name) const;
    bool has(const StatsTableIdentifier & table_identifier) const;
    void set(const StatsTableIdentifier & table_identifier, const StatsData & tableEntry);
    void drop(const StatsTableIdentifier & table_identifier);
    void initialize() const;
    bool isInitialized() const;
    UInt64 getUpdateTime() const;
    void invalidateClusterCache(const StatsTableIdentifier & table_identifier) const;
    void resetAllStats();

private:
    ContextPtr context;
};
}

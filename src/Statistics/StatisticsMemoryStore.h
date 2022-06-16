#pragma once
#include <mutex>
#include <shared_mutex>

#include <Statistics/StatisticsBase.h>
#include <Statistics/StatsTableIdentifier.h>
#include <boost/noncopyable.hpp>

namespace DB::Statistics
{

struct TableEntry
{
    StatsTableIdentifier identifier;
    StatsData data;
};

using TableEntryPtr = std::shared_ptr<TableEntry>;

struct StatisticsMemoryStore : boost::noncopyable
{
    using UniqueKey = StatsTableIdentifier::UniqueKey;
    std::shared_mutex mtx;
    std::unordered_map<UniqueKey, std::shared_ptr<TableEntry>> entries;
};

}

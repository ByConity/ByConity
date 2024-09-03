#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/StatsTableBasic.h>

namespace DB::Statistics
{
std::optional<DateTime64> getVersion(ContextPtr context, const StatsTableIdentifier & table);
std::shared_ptr<StatsTableBasic> getTableStatistics(ContextPtr context, const StatsTableIdentifier & table);
}

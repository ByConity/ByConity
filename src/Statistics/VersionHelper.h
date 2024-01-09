#include <Statistics/StatsTableIdentifier.h>

namespace DB::Statistics
{
std::optional<DateTime64> getVersion(ContextPtr context, const StatsTableIdentifier & table);
}

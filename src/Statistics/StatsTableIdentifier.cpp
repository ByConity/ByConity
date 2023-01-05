#include <Statistics/StatsTableIdentifier.h>
#include <Storages/IStorage.h>

namespace DB::Statistics
{
auto StatsTableIdentifier::getUniqueKey() const -> UUID
{
    auto uuid = getUUID();

    if (uuid == UUID{})
    {
        // some table may return empty uuid, use hash for temporary fix
        auto hash_db = std::hash<String>()(getDatabaseName());
        auto hash_tb = std::hash<String>()(getTableName());
        hash_db = (hash_db & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        hash_tb = (hash_tb & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        uuid.toUnderType().items[0] = hash_db;
        uuid.toUnderType().items[1] = hash_tb;
    }
    return uuid;
}
}

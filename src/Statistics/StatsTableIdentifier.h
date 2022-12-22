#pragma once
#include <Core/Types.h>

#include <utility>
#include <Interpreters/StorageID.h>

// this can be very different for cnch/stable_v2
namespace DB::Statistics
{

class StatsTableIdentifier
{
public:
    using UniqueKey = UUID;
    explicit StatsTableIdentifier(StorageID storage_id_) : storage_id(storage_id_) { }
    StatsTableIdentifier(const StatsTableIdentifier &) = default;
    StatsTableIdentifier(StatsTableIdentifier &&) = default;
    StatsTableIdentifier & operator=(const StatsTableIdentifier &) = default;
    StatsTableIdentifier & operator=(StatsTableIdentifier &&) = default;
    const String & getDatabaseName() const { return storage_id.database_name; }
    const String & getTableName() const { return storage_id.table_name; }

    String getDbTableName() const { return storage_id.getFullTableName(); }
    UniqueKey getUniqueKey() const;
    StorageID getStorageID() const { return storage_id; }
    UUID getUUID() const { return storage_id.uuid; }

private:
    StorageID storage_id;
    // useful only for adaptor
};

}

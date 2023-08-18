#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "Core/Types.h"
#include "hivemetastore/hive_metastore_types.h"

namespace ApacheHive = Apache::Hadoop::Hive;

namespace DB
{

class IMetaClient
{
public:
    IMetaClient() = default;
    virtual ~IMetaClient() = default;

    virtual Strings getAllDatabases() = 0;
    virtual Strings getAllTables(const String & db_name) = 0;
    virtual std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) = 0;
    virtual std::vector<ApacheHive::Partition> getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) = 0;
    /// virtual Statistics getTableStatistics(const String & db_name, const String & table_name);
};

}

#endif

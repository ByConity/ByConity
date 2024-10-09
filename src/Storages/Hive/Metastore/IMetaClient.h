#pragma once

#include "Common/config.h"
#include "Storages/Hive/CnchHiveSettings.h"
#if USE_HIVE

#include "Core/Types.h"
#include "Storages/TableStatistics.h"
#include "Storages/Hive/HiveFile/IHiveFile_fwd.h"

#include "hivemetastore/hive_metastore_types.h"

namespace ApacheHive = Apache::Hadoop::Hive;

namespace DB
{


struct HiveTableStats
{
    int64_t row_count;
     ApacheHive::TableStatsResult table_stats;
};

class IMetaClient
{
public:
    IMetaClient() = default;
    virtual ~IMetaClient() = default;

    virtual void getConfigValue(std::string & value, const std::string & name, const std::string & defaultValue);
    virtual Strings getAllDatabases() = 0;
    virtual std::shared_ptr<ApacheHive::Database> getDatabase(const String & db_name);
    virtual Strings getAllTables(const String & db_name) = 0;
    virtual std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) = 0;
    virtual bool isTableExist(const String & db_name, const String & table_name) = 0;
    virtual std::vector<ApacheHive::Partition> getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) = 0;
    virtual std::optional<TableStatistics> getTableStats(const String & db_name, const String & table_name, const Strings& col_names, bool merge_all_partition) = 0 ;
    // virtual ApacheHive::TableStatsResult getPartitionedTableStats(const String & db_name, const String & table_name, const Strings& col_names, const std::vector<ApacheHive::Partition>& partitions) = 0;

    // each partition is identified by a `key1=val1/key2=val2`
    virtual ApacheHive::PartitionsStatsResult getPartitionStats(const String & db_name, const String & table_name, const Strings& col_names, const Strings& partition_keys, const std::vector<Strings>& partition_vals ) = 0;
};
using IMetaClientPtr = std::shared_ptr<IMetaClient>;

class LakeMetaClientFactory
{
public:
    LakeMetaClientFactory() = delete;
    static std::shared_ptr<IMetaClient> create(const String & name, const std::shared_ptr<CnchHiveSettings> & settings);
};



}


#endif

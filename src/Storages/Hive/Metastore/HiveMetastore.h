#pragma once

#include "Storages/Hive/Metastore/IMetaClient.h"
#if USE_HIVE

#include "Common/PoolBase.h"
#include <ThriftHiveMetastore.h>

namespace DB
{
struct CnchHiveSettings;

using ThriftHiveMetastoreClientBuilder = std::function<std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>()>;

class ThriftHiveMetastoreClientPool : public PoolBase<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>
{
public:
    using Object = Apache::Hadoop::Hive::ThriftHiveMetastoreClient;
    using ObjectPtr = std::shared_ptr<Object>;
    using Entry = PoolBase<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>::Entry;
    explicit ThriftHiveMetastoreClientPool(ThriftHiveMetastoreClientBuilder builder_);

protected:
    ObjectPtr allocObject() override
    {
        return builder();
    }

private:
    ThriftHiveMetastoreClientBuilder builder;
};

class HiveMetastoreClient : public IMetaClient
{
public:
    explicit HiveMetastoreClient(ThriftHiveMetastoreClientBuilder builder);

    void getConfigValue(std::string& value, const std::string& name, const std::string& defaultValue) override;

    Strings getAllDatabases() override;

    std::shared_ptr<ApacheHive::Database> getDatabase(const String & db_name) override;

    Strings getAllTables(const String & db_name) override;

    std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) override;

    bool isTableExist(const String & db_name, const String & table_name) override;

    std::vector<ApacheHive::Partition> getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) override;

    std::optional<TableStatistics> getTableStats(const String & db_name, const String & table_name, const Strings& col_names, bool merge_all_partition) override;
    // ApacheHive::TableStatsResult getPartitionedTableStats(const String & db_name, const String & table_name, const Strings& col_names, const std::vector<ApacheHive::Partition>& partitions ) override;

    ApacheHive::PartitionsStatsResult getPartitionStats(
        const String & db_name, const String & table_name, const Strings & col_names,const Strings& partition_keys, const std::vector<Strings> & partition_vals) override;


private:
    void tryCallHiveClient(std::function<void(ThriftHiveMetastoreClientPool::Entry &)> func);

    ThriftHiveMetastoreClientPool client_pool;
};


using HiveMetastoreClientPtr = std::shared_ptr<HiveMetastoreClient>;
class HiveMetastoreClientFactory final : private boost::noncopyable
{
public:
    static HiveMetastoreClientFactory & instance();

    HiveMetastoreClientPtr getOrCreate(const String & name, const std::shared_ptr<CnchHiveSettings> & settings);

private:
    static std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> createThriftHiveMetastoreClient(const String & name, const std::shared_ptr<CnchHiveSettings> & settings);

    std::mutex mutex;
    std::map<String, HiveMetastoreClientPtr> clients;
};

}

#endif

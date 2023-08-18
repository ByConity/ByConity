#pragma once

#include "Storages/Hive/Metastore/IMetaClient.h"
#if USE_HIVE

#include "Common/PoolBase.h"
#include <ThriftHiveMetastore.h>

namespace DB
{
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

    Strings getAllDatabases() override;

    Strings getAllTables(const String & db_name) override;

    std::shared_ptr<ApacheHive::Table> getTable(const String & db_name, const String & table_name) override;

    std::vector<ApacheHive::Partition> getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter) override;

private:
    void tryCallHiveClient(std::function<void(ThriftHiveMetastoreClientPool::Entry &)> func);

    ThriftHiveMetastoreClientPool client_pool;
};


using HiveMetastoreClientPtr = std::shared_ptr<HiveMetastoreClient>;
class HiveMetastoreClientFactory final : private boost::noncopyable
{
public:
    static HiveMetastoreClientFactory & instance();

    HiveMetastoreClientPtr getOrCreate(const String & name);

private:
    static std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> createThriftHiveMetastoreClient(const String & name);

    std::mutex mutex;
    std::map<String, HiveMetastoreClientPtr> clients;
};

}

#endif

#include "Storages/Hive/Metastore/HiveMetastore.h"
#include <ServiceDiscovery/ServiceDiscoveryHelper.h>
#if USE_HIVE

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#    include <random>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_HIVEMETASTORE;
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
}

static const unsigned max_hive_metastore_client_connections = 16;
static const int max_hive_metastore_client_retry = 3;
static const UInt64 get_hive_metastore_client_timeout = 1000000;
static const int hive_metastore_client_conn_timeout_ms = 10000;
static const int hive_metastore_client_recv_timeout_ms = 10000;
static const int hive_metastore_client_send_timeout_ms = 10000;

ThriftHiveMetastoreClientPool::ThriftHiveMetastoreClientPool(ThriftHiveMetastoreClientBuilder builder_)
    : PoolBase<Object>(max_hive_metastore_client_connections, &Poco::Logger::get("ThriftHiveMetastoreClientPool")), builder(builder_)
{
}

HiveMetastoreClient::HiveMetastoreClient(ThriftHiveMetastoreClientBuilder builder) : client_pool(builder)
{
}

void HiveMetastoreClient::tryCallHiveClient(std::function<void(ThriftHiveMetastoreClientPool::Entry &)> func)
{
    int i = 0;
    String err_msg;
    for (; i < max_hive_metastore_client_retry; ++i)
    {
        auto client = client_pool.get(get_hive_metastore_client_timeout);
        try
        {
            func(client);
        }
        catch (apache::thrift::transport::TTransportException & e)
        {
            client.expire();
            err_msg = e.what();
            continue;
        }
        break;
    }
    if (i >= max_hive_metastore_client_retry)
        throw Exception(ErrorCodes::NO_HIVEMETASTORE, "Hive Metastore expired because {}", err_msg);
}

Strings HiveMetastoreClient::getAllDatabases()
{
    Strings databases;
    tryCallHiveClient([&](auto & client) {
        client->get_all_databases(databases);
    });
    return databases;
}

Strings HiveMetastoreClient::getAllTables(const String & db_name)
{
    Strings tables;
    tryCallHiveClient([&](auto & client) {
        client->get_all_tables(tables, db_name);
    });
    return tables;
}

std::shared_ptr<ApacheHive::Table> HiveMetastoreClient::getTable(const String & db_name, const String & table_name)
{
    auto table = std::make_shared<ApacheHive::Table>();
    tryCallHiveClient([&] (auto & client)
    {
        client->get_table(*table, db_name, table_name);
    });

    return table;
}

std::vector<ApacheHive::Partition> HiveMetastoreClient::getPartitionsByFilter(const String & db_name, const String & table_name, const String & filter)
{
    std::vector<ApacheHive::Partition> partitions;
    tryCallHiveClient([&] (auto & client)
    {
        if (filter.empty())
            client->get_partitions(partitions, db_name, table_name, -1);
        else
            client->get_partitions_by_filter(partitions, db_name, table_name, filter, -1);
    });

    return partitions;
}

HiveMetastoreClientFactory & HiveMetastoreClientFactory::instance()
{
    static HiveMetastoreClientFactory factory;
    return factory;
}

HiveMetastoreClientPtr HiveMetastoreClientFactory::getOrCreate(const String & name)
{
    std::lock_guard lock(mutex);
    auto it = clients.find(name);
    if (it == clients.end())
    {
        auto builder = [name]() { return createThriftHiveMetastoreClient(name); };

        auto client = std::make_shared<HiveMetastoreClient>(builder);
        clients.emplace(name, client);
        return client;
    }

    return it->second;
}

std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> HiveMetastoreClientFactory::createThriftHiveMetastoreClient(const String &name)
{
    using namespace apache::thrift;
    using namespace apache::thrift::protocol;
    using namespace apache::thrift::transport;
    using namespace Apache::Hadoop::Hive;

    Poco::URI hive_metastore_url(name);
    auto host = hive_metastore_url.getHost();
    auto port = hive_metastore_url.getPort();

    std::shared_ptr<TSocket> socket = std::make_shared<TSocket>(host, port);
    socket->setKeepAlive(true);
    socket->setConnTimeout(hive_metastore_client_conn_timeout_ms);
    socket->setRecvTimeout(hive_metastore_client_recv_timeout_ms);
    socket->setSendTimeout(hive_metastore_client_send_timeout_ms);
    std::shared_ptr<TTransport> transport = std::make_shared<TBufferedTransport>(socket);
    std::shared_ptr<TProtocol> protocol = std::make_shared<TBinaryProtocol>(transport);
    std::shared_ptr<ThriftHiveMetastoreClient> thrift_client = std::make_shared<ThriftHiveMetastoreClient>(protocol);
    try
    {
        transport->open();
    }
    catch (TException & tx)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "connect to hive metastore: {} failed. {}", name, tx.what());
    }
    return thrift_client;
}

}

#endif

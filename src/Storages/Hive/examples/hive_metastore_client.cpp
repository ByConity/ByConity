#include <iostream>

#include <ThriftHiveMetastore.h>
#include <hive_metastore_types.h>
#include <Storages/Hive/Metastore/HiveMetastore.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;
namespace po = boost::program_options;

po::variables_map parseOptions(int argc, char ** argv)
{
    po::options_description desc("Allowed options");
    desc.add_options()("host,h", po::value<std::string>()->default_value("127.0.0.1"), "hive thrift server host")(
        "port,p", po::value<int32_t>()->default_value(9083), "hive thrift server port")(
        "desc", po::value<std::vector<std::string>>()->multitoken(), "describe table")(
        "list", po::value<std::vector<std::string>>()->multitoken(), "describe table")(
        "stat", po::value<std::vector<std::string>>()->multitoken(), "get table statistics");
    // po::positional_options_description pd;
    // pd.add( "method", 1 );
    po::variables_map opts;
    po::store(po::parse_command_line(argc, argv, desc), opts);

    return opts;
}

#define FOR_EACH_FIELD(f) \
    f(binaryStats) \ 
    f(booleanStats) f(dateStats) f(stringStats) f(decimalStats) f(doubleStats) f(longStats)


#define OutputStat(bs) \
    if (isset.bs) \
    { \
        std::cout << indent; \
        obj.statsData.bs.printTo(std::cout); \
        \ 
        std::cout \
            << std::endl; \
    }


void convertPartitionString(const String & partition_str, std::vector<String> & partition_keys, std::vector<String> & partition_vals)
{
    std::vector<String> splitted;
    boost::split(splitted, partition_str, boost::is_any_of("/="));
    for (auto it = splitted.begin(); it != splitted.end();)
    {
        partition_keys.emplace_back(*it);
        ++it;
        partition_vals.emplace_back(*it);
        ++it;
    }
}

void printStats(const ApacheHive::ColumnStatisticsObj & obj, const std::string & indent = "\t")
{
    std::cout << obj.colName << ":" << obj.colType << std::endl;
    auto & isset = obj.statsData.__isset;
    FOR_EACH_FIELD(OutputStat)
}

void listAllDBs(DB::HiveMetastoreClientPtr & client)
{
    std::vector<std::string> databases = client->getAllDatabases();
    for (const auto & database : databases)
    {
        std::cout << database << std::endl;
    }
}

void listAllTables(DB::HiveMetastoreClientPtr & client, const std::string & hive_db_name)
{
    std::vector<std::string> tables = client->getAllTables(hive_db_name);
    for (const auto & table : tables)
    {
        std::cout << table << std::endl;
    }
}

void describe_table(DB::HiveMetastoreClientPtr & client, const std::string & hive_db_name, const std::string & hive_table_name)
{
    auto table = client->getTable(hive_db_name, hive_table_name);
    table->printTo(std::cout);
    std::cout << std::endl;
    std::vector<Partition> ret = client->getPartitionsByFilter(hive_db_name, hive_table_name, "");
    for (const auto & p : ret)
    {
        p.printTo(std::cout);
        std::cout << std::endl;
    }
}

void show_stat(DB::HiveMetastoreClientPtr & client, const std::string & hive_db_name, const std::string & hive_table_name)
{
    auto table = client->getTable(hive_db_name, hive_table_name);
    std::vector<std::string> column_names;
    for (const auto & c : table->sd.cols)
    {
        column_names.emplace_back(c.name);
    }
    auto stats = client->getTableStats(hive_db_name, hive_table_name, column_names);
    std::cout << stats.row_count << std::endl;
    for (const auto & obj : stats.table_stats.tableStats)
    {
        printStats(obj);
    }
}

void show_stat(
    DB::HiveMetastoreClientPtr & client,
    const std::string & hive_db_name,
    const std::string & hive_table_name,
    const std::string & partition_name)
{
    auto table = client->getTable(hive_db_name, hive_table_name);
    std::vector<std::string> column_names;
    for (const auto & c : table->sd.cols)
    {
        column_names.emplace_back(c.name);
    }
    std::vector<String> partition_keys;
    std::vector<String> partition_vals;
    convertPartitionString(partition_name, partition_keys, partition_vals);
    auto stats = client->getPartitionStats(hive_db_name, hive_table_name, column_names, partition_keys, {partition_vals});
    for (const auto & obj : stats.partStats[partition_name])
    {
        printStats(obj);
    }
    // stats.printTo(std::cout);
}

void show_stat(
    DB::HiveMetastoreClientPtr & client,
    const std::string & hive_db_name,
    const std::string & hive_table_name,
    const std::string & partition_name,
    const std::string & col_name)
{
    std::vector<String> partition_keys;
    std::vector<String> partition_vals;
    convertPartitionString(partition_name, partition_keys, partition_vals);
    auto stats = client->getPartitionStats(hive_db_name, hive_table_name, {col_name}, partition_keys, {partition_vals});
    for (const auto & obj : stats.partStats[partition_name])
    {
        printStats(obj);
    }
    // stats.printTo(std::cout);
}

int main(int argc, char ** argv)
{
    if (argc < 3)
    {
        std::cerr << "usage: ./hive_client host, port [database, table]" << std::endl;
        return 0;
    }

    std::string host = argv[1];
    int port = std::stoi(argv[2]);

    std::string hive_db_name;
    std::string hive_table_name;

    if (3 < argc)
    {
        hive_db_name = argv[3];
    }

    if (4 < argc)
    {
        hive_table_name = argv[4];
    }

    std::shared_ptr<TTransport> socket(new TSocket(host, port));
    std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    ThriftHiveMetastoreClient client(protocol);
    transport->open();

    /// get all database and tables;
    if (argc == 3)
    {
        std::vector<std::string> databases;
        client.get_all_databases(databases);
        for (const auto & database : databases)
        {
            std::cout << database << std::endl;
        }
    }
    else if (argc == 4)
    {
        std::vector<std::string> tables;
        client.get_all_tables(tables, hive_db_name);
        for (const auto & table : tables)
        {
            std::cout << table << std::endl;
        }
    }
    else if (argc == 5)
    {
        Table table;
        client.get_table(table, hive_db_name, hive_table_name);
        table.printTo(std::cout);
    }
}

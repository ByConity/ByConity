#include <iostream>

#include <ThriftHiveMetastore.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;

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

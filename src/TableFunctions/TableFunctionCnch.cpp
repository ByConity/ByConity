#include "TableFunctionCnch.h"

#include <TableFunctions/TableFunctionFactory.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int SYSTEM_ERROR;
}

std::shared_ptr<Cluster> mockVWCluster(const Context & context, const String & vw_name)
{
    auto & worker_pools = context.getCnchWorkerClientPools();

    CnchWorkerClientPoolPtr vw_client_pool = worker_pools.getPool(vw_name);
    if (!vw_client_pool)
        throw Exception("vw doesn't exist, vw name:" + vw_name, ErrorCodes::BAD_ARGUMENTS);

    std::vector<std::shared_ptr<CnchWorkerClient>> worker_clients;
    try
    {
        worker_clients = vw_client_pool->getAll();
    }
    catch(const Exception & e)
    {
        throw Exception("Get worker clients for vw " + vw_name + " failed. Got exception "
             + e.displayText(), e.code());
    }

    auto user_password = context.getCnchInterserverCredentials();

    std::vector<Cluster::Addresses> addresses;
    for (auto & client : worker_clients)
    {
        if (client)
        {
            Cluster::Address address(client->getTCPAddress(), user_password.first, user_password.second, context.getTCPPort(), false);
            // assume there are only one replica in each shard
            addresses.push_back({address});
        }
    }

    if (addresses.empty())
        throw Exception("There are no workers in vw name: " + vw_name, ErrorCodes::BAD_ARGUMENTS);

    //auto settings = context.getSettings();
    //settings.skip_unavailable_shards = true;
    return std::make_shared<Cluster>(context.getSettings(), addresses, false);
}

void TableFunctionCnch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2 && args.size() != 3)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);
    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<const String &>();

    String remote_database;
    String remote_table;

    args[1] = evaluateConstantExpressionForDatabaseName(args[1], context);
    remote_database = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    size_t dot = remote_database.find('.');
    if (dot != String::npos)
    {
        /// NOTE Bad - do not support identifiers in backquotes.
        remote_table = remote_database.substr(dot + 1);
        remote_database = remote_database.substr(0, dot);
    }
    else
    {
        if (args.size() != 3)
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        remote_table = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }

    cluster = (cluster_name == "server") ?
        context->mockCnchServersCluster() : mockVWCluster(*context.get(), cluster_name);

    remote_table_id.database_name = remote_database;
    remote_table_id.table_name = remote_table;
}

StoragePtr TableFunctionCnch::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const
{
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context);

    assert(cluster);

    StoragePtr res =
        StorageDistributed::create(
            StorageID(getDatabaseName(), table_name),
            cached_columns,
            ConstraintsDescription{},
            String{},
            remote_table_id.database_name,
            remote_table_id.table_name,
            String{},
            context,
            nullptr,
            String{},
            String{},
            DistributedSettings{},
            false,
            cluster);

    res->startup();

    return res;
}

ColumnsDescription TableFunctionCnch::getActualTableStructure(ContextPtr context) const
{
    assert(cluster);
    return getStructureOfRemoteTable(*cluster, remote_table_id, context);
}

void registerTableFunctionCnch(TableFunctionFactory & factory)
{
    factory.registerFunction("cnch", []() -> TableFunctionPtr {return std::make_shared<TableFunctionCnch>("cnch");});
}
}

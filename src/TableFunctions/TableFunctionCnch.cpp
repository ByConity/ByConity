/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <TableFunctions/TableFunctionCnch.h>

#include <CloudServices/CnchWorkerClientPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageDistributed.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Poco/Logger.h>
#include <Interpreters/VirtualWarehousePool.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/CommonData.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int SYSTEM_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int RESOURCE_MANAGER_ERROR;
}

static std::vector<Cluster::Addresses> lookupWorkerAddress(const Context & context, const String & vw_name)
{
    auto vw_handle = context.getVirtualWarehousePool().get(vw_name);
    const auto worker_clients = vw_handle->getAllWorkers();

    auto user_password = context.getCnchInterserverCredentials();

    std::vector<Cluster::Addresses> addresses;
    for (const auto & client : worker_clients)
    {
        if (client)
        {
            Cluster::Address address(client->getTCPAddress(), user_password.first, user_password.second, context.getTCPPort(), false);
            // assume there are only one replica in each shard
            addresses.push_back({address});
        }
    }

    return addresses;
}

std::shared_ptr<Cluster> mockVWCluster(const Context & context, const String & vw_name)
{

    auto addresses = lookupWorkerAddress(context, vw_name);

    if (addresses.empty())
        throw Exception("There are no workers in vw name: " + vw_name, ErrorCodes::BAD_ARGUMENTS);

    return std::make_shared<Cluster>(context.getSettings(), addresses, false);
}

std::shared_ptr<Cluster> mockMultiVWCluster(const Context & context, const std::vector<String> & vw_names)
{
    std::vector<Cluster::Addresses> addresses;
    for (const auto &vw_name : vw_names)
    {
        auto vw_addresses = lookupWorkerAddress(context, vw_name);
        addresses.insert(addresses.end(), vw_addresses.begin(), vw_addresses.end());
    }

    if (addresses.empty())
        throw Exception(fmt::format("There are no workers in vw name: {}", fmt::join(vw_names, ", ")), ErrorCodes::BAD_ARGUMENTS);

    return std::make_shared<Cluster>(context.getSettings(), addresses, false);
}

std::shared_ptr<Cluster> mockMultiVWCluster(const Context & context)
{
    std::vector<VirtualWarehouseData> vw_datas;
    auto rm_client = context.getResourceManagerClient();
    if (rm_client)
    {
        rm_client->getAllVirtualWarehouses(vw_datas);
    }
    else
        throw Exception("Resource Manager unavailable", ErrorCodes::RESOURCE_MANAGER_ERROR);

    if (vw_datas.empty())
    {
        throw Exception("No active vw in cluster.", ErrorCodes::SYSTEM_ERROR);
    }

    std::vector<String> vws(vw_datas.size());
    std::transform(vw_datas.begin(), vw_datas.end(), vws.begin(), [](const VirtualWarehouseData & vw_data) { return vw_data.name; });

    return mockMultiVWCluster(context, vws);
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
    if (args[0]->as<ASTLiteral &>().value.getType() == Field::Types::String)
        vw_names.emplace_back(args[0]->as<ASTLiteral &>().value.safeGet<const String &>());
    else if (args[0]->as<ASTLiteral &>().value.getType() == Field::Types::Array)
    {
        auto vw_names_in_arg = args[0]->as<ASTLiteral &>().value.get<Array>();
        vw_names.reserve(vw_names_in_arg.size());
        for (auto vw_name_in_arg : vw_names_in_arg)
            vw_names.emplace_back(vw_name_in_arg.safeGet<const String&>());
    }
    else
        throw Exception(help_message, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

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

    if (vw_names.size() == 1)
    {
        auto vw_name = vw_names[0];
        if (vw_name == "server")
            cluster = context->mockCnchServersCluster();
        else if (vw_name == "worker")
            cluster = mockMultiVWCluster(*context.get());
        else
            cluster = mockVWCluster(*context.get(), vw_name);
    }
    else
        cluster = mockMultiVWCluster(*context.get(), vw_names);

    remote_table_id.database_name = remote_database;
    remote_table_id.table_name = remote_table;
}

StoragePtr TableFunctionCnch::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const
{
    auto mutable_context = std::const_pointer_cast<Context>(context);
    mutable_context->enableWorkerFaultTolerance();
    mutable_context->setSetting("skip_unavailable_shards", Field{true});
    
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context);

    assert(cluster);

    StoragePtr res =
        StorageDistributed::create(
            StorageID(getDatabaseName(), table_name),
            cached_columns,
            ConstraintsDescription{},
            ForeignKeysDescription{},
            UniqueNotEnforcedDescription{},
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

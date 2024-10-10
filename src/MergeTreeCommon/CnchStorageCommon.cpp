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

#include <memory>
#include <MergeTreeCommon/CnchStorageCommon.h>

#include <Columns/ColumnSet.h>
#include <Core/Protocol.h>
#include <Core/UUID.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Functions/FunctionFactory.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <Common/StringUtils/StringUtils.h>
#include "Transaction/TxnTimestamp.h"

namespace DB
{

String toStr(CNCHStorageMediumType tp)
{
    switch(tp)
    {
        case CNCHStorageMediumType::HDFS:
            return "HDFS";
        case CNCHStorageMediumType::S3:
            return "S3";
    }
    throw Exception("Unknown cnch storage medium type " + toString(static_cast<int>(tp)), ErrorCodes::LOGICAL_ERROR);
}

CNCHStorageMediumType fromStr(const String& type_str)
{
    if (type_str == "HDFS")
    {
        return CNCHStorageMediumType::HDFS;
    }
    if (type_str == "S3")
    {
        return CNCHStorageMediumType::S3;
    }
    throw Exception("Unknown cnch storage medium type " + type_str, ErrorCodes::LOGICAL_ERROR);
}

CnchStorageCommonHelper::CnchStorageCommonHelper(const StorageID & table_id_, const String & remote_database_, const String & remote_table_)
    : table_id(table_id_)
    , remote_database(remote_database_)
    , remote_table(remote_table_)
{
}

bool CnchStorageCommonHelper::healthCheckForWorkerGroup(ContextPtr context, WorkerGroupHandle & worker_group)
{
    if (!context->getSettingsRef().query_worker_fault_tolerance)
        return true;

    const Settings & settings = context->getSettingsRef();
    const auto & shards = worker_group->getShardsInfo();
    size_t num_of_workers = shards.size();
    std::vector<uint64_t> remove_marks(num_of_workers, 0);
    ConnectionTimeouts connection_timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings);

    ThreadPool conn_check_pool(std::min(settings.connection_check_pool_size.value, num_of_workers));
    for (size_t i = 0; i < num_of_workers; ++i)
    {
        conn_check_pool.scheduleOrThrowOnError([&, i]
        {
            try
            {
                /// The checking task checks whether the current connection is connected or can connect.
                auto entry = shards[i].pool->get(connection_timeouts, &settings, true);
                Connection * conn = &(*entry);
                conn->tryConnect(connection_timeouts);
            }
            catch (const NetException &)
            {
                remove_marks[i] = 1;
                LOG_INFO(getLogger("CnchStorageCommonHelper"), "Unhealthy worker {} is skipped.",
                                            (worker_group->getHostWithPortsVec()[i]).id);
            }
        });
    }
    conn_check_pool.wait();

    if (std::count(remove_marks.begin(), remove_marks.end(), 1) == 0)
        return true;
    else
    {
        worker_group = worker_group->cloneAndRemoveShards(remove_marks);
        return false;
    }
}

String CnchStorageCommonHelper::getCloudTableName(ContextPtr context) const
{
    auto txn_id = context->getCurrentTransactionID();
    return table_id.table_name + '_' + txn_id.toString();
}

String CnchStorageCommonHelper::getOriginalTableName(const String & local_table_name, DB::TxnTimestamp txn_id)
{
    auto txn_id_str = "_" + txn_id.toString();
    auto found = local_table_name.find(txn_id_str);
    if (found)
    {
        return local_table_name.substr(0, found)
            + local_table_name.substr(std::min(found + txn_id_str.length(), local_table_name.length()));
    }
    return local_table_name;
}

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr CnchStorageCommonHelper::rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table)
{
    auto modified_query_ast = query->clone();

    ASTSelectQuery & select_query = modified_query_ast->as<ASTSelectQuery &>();

    select_query.replaceDatabaseAndTable(database, table);

    RestoreQualifiedNamesVisitor::Data data;
    data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query->as<ASTSelectQuery &>(), 0));
    data.remote_table.database = database;
    data.remote_table.table = table;
    RestoreQualifiedNamesVisitor(data).visit(modified_query_ast);

    return modified_query_ast;
}

void CnchStorageCommonHelper::rewritePlanSegmentQueryImpl(ASTPtr & query, const std::string & database, const std::string & table)
{
    ASTSelectQuery & select_query = query->as<ASTSelectQuery &>();

    /// restore long column names in JOIN ON expressions
    if (auto tables = select_query.tables())
    {
        RestoreQualifiedNamesVisitor::Data data;
        RestoreQualifiedNamesVisitor(data).visit(tables);
    }

    select_query.replaceDatabaseAndTable(database, table);
}

// Get all conditions.
// The method assumes that the expression are linearized,
// which only concated by 'and'.
// This is reasonable since we concat all possible conditions by 'and'
// when move these conditions from where to implicit_where.
ASTs CnchStorageCommonHelper::getConditions(const ASTPtr & ast)
{
    if (!ast)
        return {};

    ASTs ret_conditions;

    if (const auto & function = typeid_cast<const ASTFunction *>(ast.get()))
    {
        if (function->name == "and")
        {
            auto & conditions = function->arguments->children;
            for (const auto & condition : conditions)
            {
                if (const auto & func = typeid_cast<const ASTFunction *>(condition.get()))
                    if (func->name == "and")
                        throw Exception("The conditions of implicit_where should be linearized", ErrorCodes::LOGICAL_ERROR);

                ret_conditions.push_back(condition);
            }
        }
        else
            ret_conditions.push_back(ast);
    }

    return ret_conditions;
}

BlockInputStreamPtr CnchStorageCommonHelper::sendQueryPerShard(
    ContextPtr context,
    const String & query,
    const WorkerGroupHandleImpl::ShardInfo & shard_info,
    bool need_extended_profile_info)
{
    Block empty_header{};
    std::shared_ptr<RemoteBlockInputStream> remote_stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, empty_header, context);
    remote_stream->setPoolMode(PoolMode::GET_ONE);

    /// Get the extended profile info which is mainly for INSERT SELECT/INFILE
    if (need_extended_profile_info)
        context->setExtendedProfileInfo(remote_stream->getExtendedProfileInfo());
    return remote_stream;
}

void CnchStorageCommonHelper::filterCondition(
    const ASTPtr & expression,
    const ColumnsWithTypeAndName & columns,
    const std::map<String, size_t> & nameToIdx,
    ContextPtr context,
    std::vector<int> & mask,
    const SelectQueryInfo & query_info)
{
    const auto & ast_func = typeid_cast<const ASTFunction *>(expression.get());
    if (!ast_func)
        return;

    size_t rows = columns.empty() ? 0 : columns[0].column->size();
    String result_name = ast_func->getColumnName();
    DataTypePtr result_type = std::make_shared<DataTypeUInt8>();
    ColumnsWithTypeAndName func_arguments;

    Names argument_names;
    const auto & arguments = ast_func->arguments->children;
    for (const auto & argument : arguments)
        argument_names.push_back(argument->getColumnName());

    DataTypePtr left_arg_type = nullptr;
    if (ast_func->name == "in")
    {
        const ASTPtr left_arg = ast_func->arguments->children.front();
        auto it = nameToIdx.find(left_arg->getColumnName());
        left_arg_type = columns[it->second].type;
    }

    Block block;
    for (const auto & argument : arguments)
    {
        auto it = nameToIdx.find(argument->getColumnName());
        if (it != nameToIdx.end())
        {
            block.insert(columns[it->second]);
            func_arguments.push_back(columns[it->second]);
        }
        else
        {
            if (ASTLiteral * literal = typeid_cast<ASTLiteral *>(argument.get()))
            {
                DataTypePtr type = applyVisitor(FieldToDataType(), literal->value);

                ColumnWithTypeAndName column;
                if (ast_func->name == "in")
                {
                    PreparedSetKey set_key = PreparedSetKey::forLiteral(*argument, {left_arg_type});
                    auto set_it = query_info.sets.find(set_key);
                    if (set_it == query_info.sets.end())
                        throw Exception("Cannot find set when eliminate parts", ErrorCodes::LOGICAL_ERROR);
                    else
                        column.column = ColumnSet::create(1, set_it->second);
                }
                else
                    column.column = type->createColumnConst(rows, convertFieldToType(literal->value, *type));

                column.type = type;
                column.name = literal->getColumnName();
                func_arguments.push_back(column);
                block.insert(std::move(column));
            }
            else
                throw Exception("This function can only contain partition key and constant", ErrorCodes::LOGICAL_ERROR);
        }
    }

    ColumnNumbers arguments_idx(argument_names.size());
    for (size_t i = 0; i < argument_names.size(); ++i)
    {
        if (!block.has(argument_names[i]))
            throw Exception("Not found column: '" + argument_names[i] + "'", ErrorCodes::LOGICAL_ERROR);
        arguments_idx[i] = block.getPositionByName(argument_names[i]);
    }

    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, result_type, result_name});

    auto func_builder = FunctionFactory::instance().tryGet(ast_func->name, context);
    auto function = func_builder->build(func_arguments);
    if (!function || !function->isDeterministic())
        return;

    function->execute({}, result_type, rows);

    const auto & res_column = block.safeGetByPosition(num_columns_without_result);
    if (res_column.type->getName() != "UInt8")
        throw Exception("Wrong column type of IMPLICITWHERE clause's calculation result", ErrorCodes::LOGICAL_ERROR);

    const auto & column_ptr = res_column.column;

    for (size_t idx = 0; idx < column_ptr->size(); ++idx)
    {
        if (column_ptr->getUInt(idx) == 0)
            mask[idx] = 0;
    }
}

String CnchStorageCommonHelper::getCreateQueryForCloudTable(
    const String & query,
    const String & local_table_name,
    const ContextPtr & /*context*/,
    bool enable_staging_area,
    const std::optional<StorageID> & cnch_storage_id,
    const Strings & engine_args,
    const String & local_database_name,
    WorkerEngineType engine_type) const
{
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto & create_query = ast->as<ASTCreateQuery &>();
    create_query.table = local_table_name;
    if (!local_database_name.empty())
        create_query.database = local_database_name;

    replaceCnchWithCloud(
        create_query.storage,
        cnch_storage_id.value_or(table_id).getDatabaseName(),
        cnch_storage_id.value_or(table_id).getTableName(),
        engine_type,
        engine_args);

    // perhaps better to enable if_not_exists by default
    if (engine_type == WorkerEngineType::DICT)
        create_query.if_not_exists = true;

    if (enable_staging_area)
        modifyOrAddSetting(create_query, "cloud_enable_staging_area", Field(UInt64(1)));

    WriteBufferFromOwnString statement_buf;
    formatAST(create_query, statement_buf, false);
    writeChar('\n', statement_buf);
    LOG_TRACE(
        getLogger("getCreateQueryForCloudTable"), "create query for cloud table is {}", statement_buf.str());
    return statement_buf.str();
}

bool CnchStorageCommonHelper::forwardQueryToServerIfNeeded(ContextPtr query_context, const StorageID & storage_id, const String & query_to_forward, bool need_process_entry) const
{
    if (query_context->getSettingsRef().force_execute_alter)
        return false;
    auto host_port = query_context->getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(storage_id.uuid), storage_id.server_vw_name, false);
    if (isLocalServer(host_port.getRPCAddress(), std::to_string(query_context->getRPCPort())))
        return false;

    auto process_list_entry = query_context->getProcessListEntry().lock();
    if (!process_list_entry && need_process_entry)
        return false;
    // set current transaction as read_only to skip cleanTxn
    query_context->getCurrentTransaction()->setReadOnly(true);
    const auto & query_client_info = query_context->getClientInfo();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(query_context->getSettingsRef());
    Connection connection(
        host_port.getHost(),
        host_port.tcp_port,
        /*default_database=*/table_id.getDatabaseName(),
        query_client_info.current_user,
        query_client_info.current_password,
        /*cluster=*/"",
        /*cluster_secret=*/"",
        /*client_name=*/"QueryForwarding",
        Protocol::Compression::Disable,
        Protocol::Secure::Disable);

    auto query = query_to_forward;
    if (need_process_entry)
    {
        const auto query_status = process_list_entry->get().getInfo();
        query = query_status.query;
    }
    LOG_DEBUG(
        getLogger("CnchStorageCommonHelper"), "Send query `{}` to server {}", query, host_port.toDebugString());
    RemoteBlockInputStream stream(connection, query, {}, query_context);
    NullBlockOutputStream output({});

    copyData(stream, output);
    return true;
}
}

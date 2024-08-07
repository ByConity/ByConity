/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include "ClickHouseDictionarySource.h"
#include <memory>
#include <Client/ConnectionPool.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/executeQuery.h>
#include <Common/isLocalAddress.h>
#include <common/logger_useful.h>
#include <Parsers/formatTenantDatabaseName.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "ExternalQueryBuilder.h"
#include "readInvalidateQuery.h"
#include "writeParenthesisedString.h"
#include "DictionaryFactory.h"
#include "DictionarySourceHelpers.h"
#include "MergeTreeCommon/CnchTopologyMaster.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYSTEM_ERROR;
}

namespace
{
    constexpr size_t MAX_CONNECTIONS = 16;

    inline UInt16 getPortFromContext(ContextPtr context, bool secure)
    {
        return secure ? context->getTCPPortSecure().value_or(0) : context->getTCPPort();
    }

    ConnectionPoolPtrs createPoolsToCnchServer(ContextPtr context, const ClickHouseDictionarySource::Configuration & configuration)
    {
        std::shared_ptr<CnchTopologyMaster> topology_master = context->getCnchTopologyMaster();
        if (!topology_master)
            throw Exception("Failed to get topology master", ErrorCodes::SYSTEM_ERROR);
        std::list<CnchServerTopology> server_topologies = topology_master->getCurrentTopology();
        if (server_topologies.empty() || server_topologies.back().getServerList().empty())
            throw Exception("Server topology is empty, something wrong with topology", ErrorCodes::SYSTEM_ERROR);

        HostWithPortsVec endpoints = server_topologies.back().getServerList();

        ConnectionPoolPtrs res;
        std::for_each(endpoints.begin(), endpoints.end(),
            [& res, & configuration] (const HostWithPorts & host_with_port)
            {
                res.emplace_back(std::make_shared<ConnectionPool>(
                    MAX_CONNECTIONS,
                    host_with_port.getHost(),
                    host_with_port.tcp_port,
                    configuration.db,
                    configuration.user,
                    configuration.password,
                    "", /* cluster */
                    "", /* cluster_secret */
                    "ClickHouseDictionarySource",
                    Protocol::Compression::Enable,
                    configuration.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable));
            }
        );
        return res;
    }

    ConnectionPoolWithFailoverPtr createPool(ContextPtr context, const ClickHouseDictionarySource::Configuration & configuration)
    {
        if (configuration.is_local)
            return nullptr;

        ConnectionPoolPtrs pools;
        if (configuration.host.empty())
        {
            pools = createPoolsToCnchServer(std::move(context), configuration);
        }
        else
        {
            pools.emplace_back(std::make_shared<ConnectionPool>(
                MAX_CONNECTIONS,
                configuration.host,
                configuration.port,
                configuration.db,
                configuration.user,
                configuration.password,
                "", /* cluster */
                "", /* cluster_secret */
                "ClickHouseDictionarySource",
                Protocol::Compression::Enable,
                configuration.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable));
        }

        return std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
    }

}

ClickHouseDictionarySource::ClickHouseDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    const Block & sample_block_,
    ContextPtr context_)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{sample_block_}
    , context(Context::createCopy(context_))
    , pool{createPool(context, configuration)}
    , load_all_query{query_builder.composeLoadAllQuery()}
{
    /// Query context is needed because some code in executeQuery function may assume it exists.
    /// Current example is Context::getSampleBlockCache from InterpreterSelectWithUnionQuery::getSampleBlock.
    context->makeQueryContext();
}

ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
    , pool{createPool(context, configuration)}
    , load_all_query{other.load_all_query}
{
    context->makeQueryContext();
}

std::string ClickHouseDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::serverTimezoneInstance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }
    else
    {
        update_time = std::chrono::system_clock::now();
        return query_builder.composeLoadAllQuery();
    }
}

BlockInputStreamPtr ClickHouseDictionarySource::loadAllWithSizeHint(std::atomic<size_t> * result_size_hint)
{
    return createStreamForQuery(load_all_query, result_size_hint);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadAll()
{
    return createStreamForQuery(load_all_query);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadUpdatedAll()
{
    String load_update_query = getUpdateFieldAndDate();
    return createStreamForQuery(load_update_query);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    return createStreamForQuery(query_builder.composeLoadIdsQuery(ids));
}


BlockInputStreamPtr ClickHouseDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    String query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES);
    return createStreamForQuery(query);
}

bool ClickHouseDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        LOG_TRACE(log, "Invalidate query has returned: {}, previous value: {}", response, invalidate_query_response);
        if (invalidate_query_response == response)
            return false;
        invalidate_query_response = response;
    }
    return true;
}

bool ClickHouseDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

std::string ClickHouseDictionarySource::toString() const
{
    const std::string & where = configuration.where;
    return "ClickHouse: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}

BlockInputStreamPtr ClickHouseDictionarySource::createStreamForQuery(const String & query, std::atomic<size_t> * result_size_hint)
{
    BlockInputStreamPtr stream;

    /// Sample block should not contain first row default values
    auto empty_sample_block = sample_block.cloneEmpty();

    if (configuration.is_local)
    {
        auto query_context = Context::createCopy(context);
        query_context->setSetting("enable_auto_query_forwarding", false);
        query_context->setSetting("enable_optimizer", false);
        if (!configuration.tenant_id.empty())
            pushTenantId(configuration.tenant_id);
        auto block_io = executeQuery(query, query_context, true);
        stream = block_io.getInputStream();
        if (!configuration.tenant_id.empty())
            popTenantId();
        stream = std::make_shared<ConvertingBlockInputStream>(stream, empty_sample_block, ConvertingBlockInputStream::MatchColumnsMode::Position);
    }
    else
    {
        auto query_context = Context::createCopy(context);
        query_context->setSetting("enable_optimizer", false);
        stream = std::make_shared<RemoteBlockInputStream>(pool, query, empty_sample_block, query_context);
    }

    if (result_size_hint)
    {
        stream->setProgressCallback([result_size_hint](const Progress & progress)
        {
            *result_size_hint += progress.total_rows_to_read;
        });
    }

    return stream;
}

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_TRACE(log, "Performing invalidate query");
    if (configuration.is_local)
    {
        auto query_context = Context::createCopy(context);
        if (!configuration.tenant_id.empty())
            pushTenantId(configuration.tenant_id);
        auto block_io = executeQuery(request, query_context, true);
        auto input_block = block_io.getInputStream();
        if (!configuration.tenant_id.empty())
            popTenantId();
        return readInvalidateQuery(*input_block);
    }
    else
    {
        /// We pass empty block to RemoteBlockInputStream, because we don't know the structure of the result.
        Block invalidate_sample_block;
        RemoteBlockInputStream invalidate_stream(pool, request, invalidate_sample_block, context);
        return readInvalidateQuery(invalidate_stream);
    }
}

void registerDictionarySourceClickHouse(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & default_database [[maybe_unused]],
                                 bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        bool secure = config.getBool(config_prefix + ".secure", false);
        auto context_copy = Context::createCopy(context);

        UInt16 default_port = getPortFromContext(context_copy, secure);

        std::string settings_config_prefix = config_prefix + ".clickhouse";
        std::string host = config.getString(settings_config_prefix + ".host", "");
        UInt16 port = static_cast<UInt16>(config.getUInt(settings_config_prefix + ".port", 0));

        ClickHouseDictionarySource::Configuration configuration
        {
            .host = host,
            .user = config.getString(settings_config_prefix + ".user", "default"),
            .password = config.getString(settings_config_prefix + ".password", ""),
            .db = config.getString(settings_config_prefix + ".db", default_database),
            .table = config.getString(settings_config_prefix + ".table"),
            .where = config.getString(settings_config_prefix + ".where", ""),
            .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .port = port,
            .is_local = host.empty() ? (context->getServerType() == ServerType::cnch_server) : isLocalAddress({host, port}, default_port),
            .secure = config.getBool(settings_config_prefix + ".secure", false),
            .tenant_id = ""
        };

        /// We should set user info even for the case when the dictionary is loaded in-process (without TCP communication).
        if (configuration.is_local)
        {
            context_copy->setUser(configuration.user, configuration.password, Poco::Net::SocketAddress("127.0.0.1", 0));
            configuration.tenant_id = context_copy->getTenantId();
            context_copy = copyContextAndApplySettings(config_prefix, context_copy, config);
        }

        String dictionary_name = config.getString(".dictionary.name", "");
        String dictionary_database = config.getString(".dictionary.database", "");

        if (dictionary_name == configuration.table && dictionary_database == configuration.db)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouseDictionarySource table cannot be dictionary table");

        return std::make_unique<ClickHouseDictionarySource>(dict_struct, configuration, sample_block, context_copy);
    };

    factory.registerSource("clickhouse", create_table_source);
}

}

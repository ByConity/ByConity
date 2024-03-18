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

#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/System/StorageSystemCnchPartsInfo.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

StorageSystemCnchPartsInfo::StorageSystemCnchPartsInfo(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    auto ready_state = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Unloaded", static_cast<Int8>(StorageSystemCnchPartsInfoLocal::ReadyState::Unloaded)},
        {"Loading", static_cast<Int8>(StorageSystemCnchPartsInfoLocal::ReadyState::Loading)},
        {"Loaded", static_cast<Int8>(StorageSystemCnchPartsInfoLocal::ReadyState::Loaded)},
        {"Recalculated", static_cast<Int8>(StorageSystemCnchPartsInfoLocal::ReadyState::Recalculated)},
    });

    storage_metadata.setColumns(ColumnsDescription({
        {"uuid", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"partition", std::make_shared<DataTypeString>()},
        {"first_partition", std::make_shared<DataTypeString>()},
        {"total_parts_number", std::make_shared<DataTypeUInt64>()},
        {"total_parts_size", std::make_shared<DataTypeUInt64>()},
        {"total_rows_count", std::make_shared<DataTypeUInt64>()},
        {"ready_state", std::move(ready_state)},
        /// Boolean
        {"recalculating", std::make_shared<DataTypeUInt8>()},
        {"last_update_time", std::make_shared<DataTypeUInt64>()},
        {"last_snapshot_time", std::make_shared<DataTypeUInt64>()},
        {"last_modification_time", std::make_shared<DataTypeDateTime>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemCnchPartsInfo::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*max_block_size*/)
{
    if (context->getServerType() != ServerType::cnch_server)
        throw Exception("Table system.cnch_parts_info only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    /// check(column_names);

    String inner_query = "SELECT " + boost::algorithm::join(column_names, ",") + " FROM system.cnch_parts_info_local where 1=1";

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, inner_query.data(), inner_query.data() + inner_query.size(), "", 0, 0);

    // push down where condition if any.
    ASTSelectQuery * origin_select_query = query_info.query->as<ASTSelectQuery>();
    if (auto where_expression = origin_select_query->where())
        ast->as<ASTSelectQuery>()->refWhere() = where_expression;

    Block header = materializeBlock(InterpreterSelectQuery(ast, context, QueryProcessingStage::Complete).getSampleBlock());
    QueryPlan query_plan;
    Poco::Logger * log = &Poco::Logger::get("SystemPartsInfo");

    ClusterProxy::SelectStreamFactory stream_factory = ClusterProxy::SelectStreamFactory(
        header,
        {},
        storage_snapshot,
        QueryProcessingStage::Complete,
        StorageID{"system", "cnch_parts_info_local"},
        Scalars{},
        false,
        {});

    //set cluster in query_info
    query_info.cluster = context->mockCnchServersCluster();
    ClusterProxy::executeQuery(query_plan, stream_factory, log, ast, context, query_info, nullptr, {}, nullptr);
    return query_plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
}

}

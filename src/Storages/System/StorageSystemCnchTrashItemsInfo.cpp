#include <Storages/System/StorageSystemCnchTrashItemsInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataStreams/materializeBlock.h>
#include <boost/algorithm/string/join.hpp>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>

namespace DB
{

StorageSystemCnchTrashItemsInfo::StorageSystemCnchTrashItemsInfo(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {{"uuid", std::make_shared<DataTypeString>()},
         {"database", std::make_shared<DataTypeString>()},
         {"table", std::make_shared<DataTypeString>()},
         {"total_parts_number", std::make_shared<DataTypeInt64>()},
         {"total_parts_size", std::make_shared<DataTypeInt64>()},
         {"total_bitmap_number", std::make_shared<DataTypeInt64>()},
         {"total_bitmap_size", std::make_shared<DataTypeInt64>()},
         {"last_update_time", std::make_shared<DataTypeUInt64>()},
         {"last_snapshot_time", std::make_shared<DataTypeUInt64>()}}));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemCnchTrashItemsInfo::read(
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*max_block_size*/)
{
    if (context->getServerType() != ServerType::cnch_server)
        throw Exception("Table system.cnch_trash_items_info only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    /// check(column_names);

    String inner_query = "SELECT " + boost::algorithm::join(column_names, ",") + " FROM system.cnch_trash_items_info_local where 1=1";

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, inner_query.data(), inner_query.data() + inner_query.size(), "", 0, 0);

    // push down where condition if any.
    ASTSelectQuery * origin_select_query = query_info.query->as<ASTSelectQuery>();
    if (auto where_expression = origin_select_query->where())
        ast->as<ASTSelectQuery>()->refWhere() = where_expression;

    Block header = materializeBlock(InterpreterSelectQuery(ast, context, QueryProcessingStage::Complete).getSampleBlock());
    QueryPlan query_plan;
    LoggerPtr log = getLogger("SystemTrashItemsInfo");
    ClusterProxy::SelectStreamFactory stream_factory = ClusterProxy::SelectStreamFactory(
        header, {}, {}, QueryProcessingStage::Complete, StorageID{"system", "cnch_trash_items_info_local"}, Scalars{}, false, {});

    //set cluster in query_info
    query_info.cluster = context->mockCnchServersCluster();
    ClusterProxy::executeQuery(query_plan, stream_factory, log, ast, context, query_info, nullptr, {}, nullptr);
    return query_plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
}

}
